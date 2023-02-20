package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.LogCleaningAbortedException;
import cn.pockethub.permanentqueue.kafka.common.ThreadShutdownException;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.server.BrokerReconfigurable;
import cn.pockethub.permanentqueue.kafka.server.KafkaConfig;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.utils.ShutdownableThread;
import cn.pockethub.permanentqueue.kafka.utils.Throttler;
import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * <p>
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 * <p>
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 * <p>
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 * <p>
 * Once the key=>last_offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * <p>
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * <p>
 * Cleaned segments are swapped into the log as they become available.
 * <p>
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * <p>
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 * This time is tracked by setting the base timestamp of a record batch with delete markers when the batch is recopied in the first cleaning that encounters
 * it. The relative timestamps of the records in the batch are also modified when recopied in this cleaning according to the new base timestamp of the batch.
 * <p>
 * Note that cleaning is more complicated with the idempotent/transactional producer capabilities. The following
 * are the key points:
 * <p>
 * 1. In order to maintain sequence number continuity for active producers, we always retain the last batch
 * from each producerId, even if all the records from the batch have been removed. The batch will be removed
 * once the producer either writes a new batch or is expired due to inactivity.
 * 2. We do not clean beyond the last stable offset. This ensures that all records observed by the cleaner have
 * been decided (i.e. committed or aborted). In particular, this allows us to use the transaction index to
 * collect the aborted transactions ahead of time.
 * 3. Records from aborted transactions are removed by the cleaner immediately without regard to record keys.
 * 4. Transaction markers are retained until all record batches from the same transaction have been removed and
 * a sufficient amount of time has passed to reasonably ensure that an active consumer wouldn't consume any
 * data from the transaction prior to reaching the offset of the marker. This follows the same logic used for
 * tombstone deletion.
 */
@Getter
public class LogCleaner extends KafkaMetricsGroup implements BrokerReconfigurable {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    public static Set<String> ReconfigurableConfigs = new HashSet<>(Arrays.asList(KafkaConfig.LogCleanerThreadsProp,
            KafkaConfig.LogCleanerDedupeBufferSizeProp,
            KafkaConfig.LogCleanerDedupeBufferLoadFactorProp,
            KafkaConfig.LogCleanerIoBufferSizeProp,
            KafkaConfig.MessageMaxBytesProp,
            KafkaConfig.LogCleanerIoMaxBytesPerSecondProp,
            KafkaConfig.LogCleanerBackoffMsProp));

    /* Log cleaner configuration which may be dynamically updated */
    private volatile CleanerConfig config;
    private final List<File> logDirs;
    private final ConcurrentMap<TopicPartition, UnifiedLog> logs;
    private final LogDirFailureChannel logDirFailureChannel;
    private final Time time;

    /* for managing the state of partitions being cleaned. package-private to allow access in tests */
    private final LogCleanerManager cleanerManager;

    private final List<CleanerThread> cleaners = Lists.newArrayList();

    /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
    private final Throttler throttler;

    /**
     * @param initialConfig Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
     * @param logDirs       The directories where offset checkpoints reside
     * @param logs          The pool of logs
     * @param time          A way to control the passage of time
     */
    public LogCleaner(final CleanerConfig initialConfig,
                      final List<File> logDirs,
                      final ConcurrentMap<TopicPartition, UnifiedLog> logs,
                      final LogDirFailureChannel logDirFailureChannel,
                      final Time time) throws IOException, NoSuchAlgorithmException {
        this.config = initialConfig;
        this.logDirs = logDirs;
        this.logs = logs;
        this.logDirFailureChannel = logDirFailureChannel;
        this.time = time;

        this.cleanerManager = new LogCleanerManager(logDirs, logs, logDirFailureChannel);
        this.throttler = new Throttler(config.getMaxIoBytesPerSecond(),
                300,
                true,
                "cleaner-io",
                "bytes",
                time);


        /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
        newGauge("max-buffer-utilization-percent", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return maxOverCleanerThreads(cleanerThread -> cleanerThread.getLastStats().getBufferUtilization()) * 100;
            }
        }, new HashMap<>());

        /* a metric to track the recopy rate of each thread's last cleaning */
        newGauge("cleaner-recopy-percent", new Gauge<Integer>() {
            @Override
            public Integer value() {
                List<CleanerStats> cleanerStatsList = cleaners.stream()
                        .map(CleanerThread::getLastStats)
                        .collect(Collectors.toList());
                long bytesWrittenSum = cleanerStatsList.stream().mapToLong(CleanerStats::getBytesWritten).sum();
                long bytesReadSum = cleanerStatsList.stream().mapToLong(CleanerStats::getBytesRead).sum();
                double recopyRate = (double) bytesWrittenSum / Math.max(bytesReadSum, 1);
                return ((Double) (recopyRate * 100)).intValue();
            }
        }, new HashMap<>());

        /* a metric to track the maximum cleaning time for the last cleaning from each thread */
        newGauge("max-clean-time-secs", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return maxOverCleanerThreads(cleanerThread -> cleanerThread.getLastStats().elapsedSecs());
            }
        }, new HashMap<>());


        // a metric to track delay between the time when a log is required to be compacted
        // as determined by max compaction lag and the time of last cleaner run.
        newGauge("max-compaction-delay-secs", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return maxOverCleanerThreads(cleanerThread -> (cleanerThread.lastPreCleanStats.getMaxCompactionDelayMs()) / 1000d);
            }
        }, new HashMap<>());

        newGauge("DeadThreadCount", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return deadThreadCount();
            }
        }, new HashMap<>());
    }

    /**
     * scala 2.12 does not support maxOption so we handle the empty manually.
     *
     * @param f to compute the result
     * @return the max value (int value) or 0 if there is no cleaner
     */
    private Integer maxOverCleanerThreads(Function<CleanerThread, Double> f) {
        double result = 0.0d;
        for (CleanerThread cleaner : cleaners) {
            result = Math.max(result, f.apply(cleaner));
        }
        return (int) result;
    }

    protected Integer deadThreadCount() {
        return new Long(cleaners.stream().filter(ShutdownableThread::isThreadFailed).count()).intValue();
    }

    /**
     * Start the background cleaning
     */
    public void startup() throws NoSuchAlgorithmException {
        LOG.info("Starting the log cleaner");
        for (int i = 0; i < config.getNumThreads(); i++) {
            CleanerThread cleanerThread = new CleanerThread(i);
            cleaners.add(cleanerThread);
            cleanerThread.start();
        }
    }

    /**
     * Stop the background cleaning
     */
    public void shutdown() throws InterruptedException {
        LOG.info("Shutting down the log cleaner.");
        for (CleanerThread cleaner : cleaners) {
            cleaner.shutdown();
        }
        cleaners.clear();
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return LogCleaner.ReconfigurableConfigs;
    }

    @Override
    public void validateReconfiguration(KafkaConfig newConfig) {
        CleanerConfig newCleanerConfig = LogCleaner.cleanerConfig(newConfig);
        int numThreads = newCleanerConfig.getNumThreads();
        int currentThreads = config.getNumThreads();
        if (numThreads < 1) {
            throw new ConfigException("Log cleaner threads should be at least 1");
        }
        if (numThreads < currentThreads / 2) {
            throw new ConfigException("Log cleaner threads cannot be reduced to less than half the current value " + currentThreads);
        }
        if (numThreads > currentThreads * 2) {
            throw new ConfigException("Log cleaner threads cannot be increased to more than double the current value " + currentThreads);
        }
    }

    /**
     * Reconfigure log clean config. The will:
     * 1. update desiredRatePerSec in Throttler with logCleanerIoMaxBytesPerSecond, if necessary
     * 2. stop current log cleaners and create new ones.
     * That ensures that if any of the cleaners had failed, new cleaners are created to match the new config.
     */
    @Override
    public void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig) throws InterruptedException, NoSuchAlgorithmException {
        config = LogCleaner.cleanerConfig(newConfig);

        double maxIoBytesPerSecond = config.getMaxIoBytesPerSecond();
        if (maxIoBytesPerSecond != oldConfig.getLogCleanerIoMaxBytesPerSecond()) {
            LOG.info("Updating logCleanerIoMaxBytesPerSecond: {}", maxIoBytesPerSecond);
            throttler.updateDesiredRatePerSec(maxIoBytesPerSecond);
        }

        shutdown();
        startup();
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     */
    public void abortCleaning(TopicPartition topicPartition) throws Throwable {
        cleanerManager.abortCleaning(topicPartition);
    }

    /**
     * Update checkpoint file to remove partitions if necessary.
     */
    public void updateCheckpoints(File dataDir, Optional<TopicPartition> partitionToRemove) throws Throwable {
        cleanerManager.updateCheckpoints(dataDir, Optional.empty(), partitionToRemove);
    }

    /**
     * alter the checkpoint directory for the topicPartition, to remove the data in sourceLogDir, and add the data in destLogDir
     */
    public void alterCheckpointDir(TopicPartition topicPartition, File sourceLogDir, File destLogDir) throws IOException {
        cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir);
    }

    /**
     * Stop cleaning logs in the provided directory
     *
     * @param dir the absolute path of the log dir
     */
    public void handleLogDirFailure(String dir) {
        cleanerManager.handleLogDirFailure(dir);
    }

    /**
     * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
     */
    public void maybeTruncateCheckpoint(File dataDir, TopicPartition topicPartition, Long offset) throws Throwable {
        cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset);
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     */
    public void abortAndPauseCleaning(TopicPartition topicPartition) throws Throwable {
        cleanerManager.abortAndPauseCleaning(topicPartition);
    }

    /**
     * Resume the cleaning of paused partitions.
     */
    public void resumeCleaning(List<TopicPartition> topicPartitions) throws Throwable {
        cleanerManager.resumeCleaning(topicPartitions);
    }

    public Boolean awaitCleaned(TopicPartition topicPartition, Long offset) throws InterruptedException {
        return awaitCleaned(topicPartition, offset, 60000L);
    }

    /**
     * For testing, a way to know when work has completed. This method waits until the
     * cleaner has processed up to the given offset on the specified topic/partition
     *
     * @param topicPartition The topic and partition to be cleaned
     * @param offset         The first dirty offset that the cleaner doesn't have to clean
     * @param maxWaitMs      The maximum time in ms to wait for cleaner
     * @return A boolean indicating whether the work has completed before timeout
     */
    public Boolean awaitCleaned(TopicPartition topicPartition, Long offset, Long maxWaitMs) throws InterruptedException {
        boolean isCleaned = false;
        Map<TopicPartition, Long> checkpoints = cleanerManager.allCleanerCheckpoints();
        if (MapUtils.isNotEmpty(checkpoints)
                && checkpoints.containsKey(topicPartition)
                && checkpoints.get(topicPartition) >= offset) {
            isCleaned = true;
        }
        Long remainingWaitMs = maxWaitMs;
        while (!isCleaned && remainingWaitMs > 0) {
            Long sleepTime = Math.min(100, remainingWaitMs);
            Thread.sleep(sleepTime);
            remainingWaitMs -= sleepTime;
        }
        return isCleaned;
    }

    /**
     * To prevent race between retention and compaction,
     * retention threads need to make this call to obtain:
     *
     * @return A list of log partitions that retention threads can safely work on
     */
    public Iterable<Map.Entry<TopicPartition, UnifiedLog>> pauseCleaningForNonCompactedPartitions() {
        return cleanerManager.pauseCleaningForNonCompactedPartitions();
    }

    // Only for testing
    protected CleanerConfig currentConfig() {
        return config;
    }

    // Only for testing
    protected Integer cleanerCount() {
        return cleaners.size();
    }

    public static CleanerConfig cleanerConfig(KafkaConfig config) {
        return new CleanerConfig(config.getLogCleanerThreads(),
                config.getLogCleanerDedupeBufferSize(),
                config.getLogCleanerDedupeBufferLoadFactor(),
                config.getLogCleanerIoBufferSize(),
                config.messageMaxBytes(),
                config.getLogCleanerIoMaxBytesPerSecond(),
                config.getLogCleanerBackoffMs(),
                config.getLogCleanerEnable(),
                "MD5");
    }

    /**
     * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
     * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
     */
    @Getter
    protected class CleanerThread extends ShutdownableThread {
        private final Logger LOG = LoggerFactory.getLogger(CleanerThread.class);

        private Cleaner cleaner;

        private volatile CleanerStats lastStats = new CleanerStats(Time.SYSTEM);
        private volatile PreCleanStats lastPreCleanStats = new PreCleanStats();

        public CleanerThread(Integer threadId) throws NoSuchAlgorithmException {
            super("kafka-log-cleaner-thread-" + threadId, false);

            if (config.getDedupeBufferSize() / config.getNumThreads() > Integer.MAX_VALUE) {
                LOG.warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");
            }

            cleaner = new Cleaner(threadId,
                    new SkimpyOffsetMap(((Long) Math.min(config.getDedupeBufferSize() / config.getNumThreads(), Integer.MAX_VALUE)).intValue(), config.getHashAlgorithm()),
                    config.getIoBuffersize() / config.getNumThreads() / 2,
                    config.getMaxMessageSize(),
                    config.getDedupeBufferLoadFactor(),
                    throttler,
                    time,
                    this::checkDone);
        }

        private void checkDone(TopicPartition topicPartition) throws LogCleaningAbortedException {
            if (!isRunning()) {
                throw new ThreadShutdownException();
            }
            cleanerManager.checkCleaningAborted(topicPartition);
        }

        /**
         * The main loop for the cleaner thread
         * Clean a log if there is a dirty log available, otherwise sleep for a bit
         */
        @Override
        public void doWork() throws Throwable {
            Boolean cleaned = tryCleanFilthiestLog();
            if (!cleaned) {
                pause(config.getBackOffMs(), TimeUnit.MILLISECONDS);
            }

            cleanerManager.maintainUncleanablePartitions();
        }

        /**
         * Cleans a log if there is a dirty log available
         *
         * @return whether a log was cleaned
         */
        private Boolean tryCleanFilthiestLog() {
            try {
                return cleanFilthiestLog();
            } catch (LogCleaningException e) {
                LOG.warn("Unexpected exception thrown when cleaning log {}. Marking its partition ({}) as uncleanable",
                        e.getLog(), e.getLog().topicPartition(), e);
                cleanerManager.markPartitionUncleanable(e.getLog().parentDir(), e.getLog().topicPartition());

                return false;
            }
        }

        private Boolean cleanFilthiestLog() {
            PreCleanStats preCleanStats = new PreCleanStats();
            Optional<LogToClean> ltc = cleanerManager.grabFilthiestCompactedLog(time, preCleanStats);
            boolean cleaned;
            if (ltc.isPresent()) {
                LogToClean cleanable = ltc.get();
                // there's a log, clean it
                this.lastPreCleanStats = preCleanStats;
                try {
                    cleanLog(cleanable);
                    return true;
                } catch (ThreadShutdownException e) {
                    throw e;
                } catch (Exception e) {
                    throw new LogCleaningException(cleanable.getLog(), e.getMessage(), e);
                }
            } else {
                cleaned = false;
            }
            Iterable<Map.Entry<TopicPartition, UnifiedLog>> deletable = cleanerManager.deletableLogs();
            try {
                for (Map.Entry<TopicPartition, UnifiedLog> entry : deletable) {
                    try {
                        entry.getValue().deleteOldSegments();
                    } catch (ThreadShutdownException e) {
                        throw e;
                    } catch (Throwable throwable) {
                        throw new LogCleaningException(entry.getValue(), throwable.getMessage(), throwable);
                    }
                }
            } finally {
                List<TopicPartition> deletableTps = new ArrayList<>();
                for (Map.Entry<TopicPartition, UnifiedLog> entry : deletable) {
                    deletableTps.add(entry.getKey());
                }
                cleanerManager.doneDeleting(deletableTps);
            }

            return cleaned;
        }

        private void cleanLog(LogToClean cleanable) throws IOException, DigestException {
            long startOffset = cleanable.getFirstDirtyOffset();
            long endOffset = startOffset;
            try {
                Pair<Long, CleanerStats> pair = cleaner.clean(cleanable);
                Long nextDirtyOffset = pair.getKey();
                CleanerStats cleanerStats = pair.getValue();
                endOffset = nextDirtyOffset;
                recordStats(cleaner.getId(), cleanable.getLog().name(), startOffset, endOffset, cleanerStats);
            } catch (LogCleaningAbortedException e) {
                // task can be aborted, let it go.
            } catch (KafkaStorageException e) {
                // partition is already offline. let it go.
            } catch (IOException e) {
                String logDirectory = cleanable.getLog().parentDir();
                String msg = String.format("Failed to clean up log for %s in dir %s due to IOException", cleanable.getTopicPartition(), logDirectory);
                logDirFailureChannel.maybeAddOfflineLogDir(logDirectory, msg, e);
            } finally {
                cleanerManager.doneCleaning(cleanable.getTopicPartition(), cleanable.getLog().parentDirFile(), endOffset);
            }
        }

        /**
         * Log out statistics on a single run of the cleaner.
         */
        public void recordStats(Integer id, String name, Long from, Long to, CleanerStats stats) {
            this.lastStats = stats;
            Function<Double, Double> mb = new Function<Double, Double>() {
                @Override
                public Double apply(Double bytes) {
                    return bytes / (1024 * 1024);
                }
            };
            String message = String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n", id, name, from, to) +
                    String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n",
                            mb.apply(stats.getBytesRead().doubleValue()),
                            stats.elapsedSecs(),
                            mb.apply(stats.getBytesRead().doubleValue() / stats.elapsedSecs())) +
                    String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n",
                            mb.apply(stats.getMapBytesRead().doubleValue()),
                            stats.elapsedIndexSecs(),
                            mb.apply(stats.getMapBytesRead().doubleValue()) / stats.elapsedIndexSecs(),
                            100 * stats.elapsedIndexSecs() / stats.elapsedSecs()) +
                    String.format("\tBuffer utilization: %.1f%%%n", 100 * stats.getBufferUtilization()) +
                    String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n",
                            mb.apply(stats.getBytesRead().doubleValue()),
                            stats.elapsedSecs() - stats.elapsedIndexSecs(),
                            mb.apply(stats.getBytesRead().doubleValue()) / (stats.elapsedSecs() - stats.elapsedIndexSecs()),
                            100 * (stats.elapsedSecs() - stats.elapsedIndexSecs()) / stats.elapsedSecs()) +
                    String.format("\tStart size: %,.1f MB (%,d messages)%n",
                            mb.apply(stats.getBytesRead().doubleValue()), stats.getMessagesRead()) +
                    String.format("\tEnd size: %,.1f MB (%,d messages)%n",
                            mb.apply(stats.getBytesWritten().doubleValue()), stats.getMessagesWritten()) +
                    String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n",
                            100.0 * (1.0 - stats.getBytesWritten().doubleValue() / stats.getBytesRead()),
                            100.0 * (1.0 - stats.getMessagesWritten().doubleValue() / stats.getMessagesRead()));
            LOG.info(message);
            if (lastPreCleanStats.getDelayedPartitions() > 0) {
                LOG.info("\tCleanable partitions: {}, Delayed partitions: {}, max delay: {}",
                        lastPreCleanStats.getCleanablePartitions(), lastPreCleanStats.getDelayedPartitions(), lastPreCleanStats.getMaxCompactionDelayMs());
            }
            if (stats.getInvalidMessagesRead() > 0) {
                LOG.warn("\tFound {} invalid messages during compaction.", stats.getInvalidMessagesRead());
            }
        }
    }


}
