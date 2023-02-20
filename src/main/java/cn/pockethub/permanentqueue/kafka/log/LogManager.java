package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.server.*;
import cn.pockethub.permanentqueue.kafka.server.checkpoints.OffsetCheckpointFile;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.utils.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import javascalautils.Failure;
import javascalautils.Success;
import javascalautils.Try;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * <p/>
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * <p/>
 * A background thread handles log retention by periodically truncating excess log segments.
 * <p/>
 * This class is thread-safe!
 */
@Getter
//@threadsafe
public class LogManager extends KafkaMetricsGroup {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    /*static*/
    public static final String RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint";
    public static final String LogStartOffsetCheckpointFile = "log-start-offset-checkpoint";
    public static final int ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000;
    /*static*/

    /*构造方法参数 start*/
    private final List<File> logDirs;
    private final List<File> initialOfflineDirs;
    private final ConfigRepository configRepository;
    private final LogConfig initialDefaultConfig;
    private final CleanerConfig cleanerConfig;
    private final int recoveryThreadsPerDataDir;
    private final long flushCheckMs;
    private final long flushRecoveryOffsetCheckpointMs;
    private final long flushStartOffsetCheckpointMs;
    private final long retentionCheckMs;
    private final int maxTransactionTimeoutMs;
    private final int maxPidExpirationMs;
    private final MetadataVersion interBrokerProtocolVersion;
    private final Scheduler scheduler;
    private final BrokerTopicStats brokerTopicStats;
    private final LogDirFailureChannel logDirFailureChannel;
    private final Time time;
    private final Boolean keepPartitionMetadataFile;
    /*构造方法参数 end*/

    /*字段*/
    public final String LockFile = ".lock";
    public final long InitialTaskDelayMs = 30 * 1000L;

    private final Object logCreationOrDeletionLock = new Object();
    private final ConcurrentMap<TopicPartition, UnifiedLog> currentLogs = new ConcurrentHashMap<>();
    // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
    // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
    // to replace the current log of the partition after the future log catches up with the current log
    private final ConcurrentMap<TopicPartition, UnifiedLog> futureLogs = new ConcurrentHashMap<>();
    // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
    private final LinkedBlockingQueue<Pair<UnifiedLog, Long>> logsToBeDeleted = new LinkedBlockingQueue<>();

    private ConcurrentLinkedDeque<File> _liveLogDirs;
    @Setter
    private volatile LogConfig currentDefaultConfig;
    private volatile int numRecoveryThreadsPerDataDir;

    // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
    // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
    // which triggers a config reload after initialization is finished (to get the latest config value).
    // See KAFKA-8813 for more detail on the race condition
    // Visible for testing
    private final ConcurrentHashMap<TopicPartition, Boolean> partitionsInitializing = new ConcurrentHashMap<>();

    private final List<FileLock> dirLocks;
    private volatile ConcurrentMap<File, OffsetCheckpointFile> recoveryPointCheckpoints = new ConcurrentHashMap<>();
    private volatile ConcurrentMap<File, OffsetCheckpointFile> logStartOffsetCheckpoints = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<TopicPartition, String> preferredLogDirs = new ConcurrentHashMap<>();

    private volatile LogCleaner _cleaner;
    /*字段*/

    public LogManager(final List<File> logDirs,
                      final List<File> initialOfflineDirs,
                      final ConfigRepository configRepository,
                      final LogConfig initialDefaultConfig,
                      final CleanerConfig cleanerConfig,
                      final int recoveryThreadsPerDataDir,
                      final long flushCheckMs,
                      final long flushRecoveryOffsetCheckpointMs,
                      final long flushStartOffsetCheckpointMs,
                      final long retentionCheckMs,
                      final int maxTransactionTimeoutMs,
                      final int maxPidExpirationMs,
                      final MetadataVersion interBrokerProtocolVersion,
                      final Scheduler scheduler,
                      final BrokerTopicStats brokerTopicStats,
                      final LogDirFailureChannel logDirFailureChannel,
                      final Time time,
                      final Boolean keepPartitionMetadataFile) throws IOException {
        super();

        this.logDirs = logDirs;
        this.initialOfflineDirs = initialOfflineDirs;
        this.configRepository = configRepository;
        this.initialDefaultConfig = initialDefaultConfig;
        this.cleanerConfig = cleanerConfig;
        this.recoveryThreadsPerDataDir = recoveryThreadsPerDataDir;
        this.flushCheckMs = flushCheckMs;
        this.flushRecoveryOffsetCheckpointMs = flushRecoveryOffsetCheckpointMs;
        this.flushStartOffsetCheckpointMs = flushStartOffsetCheckpointMs;
        this.retentionCheckMs = retentionCheckMs;
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        this.maxPidExpirationMs = maxPidExpirationMs;
        this.interBrokerProtocolVersion = interBrokerProtocolVersion;
        this.scheduler = scheduler;
        this.brokerTopicStats = brokerTopicStats;
        this.logDirFailureChannel = logDirFailureChannel;
        this.time = time;
        this.keepPartitionMetadataFile = keepPartitionMetadataFile;

        this._liveLogDirs = createAndValidateLogDirs(logDirs, initialOfflineDirs);
        this.currentDefaultConfig = initialDefaultConfig;
        this.numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir;

        this.dirLocks = lockLogDirs(logDirs);
        for (File dir : liveLogDirs()) {
            recoveryPointCheckpoints.put(dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel));
        }
        for (File dir : liveLogDirs()) {
            logStartOffsetCheckpoints.put(dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel));
        }

        newGauge("OfflineLogDirectoryCount",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return offlineLogDirs().size();
                    }
                },
                new HashMap<>());

        for (File dir : logDirs) {
            newGauge("LogDirectoryOffline",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return _liveLogDirs.contains(dir) ? 0 : 1;
                        }
                    },
                    new HashMap<String, String>() {{
                        put("logDirectory", dir.getAbsolutePath());
                    }}
            );
        }
    }

    public static LogManager apply(KafkaConfig config,
                                   Set<String> initialOfflineDirs,
                                   ConfigRepository configRepository,
                                   KafkaScheduler kafkaScheduler,
                                   Time time,
                                   BrokerTopicStats brokerTopicStats,
                                   LogDirFailureChannel logDirFailureChannel,
                                   Boolean keepPartitionMetadataFile) throws Throwable {
        Map<Object, Object> defaultProps = new HashMap<>(LogConfig.extractLogConfigMap(config));

        LogConfig.validateValues(defaultProps);
        LogConfig defaultLogConfig = new LogConfig(defaultProps);

        CleanerConfig cleanerConfig = LogCleaner.cleanerConfig(config);

        return new LogManager(config.getLogDirs().stream().map(dir -> new File(dir).getAbsoluteFile()).collect(Collectors.toList()),
                initialOfflineDirs.stream().map(dir -> new File(dir).getAbsoluteFile()).collect(Collectors.toList()),
                configRepository,
                defaultLogConfig,
                cleanerConfig,
                config.numRecoveryThreadsPerDataDir(),
                config.getLogFlushSchedulerIntervalMs(),
                config.getLogFlushOffsetCheckpointIntervalMs(),
                config.getLogFlushStartOffsetCheckpointIntervalMs(),
                config.getLogCleanupIntervalMs(),
                config.getTransactionMaxTimeoutMs(),
                config.getTransactionalIdExpirationMs(),
                config.getInterBrokerProtocolVersion(),
                kafkaScheduler,
                brokerTopicStats,
                logDirFailureChannel,
                time,
                keepPartitionMetadataFile);
    }

    public LogCleaner getCleaner() {
        return _cleaner;
    }

    public void reconfigureDefaultLogConfig(LogConfig logConfig) {
        this.currentDefaultConfig = logConfig;
    }

    public List<File> liveLogDirs() {
        if (_liveLogDirs.size() == logDirs.size()) {
            return logDirs;
        } else {
            return new ArrayList<>(_liveLogDirs);
        }
    }

    private Collection<File> offlineLogDirs() {
        Set<File> logDirsSet = new HashSet<>(logDirs);
        _liveLogDirs.forEach(logDirsSet::remove);
        return logDirsSet;
    }

    /**
     * Create and check validity of the given directories that are not in the given offline directories, specifically:
     * <ol>
     * <li> Ensure that there are no duplicates in the directory list
     * <li> Create each directory if it doesn't exist
     * <li> Check that each path is a readable directory
     * </ol>
     */
    private ConcurrentLinkedDeque<File> createAndValidateLogDirs(final List<File> dirs, final List<File> initialOfflineDirs) {
        ConcurrentLinkedDeque<File> liveLogDirs = new ConcurrentLinkedDeque<>();
        Set<String> canonicalPaths = new HashSet<>();

        for (final File dir : dirs) {
            try {
                if (initialOfflineDirs.contains(dir)) {
                    throw new IOException("Failed to load " + dir.getAbsolutePath() + " during broker startup");
                }

                if (!dir.exists()) {
                    LOG.info("Log directory '" + dir.getAbsolutePath() + "' not found, creating it.");
                    final boolean created = dir.mkdirs();
                    if (!created) {
                        throw new IOException("Failed to create data directory " + dir.getAbsolutePath());
                    }
                    Utils.flushDir(dir.toPath().toAbsolutePath().normalize().getParent());
                }
                if (!dir.isDirectory() || !dir.canRead()) {
                    throw new IOException(dir.getAbsolutePath() + " is not a readable log directory.");
                }

                // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
                // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
                // and mark the log directory as offline.
                if (!canonicalPaths.add(dir.getCanonicalPath())) {
                    throw new KafkaException("Duplicate log directory found: " + dirs.toString());
                }

                liveLogDirs.add(dir);
            } catch (IOException e) {
                logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath(), "Failed to create or validate data directory " + dir.getAbsolutePath(), e);
            }
        }
        if (liveLogDirs.isEmpty()) {
            String dirsStr = dirs.stream().map(File::toString).collect(Collectors.joining(", "));
            LOG.error("Shutdown broker because none of the specified log dirs from {} can be created or validated", dirsStr);
            Exit.halt(1);
        }

        return liveLogDirs;
    }

    private void resizeRecoveryThreadPool(Integer newSize) {
        LOG.info("Resizing recovery thread pool size for each data dir from {} to {}", numRecoveryThreadsPerDataDir, newSize);
        numRecoveryThreadsPerDataDir = newSize;
    }

    /**
     * The log directory failure handler. It will stop log cleaning in that directory.
     *
     * @param dir the absolute path of the log directory
     */
    private void handleLogDirFailure(final String dir) {
        LOG.warn("Stopping serving logs in dir " + dir);
        synchronized (logCreationOrDeletionLock) {
            _liveLogDirs.remove(new File(dir));
            if (_liveLogDirs.isEmpty()) {
                LOG.error("Shutdown broker because all log dirs in {} have failed",
                        logDirs.stream().map(File::toString).collect(Collectors.joining(", ")));
                Exit.halt(1);
            }

            recoveryPointCheckpoints.forEach(((file, offsetCheckpoint) -> {
                if (file.getAbsolutePath().equals(dir)) {
                    recoveryPointCheckpoints.remove(file);
                }
            }));
            logStartOffsetCheckpoints.forEach(((file, offsetCheckpoint) -> {
                if (file.getAbsolutePath().equals(dir)) {
                    logStartOffsetCheckpoints.remove(file);
                }
            }));
            if (getCleaner() != null) {
                getCleaner().handleLogDirFailure(dir);
            }

            List<TopicPartition> offlineCurrentTopicPartitions = removeOfflineLogs(dir, currentLogs);
            List<TopicPartition> offlineFutureTopicPartitions = removeOfflineLogs(dir, futureLogs);

            LOG.warn("Logs for partitions {} are offline and logs for future partitions {} are offline due to failure on log directory {}",
                    offlineCurrentTopicPartitions, offlineFutureTopicPartitions, dir);
            dirLocks.stream().filter(fileLock -> fileLock.getFile().getParent().equals(dir))
                    .forEach(fileLock -> CoreUtils.swallow(fileLock::destroy, this));
        }
    }

    private List<TopicPartition> removeOfflineLogs(String dir, ConcurrentMap<TopicPartition, UnifiedLog> logs) {
        List<TopicPartition> offlineTopicPartitions = new ArrayList<>();
        logs.forEach((tp, log) -> {
            if (log.parentDir().equals(dir)) {
                offlineTopicPartitions.add(tp);
            }
        });

        offlineTopicPartitions.forEach(topicPartition -> {
            Optional<UnifiedLog> removedLog = removeLogAndMetrics(logs, topicPartition);
            removedLog.ifPresent(UnifiedLog::closeHandlers);
        });

        return offlineTopicPartitions;
    }

    /**
     * Lock all the given directories
     */
    private List<FileLock> lockLogDirs(List<File> dirs) throws IOException {
        final List<FileLock> locks = Lists.newArrayList();

        for (final File dir : dirs) {
            try {
                final FileLock lock = new FileLock(new File(dir, LockFile));
                if (!lock.tryLock()) {
                    throw new KafkaException("Failed to acquire lock on file .lock in " + lock.getFile().getParentFile().getAbsolutePath() +
                            ". A Kafka instance in another process or thread is using this directory.");
                }
                locks.add(lock);
            } catch (IOException e) {
                logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath(), "Disk error while locking directory " + dir, e);
            }
        }

        return locks;
    }

    private void addLogToBeDeleted(UnifiedLog log) {
        this.logsToBeDeleted.add(Pair.of(log, time.milliseconds()));
    }

    // Only for testing
    protected Boolean hasLogsToBeDeleted() {
        return !logsToBeDeleted.isEmpty();
    }

    protected UnifiedLog loadLog(File logDir,
                                 Boolean hadCleanShutdown,
                                 Map<TopicPartition, Long> recoveryPoints,
                                 Map<TopicPartition, Long> logStartOffsets,
                                 LogConfig defaultConfig,
                                 Map<String, LogConfig> topicConfigOverrides,
                                 ConcurrentMap<String, Integer> numRemainingSegments) throws IOException {
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogConfig config = topicConfigOverrides.getOrDefault(topicPartition.topic(), defaultConfig);
        Long logRecoveryPoint = recoveryPoints.getOrDefault(topicPartition, 0L);
        Long logStartOffset = logStartOffsets.getOrDefault(topicPartition, 0L);

        UnifiedLog log = UnifiedLog.apply(logDir,
                config,
                logStartOffset,
                logRecoveryPoint,
                scheduler,
                brokerTopicStats,
                time,
                maxTransactionTimeoutMs,
                maxPidExpirationMs,
                LogManager.ProducerIdExpirationCheckIntervalMs,
                logDirFailureChannel,
                hadCleanShutdown,
                Optional.empty(),
                keepPartitionMetadataFile,
                numRemainingSegments);

        if (logDir.getName().endsWith(UnifiedLog.DeleteDirSuffix)) {
            addLogToBeDeleted(log);
        } else {
            UnifiedLog previous = log.isFuture() ? this.futureLogs.put(topicPartition, log) : this.currentLogs.put(topicPartition, log);
            if (previous != null) {
                if (log.isFuture()) {
                    String msg = String.format("Duplicate log directories found: %s, %s", log.dir().getAbsolutePath(), previous.dir().getAbsolutePath());
                    throw new IllegalStateException(msg);
                } else {
                    String msg = String.format("Duplicate log directories for %s are found in both %s and %s. It is likely because log directory failure happened while broker was " +
                                    "replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
                                    "for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.",
                            topicPartition, log.dir().getAbsolutePath(), previous.dir().getAbsolutePath());
                    throw new IllegalStateException(msg);
                }
            }
        }

        return log;
    }

    // factory class for naming the log recovery threads used in metrics
    private class LogRecoveryThreadFactory implements ThreadFactory {
        private AtomicInteger threadNum = new AtomicInteger(0);
        private String dirPath;

        public LogRecoveryThreadFactory(String dirPath) {
            this.dirPath = dirPath;
        }


        @Override
        public Thread newThread(Runnable runnable) {
            return KafkaThread.nonDaemon(logRecoveryThreadName(dirPath, threadNum.getAndIncrement(), null), runnable);
        }
    }

    // create a unique log recovery thread name for each log dir as the format: prefix-dirPath-threadNum, ex: "log-recovery-/tmp/kafkaLogs-0"
    private String logRecoveryThreadName(String dirPath, Integer threadNum, String prefix) {
        if (StringUtils.isBlank(prefix)) {
            prefix = "log-recovery";
        }
        return String.format("%s-%s-%s", prefix, dirPath, threadNum);
    }

    /*
     * decrement the number of remaining logs
     * @return the number of remaining logs after decremented 1
     */
    protected Integer decNumRemainingLogs(ConcurrentMap<String, Integer> numRemainingLogs, String path) {
        assert path != null : "path cannot be null to update remaining logs metric.";
        Integer oldValue = numRemainingLogs.get(path);
        oldValue -= 1;
        numRemainingLogs.put(path, oldValue);
        return oldValue;
    }

    private void handleIOException(Set<Pair<String, IOException>> offlineDirs, String logDirAbsolutePath, IOException e) {
        offlineDirs.add(Pair.of(logDirAbsolutePath, e));
        LOG.error("Error while loading log dir {}", logDirAbsolutePath, e);
    }

    /**
     * Recover and load all logs in the given data directories
     */
    private void loadLogs(LogConfig defaultConfig, Map<String, LogConfig> topicConfigOverrides) throws Exception {
        LOG.info("Loading logs from log dirs {}", liveLogDirs());
        long startMs = time.hiResClockMs();
        final List<ExecutorService> threadPools = new ArrayList<>();
        final Set<Pair<String, IOException>> offlineDirs = new HashSet<>();
        final List<List<Future<?>>> jobs = new ArrayList<>();
        int numTotalLogs = 0;
        // log dir path -> number of Remaining logs map for remainingLogsToRecover metric
        final ConcurrentMap<String, Integer> numRemainingLogs = new ConcurrentHashMap<>();
        // log recovery thread name -> number of remaining segments map for remainingSegmentsToRecover metric
        final ConcurrentMap<String, Integer> numRemainingSegments = new ConcurrentHashMap<>();

        for (final File dir : liveLogDirs()) {
            String logDirAbsolutePath = dir.getAbsolutePath();
            boolean[] hadCleanShutdown = new boolean[]{false};
            try {
                final ExecutorService pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
                        new LogRecoveryThreadFactory(logDirAbsolutePath));
                threadPools.add(pool);

                File cleanShutdownFile = new File(dir, LogLoader.CleanShutdownFile);
                if (cleanShutdownFile.exists()) {
                    LOG.info("Skipping recovery for all logs in {} since clean shutdown file was found", logDirAbsolutePath);
                    // Cache the clean shutdown status and use that for rest of log loading workflow. Delete the CleanShutdownFile
                    // so that if broker crashes while loading the log, it is considered hard shutdown during the next boot up. KAFKA-10471
                    Files.deleteIfExists(cleanShutdownFile.toPath());
                    hadCleanShutdown[0] = true;
                } else {
                    // log recovery itself is being performed by `Log` class during initialization
                    LOG.info("Attempting recovery for all logs in {} since no clean shutdown file was found", logDirAbsolutePath);
                }

                final Map<TopicPartition, Long> recoveryPoints = new HashMap<>();
                try {
                    recoveryPoints.putAll(this.recoveryPointCheckpoints.get(dir).read());
                } catch (Exception e) {
                    LOG.warn("Error occurred while reading recovery-point-offset-checkpoint file of directory {}, resetting the recovery checkpoint to 0",
                            logDirAbsolutePath, e);
                }

                final Map<TopicPartition, Long> logStartOffsets = new HashMap<>();
                try {
                    logStartOffsets.putAll(this.logStartOffsetCheckpoints.get(dir).read());
                } catch (Exception e) {
                    LOG.warn("Error occurred while reading log-start-offset-checkpoint file of directory {}, resetting to the base offset of the first segment",
                            logDirAbsolutePath, e);
                }

                File[] files = dir.listFiles();
                if (null == files) {
                    files = new File[0];
                }
                List<File> logsToLoad = Arrays.stream(files)
                        .filter(logDir -> logDir.isDirectory()
                                && !Objects.equals(UnifiedLog.parseTopicPartitionName(logDir).topic(), KafkaRaftServer.MetadataTopic))
                        .collect(Collectors.toList());
                numTotalLogs += logsToLoad.size();
                numRemainingLogs.put(dir.getAbsolutePath(), logsToLoad.size());

                final List<Runnable> jobsForDir = Lists.newArrayList();
                for (File logDir : logsToLoad) {
                    jobsForDir.add(new Runnable() {
                        @Override
                        public void run() {
                            LOG.debug("Loading log {}", logDir);
                            UnifiedLog log = null;
                            long logLoadStartMs = time.hiResClockMs();
                            try {
                                log = loadLog(logDir, hadCleanShutdown[0], recoveryPoints, logStartOffsets,
                                        defaultConfig, topicConfigOverrides, numRemainingSegments);
                            } catch (IOException e) {
                                handleIOException(offlineDirs, logDirAbsolutePath, e);
                            } catch (KafkaStorageException e) {
                                if (e.getCause() instanceof IOException) {
                                    // KafkaStorageException might be thrown, ex: during writing LeaderEpochFileCache
                                    // And while converting IOException to KafkaStorageException, we've already handled the exception. So we can ignore it here.
                                }
                            } finally {
                                long logLoadDurationMs = time.hiResClockMs() - logLoadStartMs;
                                Integer remainingLogs = decNumRemainingLogs(numRemainingLogs, dir.getAbsolutePath());
                                Integer currentNumLoaded = logsToLoad.size() - remainingLogs;
                                if (null != log) {
                                    LOG.info("Completed load of {} with {} segments in {}ms ({}/{} completed in {})",
                                            log, log.numberOfSegments(), logLoadDurationMs, currentNumLoaded, logsToLoad.size(), logDirAbsolutePath);
                                } else {
                                    LOG.info("Error while loading logs in {} in {}ms ({}/{} completed in {})",
                                            logDir, logLoadDurationMs, currentNumLoaded, logsToLoad.size(), logDirAbsolutePath);
                                }
                            }
                        }
                    });
                }

                final List<Future<?>> jobResults = Lists.newArrayList();
                for (final Runnable job : jobsForDir) {
                    jobResults.add(pool.submit(job));
                }

                jobs.add(jobResults);
            } catch (IOException e) {
                handleIOException(offlineDirs, logDirAbsolutePath, e);
            }
        }

        try {
            addLogRecoveryMetrics(numRemainingLogs, numRemainingSegments);
            for (List<Future<?>> dirJobs : jobs) {
                for (Future<?> future : dirJobs) {
                    future.get();
                }
            }

            for (Pair<String, IOException> pair : offlineDirs) {
                logDirFailureChannel.maybeAddOfflineLogDir(pair.getKey(), "Error while loading log dir " + pair.getKey(), pair.getValue());
            }
        } catch (ExecutionException e) {
            LOG.error("There was an error in one of the threads during logs loading: " + e.getCause());
            throw e;
        } finally {
            removeLogRecoveryMetrics();
            for (ExecutorService pool : threadPools) {
                pool.shutdown();
            }
        }

        LOG.info("Loaded {} logs in {}ms.", numTotalLogs, time.hiResClockMs() - startMs);
    }

    protected void addLogRecoveryMetrics(ConcurrentMap<String, Integer> numRemainingLogs,
                                         ConcurrentMap<String, Integer> numRemainingSegments) {
        LOG.debug("Adding log recovery metrics");
        for (File dir : logDirs) {
            Map<String, String> remainingLogsToRecoverTags = new HashMap<>();
            remainingLogsToRecoverTags.put("dir", dir.getAbsolutePath());
            newGauge("remainingLogsToRecover", new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return numRemainingLogs.get(dir.getAbsolutePath());
                }
            }, remainingLogsToRecoverTags);
            for (int i = 0; i < numRecoveryThreadsPerDataDir; i++) {
                String threadName = logRecoveryThreadName(dir.getAbsolutePath(), i, null);
                Map<String, String> remainingSegmentsToRecoverTags = new HashMap<>();
                remainingSegmentsToRecoverTags.put("dir", dir.getAbsolutePath());
                remainingSegmentsToRecoverTags.put("threadNum", Integer.toString(i));
                newGauge("remainingSegmentsToRecover", new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return numRemainingSegments.get(threadName);
                    }
                }, remainingSegmentsToRecoverTags);
            }
        }
    }

    protected void removeLogRecoveryMetrics() {
        LOG.debug("Removing log recovery metrics");
        for (File dir : logDirs) {
            removeMetric("remainingLogsToRecover", new HashMap<String, String>() {{
                put("dir", dir.getAbsolutePath());
            }});
            for (int i = 0; i < numRecoveryThreadsPerDataDir; i++) {
                String iS = Integer.toString(i);
                removeMetric("remainingSegmentsToRecover", new HashMap<String, String>() {{
                    put("dir", dir.getAbsolutePath());
                    put("threadNum", iS);
                }});
            }
        }
    }

    /**
     * Start the background threads to flush logs and do log cleanup
     */
    public void startup(Set<String> topicNames) throws Exception {
        // ensure consistency between default config and overrides
        LogConfig defaultConfig = getCurrentDefaultConfig();
        startupWithConfigOverrides(defaultConfig, fetchTopicConfigOverrides(defaultConfig, topicNames));
    }

    // visible for testing
//    @nowarn("cat=deprecation")
    private Map<String, LogConfig> fetchTopicConfigOverrides(LogConfig defaultConfig, Set<String> topicNames) {
        Map<String, LogConfig> topicConfigOverrides = new HashMap<>();
        Map<String, Object> defaultProps = defaultConfig.originals();
        for (String topicName : topicNames) {
            final Properties[] overrides = new Properties[]{configRepository.topicConfig(topicName)};
            // save memory by only including configs for topics with overrides
            if (!overrides[0].isEmpty()) {
                Optional.ofNullable(overrides[0].getProperty(LogConfig.MessageFormatVersionProp)).ifPresent(new Consumer<String>() {
                    @Override
                    public void accept(String versionString) {
                        MessageFormatVersion messageFormatVersion = new MessageFormatVersion(versionString, interBrokerProtocolVersion.version());
                        if (messageFormatVersion.shouldIgnore()) {
                            Properties copy = new Properties();
                            copy.putAll(overrides[0]);
                            copy.remove(LogConfig.MessageFormatVersionProp);
                            overrides[0] = copy;

                            if (messageFormatVersion.shouldWarn()) {
                                LOG.warn(messageFormatVersion.topicWarningMessage(topicName));
                            }
                        }
                    }
                });

                LogConfig logConfig = LogConfig.fromProps(defaultProps, overrides[0]);
                topicConfigOverrides.put(topicName, logConfig);
            }
        }
        return topicConfigOverrides;
    }

    private LogConfig fetchLogConfig(String topicName) {
        // ensure consistency between default config and overrides
        LogConfig defaultConfig = getCurrentDefaultConfig();
        return fetchTopicConfigOverrides(defaultConfig, Sets.newHashSet(topicName)).getOrDefault(topicName, defaultConfig);
    }

    // visible for testing
    private void startupWithConfigOverrides(LogConfig defaultConfig, Map<String, LogConfig> topicConfigOverrides) throws Exception {
        loadLogs(defaultConfig, topicConfigOverrides); // this could take a while if shutdown was not clean

        /* Schedule the cleanup task to delete old logs */
        if (scheduler != null) {
            LOG.info(String.format("Starting log cleanup with a period of %d ms.", retentionCheckMs));
            scheduler.schedule("kafka-log-retention",
                    () -> {
                        try {
                            cleanupLogs();
                        } catch (Throwable throwable) {
                            LOG.error(throwable.getMessage(), throwable);
                        }
                    },
                    InitialTaskDelayMs,
                    retentionCheckMs,
                    TimeUnit.MILLISECONDS);
            LOG.info(String.format("Starting log flusher with a default period of %d ms.", flushCheckMs));
            scheduler.schedule("kafka-log-flusher",
                    () -> {
                        try {
                            flushDirtyLogs();
                        } catch (Throwable throwable) {
                            LOG.error(throwable.getMessage(), throwable);
                        }
                    },
                    InitialTaskDelayMs,
                    flushCheckMs,
                    TimeUnit.MILLISECONDS);
            scheduler.schedule("kafka-recovery-point-checkpoint",
                    () -> {
                        try {
                            checkpointLogRecoveryOffsets();
                        } catch (Throwable throwable) {
                            LOG.error(throwable.getMessage(), throwable);
                        }
                    },
                    InitialTaskDelayMs,
                    flushRecoveryOffsetCheckpointMs,
                    TimeUnit.MILLISECONDS);
            scheduler.schedule("kafka-log-start-offset-checkpoint",
                    () -> {
                        try {
                            checkpointLogStartOffsets();
                        } catch (Throwable throwable) {
                            LOG.error(throwable.getMessage(), throwable);
                        }
                    },
                    InitialTaskDelayMs,
                    flushStartOffsetCheckpointMs,
                    TimeUnit.MILLISECONDS);
            scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                    () -> {
                        try {
                            deleteLogs();
                        } catch (Throwable throwable) {
                            LOG.error(throwable.getMessage(), throwable);
                        }
                    },
                    InitialTaskDelayMs,
                    -1,
                    TimeUnit.MILLISECONDS);
        }
        if (cleanerConfig.isEnableCleaner()) {
            _cleaner = new LogCleaner(cleanerConfig, liveLogDirs(), currentLogs, logDirFailureChannel, time);
            _cleaner.startup();
        }
    }

    /**
     * Close all the logs
     */
    public void shutdown() throws Throwable {
        LOG.info("Shutting down.");

        removeMetric("OfflineLogDirectoryCount", new HashMap<>());
        for (File dir : logDirs) {
            removeMetric("LogDirectoryOffline", new HashMap<String, String>() {{
                put("logDirectory", dir.getAbsolutePath());
            }});
        }

        final List<ExecutorService> threadPools = Lists.newArrayList();
        final Map<File, List<Future<?>>> jobs = Maps.newHashMap();

        // stop the cleaner first
        if (getCleaner() != null) {
            CoreUtils.swallow(() -> getCleaner().shutdown(), this);
        }

        Map<String, Map<TopicPartition, UnifiedLog>> localLogsByDir = logsByDir();

        // close logs in each dir
        for (final File dir : logDirs) {
            LOG.debug("Flushing and closing logs at {}", dir);

            String name = String.format("log-closing-%s", dir.getAbsolutePath());
            ExecutorService pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir, runnable -> KafkaThread.nonDaemon(name, runnable));
            threadPools.add(pool);

            Collection<UnifiedLog> logs = logsInDir(localLogsByDir, dir).values();

            List<Runnable> jobsForDir = new ArrayList<>();
            for (UnifiedLog log : logs) {
                jobsForDir.add(new Runnable() {
                    @Override
                    public void run() {
                        // flush the log to ensure latest possible recovery point
                        log.flush(true);
                        log.close();
                    }
                });
            }

            List<Future<?>> jobResults = new ArrayList<>();
            for (Runnable runnable : jobsForDir) {
                jobResults.add(pool.submit(runnable));
            }
            jobs.put(dir, jobResults);
        }

        try {
            for (Map.Entry<File, List<Future<?>>> entry : jobs.entrySet()) {
                final File dir = entry.getKey();
                final List<Future<?>> dirJobs = entry.getValue();
                if (waitForAllToComplete(dirJobs, (e) -> LOG.warn("There was an error in one of the threads during LogManager shutdown: {}", e.getCause(), e))) {
                    Map<TopicPartition, UnifiedLog> logs = logsInDir(localLogsByDir, dir);

                    // update the last flush point
                    LOG.debug("Updating recovery points at {}", dir);
                    checkpointRecoveryOffsetsInDir(dir, logs);

                    LOG.debug("Updating log start offsets at {}", dir);
                    checkpointLogStartOffsetsInDir(dir, logs);

                    // mark that the shutdown was clean by creating marker file
                    LOG.debug("Writing clean shutdown marker at {}", dir);
                    CoreUtils.swallow(() -> Files.createFile(new File(dir, LogLoader.CleanShutdownFile).toPath()), this);
                }
            }
        } finally {
            for (ExecutorService pool : threadPools) {
                pool.shutdown();
            }
            // regardless of whether the close succeeded, we need to unlock the data directories
            for (FileLock lock : dirLocks) {
                lock.destroy();
            }
        }

        LOG.info("Shutdown complete.");
    }

    /**
     * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
     *
     * @param partitionOffsets Partition logs that need to be truncated
     * @param isFuture         True iff the truncation should be performed on the future log of the specified partitions
     */
    public void truncateTo(Map<TopicPartition, Long> partitionOffsets, Boolean isFuture) throws Throwable {
        List<UnifiedLog> affectedLogs = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Long truncateOffset = entry.getValue();
            UnifiedLog log = isFuture ? futureLogs.get(topicPartition) : currentLogs.get(topicPartition);
            // If the log does not exist, skip it
            if (log != null) {
                // May need to abort and pause the cleaning of the log, and resume after truncation is done.
                boolean needToStopCleaner = truncateOffset < log.activeSegment().getBaseOffset();
                if (needToStopCleaner && !isFuture) {
                    abortAndPauseCleaning(topicPartition);
                }
                try {
                    if (log.truncateTo(truncateOffset))
                        affectedLogs.add(log);
                    if (needToStopCleaner && !isFuture)
                        maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition);
                } finally {
                    if (needToStopCleaner && !isFuture)
                        resumeCleaning(topicPartition);
                }
            }
        }

        Set<File> affectedLogParentDirs = affectedLogs.stream()
                .map(UnifiedLog::parentDirFile)
                .collect(Collectors.toSet());
        for (File dir : affectedLogParentDirs) {
            checkpointRecoveryOffsetsInDir(dir);
        }
    }

    /**
     * Delete all data in a partition and start the log at the new offset
     *
     * @param topicPartition The partition whose log needs to be truncated
     * @param newOffset      The new offset to start the log with
     * @param isFuture       True iff the truncation should be performed on the future log of the specified partition
     */
    public void truncateFullyAndStartAt(TopicPartition topicPartition, Long newOffset, Boolean isFuture) throws Throwable {
        UnifiedLog log = isFuture ? futureLogs.get(topicPartition) : currentLogs.get(topicPartition);
        // If the log does not exist, skip it
        if (log != null) {
            // Abort and pause the cleaning of the log, and resume after truncation is done.
            if (!isFuture) {
                abortAndPauseCleaning(topicPartition);
            }
            try {
                log.truncateFullyAndStartAt(newOffset);
                if (!isFuture) {
                    maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition);
                }
            } finally {
                if (!isFuture) {
                    resumeCleaning(topicPartition);
                }
            }
            checkpointRecoveryOffsetsInDir(log.parentDirFile());
        }
    }

    /**
     * Write out the current recovery point for all logs to a text file in the log directory
     * to avoid recovering the whole log on startup.
     */
    public void checkpointLogRecoveryOffsets() {
        Map<String, Map<TopicPartition, UnifiedLog>> logsByDirCached = logsByDir();
        for (File logDir : liveLogDirs()) {
            Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logsByDirCached, logDir);
            checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
        }
    }

    /**
     * Write out the current log start offset for all logs to a text file in the log directory
     * to avoid exposing data that have been deleted by DeleteRecordsRequest
     */
    public void checkpointLogStartOffsets() throws Throwable {
        Map<String, Map<TopicPartition, UnifiedLog>> logsByDirCached = logsByDir();
        for (File logDir : liveLogDirs()) {
            checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir));
        }
    }

    /**
     * Checkpoint recovery offsets for all the logs in logDir.
     *
     * @param logDir the directory in which the logs to be checkpointed are
     */
    // Only for testing
    protected void checkpointRecoveryOffsetsInDir(final File logDir) {
        checkpointRecoveryOffsetsInDir(logDir, logsInDir(logDir));
    }

    /**
     * Checkpoint recovery offsets for all the provided logs.
     *
     * @param logDir           the directory in which the logs are
     * @param logsToCheckpoint the logs to be checkpointed
     */
    private void checkpointRecoveryOffsetsInDir(File logDir, Map<TopicPartition, UnifiedLog> logsToCheckpoint) {
        try {
            if (recoveryPointCheckpoints.containsKey(logDir)) {
                OffsetCheckpointFile checkpoint = recoveryPointCheckpoints.get(logDir);
                Map<TopicPartition, Long> recoveryOffsets = logsToCheckpoint.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().recoveryPoint()));
                // checkpoint.write calls Utils.atomicMoveWithFallback, which flushes the parent
                // directory and guarantees crash consistency.
                checkpoint.write(recoveryOffsets);
            }
        } catch (KafkaStorageException e) {
            LOG.error("Disk error while writing recovery offsets checkpoint in directory {}: {}", logDir, e.getMessage());
        } catch (IOException e) {
            String msg = String.format("Disk error while writing recovery offsets checkpoint in directory %s: %s", logDir, e.getMessage());
            logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath(), msg, e);
        }
    }

    /**
     * Checkpoint log start offsets for all the provided logs in the provided directory.
     *
     * @param logDir           the directory in which logs are checkpointed
     * @param logsToCheckpoint the logs to be checkpointed
     */
    private void checkpointLogStartOffsetsInDir(File logDir, Map<TopicPartition, UnifiedLog> logsToCheckpoint) throws Throwable {
        try {
            if (logStartOffsetCheckpoints.containsKey(logDir)) {
                OffsetCheckpointFile checkpoint = logStartOffsetCheckpoints.get(logDir);
                Map<TopicPartition, Long> logStartOffsets = logsToCheckpoint.entrySet().stream()
                        .filter(entry -> entry.getValue().getLogStartOffset() > CollectionUtilExt.head(entry.getValue().logSegments()).getBaseOffset())
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getLogStartOffset()));
                checkpoint.write(logStartOffsets);
            }
        } catch (KafkaStorageException e) {
            LOG.error("Disk error while writing log start offsets checkpoint in directory {}: {}", logDir, e.getMessage());
        }
    }

    // The logDir should be an absolute path
    public void maybeUpdatePreferredLogDir(TopicPartition topicPartition, String logDir) {
        // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
        Optional<UnifiedLog> currentLog = getLog(topicPartition, false);
        Optional<UnifiedLog> futureLog = getLog(topicPartition, true);
        if (!(currentLog.isPresent() && currentLog.get().parentDir().equals(logDir))
                && !(futureLog.isPresent() && futureLog.get().parentDir().equals(logDir))) {
            preferredLogDirs.put(topicPartition, logDir);
        }
    }

    /**
     * Abort and pause cleaning of the provided partition and log a message about it.
     */
    public void abortAndPauseCleaning(TopicPartition topicPartition) throws Throwable {
        if (getCleaner() != null) {
            getCleaner().abortAndPauseCleaning(topicPartition);
            LOG.info("The cleaning for partition {} is aborted and paused", topicPartition);
        }
    }

    /**
     * Abort cleaning of the provided partition and log a message about it.
     */
    public void abortCleaning(TopicPartition topicPartition) throws Throwable {
        if (getCleaner() != null) {
            getCleaner().abortCleaning(topicPartition);
            LOG.info("The cleaning for partition {} is aborted", topicPartition);
        }
    }

    /**
     * Resume cleaning of the provided partition and log a message about it.
     */
    private void resumeCleaning(TopicPartition topicPartition) throws Throwable {
        if (getCleaner() != null) {
            getCleaner().resumeCleaning(Arrays.asList(topicPartition));
            LOG.info("Cleaning for partition {} is resumed", topicPartition);
        }
    }

    /**
     * Truncate the cleaner's checkpoint to the based offset of the active segment of
     * the provided log.
     */
    private void maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(UnifiedLog log, TopicPartition topicPartition) throws Throwable {
        if (getCleaner() != null) {
            getCleaner().maybeTruncateCheckpoint(log.parentDirFile(), topicPartition, log.activeSegment().getBaseOffset());
        }
    }

    public Optional<UnifiedLog> getLog(TopicPartition topicPartition) {
        return getLog(topicPartition, false);
    }

    /**
     * Get the log if it exists, otherwise return None
     *
     * @param topicPartition the partition of the log
     * @param isFuture       True iff the future log of the specified partition should be returned
     */
    public Optional<UnifiedLog> getLog(TopicPartition topicPartition, Boolean isFuture) {
        if (null == isFuture) {
            isFuture = false;
        }
        if (isFuture) {
            return Optional.ofNullable(futureLogs.get(topicPartition));
        } else {
            return Optional.ofNullable(currentLogs.get(topicPartition));
        }
    }

    /**
     * Method to indicate that logs are getting initialized for the partition passed in as argument.
     * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
     * initialization is done.
     */
    public void initializingLog(TopicPartition topicPartition) {
        partitionsInitializing.put(topicPartition, false);
    }

    /**
     * Mark the partition configuration for all partitions that are getting initialized for topic
     * as dirty. That will result in reloading of configuration once initialization is done.
     */
    public void topicConfigUpdated(String topic) {
        for (TopicPartition topicPartition : partitionsInitializing.keySet()) {
            if (Objects.equals(topicPartition.topic(), topic)) {
                partitionsInitializing.replace(topicPartition, false, true);
            }
        }
    }

    /**
     * Update the configuration of the provided topic.
     */
    public void updateTopicConfig(String topic, Properties newTopicConfig) throws Throwable {
        topicConfigUpdated(topic);
        Set<UnifiedLog> logs = logsByTopic(topic);
        if (!logs.isEmpty()) {
            // Combine the default properties with the overrides in zk to create the new LogConfig
            LogConfig newLogConfig = LogConfig.fromProps(getCurrentDefaultConfig().originals(), newTopicConfig);
            for (UnifiedLog log : logs) {
                LogConfig oldLogConfig = log.updateConfig(newLogConfig);
                if (oldLogConfig.getCompact() && !newLogConfig.getCompact()) {
                    abortCleaning(log.topicPartition());
                }
            }
        }
    }

    /**
     * Mark all in progress partitions having dirty configuration if broker configuration is updated.
     */
    public void brokerConfigUpdated() {
        for (TopicPartition topicPartition : partitionsInitializing.keySet()) {
            partitionsInitializing.replace(topicPartition, false, true);
        }
    }

    /**
     * Method to indicate that the log initialization for the partition passed in as argument is
     * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
     * <p>
     * It will retrieve the topic configs a second time if they were updated while the
     * relevant log was being loaded.
     */
    public void finishedInitializingLog(TopicPartition topicPartition, Optional<UnifiedLog> maybeLog) throws Throwable {
        if (partitionsInitializing.containsKey(topicPartition)) {
            Boolean removedValue = partitionsInitializing.remove(topicPartition);
            if (removedValue) {
                if (maybeLog.isPresent()) {
                    UnifiedLog log = maybeLog.get();
                    log.updateConfig(fetchLogConfig(topicPartition.topic()));
                }
            }
        }
    }

    public UnifiedLog getOrCreateLog(TopicPartition topicPartition, Optional<Uuid> topicId) throws Throwable {
        return getOrCreateLog(topicPartition, false, false, topicId);
    }

    /**
     * If the log already exists, just return a copy of the existing log
     * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
     * Otherwise throw KafkaStorageException
     *
     * @param topicPartition The partition whose log needs to be returned or created
     * @param isNew          Whether the replica should have existed on the broker or not
     * @param isFuture       True if the future log of the specified partition should be returned or created
     * @param topicId        The topic ID of the partition's topic
     * @throws KafkaStorageException        if isNew=false, log is not found in the cache and there is offline log directory on the broker
     * @throws InconsistentTopicIdException if the topic ID in the log does not match the topic ID provided
     */
    public UnifiedLog getOrCreateLog(TopicPartition topicPartition, Boolean isNew, Boolean isFuture, Optional<Uuid> topicId) throws Throwable {
        synchronized (logCreationOrDeletionLock) {
            UnifiedLog log = null;
            Optional<UnifiedLog> optionalLog = getLog(topicPartition, isFuture);
            if (optionalLog.isPresent()) {
                log = optionalLog.get();
            } else {
                // create the log if it has not already been created in another thread
                if (!isNew && !offlineLogDirs().isEmpty()) {
                    String msg = String.format("Can not create log for %s because log directories %s are offline",
                            topicPartition, offlineLogDirs().stream().map(File::toString).collect(Collectors.joining(",")));
                    throw new KafkaStorageException(msg);
                }

                List<File> logDirs = null;
                {
                    String preferredLogDir = preferredLogDirs.get(topicPartition);

                    if (isFuture) {
                        if (preferredLogDir == null) {
                            throw new IllegalStateException("Can not create the future log for " + topicPartition + " without having a preferred log directory");
                        } else {
                            Optional<UnifiedLog> currentLog = getLog(topicPartition, false);
                            if (currentLog.isPresent() && Objects.equals(currentLog.get().parentDir(), preferredLogDir)) {
                                throw new IllegalStateException("Can not create the future log for " + topicPartition + " in the current log directory of this partition");
                            }
                        }
                    }

                    if (preferredLogDir != null)
                        logDirs = Lists.newArrayList(new File(preferredLogDir));
                    else
                        logDirs = nextLogDirs();
                }

                String logDirName = isFuture ? UnifiedLog.logFutureDirName(topicPartition) : UnifiedLog.logDirName(topicPartition);

                File logDir = logDirs.stream().map(dir -> createLogDirectory(dir, logDirName))
                        .filter(Try::isSuccess)
                        .findFirst()
                        .orElse(new Failure<>(new KafkaStorageException(String.format("No log directories available. Tried %s",
                                logDirs.stream().map(File::getAbsolutePath).collect(Collectors.joining(", "))))))
                        .get();

                LogConfig config = fetchLogConfig(topicPartition.topic());
                UnifiedLog tmpLog = UnifiedLog.apply(
                        logDir,
                        config,
                        0L,
                        0L,
                        scheduler,
                        brokerTopicStats,
                        time,
                        maxTransactionTimeoutMs,
                        maxPidExpirationMs,
                        LogManager.ProducerIdExpirationCheckIntervalMs,
                        logDirFailureChannel,
                        true,
                        topicId,
                        keepPartitionMetadataFile,
                        new ConcurrentHashMap<>());

                if (isFuture) {
                    futureLogs.put(topicPartition, tmpLog);
                } else {
                    currentLogs.put(topicPartition, tmpLog);
                }

                LOG.info("Created log for partition {} in {} with properties {}", topicPartition, logDir, config.overriddenConfigsAsLoggableString());
                // Remove the preferred log dir since it has already been satisfied
                preferredLogDirs.remove(topicPartition);

                log = tmpLog;
            }
            // When running a ZK controller, we may get a log that does not have a topic ID. Assign it here.
            if (!log.topicId().isPresent()) {
                topicId.ifPresent(log::assignTopicId);
            }

            // Ensure topic IDs are consistent
            if (topicId.isPresent()) {
                Uuid uuid = topicId.get();
                if (log.topicId().isPresent()) {
                    Uuid logUuid = log.topicId().get();
                    if (uuid != logUuid) {
                        String msg = String.format("Tried to assign topic ID %s to log for topic partition %s, but log already contained topic ID %s",
                                uuid, topicPartition, logUuid);
                        throw new InconsistentTopicIdException(msg);
                    }
                }
            }
            return log;
        }
    }

    protected Try<File> createLogDirectory(File logDir, String logDirName) {
        String logDirPath = logDir.getAbsolutePath();
        if (isLogDirOnline(logDirPath)) {
            File dir = new File(logDirPath, logDirName);
            try {
                Files.createDirectories(dir.toPath());
                return new Success(dir);
            } catch (IOException e) {
                String msg = String.format("Error while creating log for %s in dir %s", logDirName, logDirPath);
                logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e);
                LOG.warn(msg, e);
                return new Failure<>(new KafkaStorageException(msg, e));
            }
        } else {
            return new Failure<>(new KafkaStorageException(String.format("Can not create log %s because log directory %s is offline", logDirName, logDirPath)));
        }
    }

    /**
     * Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
     * has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
     * considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
     * after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
     * `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
     */
    private void deleteLogs() {
        long nextDelayMs = 0L;
        long fileDeleteDelayMs = getCurrentDefaultConfig().getFileDeleteDelayMs();
        try {
            nextDelayMs = nextDeleteDelayMs(fileDeleteDelayMs);
            while (nextDelayMs <= 0) {
                UnifiedLog removedLog = logsToBeDeleted.take().getKey();
                if (null != removedLog) {
                    try {
                        removedLog.delete();
                        LOG.info("Deleted log for partition {} in {}.", removedLog.topicPartition(), removedLog.dir().getAbsolutePath());
                    } catch (KafkaStorageException e) {
                        LOG.error("Exception while deleting {} in dir {}.", removedLog, removedLog.parentDir(), e);
                    }
                }
                nextDelayMs = nextDeleteDelayMs(fileDeleteDelayMs);
            }
        } catch (Throwable e) {
            LOG.error("Exception in kafka-delete-logs thread.", e);
        } finally {
            try {
                scheduler.schedule("kafka-delete-logs",
                        () -> deleteLogs(),
                        nextDelayMs,
                        -1,
                        TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                if (scheduler.isStarted()) {
                    // No errors should occur unless scheduler has been shutdown
                    LOG.error("Failed to schedule next delete in kafka-delete-logs thread", e);
                }
            }
        }
    }

    private Long nextDeleteDelayMs(long fileDeleteDelayMs) {
        if (!logsToBeDeleted.isEmpty()) {
            Pair<UnifiedLog, Long> peek = logsToBeDeleted.peek();
            Long scheduleTimeMs = peek.getValue();
            return scheduleTimeMs + fileDeleteDelayMs - time.milliseconds();
        } else {
            return fileDeleteDelayMs;
        }
    }

    /**
     * Mark the partition directory in the source log directory for deletion and
     * rename the future log of this partition in the destination log directory to be the current log
     *
     * @param topicPartition TopicPartition that needs to be swapped
     */
    public void replaceCurrentWithFutureLog(TopicPartition topicPartition) throws Throwable {
        synchronized (logCreationOrDeletionLock) {
            UnifiedLog sourceLog = currentLogs.get(topicPartition);
            UnifiedLog destLog = futureLogs.get(topicPartition);

            LOG.info("Attempting to replace current log {} with {} for {}", sourceLog, destLog, topicPartition);
            if (sourceLog == null)
                throw new KafkaStorageException("The current replica for " + topicPartition + " is offline");
            if (destLog == null)
                throw new KafkaStorageException("The future replica for " + topicPartition + " is offline");

            destLog.renameDir(UnifiedLog.logDirName(topicPartition), true);
            destLog.updateHighWatermark(sourceLog.highWatermark());

            // Now that future replica has been successfully renamed to be the current replica
            // Update the cached map and log cleaner as appropriate.
            futureLogs.remove(topicPartition);
            currentLogs.put(topicPartition, destLog);
            if (getCleaner() != null) {
                getCleaner().alterCheckpointDir(topicPartition, sourceLog.parentDirFile(), destLog.parentDirFile());
                resumeCleaning(topicPartition);
            }

            try {
                sourceLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), true);
                // Now that replica in source log directory has been successfully renamed for deletion.
                // Close the log, update checkpoint files, and enqueue this log to be deleted.
                sourceLog.close();
                File logDir = sourceLog.parentDirFile();
                Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logDir);
                checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
                checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint);
                sourceLog.removeLogMetrics();
                addLogToBeDeleted(sourceLog);
            } catch (KafkaStorageException e) {
                // If sourceLog's log directory is offline, we need close its handlers here.
                // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
                sourceLog.closeHandlers();
                sourceLog.removeLogMetrics();
                throw e;
            }

            LOG.info("The current replica is successfully replaced with the future replica for {}", topicPartition);
        }
    }

    public Optional<UnifiedLog> asyncDelete(TopicPartition topicPartition) throws Throwable {
        return asyncDelete(topicPartition, false, true);
    }

    /**
     * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
     * add it in the queue for deletion.
     *
     * @param topicPartition TopicPartition that needs to be deleted
     * @param isFuture       True iff the future log of the specified partition should be deleted
     * @param checkpoint     True if checkpoints must be written
     * @return the removed log
     */
    public Optional<UnifiedLog> asyncDelete(TopicPartition topicPartition,
                                            Boolean isFuture,
                                            Boolean checkpoint) throws Throwable {
        Optional<UnifiedLog> removedLog;
        synchronized (logCreationOrDeletionLock) {
            removedLog = removeLogAndMetrics(isFuture ? futureLogs : currentLogs, topicPartition);
        }
        if (removedLog.isPresent()) {
            // We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
            if (getCleaner() != null && !isFuture) {
                getCleaner().abortCleaning(topicPartition);
                if (checkpoint) {
                    getCleaner().updateCheckpoints(removedLog.get().parentDirFile(), Optional.ofNullable(topicPartition));
                }
            }
            removedLog.get().renameDir(UnifiedLog.logDeleteDirName(topicPartition), false);
            if (checkpoint) {
                File logDir = removedLog.get().parentDirFile();
                Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logDir);
                checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
                checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint);
            }
            addLogToBeDeleted(removedLog.get());
            LOG.info("Log for partition {} is renamed to {} and is scheduled for deletion",
                    removedLog.get().topicPartition(), removedLog.get().dir().getAbsolutePath());
        } else {
            Collection<File> offlineLogDirs = offlineLogDirs();
            if (CollectionUtils.isNotEmpty(offlineLogDirs)) {
                String msg = String.format("Failed to delete log for %s %s because it may be in one of the offline directories %s",
                        isFuture ? "future" : "", topicPartition, offlineLogDirs.stream().map(File::toString).collect(Collectors.joining(",")));
                throw new KafkaStorageException(msg);
            }
        }

        return removedLog;
    }

    /**
     * Rename the directories of the given topic-partitions and add them in the queue for
     * deletion. Checkpoints are updated once all the directories have been renamed.
     *
     * @param topicPartitions The set of topic-partitions to delete asynchronously
     * @param errorHandler    The error handler that will be called when a exception for a particular
     *                        topic-partition is raised
     */
    public void asyncDelete(Set<TopicPartition> topicPartitions, BiConsumer<TopicPartition, Throwable> errorHandler) throws Throwable {
        Set<File> logDirs = new HashSet<>();

        for (TopicPartition topicPartition : topicPartitions) {
            try {
                Optional<UnifiedLog> currentLog = getLog(topicPartition, false);
                if (currentLog.isPresent()) {
                    UnifiedLog log = currentLog.get();
                    logDirs.add(log.parentDirFile());
                    asyncDelete(topicPartition, false, false);
                }
                Optional<UnifiedLog> futureLog = getLog(topicPartition, true);
                if (futureLog.isPresent()) {
                    UnifiedLog log = futureLog.get();
                    logDirs.add(log.parentDirFile());
                    asyncDelete(topicPartition, true, false);
                }
            } catch (Throwable e) {
                errorHandler.accept(topicPartition, e);
            }
        }

        Map<String, Map<TopicPartition, UnifiedLog>> logsByDirCached = logsByDir();
        for (File logDir : logDirs) {
            if (getCleaner() != null) {
                getCleaner().updateCheckpoints(logDir, Optional.empty());
            }
            Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logsByDirCached, logDir);
            checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
            checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint);
        }
    }

    /**
     * Provides the full ordered list of suggested directories for the next partition.
     * Currently this is done by calculating the number of partitions in each directory and then sorting the
     * data directories by fewest partitions.
     */
    private List<File> nextLogDirs() {
        if (_liveLogDirs.size() == 1) {
            return new ArrayList<File>() {{
                add(_liveLogDirs.peek());
            }};
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories
            LinkedHashMap<String, Integer> dirCounts = new LinkedHashMap<>();
            for (UnifiedLog log : allLogs()) {
                String parentDir = log.parentDir();
                Integer count = dirCounts.getOrDefault(parentDir, 0);
                count++;
                dirCounts.put(parentDir, count);
            }

            for (File dir : _liveLogDirs) {
                if (!dirCounts.containsKey(dir.getPath())) {
                    dirCounts.put(dir.getPath(), 0);
                }
            }

            // choose the directory with the least logs in it
            final Ordering<Map.Entry<String, Integer>> ordering = new Ordering<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> left, Map.Entry<String, Integer> right) {
                    return left.getValue().compareTo(right.getValue());
                }
            };

            final List<Map.Entry<String, Integer>> entries = Lists.newArrayList(dirCounts.entrySet());

            // Sorts the entry sets by their value.
            entries.sort(ordering);

            return entries.stream().map(Map.Entry::getKey).map(File::new).collect(Collectors.toList());
        }
    }

    /**
     * Delete any eligible logs. Return the number of segments deleted.
     * Only consider logs that are not compacted.
     */
    public void cleanupLogs() throws Throwable {
        LOG.debug("Beginning log cleanup...");
        long total = 0;
        long startMs = time.milliseconds();

        // clean current logs.
        Iterable<Map.Entry<TopicPartition, UnifiedLog>> deletableLogs;

        if (Objects.nonNull(getCleaner())) {
            // prevent cleaner from working on same partitions when changing cleanup policy
            deletableLogs = getCleaner().pauseCleaningForNonCompactedPartitions();
        } else {
            deletableLogs = currentLogs.entrySet().stream()
                    .filter(entry -> !entry.getValue().config().getCompact())
                    .collect(Collectors.toList());
        }

        try {
            for (Map.Entry<TopicPartition, UnifiedLog> entry : deletableLogs) {
                TopicPartition topicPartition = entry.getKey();
                UnifiedLog log = entry.getValue();
                LOG.debug("Garbage collecting '{}'", log.name());
                total += log.deleteOldSegments();

                UnifiedLog futureLog = futureLogs.get(topicPartition);
                if (futureLog != null) {
                    // clean future logs
                    LOG.debug("Garbage collecting future log '{}'", futureLog.name());
                    total += futureLog.deleteOldSegments();
                }
            }
        } finally {
            if (getCleaner() != null) {
                List<TopicPartition> deletableTps = new ArrayList<>();
                for (Map.Entry<TopicPartition, UnifiedLog> entry : deletableLogs) {
                    deletableTps.add(entry.getKey());
                }
                getCleaner().resumeCleaning(deletableTps);
            }
        }

        LOG.debug("Log cleanup completed. {} files deleted in {} seconds", total, (time.milliseconds() - startMs) / 1000);
    }

    /**
     * Get all the partition logs
     */
    public Collection<UnifiedLog> allLogs() {
        List<UnifiedLog> allLogs = new ArrayList<>();
        allLogs.addAll(currentLogs.values());
        allLogs.addAll(futureLogs.values());
        return allLogs;
    }

    public Set<UnifiedLog> logsByTopic(String topic) {
        Set<UnifiedLog> logs = new HashSet<>();
        for (Map.Entry<TopicPartition, UnifiedLog> entry : currentLogs.entrySet()) {
            if (Objects.equals(entry.getKey().topic(), topic)) {
                logs.add(entry.getValue());
            }
        }
        for (Map.Entry<TopicPartition, UnifiedLog> entry : futureLogs.entrySet()) {
            if (Objects.equals(entry.getKey().topic(), topic)) {
                logs.add(entry.getValue());
            }
        }
        return logs;
    }

    /**
     * Map of log dir to logs by topic and partitions in that dir
     */
    private Map<String, Map<TopicPartition, UnifiedLog>> logsByDir() {
        // This code is called often by checkpoint processes and is written in a way that reduces
        // allocations and CPU with many topic partitions.
        // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
        Map<String, Map<TopicPartition, UnifiedLog>> byDir = new HashMap<>();
        for (Map.Entry<TopicPartition, UnifiedLog> entry : currentLogs.entrySet()) {
            addToDir(byDir, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<TopicPartition, UnifiedLog> entry : futureLogs.entrySet()) {
            addToDir(byDir, entry.getKey(), entry.getValue());
        }
        return byDir;
    }

    private void addToDir(Map<String, Map<TopicPartition, UnifiedLog>> byDir, TopicPartition tp, UnifiedLog log) {
        Map<TopicPartition, UnifiedLog> map = byDir.computeIfAbsent(log.parentDir(), k -> new HashMap<>());
        map.put(tp, log);
    }

    private Map<TopicPartition, UnifiedLog> logsInDir(File dir) {
        return logsByDir().getOrDefault(dir.getAbsolutePath(), new HashMap<>());
    }

    private Map<TopicPartition, UnifiedLog> logsInDir(Map<String, Map<TopicPartition, UnifiedLog>> cachedLogsByDir,
                                                      File dir) {
        return cachedLogsByDir.getOrDefault(dir.getAbsolutePath(), new HashMap<>());
    }

    // logDir should be an absolute path
    public Boolean isLogDirOnline(String logDir) {
        // The logDir should be an absolute path
        boolean match = false;
        for (File dir : logDirs) {
            if (dir.getAbsolutePath().equals(logDir)) {
                match = true;
            }
        }
        if (!match) {
            throw new LogDirNotFoundException("Log dir " + logDir + " is not found in the config.");
        }

        return _liveLogDirs.contains(new File(logDir));
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     */
    private void flushDirtyLogs() {
        LOG.debug("Checking for dirty logs to flush...");

        ConcurrentMap<TopicPartition, UnifiedLog> allLogs = new ConcurrentHashMap<>();
        allLogs.putAll(currentLogs);
        allLogs.putAll(futureLogs);
        for (Map.Entry<TopicPartition, UnifiedLog> entry : allLogs.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            UnifiedLog log = entry.getValue();
            try {
                long timeSinceLastFlush = time.milliseconds() - log.lastFlushTime();
                LOG.debug("Checking if flush is needed on {} flush interval {} last flushed {} time since last flush: {}",
                        topicPartition.topic(), log.config().getFlushMs(), log.lastFlushTime(), timeSinceLastFlush);
                if (timeSinceLastFlush >= log.config().getFlushMs())
                    log.flush(false);
            } catch (Throwable e) {
                LOG.error("Error flushing topic {}", topicPartition.topic(), e);
            }
        }
    }

    private Optional<UnifiedLog> removeLogAndMetrics(Map<TopicPartition, UnifiedLog> logs, TopicPartition tp) {
        UnifiedLog removedLog = logs.remove(tp);
        if (removedLog != null) {
            removedLog.removeLogMetrics();
            return Optional.of(removedLog);
        } else {
            return Optional.empty();
        }
    }

    public List<File> getLogDirs() {
        return logDirs;
    }

    /**
     * Wait all jobs to complete
     *
     * @param jobs     jobs
     * @param callback this will be called to handle the exception caused by each Future#get
     * @return true if all pass. Otherwise, false
     */
    protected static Boolean waitForAllToComplete(List<Future<?>> jobs, Consumer<Throwable> callback) {
        int errorCount = 0;
        for (Future<?> future : jobs) {
            try {
                future.get();
            } catch (Throwable throwable) {
                callback.accept(throwable);
                errorCount++;
            }
        }
        return errorCount == 0;
    }
}
