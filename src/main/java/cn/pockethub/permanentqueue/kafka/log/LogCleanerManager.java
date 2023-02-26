package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.LogCleaningAbortedException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithInterruptedException;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.checkpoints.OffsetCheckpointFile;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import com.yammer.metrics.core.Gauge;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class manages the state of each partition being cleaned.
 * LogCleaningState defines the cleaning states that a TopicPartition can be in.
 * 1. None                    : No cleaning state in a TopicPartition. In this state, it can become LogCleaningInProgress
 * or LogCleaningPaused(1). Valid previous state are LogCleaningInProgress and LogCleaningPaused(1)
 * 2. LogCleaningInProgress   : The cleaning is currently in progress. In this state, it can become None when log cleaning is finished
 * or become LogCleaningAborted. Valid previous state is None.
 * 3. LogCleaningAborted      : The cleaning abort is requested. In this state, it can become LogCleaningPaused(1).
 * Valid previous state is LogCleaningInProgress.
 * 4-a. LogCleaningPaused(1)  : The cleaning is paused once. No log cleaning can be done in this state.
 * In this state, it can become None or LogCleaningPaused(2).
 * Valid previous state is None, LogCleaningAborted or LogCleaningPaused(2).
 * 4-b. LogCleaningPaused(i)  : The cleaning is paused i times where i>= 2. No log cleaning can be done in this state.
 * In this state, it can become LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
 * Valid previous state is LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
 */
public class LogCleanerManager extends KafkaMetricsGroup {

    private static final Logger LOG = LoggerFactory.getLogger(LogCleanerManager.class);

    private List<File> logDirs;
    private Map<TopicPartition, UnifiedLog> logs;
    private LogDirFailureChannel logDirFailureChannel;

    // package-private for testing
    protected String offsetCheckpointFile = "cleaner-offset-checkpoint";

    /* the offset checkpoints holding the last cleaned point for each log */
    private volatile Map<File, OffsetCheckpointFile> checkpoints = new HashMap<>();

    /* the set of logs currently being cleaned */
    private Map<TopicPartition, LogCleaningState> inProgress = new HashMap<>();

    /* the set of uncleanable partitions (partitions that have raised an unexpected error during cleaning)
     *   for each log directory */
    private Map<String, Set<TopicPartition>> uncleanablePartitions = new HashMap<>();

    /* a global lock used to control all access to the in-progress set and the offset checkpoints */
    private ReentrantLock lock = new ReentrantLock();

    /* for coordinating the pausing and the cleaning of a partition */
    private Condition pausedCleaningCond = lock.newCondition();

    /* a gauge for tracking the cleanable ratio of the dirtiest log */
    private volatile double dirtiestLogCleanableRatio = 0.0;

    /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
    private volatile Long timeOfLastRun = Time.SYSTEM.milliseconds();

    public LogCleanerManager(List<File> logDirs,
                             Map<TopicPartition, UnifiedLog> logs,
                             LogDirFailureChannel logDirFailureChannel) throws IOException {
        this.logDirs = logDirs;
        this.logs = logs;
        this.logDirFailureChannel = logDirFailureChannel;

        for (File dir : logDirs) {
            checkpoints.put(dir, new OffsetCheckpointFile(new File(dir, offsetCheckpointFile), logDirFailureChannel));
        }

        /* gauges for tracking the number of partitions marked as uncleanable for each log directory */
        for (File dir : logDirs) {
            newGauge("uncleanable-partitions-count",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return CoreUtils.inLock(lock, () -> {
                                if (uncleanablePartitions.containsKey(dir.getAbsolutePath())) {
                                    return uncleanablePartitions.get(dir.getAbsolutePath()).size();
                                } else {
                                    return 0;
                                }
                            });
                        }
                    },
                    new HashMap<String, String>() {{
                        put("logDirectory", dir.getAbsolutePath());
                    }}
            );
        }

        /* gauges for tracking the number of uncleanable bytes from uncleanable partitions for each log directory */
        for (File dir : logDirs) {
            newGauge("uncleanable-bytes",
                    new Gauge<Long>() {
                        @Override
                        public Long value() {
                            return CoreUtils.inLock(lock,
                                    (Supplier<Long>) () -> {
                                        try {
                                            if (uncleanablePartitions.containsKey(dir.getAbsolutePath())
                                                    && CollectionUtils.isNotEmpty(uncleanablePartitions.get(dir.getAbsolutePath()))) {
                                                Set<TopicPartition> partitions = uncleanablePartitions.get(dir.getAbsolutePath());
                                                Map<TopicPartition, Long> lastClean = allCleanerCheckpoints();
                                                long now = Time.SYSTEM.milliseconds();
                                                long sum = 0;
                                                for (TopicPartition tp : partitions) {
                                                    if (logs.containsKey(tp)) {
                                                        UnifiedLog log = logs.get(tp);
                                                        Optional<Long> lastCleanOffset = Optional.ofNullable(lastClean.get(tp));
                                                        OffsetsToClean offsetsToClean = cleanableOffsets(log, lastCleanOffset, now);
                                                        Pair<Long, Long> pair = calculateCleanableBytes(log, offsetsToClean.getFirstDirtyOffset(), offsetsToClean.getFirstUncleanableDirtyOffset());
                                                        Long uncleanableBytes = pair.getValue();
                                                        sum += uncleanableBytes;
                                                    }
                                                }
                                                return sum;
                                            } else {
                                                return 0L;
                                            }
                                        } catch (Throwable throwable) {
                                            return 0L;
                                        }

                                    });
                        }
                    },
                    new HashMap<String, String>() {{
                        put("logDirectory", dir.getAbsolutePath());
                    }});
        }

        newGauge("max-dirty-percent",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return new Double(100 * dirtiestLogCleanableRatio).intValue();
                    }
                },
                new HashMap<>());

        newGauge("time-since-last-run-ms",
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return Time.SYSTEM.milliseconds() - timeOfLastRun;
                    }
                },
                new HashMap<>());
    }

    /**
     * @return the position processed for all logs.
     */
    public Map<TopicPartition, Long> allCleanerCheckpoints() {
        return CoreUtils.inLock(lock, new Supplier<Map<TopicPartition, Long>>() {
            @Override
            public Map<TopicPartition, Long> get() {
                Map<TopicPartition, Long> map = new HashMap<>();
                for (OffsetCheckpointFile checkpoint : checkpoints.values()) {
                    try {
                        map.putAll(checkpoint.read());
                    } catch (KafkaStorageException e) {
                        LOG.error("Failed to access checkpoint file {} in dir {}",
                                checkpoint.getFile().getName(), checkpoint.getFile().getParentFile().getAbsolutePath(), e);
                    }
                }
                return map;
            }
        });
    }

    /**
     * Package private for unit test. Get the cleaning state of the partition.
     */
    protected Optional<LogCleaningState> cleaningState(TopicPartition tp) {
        return CoreUtils.inLock(lock, new Supplier<Optional<LogCleaningState>>() {
            @Override
            public Optional<LogCleaningState> get() {
                return Optional.ofNullable(inProgress.get(tp));
            }
        });
    }

    /**
     * Package private for unit test. Set the cleaning state of the partition.
     */
    protected void setCleaningState(TopicPartition tp, LogCleaningState state) {
        CoreUtils.inLock(lock, () -> inProgress.put(tp, state));
    }

    public Optional<LogToClean> grabFilthiestCompactedLog(Time time) {
        return grabFilthiestCompactedLog(time, new PreCleanStats());
    }

    /**
     * Choose the log to clean next and add it to the in-progress set. We recompute this
     * each time from the full set of logs to allow logs to be dynamically added to the pool of logs
     * the log manager maintains.
     */
    public Optional<LogToClean> grabFilthiestCompactedLog(Time time, PreCleanStats preCleanStats) {
        return CoreUtils.inLock(lock, new Supplier<Optional<LogToClean>>() {
            @Override
            public Optional<LogToClean> get() {
                long now = time.milliseconds();
                timeOfLastRun = now;
                Map<TopicPartition, Long> lastClean = allCleanerCheckpoints();

                List<LogToClean> dirtyLogs = new ArrayList<>();
                for (Map.Entry<TopicPartition, UnifiedLog> entry : logs.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    UnifiedLog log = entry.getValue();
                    if (!log.config().getCompact()) {
                        continue;
                    }
                    if (!(inProgress.containsKey(topicPartition) || isUncleanablePartition(log, topicPartition))) {
                        continue;
                    }
                    try {
                        // create a LogToClean instance for each
                        Optional<Long> lastCleanOffset = Optional.ofNullable(lastClean.get(topicPartition));
                        OffsetsToClean offsetsToClean = cleanableOffsets(log, lastCleanOffset, now);
                        // update checkpoint for logs with invalid checkpointed offsets
                        if (offsetsToClean.getForceUpdateCheckpoint()) {
                            updateCheckpoints(log.parentDirFile(), Optional.of(Pair.of(topicPartition, offsetsToClean.getFirstDirtyOffset())), Optional.empty());
                        }
                        long compactionDelayMs = maxCompactionDelay(log, offsetsToClean.getFirstDirtyOffset(), now);
                        preCleanStats.updateMaxCompactionDelay(compactionDelayMs);

                        LogToClean logToClean = new LogToClean(topicPartition,
                                log,
                                offsetsToClean.getFirstDirtyOffset(),
                                offsetsToClean.getFirstUncleanableDirtyOffset(),
                                compactionDelayMs > 0);
                        if (logToClean.getTotalBytes() > 0) {
                            // skip any empty logs
                            dirtyLogs.add(logToClean);
                        }
                    } catch (Throwable e) {
                        String msg = String.format("Failed to calculate log cleaning stats for partition %s", topicPartition);
                        throw new LogCleaningException(log, msg, e);
                    }
                }

                dirtiestLogCleanableRatio = CollectionUtils.isNotEmpty(dirtyLogs) ? dirtyLogs.stream().max(LogToClean::compareTo).get().getCleanableRatio() : 0;
                // and must meet the minimum threshold for dirty byte ratio or have some bytes required to be compacted
                List<LogToClean> cleanableLogs = dirtyLogs.stream().filter(ltc -> {
                    return (ltc.getNeedCompactionNow() && ltc.getCleanableBytes() > 0) || ltc.getCleanableRatio() > ltc.getLog().config().getMinCleanableRatio();
                }).collect(Collectors.toList());

                if (CollectionUtils.isEmpty(cleanableLogs)) {
                    return Optional.empty();
                } else {
                    preCleanStats.recordCleanablePartitions(cleanableLogs.size());
                    LogToClean filthiest = cleanableLogs.stream().max(LogToClean::compareTo).get();
                    inProgress.put(filthiest.getTopicPartition(), new LogCleaningState.LogCleaningInProgress());
                    return Optional.of(filthiest);
                }
            }
        });
    }

    /**
     * Pause logs cleaning for logs that do not have compaction enabled
     * and do not have other deletion or compaction in progress.
     * This is to handle potential race between retention and cleaner threads when users
     * switch topic configuration between compacted and non-compacted topic.
     *
     * @return retention logs that have log cleaning successfully paused
     */
    public List<Map.Entry<TopicPartition, UnifiedLog>> pauseCleaningForNonCompactedPartitions() {
        return CoreUtils.inLock(lock, new Supplier<List<Map.Entry<TopicPartition, UnifiedLog>>>() {
            @Override
            public List<Map.Entry<TopicPartition, UnifiedLog>> get() {
                List<Map.Entry<TopicPartition, UnifiedLog>> deletableLogs = logs.entrySet().stream()
                        // pick non-compacted logs
                        .filter(entry -> !entry.getValue().config().getCompact())
                        // skip any logs already in-progress
                        .filter(entry -> !inProgress.containsKey(entry.getKey()))
                        .collect(Collectors.toList());
                for (Map.Entry<TopicPartition, UnifiedLog> entry : deletableLogs) {
                    inProgress.put(entry.getKey(), new LogCleaningState.LogCleaningPaused(1));
                }

                return deletableLogs;
            }
        });
    }

    /**
     * Find any logs that have compaction enabled. Mark them as being cleaned
     * Include logs without delete enabled, as they may have segments
     * that precede the start offset.
     */
    public List<Map.Entry<TopicPartition, UnifiedLog>> deletableLogs() {
        return CoreUtils.inLock(lock, new Supplier<List<Map.Entry<TopicPartition, UnifiedLog>>>() {
            @Override
            public List<Map.Entry<TopicPartition, UnifiedLog>> get() {
                List<Map.Entry<TopicPartition, UnifiedLog>> toClean = logs.entrySet().stream()
                        .filter(entry -> !inProgress.containsKey(entry.getKey())
                                && entry.getValue().config().getCompact()
                                && !isUncleanablePartition(entry.getValue(), entry.getKey()))
                        .collect(Collectors.toList());
                for (Map.Entry<TopicPartition, UnifiedLog> entry : toClean) {
                    inProgress.put(entry.getKey(), new LogCleaningState.LogCleaningInProgress());
                }
                return toClean;
            }
        });
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     * This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
     */
    public void abortCleaning(TopicPartition topicPartition) throws InterruptedException {
        CoreUtils.inLockWithInterruptedException(lock, () -> {
            abortAndPauseCleaning(topicPartition);
            resumeCleaning(Arrays.asList(topicPartition));
            return null;
        });
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     * 1. If the partition is not in progress, mark it as paused.
     * 2. Otherwise, first mark the state of the partition as aborted.
     * 3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
     * throws a LogCleaningAbortedException to stop the cleaning task.
     * 4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
     * 5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
     * 6. If the partition is already paused, a new call to this function
     * will increase the paused count by one.
     */
    public void abortAndPauseCleaning(TopicPartition topicPartition) throws InterruptedException {
        CoreUtils.inLockWithInterruptedException(lock, new SupplierWithInterruptedException<Void>() {
            @Override
            public Void get() throws InterruptedException {
                LogCleaningState logCleaningState = inProgress.get(topicPartition);
                if (Objects.isNull(logCleaningState)) {
                    inProgress.put(topicPartition, new LogCleaningState.LogCleaningPaused(1));
                } else if (logCleaningState instanceof LogCleaningState.LogCleaningInProgress) {
                    inProgress.put(topicPartition, new LogCleaningState.LogCleaningAborted());
                } else if (logCleaningState instanceof LogCleaningState.LogCleaningPaused) {
                    inProgress.put(topicPartition, new LogCleaningState.LogCleaningPaused(((LogCleaningState.LogCleaningPaused) logCleaningState).getPausedCount() + 1));
                } else {
                    String msg = String.format("Compaction for partition %s cannot be aborted and paused since it is in %s state.", topicPartition, logCleaningState);
                    throw new IllegalStateException(msg);
                }
                while (!isCleaningInStatePaused(topicPartition)) {
                    pausedCleaningCond.await(100, TimeUnit.MILLISECONDS);
                }
                return null;
            }
        });
    }

    /**
     * Resume the cleaning of paused partitions.
     * Each call of this function will undo one pause.
     */
    public void resumeCleaning(List<TopicPartition> topicPartitions) {
        CoreUtils.inLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                for (TopicPartition topicPartition : topicPartitions) {
                    LogCleaningState state = inProgress.get(topicPartition);
                    if (Objects.isNull(state)) {
                        String msg = String.format("Compaction for partition topicPartition cannot be resumed since it is not paused.", topicPartition);
                        throw new IllegalStateException(msg);
                    } else {
                        if (state instanceof LogCleaningState.LogCleaningPaused) {
                            Integer count = ((LogCleaningState.LogCleaningPaused) state).getPausedCount();
                            if (count == 1) {
                                inProgress.remove(topicPartition);
                            } else if (count > 1) {
                                inProgress.put(topicPartition, new LogCleaningState.LogCleaningPaused(count - 1));
                            }
                        } else {
                            String msg = String.format("Compaction for partition %s cannot be resumed since it is in %s state.", topicPartition, state);
                            throw new IllegalStateException(msg);
                        }
                    }
                }

                return null;
            }
        });
    }

    /**
     * Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
     */
    private Boolean isCleaningInState(TopicPartition topicPartition, LogCleaningState expectedState) {
        LogCleaningState state = inProgress.get(topicPartition);
        if (Objects.isNull(state)) {
            return false;
        } else {
            return state == expectedState;
        }
    }

    /**
     * Check if the cleaning for a partition is paused. The caller is expected to hold lock while making the call.
     */
    private Boolean isCleaningInStatePaused(TopicPartition topicPartition) {
        LogCleaningState state = inProgress.get(topicPartition);
        if (Objects.isNull(state)) {
            return false;
        } else {
            return state instanceof LogCleaningState.LogCleaningPaused;
        }
    }

    /**
     * Check if the cleaning for a partition is aborted. If so, throw an exception.
     */
    public void checkCleaningAborted(TopicPartition topicPartition) throws LogCleaningAbortedException {
        lock.lock();
        try {
            if (isCleaningInState(topicPartition, new LogCleaningState.LogCleaningAborted())) {
                throw new LogCleaningAbortedException();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Update checkpoint file, adding or removing partitions if necessary.
     *
     * @param dataDir                The File object to be updated
     * @param partitionToUpdateOrAdd The [TopicPartition, Long] map data to be updated. pass "none" if doing remove, not add
     * @param partitionToRemove      The TopicPartition to be removed
     */
    public void updateCheckpoints(File dataDir,
                                  Optional<Pair<TopicPartition, Long>> partitionToUpdateOrAdd,
                                  Optional<TopicPartition> partitionToRemove) throws IOException {
        CoreUtils.inLockWithIOException(lock, new SupplierWithIOException<Void>() {
            @Override
            public Void get() throws IOException {
                OffsetCheckpointFile checkpoint = checkpoints.get(dataDir);
                if (Objects.nonNull(checkpoint)) {
                    try {
                        Map<TopicPartition, Long> currentCheckpoint = new HashMap<>();
                        for (Map.Entry<TopicPartition, Long> entry : checkpoint.read().entrySet()) {
                            TopicPartition tp = entry.getKey();
                            if (logs.containsKey(tp)) {
                                currentCheckpoint.put(entry.getKey(), entry.getValue());
                            }
                        }
                        // remove the partition offset if any
                        Map<TopicPartition, Long> updatedCheckpoint = new HashMap<>(currentCheckpoint);
                        if (partitionToRemove.isPresent()) {
                            TopicPartition topicPartition = partitionToRemove.get();
                            updatedCheckpoint.remove(topicPartition);
                        }
                        // update or add the partition offset if any
                        if (partitionToUpdateOrAdd.isPresent()) {
                            Pair<TopicPartition, Long> updatedOffset = partitionToUpdateOrAdd.get();
                            updatedCheckpoint.put(updatedOffset.getKey(), updatedOffset.getValue());
                        }

                        checkpoint.write(updatedCheckpoint);
                    } catch (KafkaStorageException e) {
                        LOG.error("Failed to access checkpoint file {} in dir {}",
                                checkpoint.getFile().getName(), checkpoint.getFile().getParentFile().getAbsolutePath(),e);
                    }
                }
                return null;
            }
        });
    }

    /**
     * alter the checkpoint directory for the topicPartition, to remove the data in sourceLogDir, and add the data in destLogDir
     */
    public void alterCheckpointDir(TopicPartition topicPartition, File sourceLogDir, File destLogDir) throws IOException {
        CoreUtils.inLockWithIOException(lock, new SupplierWithIOException<Void>() {
            @Override
            public Void get() throws IOException {
                try {
                    OffsetCheckpointFile checkpointFile = checkpoints.get(sourceLogDir);
                    if (Objects.nonNull(checkpointFile)) {
                        Long offset = checkpointFile.read().get(topicPartition);
                        if (Objects.nonNull(offset)) {
                            LOG.debug("Removing the partition offset data in checkpoint file for '{}' from {} directory.",
                                    topicPartition, sourceLogDir.getAbsoluteFile());
                            updateCheckpoints(sourceLogDir, Optional.empty(), Optional.of(topicPartition));
                            LOG.debug("Adding the partition offset data in checkpoint file for '{}' to {} directory.",
                                    topicPartition, destLogDir.getAbsoluteFile());
                            updateCheckpoints(destLogDir, Optional.of(Pair.of(topicPartition, offset)), Optional.empty());
                        }
                    }
                } catch (KafkaStorageException e) {
                    LOG.error("Failed to access checkpoint file in dir {}", sourceLogDir.getAbsolutePath(),e);
                }

                Set<TopicPartition> logUncleanablePartitions = uncleanablePartitions.getOrDefault(sourceLogDir.toString(), new HashSet<>());
                if (logUncleanablePartitions.contains(topicPartition)) {
                    logUncleanablePartitions.remove(topicPartition);
                    markPartitionUncleanable(destLogDir.toString(), topicPartition);
                }
                return null;
            }
        });
    }

    /**
     * Stop cleaning logs in the provided directory
     *
     * @param dir the absolute path of the log dir
     */
    public void handleLogDirFailure(String dir) {
        LOG.warn("Stopping cleaning logs in dir {}", dir);
        CoreUtils.inLock(lock, () -> {
            Map<File, OffsetCheckpointFile> newCheckpoints = new HashMap<>();
            for (Map.Entry<File, OffsetCheckpointFile> entry : checkpoints.entrySet()) {
                if (!entry.getKey().getAbsolutePath().equals(dir)) {
                    newCheckpoints.put(entry.getKey(), entry.getValue());
                }
            }
            checkpoints = newCheckpoints;
            return null;
        });
    }

    /**
     * Truncate the checkpointed offset for the given partition if its checkpointed offset is larger than the given offset
     */
    public void maybeTruncateCheckpoint(File dataDir, TopicPartition topicPartition, Long offset) throws IOException {
        CoreUtils.inLockWithIOException(lock, () -> {
            if (logs.get(topicPartition).config().getCompact()) {
                OffsetCheckpointFile checkpoint = checkpoints.get(dataDir);
                if (checkpoint != null) {
                    Map<TopicPartition, Long> existing = checkpoint.read();
                    if (existing.getOrDefault(topicPartition, 0L) > offset) {
                        Map<TopicPartition, Long> newCheckpoints = new HashMap<>(existing);
                        newCheckpoints.put(topicPartition, offset);
                        checkpoint.write(newCheckpoints);
                    }
                }
            }
            return null;
        });
    }

    /**
     * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
     */
    public void doneCleaning(TopicPartition topicPartition, File dataDir, Long endOffset) throws IOException {
        CoreUtils.inLockWithIOException(lock, () -> {
            LogCleaningState state = inProgress.get(topicPartition);
            if (Objects.isNull(state)) {
                throw new IllegalStateException(String.format("State for partition %s should exist.", topicPartition));
            } else if (state instanceof LogCleaningState.LogCleaningInProgress) {
                updateCheckpoints(dataDir, Optional.of(Pair.of(topicPartition, endOffset)), Optional.empty());
                inProgress.remove(topicPartition);
            } else if (state instanceof LogCleaningState.LogCleaningAborted) {
                inProgress.put(topicPartition, new LogCleaningState.LogCleaningPaused(1));
                pausedCleaningCond.signalAll();
            } else {
                throw new IllegalStateException(String.format("In-progress partition %s cannot be in %s state.", topicPartition, state));
            }
            return null;
        });
    }

    public void doneDeleting(Iterable<TopicPartition> topicPartitions) {
        CoreUtils.inLock(lock, () -> {
            for (TopicPartition topicPartition : topicPartitions) {
                LogCleaningState state = inProgress.get(topicPartition);
                if (Objects.isNull(state)) {
                    throw new IllegalStateException(String.format("State for partition %s should exist.", topicPartition));
                } else if (state instanceof LogCleaningState.LogCleaningInProgress) {
                    inProgress.remove(topicPartition);
                } else if (state instanceof LogCleaningState.LogCleaningAborted) {
                    inProgress.put(topicPartition, new LogCleaningState.LogCleaningPaused(1));
                    pausedCleaningCond.signalAll();
                } else {
                    throw new IllegalStateException(String.format("In-progress partition %s cannot be in %s state.", topicPartition, state));
                }
            }
            return null;
        });
    }

    /**
     * Returns an immutable set of the uncleanable partitions for a given log directory
     * Only used for testing
     */
    protected Set<TopicPartition> uncleanablePartitions(String logDir) {
        Set<TopicPartition> partitions = new HashSet<>();
        CoreUtils.inLock(lock, () -> {
            partitions.addAll(uncleanablePartitions.getOrDefault(logDir, partitions));
            return null;
        });
        return partitions;
    }

    public void markPartitionUncleanable(String logDir, TopicPartition partition) {
        CoreUtils.inLock(lock, () -> {
            Set<TopicPartition> partitions = uncleanablePartitions.get(logDir);
            if (Objects.nonNull(partitions)) {
                partitions.add(partition);
            } else {
                uncleanablePartitions.put(logDir, new HashSet<>(Arrays.asList(partition)));
            }
            return null;
        });
    }

    private Boolean isUncleanablePartition(UnifiedLog log, TopicPartition topicPartition) {
        return CoreUtils.inLock(lock, () -> uncleanablePartitions.containsKey(log.parentDir())
                && CollectionUtils.isNotEmpty(uncleanablePartitions.get(log.parentDir()))
                && uncleanablePartitions.get(log.parentDir()).contains(topicPartition)
        );
    }

    public void maintainUncleanablePartitions() {
        // Remove deleted partitions from uncleanablePartitions
        CoreUtils.inLock(lock, () -> {
            // Note: we don't use retain or filterInPlace method in this function because retain is deprecated in
            // scala 2.13 while filterInPlace is not available in scala 2.12.

            // Remove deleted partitions
            for (Set<TopicPartition> partitions : uncleanablePartitions.values()) {
                List<TopicPartition> partitionsToRemove = partitions.stream()
                        .filter(partition -> !logs.containsKey(partition))
                        .collect(Collectors.toList());
                for (TopicPartition partition : partitionsToRemove) {
                    partitions.remove(partition);
                }
            }

            // Remove entries with empty partition set.
            Set<String> logDirsToRemove = new HashSet<>();
            for (Map.Entry<String, Set<TopicPartition>> entry : uncleanablePartitions.entrySet()) {
                if (CollectionUtils.isEmpty(entry.getValue())) {
                    logDirsToRemove.add(entry.getKey());
                }
            }
            for (String dir : logDirsToRemove) {
                uncleanablePartitions.remove(dir);
            }
            return null;
        });
    }


    public static Boolean isCompactAndDelete(UnifiedLog log) {
        return log.config().getCompact() && log.config().getDelete();
    }

    /**
     * get max delay between the time when log is required to be compacted as determined
     * by maxCompactionLagMs and the current time.
     */
    public static Long maxCompactionDelay(UnifiedLog log, Long firstDirtyOffset, Long now) {
        Iterable<LogSegment> dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset);
        List<Long> firstBatchTimestamps = new ArrayList<>();
        for (Long timestamp : log.getFirstBatchTimestampForSegments(dirtyNonActiveSegments)) {
            if (timestamp > 0) {
                firstBatchTimestamps.add(timestamp);
            }
        }

        long earliestDirtySegmentTimestamp;
        if (CollectionUtils.isNotEmpty(firstBatchTimestamps)) {
            earliestDirtySegmentTimestamp = firstBatchTimestamps.stream().min(Long::compareTo).get();
        } else {
            earliestDirtySegmentTimestamp = Long.MAX_VALUE;
        }

        long maxCompactionLagMs = Math.max(log.config().getMaxCompactionLagMs(), 0L);
        long cleanUntilTime = now - maxCompactionLagMs;

        if (earliestDirtySegmentTimestamp < cleanUntilTime) {
            return cleanUntilTime - earliestDirtySegmentTimestamp;
        } else {
            return 0L;
        }
    }

    /**
     * Returns the range of dirty offsets that can be cleaned.
     *
     * @param log             the log
     * @param lastCleanOffset the last checkpointed offset
     * @param now             the current time in milliseconds of the cleaning operation
     * @return OffsetsToClean containing offsets for cleanable portion of log and whether the log checkpoint needs updating
     */
    public static OffsetsToClean cleanableOffsets(UnifiedLog log, Optional<Long> lastCleanOffset, Long now) throws IOException {
        // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
        // reset to the log starting offset and log the error
        long firstDirtyOffset;
        boolean forceUpdateCheckpoint;

        long logStartOffset = log.getLogStartOffset();
        long checkpointDirtyOffset = lastCleanOffset.orElse(logStartOffset);

        if (checkpointDirtyOffset < logStartOffset) {
            // Don't bother with the warning if compact and delete are enabled.
            if (!isCompactAndDelete(log)) {
                LOG.warn("Resetting first dirty offset of {} to log start offset {} since the checkpointed offset {} is invalid.",
                        log.name(), logStartOffset, checkpointDirtyOffset);
            }
            firstDirtyOffset = logStartOffset;
            forceUpdateCheckpoint = true;
        } else if (checkpointDirtyOffset > log.logEndOffset()) {
            // The dirty offset has gotten ahead of the log end offset. This could happen if there was data
            // corruption at the end of the log. We conservatively assume that the full log needs cleaning.
            LOG.warn("The last checkpoint dirty offset for partition {} is {}, " +
                            "which is larger than the log end offset {}. Resetting to the log start offset {}.",
                    log.name(), checkpointDirtyOffset, log.logEndOffset(), logStartOffset);
            firstDirtyOffset = logStartOffset;
            forceUpdateCheckpoint = true;
        } else {
            firstDirtyOffset = checkpointDirtyOffset;
            forceUpdateCheckpoint = false;
        }

        long minCompactionLagMs = Math.max(log.config().getCompactionLagMs(), 0L);

        // Find the first segment that cannot be cleaned. We cannot clean past:
        // 1. The active segment
        // 2. The last stable offset (including the high watermark)
        // 3. Any segments closer to the head of the log than the minimum compaction lag time
        ArrayList<Optional<Long>> optionals = new ArrayList<>();
        optionals.add(Optional.of(log.lastStableOffset()));
        optionals.add(Optional.ofNullable(log.activeSegment().getBaseOffset()));
        if (minCompactionLagMs > 0) {
            // dirty log segments
            Iterable<LogSegment> dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset);
            for (LogSegment s : dirtyNonActiveSegments) {
                boolean isUncleanable = s.largestTimestamp() > now - minCompactionLagMs;
                LOG.debug("Checking if log segment may be cleaned: log='{}' segment.baseOffset={} " +
                                "segment.largestTimestamp={}; now - compactionLag={}; is uncleanable={}",
                        log.name(), s.getBaseOffset(), s.largestTimestamp(), now - minCompactionLagMs, isUncleanable);
                if (isUncleanable) {
                    optionals.add(Optional.of(s.getBaseOffset()));
                    break;
                }
            }
        } else {
            optionals.add(Optional.empty());
        }
        Long firstUncleanableDirtyOffset = optionals.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .min(Long::compareTo)
                .get();

        LOG.debug("Finding range of cleanable offsets for log={}. Last clean offset={} " +
                        "now=$now => firstDirtyOffset={} firstUncleanableOffset={} activeSegment.baseOffset={}",
                log.name(), lastCleanOffset, firstDirtyOffset, firstUncleanableDirtyOffset, log.activeSegment().getBaseOffset());

        return new OffsetsToClean(firstDirtyOffset, Math.max(firstDirtyOffset, firstUncleanableDirtyOffset), forceUpdateCheckpoint);
    }

    /**
     * Given the first dirty offset and an uncleanable offset, calculates the total cleanable bytes for this log
     *
     * @return the biggest uncleanable offset and the total amount of cleanable bytes
     */
    public static Pair<Long, Long> calculateCleanableBytes(UnifiedLog log, Long firstDirtyOffset, Long uncleanableOffset) {
        Iterable<LogSegment> logSegments = log.nonActiveLogSegmentsFrom(uncleanableOffset);
        Iterator<LogSegment> iterator = logSegments.iterator();
        LogSegment firstUncleanableSegment;
        if (iterator.hasNext()) {
            firstUncleanableSegment = iterator.next();
        } else {
            firstUncleanableSegment = log.activeSegment();
        }
        long firstUncleanableOffset = firstUncleanableSegment.getBaseOffset();
        long cleanableBytes = 0;
        for (LogSegment seg : log.logSegments(Math.min(firstDirtyOffset, firstUncleanableOffset), firstUncleanableOffset)) {
            cleanableBytes += seg.size();
        }
        return Pair.of(firstUncleanableOffset, cleanableBytes);
    }
}
