package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.TestUtils;
import cn.pockethub.permanentqueue.kafka.log.LogCleaningState.*;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.epoch.LeaderEpochFileCache;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cn.pockethub.permanentqueue.kafka.log.LogStartOffsetIncrementReason.clientRecordDeletion;
import static org.junit.jupiter.api.Assertions.*;

public class LogCleanerManagerTest {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleanerManagerTest.class);

    private File tmpDir = TestUtils.tempDir();
    private File tmpDir2 = TestUtils.tempDir();
    private File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private File logDir2 = TestUtils.randomPartitionLogDir(tmpDir);
    private TopicPartition topicPartition = new TopicPartition("log", 0);
    private TopicPartition topicPartition2 = new TopicPartition("log2", 0);
    private Properties logProps = new Properties();
    private LogConfig logConfig;
    // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
    private MockTime time = new MockTime(1400000000000L, 1000L);
    private long offset = 999L;
    private Map<TopicPartition, Long> cleanerCheckpoints = new HashMap<>();

    @BeforeEach
    public void setUp() throws Exception {
        logProps.put(LogConfig.SegmentBytesProp, 1024);
        logProps.put(LogConfig.SegmentIndexBytesProp, 1024);
        logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact);

        this.logConfig = new LogConfig(logProps);
    }

    class LogCleanerManagerMock extends LogCleanerManager {

        public LogCleanerManagerMock(List<File> logDirs,
                                     Map<TopicPartition, UnifiedLog> logs,
                                     LogDirFailureChannel logDirFailureChannel) throws IOException {
            super(logDirs, logs, logDirFailureChannel);
        }

        @Override
        public Map<TopicPartition, Long> allCleanerCheckpoints() {
            return cleanerCheckpoints;
        }

        @Override
        public void updateCheckpoints(File dataDir,
                                      Optional<Pair<TopicPartition, Long>> partitionToUpdateOrAdd,
                                      Optional<TopicPartition> partitionToRemove) {
            assert !partitionToRemove.isPresent() : "partitionToRemove argument with value not yet handled";
            if (partitionToUpdateOrAdd.isPresent()) {
                TopicPartition tp = partitionToUpdateOrAdd.get().getKey();
                Long offset = partitionToUpdateOrAdd.get().getValue();
                cleanerCheckpoints.put(tp, offset);
            } else {
                throw new IllegalArgumentException("partitionToUpdateOrAdd==None argument not yet handled");
            }
        }
    }


    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(tmpDir);
    }

    private Map<TopicPartition, UnifiedLog> setupIncreasinglyFilthyLogs(List<TopicPartition> partitions,
                                                                        Integer startNumBatches,
                                                                        Integer batchIncrement) throws IOException {
        Map<TopicPartition, UnifiedLog> logs = new HashMap<>();
        int numBatches = startNumBatches;

        for (TopicPartition tp : partitions) {
            UnifiedLog log = createLog(2048, LogConfig.Compact, topicPartition = tp);
            logs.put(tp, log);

            writeRecords(log, numBatches, 1, 5);
            numBatches += batchIncrement;
        }
        return logs;
    }

//    @Test
    public void testGrabFilthiestCompactedLogThrowsException() throws IOException {
        TopicPartition tp = new TopicPartition("A", 1);
        int logSegmentSize = TestUtils.singletonRecords("test".getBytes()).sizeInBytes() * 10;
        int logSegmentsCount = 2;
        File tpDir = new File(logDir, "A-1");
        Files.createDirectories(tpDir.toPath());
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
        LogConfig config = createLowRetentionLogConfig(logSegmentSize, LogConfig.Compact);
        int maxTransactionTimeoutMs = 5 * 60 * 1000;
        int maxProducerIdExpirationMs = 60 * 60 * 1000;
        LogSegments segments = new LogSegments(tp);
        Optional<LeaderEpochFileCache> leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(tpDir, topicPartition, logDirFailureChannel, config.recordVersion(), "");
        ProducerStateManager producerStateManager = new ProducerStateManager(topicPartition, tpDir, maxTransactionTimeoutMs, maxProducerIdExpirationMs, time);
        LogLoader.LoadedLogOffsets offsets = new LogLoader(
                tpDir,
                tp,
                config,
                time.getScheduler(),
                time,
                logDirFailureChannel,
                true,
                segments,
                0L,
                0L,
                leaderEpochCache,
                producerStateManager,
                new ConcurrentHashMap<>()
        ).load();
        LocalLog localLog = new LocalLog(tpDir, config, segments, offsets.getRecoveryPoint(),
                offsets.getNextOffsetMetadata(), time.getScheduler(), time, tp, logDirFailureChannel);


        UnifiedLog log = new LogMock(offsets.getLogStartOffset(), localLog, leaderEpochCache, producerStateManager);
        writeRecords(log = log, logSegmentsCount * 2, 10, 2);

        Map<TopicPartition, UnifiedLog> logsPool = new HashMap<>();
        logsPool.put(tp, log);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logsPool);
        cleanerCheckpoints.put(tp, 1L);

        LogCleaningException thrownException = assertThrows(LogCleaningException.class, () -> cleanerManager.grabFilthiestCompactedLog(time).get());
        assertEquals(log, thrownException.getLog());
        assertTrue(thrownException.getCause() instanceof IllegalStateException);
    }

    // the exception should be caught and the partition that caused it marked as uncleanable
    class LogMock extends UnifiedLog {

        public LogMock(Long logStartOffset,
                       LocalLog localLog,
                       Optional<LeaderEpochFileCache> leaderEpochCache,
                       ProducerStateManager producerStateManager) throws OffsetOutOfRangeException {
            super(logStartOffset,
                    localLog,
                    new BrokerTopicStats(),
                    LogManager.ProducerIdExpirationCheckIntervalMs,
                    leaderEpochCache,
                    producerStateManager,
                    Optional.empty(),
                    true);
        }

        // Throw an error in getFirstBatchTimestampForSegments since it is called in grabFilthiestLog()
        @Override
        public List<Long> getFirstBatchTimestampForSegments(Iterable<LogSegment> segments) {
            throw new IllegalStateException("Error!");
        }
    }

//    @Test
    public void testGrabFilthiestCompactedLogReturnsLogWithDirtiestRatio() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = Arrays.asList(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions, 20, 5);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerCheckpoints.put(partition, 20L));

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get();
        assertEquals(tp2, filthiestLog.getTopicPartition());
        assertEquals(tp2, filthiestLog.getLog().topicPartition());
    }

//    @Test
    public void testGrabFilthiestCompactedLogIgnoresUncleanablePartitions() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = Arrays.asList(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions, 20, 5);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerCheckpoints.put(partition, 20L));

        cleanerManager.markPartitionUncleanable(logs.get(tp2).dir().getParent(), tp2);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get();
        assertEquals(tp1, filthiestLog.getTopicPartition());
        assertEquals(tp1, filthiestLog.getLog().topicPartition());
    }

//    @Test
    public void testGrabFilthiestCompactedLogIgnoresInProgressPartitions() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = Arrays.asList(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions, 20, 5);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerCheckpoints.put(partition, 20L));

        cleanerManager.setCleaningState(tp2, new LogCleaningInProgress());

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get();
        assertEquals(tp1, filthiestLog.getTopicPartition());
        assertEquals(tp1, filthiestLog.getLog().topicPartition());
    }

//    @Test
    public void testGrabFilthiestCompactedLogIgnoresBothInProgressPartitionsAndUncleanablePartitions() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = Arrays.asList(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions, 20, 5);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerCheckpoints.put(partition, 20L));

        cleanerManager.setCleaningState(tp2, new LogCleaningInProgress());
        cleanerManager.markPartitionUncleanable(logs.get(tp1).dir().getParent(), tp1);

        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(time);
        assertEquals(Optional.empty(), filthiestLog);
    }

//    @Test
    public void testDirtyOffsetResetIfLargerThanEndOffset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(Arrays.asList(tp), 20, 5);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerCheckpoints.put(tp, 200L);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get();
        assertEquals(0L, filthiestLog.getFirstDirtyOffset());
    }

//    @Test
    public void testDirtyOffsetResetIfSmallerThanStartOffset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(Arrays.asList(tp), 20, 5);

        logs.get(tp).maybeIncrementLogStartOffset(10L, clientRecordDeletion);

        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerCheckpoints.put(tp, 0L);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get();
        assertEquals(10L, filthiestLog.getFirstDirtyOffset());
    }

    @Test
    public void testLogStartOffsetLargerThanActiveSegmentBaseOffset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = createLog(2048, LogConfig.Compact, tp);

        Map<TopicPartition, UnifiedLog> logs = new HashMap<>();
        logs.put(tp, log);

        appendRecords(log, 3);
        appendRecords(log, 3);
        appendRecords(log, 3);

        assertEquals(1, log.logSegments().size());

        log.maybeIncrementLogStartOffset(2L, clientRecordDeletion);

        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerCheckpoints.put(tp, 0L);

        // The active segment is uncleanable and hence not filthy from the POV of the CleanerManager.
        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(time);
        assertEquals(Optional.empty(), filthiestLog);
    }

    @Test
    public void testDirtyOffsetLargerThanActiveSegmentBaseOffset() throws IOException {
        // It is possible in the case of an unclean leader election for the checkpoint
        // dirty offset to get ahead of the active segment base offset, but still be
        // within the range of the log.

        TopicPartition tp = new TopicPartition("foo", 0);

        Map<TopicPartition, UnifiedLog> logs = new HashMap<>();
        UnifiedLog log = createLog(2048, LogConfig.Compact, topicPartition = tp);
        logs.put(tp, log);

        appendRecords(log, 3);
        appendRecords(log, 3);

        assertEquals(1, log.logSegments().size());
        assertEquals(0L, log.activeSegment().getBaseOffset());

        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerCheckpoints.put(tp, 3L);

        // These segments are uncleanable and hence not filthy
        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(time);
        assertEquals(Optional.empty(), filthiestLog);
    }

    /**
     * When checking for logs with segments ready for deletion
     * we shouldn't consider logs where cleanup.policy=delete
     * as they are handled by the LogManager
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyDeleteLogs() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Delete);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(0, readyToDelete, "should have 0 logs ready to be deleted");
    }

    /**
     * We should find logs with segments ready to be deleted when cleanup.policy=compact,delete
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactDeleteLogs() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact + "," + LogConfig.Delete);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(1, readyToDelete, "should have 1 logs ready to be deleted");
    }

    /**
     * When looking for logs with segments ready to be deleted we should consider
     * logs with cleanup.policy=compact because they may have segments from before the log start offset
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactLogs() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(1, readyToDelete, "should have 1 logs ready to be deleted");
    }

    /**
     * log under cleanup should be ineligible for compaction
     */
//    @Test
    public void testLogsUnderCleanupIneligibleForCompaction() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Delete);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        log.appendAsLeader(records, 0);
        log.roll();
        log.appendAsLeader(records, 0);
        log.updateHighWatermark(2L);

        // simulate cleanup thread working on the log partition
        List<Map.Entry<TopicPartition, UnifiedLog>> deletableLog = cleanerManager.pauseCleaningForNonCompactedPartitions();
        assertEquals(1, deletableLog.size(), "should have 1 logs ready to be deleted");

        // change cleanup policy from delete to compact
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, log.config().getSegmentSize());
        logProps.put(LogConfig.RetentionMsProp, log.config().getRetentionMs());
        logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact);
        logProps.put(LogConfig.MinCleanableDirtyRatioProp, 0);
        LogConfig config = new LogConfig(logProps);
        log.updateConfig(config);

        // log cleanup inprogress, the log is not available for compaction
        Optional<LogToClean> cleanable = cleanerManager.grabFilthiestCompactedLog(time);
        assertFalse(cleanable.isPresent(), "should have 0 logs ready to be compacted");

        // log cleanup finished, and log can be picked up for compaction
        cleanerManager.resumeCleaning(deletableLog.stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        Optional<LogToClean> cleanable2 = cleanerManager.grabFilthiestCompactedLog(time);
        assertTrue(cleanable2.isPresent(), "should have 1 logs ready to be compacted");

        // update cleanup policy to delete
        logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Delete);
        LogConfig config2 = new LogConfig(logProps);
        log.updateConfig(config2);

        // compaction in progress, should have 0 log eligible for log cleanup
        List<Map.Entry<TopicPartition, UnifiedLog>> deletableLog2 = cleanerManager.pauseCleaningForNonCompactedPartitions();
        assertEquals(0, deletableLog2.size(), "should have 0 logs ready to be deleted");

        // compaction done, should have 1 log eligible for log cleanup
        cleanerManager.doneDeleting(Arrays.asList(cleanable2.get().getTopicPartition()));
        List<Map.Entry<TopicPartition, UnifiedLog>> deletableLog3 = cleanerManager.pauseCleaningForNonCompactedPartitions();
        assertEquals(1, deletableLog3.size(), "should have 1 logs ready to be deleted");
    }

    @Test
    public void testUpdateCheckpointsShouldAddOffsetToPartition() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // expect the checkpoint offset is not the expectedOffset before doing updateCheckpoints
        assertNotEquals(offset, cleanerManager.allCleanerCheckpoints().getOrDefault(topicPartition, 0L));

        cleanerManager.updateCheckpoints(logDir, Optional.of(Pair.of(topicPartition, offset)), Optional.empty());
        // expect the checkpoint offset is now updated to the expected offset after doing updateCheckpoints
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));
    }

    @Test
    public void testUpdateCheckpointsShouldRemovePartitionData() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // write some data into the cleaner-offset-checkpoint file
        cleanerManager.updateCheckpoints(logDir, Optional.of(Pair.of(topicPartition, offset)), Optional.empty());
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));

        // updateCheckpoints should remove the topicPartition data in the logDir
        cleanerManager.updateCheckpoints(logDir, Optional.empty(), Optional.of(topicPartition));
        assertFalse(cleanerManager.allCleanerCheckpoints().containsKey(topicPartition));
    }

    @Test
    public void testHandleLogDirFailureShouldRemoveDirAndData() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // write some data into the cleaner-offset-checkpoint file in logDir and logDir2
        cleanerManager.updateCheckpoints(logDir, Optional.of(Pair.of(topicPartition, offset)), Optional.empty());
        cleanerManager.updateCheckpoints(logDir2, Optional.of(Pair.of(topicPartition2, offset)), Optional.empty());
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition2));

        cleanerManager.handleLogDirFailure(logDir.getAbsolutePath());
        // verify the partition data in logDir is gone, and data in logDir2 is still there
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition2));
        assertFalse(cleanerManager.allCleanerCheckpoints().containsKey(topicPartition));
    }

    @Test
    public void testMaybeTruncateCheckpointShouldTruncateData() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);
        long lowerOffset = 1L;
        long higherOffset = 1000L;

        // write some data into the cleaner-offset-checkpoint file in logDir
        cleanerManager.updateCheckpoints(logDir, Optional.of(Pair.of(topicPartition, offset)), Optional.empty());
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));

        // we should not truncate the checkpoint data for checkpointed offset <= the given offset (higherOffset)
        cleanerManager.maybeTruncateCheckpoint(logDir, topicPartition, higherOffset);
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));
        // we should truncate the checkpoint data for checkpointed offset > the given offset (lowerOffset)
        cleanerManager.maybeTruncateCheckpoint(logDir, topicPartition, lowerOffset);
        assertEquals(lowerOffset, cleanerManager.allCleanerCheckpoints().get(topicPartition));
    }

    @Test
    public void testAlterCheckpointDirShouldRemoveDataInSrcDirAndAddInNewDir() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // write some data into the cleaner-offset-checkpoint file in logDir
        cleanerManager.updateCheckpoints(logDir, Optional.of(Pair.of(topicPartition, offset)), Optional.empty());
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));

        cleanerManager.alterCheckpointDir(topicPartition, logDir, logDir2);
        // verify we still can get the partition offset after alterCheckpointDir
        // This data should locate in logDir2, not logDir
        assertEquals(offset, cleanerManager.allCleanerCheckpoints().get(topicPartition));

        // force delete the logDir2 from checkpoints, so that the partition data should also be deleted
        cleanerManager.handleLogDirFailure(logDir2.getAbsolutePath());
        assertFalse(cleanerManager.allCleanerCheckpoints().containsKey(topicPartition));
    }

    /**
     * log under cleanup should still be eligible for log truncation
     */
    @Test
    public void testConcurrentLogCleanupAndLogTruncation() throws IOException, InterruptedException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Delete);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // log cleanup starts
        List<Map.Entry<TopicPartition, UnifiedLog>> pausedPartitions = cleanerManager.pauseCleaningForNonCompactedPartitions();
        // Log truncation happens due to unclean leader election
        cleanerManager.abortAndPauseCleaning(log.topicPartition());
        cleanerManager.resumeCleaning(Arrays.asList(log.topicPartition()));
        // log cleanup finishes and pausedPartitions are resumed
        cleanerManager.resumeCleaning(pausedPartitions.stream().map(Map.Entry::getKey).collect(Collectors.toList()));

        assertEquals(Optional.empty(), cleanerManager.cleaningState(log.topicPartition()));
    }

    /**
     * log under cleanup should still be eligible for topic deletion
     */
    @Test
    public void testConcurrentLogCleanupAndTopicDeletion() throws IOException, InterruptedException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Delete);
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // log cleanup starts
        List<Map.Entry<TopicPartition, UnifiedLog>> pausedPartitions = cleanerManager.pauseCleaningForNonCompactedPartitions();
        // Broker processes StopReplicaRequest with delete=true
        cleanerManager.abortCleaning(log.topicPartition());
        // log cleanup finishes and pausedPartitions are resumed
        cleanerManager.resumeCleaning(pausedPartitions.stream().map(Map.Entry::getKey).collect(Collectors.toList()));

        assertEquals(Optional.empty(), cleanerManager.cleaningState(log.topicPartition()));
    }

    /**
     * When looking for logs with segments ready to be deleted we shouldn't consider
     * logs that have had their partition marked as uncleanable.
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldNotConsiderUncleanablePartitions() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact);
        LogCleanerManager cleanerManager = createCleanerManager(log);
        cleanerManager.markPartitionUncleanable(log.dir().getParent(), topicPartition);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(0, readyToDelete, "should have 0 logs ready to be deleted");
    }

    /**
     * Test computation of cleanable range with no minimum compaction lag settings active where bounded by LSO
     */
    @Test
    public void testCleanableOffsetsForNone() throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        while (log.numberOfSegments() < 8) {
            log.appendAsLeader(records(log.logEndOffset().intValue(), log.logEndOffset().intValue(), time.milliseconds()), 0);
        }

        log.updateHighWatermark(50L);

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset(), "The first cleanable offset starts at the beginning of the log.");
        assertEquals(log.highWatermark(), log.lastStableOffset(), "The high watermark equals the last stable offset as no transactions are in progress");
        assertEquals(log.lastStableOffset(), cleanableOffsets.getFirstUncleanableDirtyOffset(), "The first uncleanable offset is bounded by the last stable offset.");
    }

    /**
     * Test computation of cleanable range with no minimum compaction lag settings active where bounded by active segment
     */
    @Test
    public void testCleanableOffsetsActiveSegment() throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        while (log.numberOfSegments() < 8) {
            log.appendAsLeader(records(log.logEndOffset().intValue(), log.logEndOffset().intValue(), time.milliseconds()), 0);
        }

        log.updateHighWatermark(log.logEndOffset());

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset(),
                "The first cleanable offset starts at the beginning of the log.");
        assertEquals(log.activeSegment().getBaseOffset(), cleanableOffsets.getFirstUncleanableDirtyOffset(),
                "The first uncleanable offset begins with the active segment.");
    }

    /**
     * Test computation of cleanable range with a minimum compaction lag time
     */
    @Test
    public void testCleanableOffsetsForTime() throws IOException {
        int compactionLag = 60 * 60 * 1000;
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, 1024);
        logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        long t0 = time.milliseconds();
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(records(log.logEndOffset().intValue(), log.logEndOffset().intValue(), t0), 0);
        }

        LogSegment activeSegAtT0 = log.activeSegment();

        time.sleep(compactionLag + 1);
        long t1 = time.milliseconds();

        while (log.numberOfSegments() < 8) {
            log.appendAsLeader(records(log.logEndOffset().intValue(), log.logEndOffset().intValue(), t1), 0);
        }

        log.updateHighWatermark(log.logEndOffset());

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset(),
                "The first cleanable offset starts at the beginning of the log.");
        assertEquals(activeSegAtT0.getBaseOffset(), cleanableOffsets.getFirstUncleanableDirtyOffset(),
                "The first uncleanable offset begins with the second block of log entries.");
    }

    /**
     * Test computation of cleanable range with a minimum compaction lag time that is small enough that
     * the active segment contains it.
     */
    @Test
    public void testCleanableOffsetsForShortTime() throws IOException {
        int compactionLag = 60 * 60 * 1000;
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, 1024);
        logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        long t0 = time.milliseconds();
        while (log.numberOfSegments() < 8) {
            log.appendAsLeader(records(log.logEndOffset().intValue(), log.logEndOffset().intValue(), t0), 0);
        }

        log.updateHighWatermark(log.logEndOffset());

        time.sleep(compactionLag + 1);

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset(),
                "The first cleanable offset starts at the beginning of the log.");
        assertEquals(log.activeSegment().getBaseOffset(), cleanableOffsets.getFirstUncleanableDirtyOffset(),
                "The first uncleanable offset begins with active segment.");
    }

    @Test
    public void testCleanableOffsetsNeedsCheckpointReset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(Arrays.asList(tp), 20, 5);
        logs.get(tp).maybeIncrementLogStartOffset(10L, clientRecordDeletion);

        Optional<Long> lastCleanOffset = Optional.of(15L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, time.milliseconds());
        assertFalse(cleanableOffsets.getForceUpdateCheckpoint(), "Checkpoint offset should not be reset if valid");

        logs.get(tp).maybeIncrementLogStartOffset(20L, clientRecordDeletion);
        cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, time.milliseconds());
        assertTrue(cleanableOffsets.getForceUpdateCheckpoint(), "Checkpoint offset needs to be reset if less than log start offset");

        lastCleanOffset = Optional.of(25L);
        cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, time.milliseconds());
        assertTrue(cleanableOffsets.getForceUpdateCheckpoint(), "Checkpoint offset needs to be reset if greater than log end offset");
    }

//    @Test
    public void testUndecidedTransactionalDataNotCleanable() throws IOException {
        int compactionLag = 60 * 60 * 1000;
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, 1024);
        logProps.put(LogConfig.MinCompactionLagMsProp, compactionLag);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        long producerId = 15L;
        short producerEpoch = 0;
        int sequence = 0;
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, producerEpoch, sequence,
                new SimpleRecord(time.milliseconds(), "1".getBytes(), "a".getBytes()),
                new SimpleRecord(time.milliseconds(), "2".getBytes(), "b".getBytes())), 0);
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, producerEpoch, sequence + 2,
                new SimpleRecord(time.milliseconds(), "3".getBytes(), "c".getBytes())), 0);
        log.roll();
        log.updateHighWatermark(3L);

        time.sleep(compactionLag + 1);
        // although the compaction lag has been exceeded, the undecided data should not be cleaned
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Optional.of(0L), time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset());
        assertEquals(0L, cleanableOffsets.getFirstUncleanableDirtyOffset());

        log.appendAsLeader(
                MemoryRecords.withEndTransactionMarker(time.milliseconds(),
                        producerId,
                        producerEpoch,
                        new EndTransactionMarker(ControlRecordType.ABORT, 15)
                ),
                0,
                AppendOrigin.Coordinator);
        log.roll();
        log.updateHighWatermark(4L);

        // the first segment should now become cleanable immediately
        cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Optional.of(0L), time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset());
        assertEquals(3L, cleanableOffsets.getFirstUncleanableDirtyOffset());

        time.sleep(compactionLag + 1);

        // the second segment becomes cleanable after the compaction lag
        cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Optional.of(0L), time.milliseconds());
        assertEquals(0L, cleanableOffsets.getFirstDirtyOffset());
        assertEquals(4L, cleanableOffsets.getFirstUncleanableDirtyOffset());
    }

//    @Test
    public void testDoneCleaning() throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));
        while (log.numberOfSegments() < 8) {
            log.appendAsLeader(records(log.logEndOffset().intValue(), log.logEndOffset().intValue(), time.milliseconds()), 0);
        }

        LogCleanerManager cleanerManager = createCleanerManager(log);

        assertThrows(IllegalStateException.class, () -> cleanerManager.doneCleaning(topicPartition, log.dir(), 1L));

        cleanerManager.setCleaningState(topicPartition, new LogCleaningPaused(1));
        assertThrows(IllegalStateException.class, () -> cleanerManager.doneCleaning(topicPartition, log.dir(), 1L));

        cleanerManager.setCleaningState(topicPartition, new LogCleaningInProgress());
        long endOffset = 1L;
        cleanerManager.doneCleaning(topicPartition, log.dir(), endOffset);
        assertTrue(!cleanerManager.cleaningState(topicPartition).isPresent());
        assertTrue(cleanerManager.allCleanerCheckpoints().containsKey(topicPartition));
        assertEquals(Optional.of(endOffset), cleanerManager.allCleanerCheckpoints().get(topicPartition));

        cleanerManager.setCleaningState(topicPartition, new LogCleaningAborted());
        cleanerManager.doneCleaning(topicPartition, log.dir(), endOffset);
        assertEquals(new LogCleaningPaused(1), cleanerManager.cleaningState(topicPartition).get());
        assertTrue(cleanerManager.allCleanerCheckpoints().containsKey(topicPartition));
    }

//    @Test
    public void testDoneDeleting() throws IOException {
        MemoryRecords records = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, LogConfig.Compact + "," + LogConfig.Delete);
        LogCleanerManager cleanerManager = createCleanerManager(log);
        TopicPartition tp = new TopicPartition("log", 0);

        assertThrows(IllegalStateException.class, () -> cleanerManager.doneDeleting(Arrays.asList(tp)));

        cleanerManager.setCleaningState(tp, new LogCleaningPaused(1));
        assertThrows(IllegalStateException.class, () -> cleanerManager.doneDeleting(Arrays.asList(tp)));

        cleanerManager.setCleaningState(tp, new LogCleaningInProgress());
        cleanerManager.doneDeleting(Arrays.asList(tp));
        assertTrue(!cleanerManager.cleaningState(tp).isPresent());

        cleanerManager.setCleaningState(tp, new LogCleaningAborted());
        cleanerManager.doneDeleting(Arrays.asList(tp));
        assertEquals(new LogCleaningPaused(1), cleanerManager.cleaningState(tp).get());
    }

    /**
     * Logs with invalid checkpoint offsets should update their checkpoint offset even if the log doesn't need cleaning
     */
//    @Test
    public void testCheckpointUpdatedForInvalidOffsetNoCleaning() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(Arrays.asList(tp), 20, 5);

        logs.get(tp).maybeIncrementLogStartOffset(20L, clientRecordDeletion);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerCheckpoints.put(tp, 15L);

        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(time);
        assertEquals(Optional.empty(), filthiestLog, "Log should not be selected for cleaning");
        assertEquals(20L, cleanerCheckpoints.get(tp), "Unselected log should have checkpoint offset updated");
    }

    /**
     * Logs with invalid checkpoint offsets should update their checkpoint offset even if they aren't selected
     * for immediate cleaning
     */
//    @Test
    public void testCheckpointUpdatedForInvalidOffsetNotSelected() throws IOException {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        List<TopicPartition> partitions = Arrays.asList(tp0, tp1);

        // create two logs, one with an invalid offset, and one that is dirtier than the log with an invalid offset
        Map<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions, 20, 5);
        logs.get(tp0).maybeIncrementLogStartOffset(15L, clientRecordDeletion);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerCheckpoints.put(tp0, 10L);
        cleanerCheckpoints.put(tp1, 5L);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(time).get();
        assertEquals(tp1, filthiestLog.getTopicPartition(), "Dirtier log should be selected");
        assertEquals(15L, cleanerCheckpoints.get(tp0), "Unselected log should have checkpoint offset updated");
    }

    private LogCleanerManager createCleanerManager(UnifiedLog log) throws IOException {
        Map<TopicPartition, UnifiedLog> logs = new HashMap<>();
        logs.put(topicPartition, log);
        return new LogCleanerManager(Arrays.asList(logDir, logDir2), logs, null);
    }

    private LogCleanerManagerMock createCleanerManagerMock(Map<TopicPartition, UnifiedLog> pool) throws IOException {
        return new LogCleanerManagerMock(Arrays.asList(logDir), pool, null);
    }

    private UnifiedLog createLog(Integer segmentSize,
                                 String cleanupPolicy) throws IOException {
        return createLog(segmentSize, cleanupPolicy, new TopicPartition("log", 0));
    }

    private UnifiedLog createLog(Integer segmentSize,
                                 String cleanupPolicy,
                                 TopicPartition topicPartition) throws IOException {
        LogConfig config = createLowRetentionLogConfig(segmentSize, cleanupPolicy);
        File partitionDir = new File(logDir, UnifiedLog.logDirName(topicPartition));

        return UnifiedLog.apply(partitionDir,
                config,
                0L,
                0L,
                time.getScheduler(),
                new BrokerTopicStats(),
                time,
                5 * 60 * 1000,
                60 * 60 * 1000,
                LogManager.ProducerIdExpirationCheckIntervalMs,
                new LogDirFailureChannel(10),
                true,
                Optional.empty(),
                true,
                new ConcurrentHashMap<>());
    }

    private LogConfig createLowRetentionLogConfig(Integer segmentSize, String cleanupPolicy) {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp, segmentSize);
        logProps.put(LogConfig.RetentionMsProp, 1);
        logProps.put(LogConfig.CleanupPolicyProp, cleanupPolicy);
        logProps.put(LogConfig.MinCleanableDirtyRatioProp, 0.05d); // small for easier and clearer tests

        return new LogConfig(logProps);
    }

    private void writeRecords(UnifiedLog log,
                              Integer numBatches,
                              Integer recordsPerBatch,
                              Integer batchesPerSegment) throws OffsetOutOfRangeException, IOException {
        for (int i = 0; i < numBatches; i++) {
            appendRecords(log, recordsPerBatch);
            if (i % batchesPerSegment == 0) {
                log.roll();
            }
        }
        log.roll();
    }

    private void appendRecords(UnifiedLog log, Integer numRecords) {
        long startOffset = log.logEndOffset();
        long endOffset = startOffset + numRecords;
        long lastTimestamp = 0L;
        List<SimpleRecord> records = new ArrayList<>();
        for (long offset = startOffset; offset < endOffset; offset++) {
            long currentTimestamp = time.milliseconds();
            if (offset == endOffset - 1) {
                lastTimestamp = currentTimestamp;
            }
            String key = String.format("key-%s", offset);
            String value = String.format("value-%s", offset);
            records.add(new SimpleRecord(currentTimestamp, key.getBytes(), value.getBytes()));
        }

        log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, records.toArray(new SimpleRecord[0])), 1);
        log.maybeIncrementHighWatermark(log.logEndOffsetMetadata());
    }

    private UnifiedLog makeLog(LogConfig config) throws IOException {
        return makeLog(logDir, config);
    }

    private UnifiedLog makeLog(File dir, LogConfig config) throws IOException {
        return UnifiedLog.apply(dir,
                config,
                0L,
                0L,
                time.getScheduler(),
                new BrokerTopicStats(),
                time = time,
                5 * 60 * 1000,
                60 * 60 * 1000,
                LogManager.ProducerIdExpirationCheckIntervalMs,
                new LogDirFailureChannel(10),
                true,
                Optional.empty(),
                true,
                new ConcurrentHashMap<>());
    }

    private MemoryRecords records(Integer key, Integer value, Long timestamp) {
        return MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord(timestamp, key.toString().getBytes(), value.toString().getBytes())
        );
    }

}
