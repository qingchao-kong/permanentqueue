package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.TestUtils;
import cn.pockethub.permanentqueue.kafka.metadata.MockConfigRepository;
import cn.pockethub.permanentqueue.kafka.server.*;
import cn.pockethub.permanentqueue.kafka.server.checkpoints.OffsetCheckpointFile;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.server.metrics.KafkaYammerMetrics;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import javascalautils.Try;
import org.apache.directory.api.util.FileUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static cn.pockethub.permanentqueue.kafka.server.FetchIsolation.FetchLogEnd;
import static javascalautils.TryCompanion.Failure;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LogManagerTest {
    private MockTime time = new MockTime();
    private int maxRollInterval = 100;
    private long maxLogAgeMs = 10 * 60 * 1000;
    private Properties logProps = new Properties();
    private LogConfig logConfig;
    private File logDir = null;
    private LogManager logManager = null;
    private String name = "kafka";
    private long veryLargeLogFlushInterval = 10000000L;

    @BeforeEach
    public void setUp() throws Throwable {
        logProps.put(LogConfig.SegmentBytesProp, 1024);
        logProps.put(LogConfig.SegmentIndexBytesProp, 4096);
        logProps.put(LogConfig.RetentionMsProp, maxLogAgeMs);
        logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.toString(Long.MAX_VALUE));

        this.logConfig = new LogConfig(logProps);

        logDir = TestUtils.tempDir();
        logManager = createLogManager();
        logManager.startup(new HashSet<>());
    }

    @AfterEach
    public void tearDown() throws Throwable {
        if (logManager != null) {
            logManager.shutdown();
        }
        Utils.delete(logDir);
        // Some tests assign a new LogManager
        if (logManager != null) {
            for (File file : logManager.liveLogDirs()) {
                Utils.delete(file);
            }
        }
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     */
    @Test
    public void testCreateLog() throws Throwable {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
        assertEquals(1, logManager.liveLogDirs().size());

        File logFile = new File(logDir, name + "-0");
        assertTrue(logFile.exists());
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Tests that all internal futures are completed before LogManager.shutdown() returns to the
     * caller during error situations.
     */
    @Test
    public void testHandlingExceptionsDuringShutdown() throws Throwable {
        // We create two directories logDir1 and logDir2 to help effectively test error handling
        // during LogManager.shutdown().
        File logDir1 = TestUtils.tempDir();
        File logDir2 = TestUtils.tempDir();
        Optional<LogManager> logManagerForTest = Optional.empty();
        try {
            logManagerForTest = Optional.of(createLogManager(Arrays.asList(logDir1, logDir2)));

            assertEquals(2, logManagerForTest.get().liveLogDirs().size());
            logManagerForTest.get().startup(new HashSet<>());

            UnifiedLog log1 = logManagerForTest.get().getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
            UnifiedLog log2 = logManagerForTest.get().getOrCreateLog(new TopicPartition(name, 1), Optional.empty());

            File logFile1 = new File(logDir1, name + "-0");
            assertTrue(logFile1.exists());
            File logFile2 = new File(logDir2, name + "-1");
            assertTrue(logFile2.exists());

            log1.appendAsLeader(TestUtils.singletonRecords("test1".getBytes()), 0);
            log1.takeProducerSnapshot();
            log1.appendAsLeader(TestUtils.singletonRecords("test1".getBytes()), 0);

            log2.appendAsLeader(TestUtils.singletonRecords("test2".getBytes()), 0);
            log2.takeProducerSnapshot();
            log2.appendAsLeader(TestUtils.singletonRecords("test2".getBytes()), 0);

            // This should cause log1.close() to fail during LogManger shutdown sequence.
            FileUtils.deleteDirectory(logFile1);

            logManagerForTest.get().shutdown();

            assertFalse(Files.exists(new File(logDir1, LogLoader.CleanShutdownFile).toPath()));
            assertTrue(Files.exists(new File(logDir2, LogLoader.CleanShutdownFile).toPath()));
        } finally {
            if (logManagerForTest.isPresent()) {
                LogManager manager = logManagerForTest.get();
                for (File dir : manager.liveLogDirs()) {
                    Utils.delete(dir);
                }
            }
        }
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     * The LogManager is configured with one invalid log directory which should be marked as offline.
     */
    @Test
    public void testCreateLogWithInvalidLogDir() throws Throwable {
        // Configure the log dir with the Nul character as the path, which causes dir.getCanonicalPath() to throw an
        // IOException. This simulates the scenario where the disk is not properly mounted (which is hard to achieve in
        // a unit test)
        List<File> dirs = Arrays.asList(logDir, new File("\u0000"));

        logManager.shutdown();
        logManager = createLogManager(dirs);
        logManager.startup(new HashSet<>());

        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), true, false, Optional.empty());
        File logFile = new File(logDir, name + "-0");
        assertTrue(logFile.exists());
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()),
                0,
                AppendOrigin.Client,
                MetadataVersion.latest(),
                RequestLocal.NoCaching);
    }

    @Test
    public void testCreateLogWithLogDirFallback() throws Throwable {
        // Configure a number of directories one level deeper in logDir,
        // so they all get cleaned up in tearDown().
        List<File> dirs = Stream.of("0", "1", "2", "3", "4")
                .map(name -> logDir.toPath().resolve(name).toFile())
                .collect(Collectors.toList());

        // Create a new LogManager with the configured directories and an overridden createLogDirectory.
        logManager.shutdown();
        logManager = spy(createLogManager(dirs));
        Set<File> brokenDirs = new HashSet<>();
        doAnswer(invocation -> {
            // The first half of directories tried will fail, the rest goes through.
            File logDir = invocation.getArgument(0, File.class);
            if (brokenDirs.contains(logDir) || brokenDirs.size() < dirs.size() / 2) {
                brokenDirs.add(logDir);
                return Failure(new Throwable("broken dir"));
            } else {
                return (Try<File>) invocation.callRealMethod();
            }
        }).when(logManager).createLogDirectory(any(), any());
        logManager.startup(new HashSet<>());

        // Request creating a new log.
        // LogManager should try using all configured log directories until one succeeds.
        logManager.getOrCreateLog(new TopicPartition(name, 0), true, false, Optional.empty());

        // Verify that half the directories were considered broken,
        assertEquals(dirs.size() / 2, brokenDirs.size());

        // and that exactly one log file was created,
        assertEquals(1, dirs.stream().filter(this::containsLogFile).count(), "More than one log file created");

        // and that it wasn't created in one of the broken directories.
        assertFalse(brokenDirs.stream().anyMatch(this::containsLogFile));
    }

    private Boolean containsLogFile(File dir) {
        return new File(dir, name + "-0").exists();
    }

    /**
     * Test that get on a non-existent returns None and no log is created.
     */
    @Test
    public void testGetNonExistentLog() {
        Optional<UnifiedLog> log = logManager.getLog(new TopicPartition(name, 0));
        assertEquals(Optional.empty(), log, "No log should be found.");
        File logFile = new File(logDir, name + "-0");
        assertFalse(logFile.exists());
    }

    /**
     * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
     */
    @Test
    public void testCleanupExpiredSegments() throws Throwable {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
        long offset = 0L;
        for (int i = 0; i < 200; i++) {
            MemoryRecords set = TestUtils.singletonRecords("test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.getLastOffset();
        }
        assertTrue(log.numberOfSegments() > 1, "There should be more than one segment now.");
        log.updateHighWatermark(log.logEndOffset());

        for (LogSegment segment : log.logSegments()) {
            segment.getLog().file().setLastModified(time.milliseconds());
        }

        time.sleep(maxLogAgeMs + 1);
        assertEquals(1, log.numberOfSegments(), "Now there should only be only one segment in the index.");
        time.sleep(log.config().getFileDeleteDelayMs() + 1);

        for (LogSegment s : log.logSegments()) {
            s.getLazyOffsetIndex().get();
            s.getLazyTimeIndex().get();
        }

        // there should be a log file, two indexes, one producer snapshot, and the leader epoch checkpoint
        assertEquals(log.numberOfSegments() * 4 + 1, log.dir().list().length, "Files should have been deleted");
        assertEquals(0, readLog(log, offset + 1).getRecords().sizeInBytes(), "Should get empty fetch off new log.");

        assertThrows(OffsetOutOfRangeException.class, () -> readLog(log, 0L), () -> "Should get exception from fetching earlier.");
        // log should still be appendable
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
     */
    @Test
    public void testCleanupSegmentsToMaintainSize() throws Throwable {
        int setSize = TestUtils.singletonRecords("test".getBytes()).sizeInBytes();
        logManager.shutdown();
        Integer segmentBytes = 10 * setSize;
        Properties properties = new Properties();
        properties.put(LogConfig.SegmentBytesProp, segmentBytes.toString());
        properties.put(LogConfig.RetentionBytesProp, new Long(5L * 10L * setSize + 10L).toString());
        ConfigRepository configRepository = MockConfigRepository.forTopic(name, properties);

        logManager = createLogManager(configRepository);
        logManager.startup(new HashSet<>());

        // create a log
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
        long offset = 0L;

        // add a bunch of messages that should be larger than the retentionSize
        int numMessages = 200;
        for (int i = 0; i < numMessages; i++) {
            MemoryRecords set = TestUtils.singletonRecords("test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.getFirstOffset().get().getMessageOffset();
        }

        log.updateHighWatermark(log.logEndOffset());
        assertEquals(numMessages * setSize / segmentBytes, log.numberOfSegments(), "Check we have the expected number of segments.");

        // this cleanup shouldn't find any expired segments but should delete some to reduce size
        time.sleep(logManager.InitialTaskDelayMs);
        assertEquals(6, log.numberOfSegments(), "Now there should be exactly 6 segments");
        time.sleep(log.config().getFileDeleteDelayMs() + 1);

        // there should be a log file, two indexes (the txn index is created lazily),
        // and a producer snapshot file per segment, and the leader epoch checkpoint.
        assertEquals(log.numberOfSegments() * 4 + 1, log.dir().list().length, "Files should have been deleted");
        assertEquals(0, readLog(log, offset + 1).getRecords().sizeInBytes(), "Should get empty fetch off new log.");
        assertThrows(OffsetOutOfRangeException.class, () -> readLog(log, 0L));
        // log should still be appendable
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Ensures that LogManager doesn't run on logs with cleanup.policy=compact,delete
     * LogCleaner.CleanerThread handles all logs where compaction is enabled.
     */
    @Test
    public void testDoesntCleanLogsWithCompactDeletePolicy() throws Throwable {
        testDoesntCleanLogs(LogConfig.Compact + "," + LogConfig.Delete);
    }

    /**
     * Ensures that LogManager doesn't run on logs with cleanup.policy=compact
     * LogCleaner.CleanerThread handles all logs where compaction is enabled.
     */
    @Test
    public void testDoesntCleanLogsWithCompactPolicy() throws Throwable {
        testDoesntCleanLogs(LogConfig.Compact);
    }

    private void testDoesntCleanLogs(String policy) throws Throwable {
        logManager.shutdown();
        ConfigRepository configRepository = MockConfigRepository.forTopic(name, LogConfig.CleanupPolicyProp, policy);

        logManager = createLogManager(configRepository);
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
        long offset = 0L;
        for (int i = 0; i < 200; i++) {
            MemoryRecords set = TestUtils.singletonRecords("test".getBytes(), "test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.getLastOffset();
        }

        int numSegments = log.numberOfSegments();
        assertTrue(log.numberOfSegments() > 1, "There should be more than one segment now.");

        for (LogSegment segment : log.logSegments()) {
            segment.getLog().file().setLastModified(time.milliseconds());
        }

        time.sleep(maxLogAgeMs + 1);
        assertEquals(numSegments, log.numberOfSegments(), "number of segments shouldn't have changed");
    }

    /**
     * Test that flush is invoked by the background scheduler thread.
     */
    @Test
    public void testTimeBasedFlush() throws Throwable {
        logManager.shutdown();
        ConfigRepository configRepository = MockConfigRepository.forTopic(name, LogConfig.FlushMsProp, "1000");

        logManager = createLogManager(configRepository);
        logManager.startup(new HashSet<>());
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
        long lastFlush = log.lastFlushTime();
        for (int i = 0; i < 200; i++) {
            MemoryRecords set = TestUtils.singletonRecords("test".getBytes());
            log.appendAsLeader(set, 0);
        }
        time.sleep(logManager.InitialTaskDelayMs);
        assertTrue(lastFlush != log.lastFlushTime(), "Time based flush should have been triggered");
    }

    /**
     * Test that new logs that are created are assigned to the least loaded log directory
     */
    @Test
    public void testLeastLoadedAssignment() throws Throwable {
        // create a log manager with multiple data directories
        List<File> dirs = Arrays.asList(TestUtils.tempDir(),
                TestUtils.tempDir(),
                TestUtils.tempDir());
        logManager.shutdown();
        logManager = createLogManager(dirs);

        // verify that logs are always assigned to the least loaded partition
        for (int partition = 0; partition < 20; partition++) {
            logManager.getOrCreateLog(new TopicPartition("test", partition), Optional.empty());
            assertEquals(partition + 1, logManager.allLogs().size(), "We should have created the right number of logs");
            Map<String, Long> counts = new HashMap<>();
            for (UnifiedLog log : logManager.allLogs()) {
                Long old = counts.getOrDefault(log.dir().getParent(), 0L);
                old++;
                counts.put(log.dir().getParent(), old);
            }
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                max = Math.max(max, entry.getValue());
                min = Math.min(min, entry.getValue());
            }
            assertTrue(max <= min + 1, "Load should balance evenly");
        }
    }

    /**
     * Test that it is not possible to open two log managers using the same data directory
     */
    @Test
    public void testTwoLogManagersUsingSameDirFails() {
        assertThrows(KafkaException.class, () -> createLogManager());
    }

    /**
     * Test that recovery points are correctly written out to disk
     */
    @Test
    public void testCheckpointRecoveryPoints() throws Throwable {
        verifyCheckpointRecovery(Arrays.asList(new TopicPartition("test-a", 1), new TopicPartition("test-b", 1)), logManager, logDir);
    }

    /**
     * Test that recovery points directory checking works with trailing slash
     */
    @Test
    public void testRecoveryDirectoryMappingWithTrailingSlash() throws Throwable {
        logManager.shutdown();
        logManager = TestUtils.createLogManager(Arrays.asList(new File(TestUtils.tempDir().getAbsolutePath() + File.separator)));
        logManager.startup(new HashSet<>());
        verifyCheckpointRecovery(Arrays.asList(new TopicPartition("test-a", 1)), logManager, CollectionUtilExt.head(logManager.liveLogDirs()));
    }

    /**
     * Test that recovery points directory checking works with relative directory
     */
    @Test
    public void testRecoveryDirectoryMappingWithRelativeDirectory() throws Throwable {
        logManager.shutdown();
        logManager = createLogManager(Arrays.asList(new File("data", logDir.getName()).getAbsoluteFile()));
        logManager.startup(new HashSet<>());
        verifyCheckpointRecovery(Arrays.asList(new TopicPartition("test-a", 1)), logManager, CollectionUtilExt.head(logManager.liveLogDirs()));
    }

    private void verifyCheckpointRecovery(List<TopicPartition> topicPartitions, LogManager logManager, File logDir) throws Throwable {
        List<UnifiedLog> logs = new ArrayList<>();
        for (TopicPartition tp : topicPartitions) {
            UnifiedLog log = logManager.getOrCreateLog(tp, Optional.empty());
            logs.add(log);
        }
        for (UnifiedLog log : logs) {
            for (int i = 0; i < 50; i++) {
                log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), 0);

                log.flush(false);
            }
        }

        logManager.checkpointLogRecoveryOffsets();
        Map<TopicPartition, Long> checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile)).read();

        int size = Math.min(topicPartitions.size(), logs.size());
        for (int i = 0; i < size; i++) {
            TopicPartition tp = topicPartitions.get(i);
            UnifiedLog log = logs.get(i);
            assertEquals(checkpoints.get(tp), log.recoveryPoint(), "Recovery point should equal checkpoint");
        }
    }

    private LogManager createLogManager() throws Throwable {
        return createLogManager(Arrays.asList(this.logDir), new MockConfigRepository(), 1);
    }

    private LogManager createLogManager(List<File> logDirs) throws Throwable {
        return createLogManager(logDirs, new MockConfigRepository(), 1);
    }

    private LogManager createLogManager(ConfigRepository configRepository) throws Throwable {
        return createLogManager(Arrays.asList(this.logDir), configRepository, 1);
    }

    private LogManager createLogManager(List<File> logDirs,
                                        Integer recoveryThreadsPerDataDir) throws Throwable {
        return createLogManager(logDirs, new MockConfigRepository(), recoveryThreadsPerDataDir);
    }

    private LogManager createLogManager(List<File> logDirs,
                                        ConfigRepository configRepository,
                                        Integer recoveryThreadsPerDataDir) throws Throwable {
        return TestUtils.createLogManager(logDirs,
                logConfig,
                configRepository,
                new CleanerConfig(
                        CleanerConfig.Defaults.NUM_THREADS,
                        CleanerConfig.Defaults.DEDUPE_BUFFER_SIZE,
                        CleanerConfig.Defaults.DEDUPE_BUFFER_LOAD_FACTOR,
                        CleanerConfig.Defaults.IO_BUFFER_SIZE,
                        CleanerConfig.Defaults.MAX_MESSAGE_SIZE,
                        CleanerConfig.Defaults.MAX_IO_BYTES_PER_SECOND,
                        CleanerConfig.Defaults.BACK_OFF_MS,
                        false,
                        CleanerConfig.Defaults.HASH_ALGORITHM),
                this.time,
                MetadataVersion.latest(),
                recoveryThreadsPerDataDir);
    }

    @Test
    public void testFileReferencesAfterAsyncDelete() throws Throwable {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(name, 0), Optional.empty());
        LogSegment activeSegment = log.activeSegment();
        String logName = activeSegment.getLog().file().getName();
        String indexName = activeSegment.offsetIndex().file().getName();
        String timeIndexName = activeSegment.timeIndex().file().getName();
        String txnIndexName = activeSegment.getTxnIndex().file().getName();
        List<File> indexFilesOnDiskBeforeDelete = Arrays.stream(activeSegment.getLog().file().getParentFile().listFiles())
                .filter(file -> file.getName().endsWith("index")).collect(Collectors.toList());

        UnifiedLog removedLog = logManager.asyncDelete(new TopicPartition(name, 0)).get();
        LogSegment removedSegment = removedLog.activeSegment();
        List<File> indexFilesAfterDelete = Arrays.asList(removedSegment.getLazyOffsetIndex().file(), removedSegment.getLazyTimeIndex().file(),
                removedSegment.getTxnIndex().file());

        assertEquals(new File(removedLog.dir(), logName), removedSegment.getLog().file());
        assertEquals(new File(removedLog.dir(), indexName), removedSegment.getLazyOffsetIndex().file());
        assertEquals(new File(removedLog.dir(), timeIndexName), removedSegment.getLazyTimeIndex().file());
        assertEquals(new File(removedLog.dir(), txnIndexName), removedSegment.getTxnIndex().file());

        // Try to detect the case where a new index type was added and we forgot to update the pointer
        // This will only catch cases where the index file is created eagerly instead of lazily
        for (File fileBeforeDelete : indexFilesOnDiskBeforeDelete) {
            Optional<File> fileInIndex = indexFilesAfterDelete.stream()
                    .filter(file -> file.getName().equals(fileBeforeDelete.getName()))
                    .findFirst();
            assertEquals(Optional.of(fileBeforeDelete.getName()), fileInIndex.map(File::getName),
                    String.format("Could not find index file %s in indexFilesAfterDelete", fileBeforeDelete.getName()));
            assertNotEquals("File reference was not updated in index", fileBeforeDelete.getAbsolutePath(),
                    fileInIndex.get().getAbsolutePath());
        }

        time.sleep(logManager.InitialTaskDelayMs);
        assertTrue(logManager.hasLogsToBeDeleted(), "Logs deleted too early");
        time.sleep(logManager.getCurrentDefaultConfig().getFileDeleteDelayMs() - logManager.InitialTaskDelayMs);
        assertFalse(logManager.hasLogsToBeDeleted(), "Logs not deleted");
    }

    @Test
    public void testCreateAndDeleteOverlyLongTopic() throws Throwable {
        String invalidTopicName = String.join("", Collections.nCopies(253, "x"));
        logManager.getOrCreateLog(new TopicPartition(invalidTopicName, 0), Optional.empty());
        logManager.asyncDelete(new TopicPartition(invalidTopicName, 0));
    }

    @Test
    public void testCheckpointForOnlyAffectedLogs() throws Throwable {
        List<TopicPartition> tps = Arrays.asList(new TopicPartition("test-a", 0),
                new TopicPartition("test-a", 1),
                new TopicPartition("test-a", 2),
                new TopicPartition("test-b", 0),
                new TopicPartition("test-b", 1));

        List<UnifiedLog> allLogs = new ArrayList<>();
        for (TopicPartition tp : tps) {
            UnifiedLog log = logManager.getOrCreateLog(tp, Optional.empty());
            allLogs.add(log);
        }
        for (UnifiedLog log : allLogs) {
            for (int i = 0; i < 50; i++) {
                log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), 0);
                log.flush(false);
            }
        }

        logManager.checkpointRecoveryOffsetsInDir(logDir);

        Map<TopicPartition, Long> checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile)).read();

        int size = Math.min(tps.size(), allLogs.size());
        for (int i = 0; i < size; i++) {
            TopicPartition tp = tps.get(i);
            UnifiedLog log = allLogs.get(i);
            assertEquals(checkpoints.get(tp), log.recoveryPoint(),
                    "Recovery point should equal checkpoint");
        }
    }

    private FetchDataInfo readLog(UnifiedLog log, Long offset) {
        return readLog(log, offset, 1024);
    }

    private FetchDataInfo readLog(UnifiedLog log, Long offset, Integer maxLength) {
        return log.read(offset, maxLength, FetchLogEnd, true);
    }

    /**
     * Test when a configuration of a topic is updated while its log is getting initialized,
     * the config is refreshed when log initialization is finished.
     */
    @Test
    public void testTopicConfigChangeUpdatesLogConfig() throws Throwable {
        logManager.shutdown();
        ConfigRepository spyConfigRepository = spy(new MockConfigRepository());
        logManager = createLogManager(spyConfigRepository);
        LogManager spyLogManager = spy(logManager);
        UnifiedLog mockLog = mock(UnifiedLog.class);

        String testTopicOne = "test-topic-one";
        String testTopicTwo = "test-topic-two";
        TopicPartition testTopicOnePartition = new TopicPartition(testTopicOne, 1);
        TopicPartition testTopicTwoPartition = new TopicPartition(testTopicTwo, 1);

        spyLogManager.initializingLog(testTopicOnePartition);
        spyLogManager.initializingLog(testTopicTwoPartition);

        spyLogManager.topicConfigUpdated(testTopicOne);

        spyLogManager.finishedInitializingLog(testTopicOnePartition, Optional.of(mockLog));
        spyLogManager.finishedInitializingLog(testTopicTwoPartition, Optional.of(mockLog));

        // testTopicOne configs loaded again due to the update
        verify(spyLogManager).initializingLog(ArgumentMatchers.eq(testTopicOnePartition));
        verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(testTopicOnePartition), ArgumentMatchers.any());
        verify(spyConfigRepository, times(1)).topicConfig(testTopicOne);

        // testTopicTwo configs not loaded again since there was no update
        verify(spyLogManager).initializingLog(ArgumentMatchers.eq(testTopicTwoPartition));
        verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(testTopicTwoPartition), ArgumentMatchers.any());
        verify(spyConfigRepository, never()).topicConfig(testTopicTwo);
    }

    /**
     * Test if an error occurs when creating log, log manager removes corresponding
     * topic partition from the list of initializing partitions and no configs are retrieved.
     */
    @Test
    public void testConfigChangeGetsCleanedUp() throws Throwable {
        logManager.shutdown();
        ConfigRepository spyConfigRepository = spy(new MockConfigRepository());
        logManager = createLogManager(spyConfigRepository);
        LogManager spyLogManager = spy(logManager);

        TopicPartition testTopicPartition = new TopicPartition("test-topic", 1);
        spyLogManager.initializingLog(testTopicPartition);
        spyLogManager.finishedInitializingLog(testTopicPartition, Optional.empty());

        assertTrue(logManager.getPartitionsInitializing().isEmpty());
        verify(spyConfigRepository, never()).topicConfig(testTopicPartition.topic());
    }

    /**
     * Test when a broker configuration change happens all logs in process of initialization
     * pick up latest config when finished with initialization.
     */
    @Test
    public void testBrokerConfigChangeDeliveredToAllLogs() throws Throwable {
        logManager.shutdown();
        ConfigRepository spyConfigRepository = spy(new MockConfigRepository());
        logManager = createLogManager(spyConfigRepository);
        LogManager spyLogManager = spy(logManager);
        UnifiedLog mockLog = mock(UnifiedLog.class);

        String testTopicOne = "test-topic-one";
        String testTopicTwo = "test-topic-two";
        TopicPartition testTopicOnePartition = new TopicPartition(testTopicOne, 1);
        TopicPartition testTopicTwoPartition = new TopicPartition(testTopicTwo, 1);

        spyLogManager.initializingLog(testTopicOnePartition);
        spyLogManager.initializingLog(testTopicTwoPartition);

        spyLogManager.brokerConfigUpdated();

        spyLogManager.finishedInitializingLog(testTopicOnePartition, Optional.of(mockLog));
        spyLogManager.finishedInitializingLog(testTopicTwoPartition, Optional.of(mockLog));

        verify(spyConfigRepository, times(1)).topicConfig(testTopicOne);
        verify(spyConfigRepository, times(1)).topicConfig(testTopicTwo);
    }

    /**
     * Test when compact is removed that cleaning of the partitions is aborted.
     */
    @Test
    public void testTopicConfigChangeStopCleaningIfCompactIsRemoved() throws Throwable {
        logManager.shutdown();
        logManager = createLogManager(new MockConfigRepository());
        LogManager spyLogManager = spy(logManager);

        String topic = "topic";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        Properties oldProperties = new Properties();
        oldProperties.put(LogConfig.CleanupPolicyProp, LogConfig.Compact);
        LogConfig oldLogConfig = LogConfig.fromProps(logConfig.originals(), oldProperties);

        UnifiedLog log0 = spyLogManager.getOrCreateLog(tp0, Optional.empty());
        log0.updateConfig(oldLogConfig);
        UnifiedLog log1 = spyLogManager.getOrCreateLog(tp1, Optional.empty());
        log1.updateConfig(oldLogConfig);

        assertEquals(new HashSet<>(Arrays.asList(log0, log1)), spyLogManager.logsByTopic(topic));

        Properties newProperties = new Properties();
        newProperties.put(LogConfig.CleanupPolicyProp, LogConfig.Delete);

        spyLogManager.updateTopicConfig(topic, newProperties);

        assertTrue(log0.config().getDelete());
        assertTrue(log1.config().getDelete());
        assertFalse(log0.config().getCompact());
        assertFalse(log1.config().getCompact());

        verify(spyLogManager, times(1)).topicConfigUpdated(topic);
        verify(spyLogManager, times(1)).abortCleaning(tp0);
        verify(spyLogManager, times(1)).abortCleaning(tp1);
    }

    /**
     * Test even if no log is getting initialized, if config change events are delivered
     * things continue to work correctly. This test should not throw.
     * <p>
     * This makes sure that events can be delivered even when no log is getting initialized.
     */
    @Test
    public void testConfigChangesWithNoLogGettingInitialized() {
        logManager.brokerConfigUpdated();
        logManager.topicConfigUpdated("test-topic");
        assertTrue(logManager.getPartitionsInitializing().isEmpty());
    }

    private void appendRecordsToLog(MockTime time,
                                    File parentLogDir,
                                    Integer partitionId,
                                    BrokerTopicStats brokerTopicStats,
                                    Integer expectedSegmentsPerLog) throws Throwable {
        File tpFile = new File(parentLogDir, String.format("%s-%s", name, partitionId));
        int segmentBytes = 1024;

        UnifiedLog log = LogTestUtils.createLog(tpFile,
                logConfig,
                brokerTopicStats,
                time.getScheduler(),
                time,
                0L,
                0L,
                5 * 60 * 1000,
                60 * 60 * 1000,
                LogManager.ProducerIdExpirationCheckIntervalMs,
                true,
                Optional.empty(),
                true,
                new ConcurrentHashMap<>());

        assertTrue(expectedSegmentsPerLog > 0);
        // calculate numMessages to append to logs. It'll create "expectedSegmentsPerLog" log segments with segment.bytes=1024
        int numMessages = new Double(Math.floor(segmentBytes * expectedSegmentsPerLog / createRecord().sizeInBytes())).intValue();
        try {
            for (int i = 0; i < numMessages; i++) {
                log.appendAsLeader(createRecord(), 0);
            }

            assertEquals(expectedSegmentsPerLog, log.numberOfSegments());
        } finally {
            log.close();
        }
    }

    private MemoryRecords createRecord(){
        return TestUtils.singletonRecords("test".getBytes(), time.milliseconds());
    }

    private void verifyRemainingLogsToRecoverMetric(LogManager spyLogManager, Map<String, Integer> expectedParams) {
        String spyLogManagerClassName = spyLogManager.getClass().getSimpleName();
        // get all `remainingLogsToRecover` metrics
        List<Gauge<Integer>> logMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(entry -> {
                    MetricName metric = entry.getKey();
                    return Objects.equals(metric.getType(), spyLogManagerClassName)
                            && Objects.equals(metric.getName(), "remainingLogsToRecover");
                })
                .map(entry -> (Gauge<Integer>) entry.getValue())
                .collect(Collectors.toList());

        assertEquals(expectedParams.size(), logMetrics.size());

        ArgumentCaptor<String> capturedPath = ArgumentCaptor.forClass(String.class);

        int expectedCallTimes = expectedParams.values().stream().mapToInt(i -> i).sum();
        verify(spyLogManager, times(expectedCallTimes)).decNumRemainingLogs(any(), capturedPath.capture());

        List<String> paths = capturedPath.getAllValues();
        for (Map.Entry<String, Integer> entry : expectedParams.entrySet()) {
            String path = entry.getKey();
            Integer totalLogs = entry.getValue();
            // make sure each path is called "totalLogs" times, which means it is decremented to 0 in the end
            assertEquals(totalLogs, Collections.frequency(paths, path));
        }

        // expected the end value is 0
        for (Gauge gauge : logMetrics) {
            assertEquals(0, gauge.value());
        }
    }

    private void verifyRemainingSegmentsToRecoverMetric(LogManager spyLogManager,
                                                        List<File> logDirs,
                                                        Integer recoveryThreadsPerDataDir,
                                                        ConcurrentHashMap<String, Integer> mockMap,
                                                        Map<String, Integer> expectedParams) {
        String spyLogManagerClassName = spyLogManager.getClass().getSimpleName();
        // get all `remainingSegmentsToRecover` metrics
        List<Gauge<Integer>> logSegmentMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(entry -> {
                    MetricName metric = entry.getKey();
                    return metric.getType().equals(spyLogManagerClassName) && metric.getName().equals("remainingSegmentsToRecover");
                })
                .map(entry -> (Gauge<Integer>) entry.getValue())
                .collect(Collectors.toList());

        // expected each log dir has 1 metrics for each thread
        assertEquals(recoveryThreadsPerDataDir * logDirs.size(), logSegmentMetrics.size());

        ArgumentCaptor<String> capturedThreadName = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> capturedNumRemainingSegments = ArgumentCaptor.forClass(Integer.class);

        // Since we'll update numRemainingSegments from totalSegments to 0 for each thread, so we need to add 1 here
        int expectedCallTimes = expectedParams.values().stream().mapToInt(num -> num + 1).sum();
        verify(mockMap, times(expectedCallTimes)).put(capturedThreadName.capture(), capturedNumRemainingSegments.capture());

        // expected the end value is 0
        for (Gauge<Integer> gauge : logSegmentMetrics) {
            //todo 这里会失败，因mockMap
            assertEquals(0, gauge.value());
        }

        List<String> threadNames = capturedThreadName.getAllValues();
        List<Integer> numRemainingSegments = capturedNumRemainingSegments.getAllValues();

        for (Map.Entry<String, Integer> entry : expectedParams.entrySet()) {
            String threadName = entry.getKey();
            int totalSegments = entry.getValue();
            // make sure we update the numRemainingSegments from totalSegments to 0 in order for each thread
            int expectedCurRemainingSegments = totalSegments + 1;
            for (int i = 0; i < threadNames.size(); i++) {
                if (threadNames.get(i).contains(threadName)) {
                    expectedCurRemainingSegments -= 1;
                    assertEquals(expectedCurRemainingSegments, numRemainingSegments.get(i));
                }
            }
            assertEquals(0, expectedCurRemainingSegments);
        }
    }

    private void verifyLogRecoverMetricsRemoved(LogManager spyLogManager) {
        String spyLogManagerClassName = spyLogManager.getClass().getSimpleName();
        // get all `remainingLogsToRecover` metrics
        Set<MetricName> logMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> metric.getType().equals(spyLogManagerClassName) && metric.getName().equals("remainingLogsToRecover"))
                .collect(Collectors.toSet());

        assertTrue(logMetrics.isEmpty());

        // get all `remainingSegmentsToRecover` metrics
        Set<MetricName> logSegmentMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> Objects.equals(metric.getType(), spyLogManagerClassName) && Objects.equals(metric.getName(), "remainingSegmentsToRecover"))
                .collect(Collectors.toSet());

        assertTrue(logSegmentMetrics.isEmpty());
    }

    @Test
    public void  scopeTest(){
        ConcurrentHashMap<String, Integer> mockMap = mock(ConcurrentHashMap.class);
        mockMap.put("1",1);
        System.out.println("ok");
    }

    @Test
    public void testLogRecoveryMetrics() throws Throwable {
        logManager.shutdown();
        File logDir1 = TestUtils.tempDir();
        File logDir2 = TestUtils.tempDir();
        List<File> logDirs = Arrays.asList(logDir1, logDir2);
        int recoveryThreadsPerDataDir = 2;
        // create logManager with expected recovery thread number
        logManager = createLogManager(logDirs, recoveryThreadsPerDataDir);
        LogManager spyLogManager = spy(logManager);

        assertEquals(2, spyLogManager.liveLogDirs().size());

        MockTime mockTime = new MockTime();
        ConcurrentHashMap<String, Integer> mockMap = mock(ConcurrentHashMap.class);
        doAnswer(invocation-> 0).when(mockMap).get(any());
        BrokerTopicStats mockBrokerTopicStats = mock(BrokerTopicStats.class);
        int expectedSegmentsPerLog = 2;

        // create log segments for log recovery in each log dir
        appendRecordsToLog(mockTime, logDir1, 0, mockBrokerTopicStats, expectedSegmentsPerLog);
        appendRecordsToLog(mockTime, logDir2, 1, mockBrokerTopicStats, expectedSegmentsPerLog);

        // intercept loadLog method to pass expected parameter to do log recovery
        doAnswer(invocation -> {
            File dir = invocation.getArgument(0);
            Map<String, LogConfig> topicConfigOverrides = invocation.getArgument(5);

            TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
            LogConfig config = topicConfigOverrides.getOrDefault(topicPartition.topic(), logConfig);

            return UnifiedLog.apply(
                    dir,
                    config,
                    0L,
                    0L,
                    mockTime.getScheduler(),
                    mockBrokerTopicStats,
                    mockTime,
                    5 * 60 * 1000,
                    5 * 60 * 1000,
                    LogManager.ProducerIdExpirationCheckIntervalMs,
                    mock(LogDirFailureChannel.class),
                    // not clean shutdown
                    false,
                    Optional.empty(),
                    false,
                    // pass mock map for verification later
                    mockMap);

        }).when(spyLogManager).loadLog(any(), any(), any(), any(), any(), any(), any());

        // do nothing for removeLogRecoveryMetrics for metrics verification
        doNothing().when(spyLogManager).removeLogRecoveryMetrics();

        // start the logManager to do log recovery
        spyLogManager.startup(new HashSet<>());

        // make sure log recovery metrics are added and removed
        verify(spyLogManager, times(1)).addLogRecoveryMetrics(any(), any());
        verify(spyLogManager, times(1)).removeLogRecoveryMetrics();

        // expected 1 log in each log dir since we created 2 partitions with 2 log dirs
        Map<String, Integer> expectedRemainingLogsParams = new HashMap<String, Integer>() {{
            put(logDir1.getAbsolutePath(), 1);
            put(logDir2.getAbsolutePath(), 1);
        }};
        verifyRemainingLogsToRecoverMetric(spyLogManager, expectedRemainingLogsParams);

        Map<String, Integer> expectedRemainingSegmentsParams = new HashMap<String, Integer>() {{
            put(logDir1.getAbsolutePath(), expectedSegmentsPerLog);
            put(logDir2.getAbsolutePath(), expectedSegmentsPerLog);
        }};
        verifyRemainingSegmentsToRecoverMetric(spyLogManager, logDirs, recoveryThreadsPerDataDir, mockMap, expectedRemainingSegmentsParams);
    }

    @Test
    public void testLogRecoveryMetricsShouldBeRemovedAfterLogRecovered() throws Throwable {
        logManager.shutdown();
        File logDir1 = TestUtils.tempDir();
        File logDir2 = TestUtils.tempDir();
        List<File> logDirs = Arrays.asList(logDir1, logDir2);
        int recoveryThreadsPerDataDir = 2;
        // create logManager with expected recovery thread number
        logManager = createLogManager(logDirs, recoveryThreadsPerDataDir);
        LogManager spyLogManager = spy(logManager);

        assertEquals(2, spyLogManager.liveLogDirs().size());

        // start the logManager to do log recovery
        spyLogManager.startup(new HashSet<>());

        // make sure log recovery metrics are added and removed once
        verify(spyLogManager, times(1)).addLogRecoveryMetrics(any(), any());
        verify(spyLogManager, times(1)).removeLogRecoveryMetrics();

        verifyLogRecoverMetricsRemoved(spyLogManager);
    }

    @Test
    public void testMetricsExistWhenLogIsRecreatedBeforeDeletion() throws Throwable {
        String topicName = "metric-test";

        TopicPartition tp = new TopicPartition(topicName, 0);
        String metricTag = String.format("topic=%s,partition=%s", tp.topic(), tp.partition());

        // Create the Log and assert that the metrics are present
        logManager.getOrCreateLog(tp, Optional.empty());
        verifyMetrics(topicName, metricTag);

        // Trigger the deletion and assert that the metrics have been removed
        UnifiedLog removedLog = logManager.asyncDelete(tp).get();
        assertTrue(logMetrics(topicName).isEmpty());

        // Recreate the Log and assert that the metrics are present
        logManager.getOrCreateLog(tp, Optional.empty());
        verifyMetrics(topicName, metricTag);

        // Advance time past the file deletion delay and assert that the removed log has been deleted but the metrics
        // are still present
        time.sleep(logConfig.getFileDeleteDelayMs() + 1);
        assertTrue(removedLog.logSegments().isEmpty());
        verifyMetrics(topicName, metricTag);
    }

    private Set<MetricName> logMetrics(String topicName) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> Objects.equals(metric.getType(), "Log") && metric.getScope().contains(topicName))
                .collect(Collectors.toSet());
    }

    private void verifyMetrics(String topicName, String metricTag) {
        Set<MetricName> metricNames = logMetrics(topicName);
        assertEquals(LogMetricNames.allMetricNames().size(), metricNames.size());
        for (MetricName metric : metricNames) {
            assertTrue(metric.getMBeanName().contains(metricTag));
        }
    }

    @Test
    public void testMetricsAreRemovedWhenMovingCurrentToFutureLog() throws Throwable {
        File dir1 = TestUtils.tempDir();
        File dir2 = TestUtils.tempDir();
        logManager = createLogManager(Arrays.asList(dir1, dir2));
        logManager.startup(new HashSet<>());

        String topicName = "future-log";

        TopicPartition tp = new TopicPartition(topicName, 0);
        String metricTag = String.format("topic=%s,partition=%s", tp.topic(), tp.partition());

        // Create the current and future logs and verify that metrics are present for both current and future logs
        logManager.maybeUpdatePreferredLogDir(tp, dir1.getAbsolutePath());
        logManager.getOrCreateLog(tp, Optional.empty());
        logManager.maybeUpdatePreferredLogDir(tp, dir2.getAbsolutePath());
        logManager.getOrCreateLog(tp, false, true, Optional.empty());
        verifyMetrics(topicName, 2, metricTag);

        // Replace the current log with the future one and verify that only one set of metrics are present
        logManager.replaceCurrentWithFutureLog(tp);
        verifyMetrics(topicName, 1, metricTag);

        // Trigger the deletion of the former current directory and verify that one set of metrics is still present
        time.sleep(logConfig.getFileDeleteDelayMs() + 1);
        verifyMetrics(topicName, 1, metricTag);
    }

    private void verifyMetrics(String topicName, Integer logCount, String metricTag) {
        Set<MetricName> metricNames = logMetrics(topicName);
        assertEquals(LogMetricNames.allMetricNames().size() * logCount, metricNames.size());
        for (MetricName metric : metricNames) {
            assertTrue(metric.getMBeanName().contains(metricTag));
        }
    }

    @Test
    public void testWaitForAllToComplete() throws InterruptedException, ExecutionException {
        final int[] invokedCountArr = new int[]{0};
        Future<Boolean> success = Mockito.mock(Future.class);
        Mockito.when(success.get()).thenAnswer(i -> {
            invokedCountArr[0] += 1;
            return true;
        });
        Future<Boolean> failure = Mockito.mock(Future.class);
        Mockito.when(failure.get()).thenAnswer(i -> {
            invokedCountArr[0] += 1;
            throw new RuntimeException();
        });

        final int[] failureCountArr = new int[]{0};
        // all futures should be evaluated
        assertFalse(LogManager.waitForAllToComplete(Arrays.asList(success, failure), i -> failureCountArr[0] += 1));
        assertEquals(2, invokedCountArr[0]);
        assertEquals(1, failureCountArr[0]);
        assertFalse(LogManager.waitForAllToComplete(Arrays.asList(failure, success), i -> failureCountArr[0] += 1));
        assertEquals(4, invokedCountArr[0]);
        assertEquals(2, failureCountArr[0]);
        assertTrue(LogManager.waitForAllToComplete(Arrays.asList(success, success), i -> failureCountArr[0] += 1));
        assertEquals(6, invokedCountArr[0]);
        assertEquals(2, failureCountArr[0]);
        assertFalse(LogManager.waitForAllToComplete(Arrays.asList(failure, failure), i -> failureCountArr[0] += 1));
        assertEquals(8, invokedCountArr[0]);
        assertEquals(4, failureCountArr[0]);
    }
}
