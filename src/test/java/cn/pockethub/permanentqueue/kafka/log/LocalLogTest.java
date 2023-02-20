package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.TestUtils;
import cn.pockethub.permanentqueue.kafka.log.LogConfig.*;
import cn.pockethub.permanentqueue.kafka.server.FetchDataInfo;
import cn.pockethub.permanentqueue.kafka.server.KafkaConfig;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import cn.pockethub.permanentqueue.kafka.utils.Scheduler;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class LocalLogTest {

    private KafkaConfig config = null;
    private File tmpDir = TestUtils.tempDir();
    private File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private TopicPartition topicPartition = new TopicPartition("test_topic", 1);
    private LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
    private MockTime mockTime = new MockTime();
    private LocalLog log;

    @BeforeEach
    public void setUp() throws Exception {
        Properties props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", -1);
        config = KafkaConfig.fromProps(props);

        this.log = createLocalLogWithActiveSegment(LogTestUtils.createLogConfig());
    }

    @AfterEach
    public void tearDown() throws IOException {
        try {
            log.close();
        } catch (KafkaStorageException e) {
            // ignore
        }
        Utils.delete(tmpDir);
    }

    @EqualsAndHashCode
    static class KeyValue {
        private String key;
        private String value;

        public KeyValue(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public SimpleRecord toRecord(Long timestamp) {
            return new SimpleRecord(timestamp, key.getBytes(), value.getBytes());
        }

        public static KeyValue fromRecord(Record record) {
            String key = record.hasKey() ? StandardCharsets.UTF_8.decode(record.key()).toString() : "";
            String value = record.hasValue() ? StandardCharsets.UTF_8.decode(record.value()).toString() : "";
            return new KeyValue(key, value);
        }
    }

    private Collection<SimpleRecord> kvsToRecords(Collection<KeyValue> keyValues) {
        return keyValues.stream()
                .map(kv -> kv.toRecord(mockTime.milliseconds()))
                .collect(Collectors.toList());
    }

    private Collection<KeyValue> recordsToKvs(Collection<Record> records) {
        return records.stream()
                .map(KeyValue::fromRecord)
                .collect(Collectors.toList());
    }

    private void appendRecords(Collection<SimpleRecord> records,
                               LocalLog log,
                               Long initialOffset) throws IOException {
        log.append(initialOffset + records.size() - 1,
                CollectionUtilExt.head(records).timestamp(),
                initialOffset,
                MemoryRecords.withRecords(initialOffset, CompressionType.NONE, 0, records.toArray(new SimpleRecord[0])));
    }

    private FetchDataInfo readRecords() {
        return readRecords(log, 0L, log.segments.activeSegment().size(), false, log.logEndOffsetMetadata(), false);
    }

    private FetchDataInfo readRecords(Long startOffset) {
        return readRecords(log, startOffset, log.segments.activeSegment().size(), false, log.logEndOffsetMetadata(), false);
    }

    private FetchDataInfo readRecords(LocalLog log,
                                      Long startOffset,
                                      Integer maxLength,
                                      Boolean minOneMessage,
                                      LogOffsetMetadata maxOffsetMetadata,
                                      Boolean includeAbortedTxns) {
        return log.read(startOffset,
                maxLength,
                minOneMessage,
                maxOffsetMetadata,
                includeAbortedTxns);
    }

    @Test
    public void testLogDeleteSegmentsSuccess() throws IOException {
        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        appendRecords(Arrays.asList(record), log, 0L);
        log.roll(Optional.empty());
        assertEquals(2, log.segments.numberOfSegments());
        assertFalse(ArrayUtils.isEmpty(logDir.listFiles()));
        List<LogSegment> segmentsBeforeDelete = new ArrayList<>(log.segments.values());
        Collection<LogSegment> deletedSegments = log.deleteAllSegments();
        assertTrue(log.segments.isEmpty());
        assertEquals(segmentsBeforeDelete, deletedSegments);
        assertThrows(KafkaStorageException.class, () -> log.checkIfMemoryMappedBufferClosed());
        assertTrue(logDir.exists());
    }

    @Test
    public void testRollEmptyActiveSegment() {
        LogSegment oldActiveSegment = log.segments.activeSegment();
        log.roll(Optional.empty());
        assertEquals(1, log.segments.numberOfSegments());
        assertNotEquals(oldActiveSegment, log.segments.activeSegment());
        assertFalse(ArrayUtils.isEmpty(logDir.listFiles()));
        assertTrue(oldActiveSegment.hasSuffix(LocalLog.DeletedFileSuffix));
    }

    @Test
    public void testLogDeleteDirSuccessWhenEmptyAndFailureWhenNonEmpty() throws IOException {
        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        appendRecords(Arrays.asList(record), log, 0L);
        log.roll(Optional.empty());
        assertEquals(2, log.segments.numberOfSegments());
        assertFalse(ArrayUtils.isEmpty(logDir.listFiles()));

        assertThrows(IllegalStateException.class, () -> log.deleteEmptyDir());
        assertTrue(logDir.exists());

        log.deleteAllSegments();
        log.deleteEmptyDir();
        assertFalse(logDir.exists());
    }

    @Test
    public void testUpdateConfig() {
        LogConfig oldConfig = log.config;
        assertEquals(oldConfig, log.config);

        LogConfig newConfig = LogTestUtils.createLogConfig(Defaults.SegmentMs,
                oldConfig.getSegmentSize() + 1,
                Defaults.RetentionMs,
                Defaults.RetentionSize,
                Defaults.SegmentJitterMs,
                Defaults.CleanupPolicy,
                Defaults.MaxMessageSize,
                Defaults.IndexInterval,
                Defaults.MaxIndexSize,
                Defaults.FileDeleteDelayMs);
        log.updateConfig(newConfig);
        assertEquals(newConfig, log.config);
    }

    @Test
    public void testLogDirRenameToNewDir() throws IOException {
        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        appendRecords(Arrays.asList(record), log, 0L);
        log.roll(Optional.empty());
        assertEquals(2, log.segments.numberOfSegments());
        File newLogDir = TestUtils.randomPartitionLogDir(tmpDir);
        assertTrue(log.renameDir(newLogDir.getName()));
        assertFalse(logDir.exists());
        assertTrue(newLogDir.exists());
        assertEquals(newLogDir, log.dir());
        assertEquals(newLogDir.getParent(), log.parentDir());
        assertEquals(newLogDir.getParent(), log.dir().getParent());
        for (LogSegment segment : log.segments.values()) {
            assertEquals(newLogDir.getPath(), segment.getLog().file().getParentFile().getPath());
        }
        assertEquals(2, log.segments.numberOfSegments());
    }

    @Test
    public void testLogDirRenameToExistingDir() {
        assertFalse(log.renameDir(log.dir().getName()));
    }

    @Test
    public void testLogFlush() throws IOException {
        assertEquals(0L, log.recoveryPoint);
        assertEquals(mockTime.milliseconds(), log.lastFlushTime());

        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        appendRecords(Arrays.asList(record), log, 0L);
        mockTime.sleep(1);
        LogSegment newSegment = log.roll(Optional.empty());
        log.flush(newSegment.getBaseOffset());
        log.markFlushed(newSegment.getBaseOffset());
        assertEquals(1L, log.recoveryPoint);
        assertEquals(mockTime.milliseconds(), log.lastFlushTime());
    }

    @Test
    public void testLogAppend() throws IOException {
        FetchDataInfo fetchDataInfoBeforeAppend = readRecords(log, 0L, 1, false, log.logEndOffsetMetadata(), false);
        assertTrue(!fetchDataInfoBeforeAppend.getRecords().records().iterator().hasNext());

        mockTime.sleep(1);
        List<KeyValue> keyValues = Arrays.asList(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), log, 0L);
        assertEquals(2L, log.logEndOffset());
        assertEquals(0L, log.recoveryPoint);
        FetchDataInfo fetchDataInfo = readRecords(0L);
        assertEquals(2L, CollectionUtilExt.size(fetchDataInfo.getRecords().records()));
        assertEquals(keyValues, recordsToKvs(ImmutableList.copyOf(fetchDataInfo.getRecords().records())));
    }

    @Test
    public void testLogCloseSuccess() throws IOException {
        List<KeyValue> keyValues = Arrays.asList(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), log, 0L);
        log.close();
        assertThrows(ClosedChannelException.class, () -> appendRecords(kvsToRecords(keyValues), log, 2L));
    }

    @Test
    public void testLogCloseIdempotent() {
        log.close();
        // Check that LocalLog.close() is idempotent
        log.close();
    }

    @Test
    public void testLogCloseFailureWhenInMemoryBufferClosed() throws IOException {
        List<KeyValue> keyValues = Arrays.asList(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), log, 0L);
        log.closeHandlers();
        assertThrows(KafkaStorageException.class, () -> log.close());
    }

    @Test
    public void testLogCloseHandlers() throws IOException {
        List<KeyValue> keyValues = Arrays.asList(new KeyValue("abc", "ABC"), new KeyValue("de", "DE"));
        appendRecords(kvsToRecords(keyValues), log, 0L);
        log.closeHandlers();
        assertThrows(ClosedChannelException.class, () -> appendRecords(kvsToRecords(keyValues), log, 2L));
    }

    @Test
    public void testLogCloseHandlersIdempotent() {
        log.closeHandlers();
        // Check that LocalLog.closeHandlers() is idempotent
        log.closeHandlers();
    }

    private void testRemoveAndDeleteSegments(Boolean asyncDelete) throws IOException {
        for (int offset = 0; offset <= 8; offset++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(record), log, (long) offset);
            log.roll(Optional.empty());
        }

        assertEquals(10, log.segments.numberOfSegments());

        TestDeletionReason reason = new TestDeletionReason();
        List<LogSegment> toDelete = new ArrayList<>(log.segments.values());
        log.removeAndDeleteSegments(toDelete, asyncDelete, reason);
        if (asyncDelete) {
            mockTime.sleep(log.config.getFileDeleteDelayMs() + 1);
        }
        assertTrue(log.segments.isEmpty());
        assertEquals(toDelete, reason.deletedSegments());
        for (LogSegment segment : toDelete) {
            assertTrue(segment.deleted());
        }
    }

    class TestDeletionReason implements SegmentDeletionReason {
        private Collection<LogSegment> _deletedSegments = new ArrayList<>();

        @Override
        public void logReason(List<LogSegment> toDelete) {
            _deletedSegments = new ArrayList<>(toDelete);
        }

        public Collection<LogSegment> deletedSegments() {
            return _deletedSegments;
        }
    }

    @Test
    public void testRemoveAndDeleteSegmentsSync() throws IOException {
        testRemoveAndDeleteSegments(false);
    }

    @Test
    public void testRemoveAndDeleteSegmentsAsync() throws IOException {
        testRemoveAndDeleteSegments(true);
    }

    private void testDeleteSegmentFiles(Boolean asyncDelete) throws IOException {
        for (int offset = 0; offset <= 8; offset++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(record), log, (long) offset);
            log.roll(Optional.empty());
        }

        assertEquals(10, log.segments.numberOfSegments());

        List<LogSegment> toDelete = new ArrayList<>(log.segments.values());
        LocalLog.deleteSegmentFiles(toDelete, asyncDelete, log.dir(), log.topicPartition, log.config, log.scheduler, log.logDirFailureChannel, "");
        if (asyncDelete) {
            for (LogSegment segment : toDelete) {
                assertFalse(segment.deleted());
                assertTrue(segment.hasSuffix(LocalLog.DeletedFileSuffix));
            }
            mockTime.sleep(log.config.getFileDeleteDelayMs() + 1);
        }
        for (LogSegment segment : toDelete) {
            assertTrue(segment.deleted());
        }
    }

    @Test
    public void testDeleteSegmentFilesSync() throws IOException {
        testDeleteSegmentFiles(false);
    }

    @Test
    public void testDeleteSegmentFilesAsync() throws IOException {
        testDeleteSegmentFiles(true);
    }

    @Test
    public void testDeletableSegmentsFilter() throws IOException {
        for (int offset = 0; offset <= 8; offset++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(record), log, (long) offset);
            log.roll(Optional.empty());
        }

        assertEquals(10, log.segments.numberOfSegments());

        {
            Collection<LogSegment> deletable = log.deletableSegments((segment, segmentOptional) -> segment.getBaseOffset() <= 5);
            List<LogSegment> expected = new ArrayList<>();
            for (LogSegment segment : log.segments.nonActiveLogSegmentsFrom(0L)) {
                if (segment.getBaseOffset() <= 5) {
                    expected.add(segment);
                }
            }
            assertEquals(6, expected.size());
            assertEquals(expected, new ArrayList<>(deletable));
        }

        {
            Collection<LogSegment> deletable = log.deletableSegments((segment, segmentOptional) -> true);
            ImmutableList<LogSegment> expected = ImmutableList.copyOf(log.segments.nonActiveLogSegmentsFrom(0L));
            assertEquals(9, expected.size());
            assertEquals(expected, ImmutableList.copyOf(deletable));
        }

        {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(record), log, 9L);
            Collection<LogSegment> deletable = log.deletableSegments((segment, segmentOptional) -> true);
            ImmutableList<LogSegment> expected = ImmutableList.copyOf(log.segments.values());
            assertEquals(10, expected.size());
            assertEquals(expected, ImmutableList.copyOf(deletable));
        }
    }

    @Test
    public void testDeletableSegmentsIteration() throws IOException {
        for (int offset = 0; offset <= 8; offset++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(record), log, (long) offset);
            log.roll(Optional.empty());
        }

        assertEquals(10, log.segments.numberOfSegments());

        final int[] offsetArr = new int[]{0};
        Collection<LogSegment> deletableSegments = log.deletableSegments(
                (segment, nextSegmentOpt) -> {
                    assertEquals(offsetArr[0], segment.getBaseOffset());
                    Optional<LogSegment> floorSegmentOpt = log.segments.floorSegment((long) offsetArr[0]);
                    assertTrue(floorSegmentOpt.isPresent());
                    assertEquals(floorSegmentOpt.get(), segment);
                    if (offsetArr[0] == log.logEndOffset()) {
                        assertFalse(nextSegmentOpt.isPresent());
                    } else {
                        assertTrue(nextSegmentOpt.isPresent());
                        Optional<LogSegment> higherSegmentOpt = log.segments.higherSegment(segment.getBaseOffset());
                        assertTrue(higherSegmentOpt.isPresent());
                        assertEquals(segment.getBaseOffset() + 1, higherSegmentOpt.get().getBaseOffset());
                        assertEquals(higherSegmentOpt.get(), nextSegmentOpt.get());
                    }
                    offsetArr[0] += 1;
                    return true;
                });
        assertEquals(10, log.segments.numberOfSegments());
        assertEquals(ImmutableList.copyOf(log.segments.nonActiveLogSegmentsFrom(0L)), ImmutableList.copyOf(deletableSegments));
    }

    @Test
    public void testCreateAndDeleteSegment() throws IOException {
        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        appendRecords(Arrays.asList(record), log, 0L);
        long newOffset = log.segments.activeSegment().getBaseOffset() + 1;
        LogSegment oldActiveSegment = log.segments.activeSegment();
        LogSegment newActiveSegment = log.createAndDeleteSegment(newOffset,
                log.segments.activeSegment(),
                true,
                new SegmentDeletionReason.LogTruncation(log));
        assertEquals(1, log.segments.numberOfSegments());
        assertEquals(newActiveSegment, log.segments.activeSegment());
        assertNotEquals(oldActiveSegment, log.segments.activeSegment());
        assertTrue(oldActiveSegment.hasSuffix(LocalLog.DeletedFileSuffix));
        assertEquals(newOffset, log.segments.activeSegment().getBaseOffset());
        assertEquals(0L, log.recoveryPoint);
        assertEquals(newOffset, log.logEndOffset());
        FetchDataInfo fetchDataInfo = readRecords(newOffset);
        assertTrue(!fetchDataInfo.getRecords().records().iterator().hasNext());
    }

    @Test
    public void testTruncateFullyAndStartAt() throws IOException {
        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        for (int offset = 0; offset <= 7; offset++) {
            appendRecords(Arrays.asList(record), log, (long) offset);
            if (offset % 2 != 0) {
                log.roll(Optional.empty());
            }
        }
        for (int offset = 8; offset <= 12; offset++) {
            SimpleRecord tmpRecord = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(tmpRecord), log, (long) offset);
        }
        assertEquals(5, log.segments.numberOfSegments());
        assertNotEquals(10L, log.segments.activeSegment().getBaseOffset());
        List<LogSegment> expected = new ArrayList<>(log.segments.values());
        Iterable<LogSegment> deleted = log.truncateFullyAndStartAt(10L);
        assertEquals(expected, deleted);
        assertEquals(1, log.segments.numberOfSegments());
        assertEquals(10L, log.segments.activeSegment().getBaseOffset());
        assertEquals(0L, log.recoveryPoint);
        assertEquals(10L, log.logEndOffset());
        FetchDataInfo fetchDataInfo = readRecords(10L);
        assertTrue(!fetchDataInfo.getRecords().records().iterator().hasNext());
    }

    @Test
    public void testTruncateTo() throws IOException {
        for (int offset = 0; offset <= 11; offset++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
            appendRecords(Arrays.asList(record), log, (long) offset);
            if (offset % 3 == 2) {
                log.roll(Optional.empty());
            }
        }
        assertEquals(5, log.segments.numberOfSegments());
        assertEquals(12L, log.logEndOffset());

        List<LogSegment> expected = new ArrayList<>(log.segments.values(9L, log.logEndOffset() + 1));
        // Truncate to an offset before the base offset of the active segment
        Collection<LogSegment> deleted = log.truncateTo(7L);
        assertEquals(expected, deleted);
        assertEquals(3, log.segments.numberOfSegments());
        assertEquals(6L, log.segments.activeSegment().getBaseOffset());
        assertEquals(0L, log.recoveryPoint);
        assertEquals(7L, log.logEndOffset());
        FetchDataInfo fetchDataInfo = readRecords(6L);
        assertEquals(1, CollectionUtilExt.size(fetchDataInfo.getRecords().records()));
        assertEquals(Arrays.asList(new KeyValue("", "a")), recordsToKvs(ImmutableList.copyOf(fetchDataInfo.getRecords().records())));

        // Verify that we can still append to the active segment
        SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), "a".getBytes());
        appendRecords(Arrays.asList(record), log, 7L);
        assertEquals(8L, log.logEndOffset());
    }

    @Test
    public void testNonActiveSegmentsFrom() throws IOException {
        for (int i = 0; i < 5; i++) {
            List<KeyValue> keyValues = Arrays.asList(new KeyValue(Integer.toString(i), Integer.toString(i)));
            appendRecords(kvsToRecords(keyValues), log, (long) i);
            log.roll(Optional.empty());
        }

        assertEquals(5L, log.segments.activeSegment().getBaseOffset());
        assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), nonActiveBaseOffsetsFrom(0L));
        assertEquals(new ArrayList<>(), nonActiveBaseOffsetsFrom(5L));
        assertEquals(Arrays.asList(2L, 3L, 4L), nonActiveBaseOffsetsFrom(2L));
        assertEquals(new ArrayList<>(), nonActiveBaseOffsetsFrom(6L));
    }

    private List<Long> nonActiveBaseOffsetsFrom(Long startOffset) {
        List<Long> res = new ArrayList<>();
        for (LogSegment segment : log.segments.nonActiveLogSegmentsFrom(startOffset)) {
            res.add(segment.getBaseOffset());
        }
        return res;
    }

    private String topicPartitionName(String topic, String partition) {
        return topic + "-" + partition;
    }

    @Test
    public void testParseTopicPartitionName() {
        String topic = "test_topic";
        String partition = "143";
        File dir = new File(logDir, topicPartitionName(topic, partition));
        TopicPartition topicPartition = LocalLog.parseTopicPartitionName(dir);
        assertEquals(topic, topicPartition.topic());
        assertEquals(Integer.parseInt(partition), topicPartition.partition());
    }

    /**
     * Tests that log directories with a period in their name that have been marked for deletion
     * are parsed correctly by `Log.parseTopicPartitionName` (see KAFKA-5232 for details).
     */
    @Test
    public void testParseTopicPartitionNameWithPeriodForDeletedTopic() {
        String topic = "foo.bar-testtopic";
        String partition = "42";
        File dir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, Integer.parseInt(partition))));
        TopicPartition topicPartition = LocalLog.parseTopicPartitionName(dir);
        assertEquals(topic, topicPartition.topic(), "Unexpected topic name parsed");
        assertEquals(Integer.parseInt(partition), topicPartition.partition(), "Unexpected partition number parsed");
    }

    @Test
    public void testParseTopicPartitionNameForEmptyName() {
        File dir = new File("");
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir));
    }

    @Test
    public void testParseTopicPartitionNameForNull() {
        File dir = null;
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + dir);
    }

    @Test
    public void testParseTopicPartitionNameForMissingSeparator() {
        String topic = "test_topic";
        String partition = "1999";
        File dir = new File(logDir, topic + partition);
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir));
        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, topic + partition + "." + LocalLog.DeleteDirSuffix);
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(deleteMarkerDir));
    }

    @Test
    public void testParseTopicPartitionNameForMissingTopic() {
        String topic = "";
        String partition = "1999";
        File dir = new File(logDir, topicPartitionName(topic, partition));
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir));

        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, Integer.parseInt(partition))));

        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(deleteMarkerDir));
    }

    @Test
    public void testParseTopicPartitionNameForMissingPartition() {
        String topic = "test_topic";
        String partition = "";
        File dir = new File(logDir.getPath() + topicPartitionName(topic, partition));
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir));

        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, topicPartitionName(topic, partition) + "." + LocalLog.DeleteDirSuffix);
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(deleteMarkerDir));
    }

    @Test
    public void testParseTopicPartitionNameForInvalidPartition() {
        String topic = "test_topic";
        String partition = "1999a";
        File dir = new File(logDir, topicPartitionName(topic, partition));
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir));

        // also test the "-delete" marker case
        File deleteMarkerDir = new File(logDir, topic + partition + "." + LocalLog.DeleteDirSuffix);
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(deleteMarkerDir),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(deleteMarkerDir));
    }

    @Test
    public void testParseTopicPartitionNameForExistingInvalidDir() {
        File dir1 = new File(logDir.getPath() + "/non_kafka_dir");
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir1),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir1));
        File dir2 = new File(logDir.getPath() + "/non_kafka_dir-delete");
        assertThrows(KafkaException.class, () -> LocalLog.parseTopicPartitionName(dir2),
                () -> "KafkaException should have been thrown for dir: " + getCanonicalPath(dir2));
    }

    private String getCanonicalPath(File file) {
        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            return "";
        }
    }

    @Test
    public void testLogDeleteDirName() {
        String name1 = LocalLog.logDeleteDirName(new TopicPartition("foo", 3));
        assertTrue(name1.length() <= 255);
        assertTrue(Pattern.compile("foo-3\\.[0-9a-z]{32}-delete").matcher(name1).matches());
        assertTrue(LocalLog.DeleteDirPattern.matcher(name1).matches());
        assertFalse(LocalLog.FutureDirPattern.matcher(name1).matches());
        String name2 = LocalLog.logDeleteDirName(
                new TopicPartition("n" + String.join("", Collections.nCopies(248, "o")), 5));
        assertEquals(255, name2.length());
        assertTrue(Pattern.compile("n[o]{212}-5\\.[0-9a-z]{32}-delete").matcher(name2).matches());
        assertTrue(LocalLog.DeleteDirPattern.matcher(name2).matches());
        assertFalse(LocalLog.FutureDirPattern.matcher(name2).matches());
    }

    @Test
    public void testOffsetFromFile() {
        long offset = 23423423L;

        File logFile = LocalLog.logFile(tmpDir, offset, "");
        assertEquals(offset, LocalLog.offsetFromFile(logFile));

        File offsetIndexFile = LocalLog.offsetIndexFile(tmpDir, offset, "");
        assertEquals(offset, LocalLog.offsetFromFile(offsetIndexFile));

        File timeIndexFile = LocalLog.timeIndexFile(tmpDir, offset, "");
        assertEquals(offset, LocalLog.offsetFromFile(timeIndexFile));
    }

    @Test
    public void testRollSegmentThatAlreadyExists() throws IOException {
        assertEquals(1, log.segments.numberOfSegments(), "Log begins with a single empty segment.");

        // roll active segment with the same base offset of size zero should recreate the segment
        log.roll(Optional.of(0L));
        assertEquals(1, log.segments.numberOfSegments(), "Expect 1 segment after roll() empty segment with base offset.");

        // should be able to append records to active segment
        List<KeyValue> keyValues1 = Arrays.asList(new KeyValue("k1", "v1"));
        appendRecords(kvsToRecords(keyValues1), log, 0L);
        assertEquals(0L, log.segments.activeSegment().getBaseOffset());
        // make sure we can append more records
        List<KeyValue> keyValues2 = Arrays.asList(new KeyValue("k2", "v2"));

        appendRecords(keyValues2.stream()
                        .map(kv -> kv.toRecord(mockTime.milliseconds() + 10))
                        .collect(Collectors.toList()),
                log,
                1L);
        assertEquals(2, log.logEndOffset(), "Expect two records in the log");
        FetchDataInfo readResult = readRecords();
        assertEquals(2L, CollectionUtilExt.size(readResult.getRecords().records()));
        ArrayList<KeyValue> kvs = new ArrayList<>(keyValues1);
        kvs.addAll(keyValues2);
        assertEquals(kvs, recordsToKvs(ImmutableList.copyOf(readResult.getRecords().records())));

        // roll so that active segment is empty
        log.roll(Optional.empty());
        assertEquals(2L, log.segments.activeSegment().getBaseOffset(), "Expect base offset of active segment to be LEO");
        assertEquals(2, log.segments.numberOfSegments(), "Expect two segments.");
        assertEquals(2L, log.logEndOffset());
    }

    @Test
    public void testNewSegmentsAfterRoll() throws IOException {
        assertEquals(1, log.segments.numberOfSegments(), "Log begins with a single empty segment.");

        // roll active segment with the same base offset of size zero should recreate the segment
        {
            LogSegment newSegment = log.roll(Optional.empty());
            assertEquals(0L, newSegment.getBaseOffset());
            assertEquals(1, log.segments.numberOfSegments());
            assertEquals(0L, log.logEndOffset());
        }

        appendRecords(Arrays.asList(new KeyValue("k1", "v1").toRecord(mockTime.milliseconds())),
                log,
                0L);

        {
            LogSegment newSegment = log.roll(Optional.empty());
            assertEquals(1L, newSegment.getBaseOffset());
            assertEquals(2, log.segments.numberOfSegments());
            assertEquals(1L, log.logEndOffset());
        }

        appendRecords(Arrays.asList(new KeyValue("k2", "v2").toRecord(mockTime.milliseconds())), log, 1L);

        {
            LogSegment newSegment = log.roll(Optional.of(1L));
            assertEquals(2L, newSegment.getBaseOffset());
            assertEquals(3, log.segments.numberOfSegments());
            assertEquals(2L, log.logEndOffset());
        }
    }

    @Test
    public void testRollSegmentErrorWhenNextOffsetIsIllegal() throws IOException {
        assertEquals(1, log.segments.numberOfSegments(), "Log begins with a single empty segment.");

        List<KeyValue> keyValues = Arrays.asList(new KeyValue("k1", "v1"), new KeyValue("k2", "v2"), new KeyValue("k3", "v3"));
        appendRecords(kvsToRecords(keyValues), log, 0L);
        assertEquals(0L, log.segments.activeSegment().getBaseOffset());
        assertEquals(3, log.logEndOffset(), "Expect two records in the log");

        // roll to create an empty active segment
        log.roll(Optional.empty());
        assertEquals(3L, log.segments.activeSegment().getBaseOffset());

        // intentionally setup the logEndOffset to introduce an error later
        log.updateLogEndOffset(1L);

        // expect an error because of attempt to roll to a new offset (1L) that's lower than the
        // base offset (3L) of the active segment
        assertThrows(KafkaException.class, () -> log.roll(Optional.empty()));
    }

    private LocalLog createLocalLogWithActiveSegment(LogConfig config) throws IOException {
        return createLocalLogWithActiveSegment(logDir,
                config,
                new LogSegments(topicPartition),
                0L,
                new LogOffsetMetadata(0L, 0L, 0),
                mockTime.getScheduler(),
                mockTime,
                topicPartition,
                logDirFailureChannel);
    }

    private LocalLog createLocalLogWithActiveSegment(File dir,
                                                     LogConfig config,
                                                     LogSegments segments,
                                                     Long recoveryPoint,
                                                     LogOffsetMetadata nextOffsetMetadata,
                                                     Scheduler scheduler,
                                                     Time time,
                                                     TopicPartition topicPartition,
                                                     LogDirFailureChannel logDirFailureChannel) throws IOException {
        segments.add(LogSegment.open(dir,
                0L,
                config,
                time,
                false,
                config.initFileSize(),
                config.getPreallocate(),
                ""));
        return new LocalLog(dir,
                config,
                segments,
                recoveryPoint,
                nextOffsetMetadata,
                scheduler,
                time,
                topicPartition,
                logDirFailureChannel);
    }
}
