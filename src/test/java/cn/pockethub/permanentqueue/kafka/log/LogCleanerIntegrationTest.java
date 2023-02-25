package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.TestUtils;
import cn.pockethub.permanentqueue.kafka.server.metrics.KafkaYammerMetrics;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Predicate;

import static cn.pockethub.permanentqueue.kafka.utils.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.*;

public class LogCleanerIntegrationTest extends AbstractLogCleanerIntegrationTest {

    private CompressionType codec = CompressionType.LZ4;

    private MockTime time = new MockTime();
    private List<TopicPartition> topicPartitions = Arrays.asList(
            new TopicPartition("log", 0),
            new TopicPartition("log", 1),
            new TopicPartition("log", 2)
    );

    @AfterEach
    public void cleanup() {
        TestUtils.clearYammerMetrics();
    }

//    @Test
    public void testMarksPartitionsAsOfflineAndPopulatesUncleanableMetrics() throws IOException, NoSuchAlgorithmException, InterruptedException {
        int largeMessageKey = 20;
        Pair<String, MemoryRecords> pair = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE, codec);
        MemoryRecords largeMessageSet = pair.getValue();
        int maxMessageSize = largeMessageSet.sizeInBytes();
        cleaner = makeCleaner(topicPartitions, 100L, maxMessageSize);

        breakPartitionLog(topicPartitions.get(0));
        breakPartitionLog(topicPartitions.get(1));

        cleaner.startup();

        UnifiedLog log = cleaner.getLogs().get(topicPartitions.get(0));
        UnifiedLog log2 = cleaner.getLogs().get(topicPartitions.get(1));
        String uncleanableDirectory = log.dir().getParent();
        Gauge<Integer> uncleanablePartitionsCountGauge = getGauge("uncleanable-partitions-count", uncleanableDirectory);
        Gauge<Long> uncleanableBytesGauge = getGauge("uncleanable-bytes", uncleanableDirectory);

        TestUtils.waitUntilTrue(() -> uncleanablePartitionsCountGauge.value() == 2,
                "There should be 2 uncleanable partitions",
                2000L,
                100L);
        long expectedTotalUncleanableBytes = LogCleanerManager.calculateCleanableBytes(log, 0L, CollectionUtilExt.last(log.logSegments()).getBaseOffset()).getValue() +
                LogCleanerManager.calculateCleanableBytes(log2, 0L, CollectionUtilExt.last(log2.logSegments()).getBaseOffset()).getValue();
        TestUtils.waitUntilTrue(() -> uncleanableBytesGauge.value() == expectedTotalUncleanableBytes,
                String.format("There should be %s uncleanable bytes", expectedTotalUncleanableBytes),
                1000L,
                100L);

        Set<TopicPartition> uncleanablePartitions = cleaner.getCleanerManager().uncleanablePartitions(uncleanableDirectory);
        assertTrue(uncleanablePartitions.contains(topicPartitions.get(0)));
        assertTrue(uncleanablePartitions.contains(topicPartitions.get(1)));
        assertFalse(uncleanablePartitions.contains(topicPartitions.get(2)));

        // Delete one partition
        cleaner.getLogs().remove(topicPartitions.get(0));
        TestUtils.waitUntilTrue(
                () -> {
                    time.sleep(1000);
                    return uncleanablePartitionsCountGauge.value() == 1;
                },
                "There should be 1 uncleanable partitions",
                2000L,
                100L);

        Set<TopicPartition> uncleanablePartitions2 = cleaner.getCleanerManager().uncleanablePartitions(uncleanableDirectory);
        assertFalse(uncleanablePartitions2.contains(topicPartitions.get(0)));
        assertTrue(uncleanablePartitions2.contains(topicPartitions.get(1)));
        assertFalse(uncleanablePartitions2.contains(topicPartitions.get(2)));
    }

    private void breakPartitionLog(TopicPartition tp) throws FileNotFoundException {
        UnifiedLog log = cleaner.getLogs().get(tp);
        writeDups(20, 3, log, codec, 0, RecordBatch.CURRENT_MAGIC_VALUE);

        File partitionFile = CollectionUtilExt.last(log.logSegments()).getLog().file();
        PrintWriter writer = new PrintWriter(partitionFile);
        writer.write("jogeajgoea");
        writer.close();

        writeDups(20, 3, log, codec, 0, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    private <T> Gauge<T> getGauge(Predicate<MetricName> filter) {
        Optional<Map.Entry<MetricName, Metric>> first = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()
                .stream()
                .filter(entry -> filter.test(entry.getKey()))
                .findFirst();
        if (first.isPresent()) {
            return (Gauge<T>) first.get().getValue();
        } else {
            return fail("Unable to find metric");
        }
    }

    private <T> Gauge<T> getGauge(String metricName) {
        return getGauge(mName -> mName.getName().endsWith(metricName) && mName.getScope() == null);
    }

    private <T> Gauge<T> getGauge(String metricName, String metricScope) {
        return getGauge(k -> k.getName().endsWith(metricName) && k.getScope().endsWith(metricScope));
    }

//    @Test
    public void testMaxLogCompactionLag() throws NoSuchAlgorithmException, IOException, InterruptedException {
        int msPerHour = 60 * 60 * 1000;

        long minCompactionLagMs = 1 * msPerHour;
        long maxCompactionLagMs = 6 * msPerHour;

        long cleanerBackOffMs = 200L;
        int segmentSize = 512;
        List<TopicPartition> topicPartitions = Arrays.asList(
                new TopicPartition("log", 0),
                new TopicPartition("log", 1),
                new TopicPartition("log", 2)
        );
        float minCleanableDirtyRatio = 1.0F;

        cleaner = makeCleaner(topicPartitions,
                minCleanableDirtyRatio,
                cleanerBackOffMs,
                minCompactionLagMs,
                segmentSize,
                maxCompactionLagMs);
        UnifiedLog log = cleaner.getLogs().get(topicPartitions.get(0));

        long T0 = time.milliseconds();
        writeKeyDups(100, 3, log, CompressionType.NONE, T0, 0, 1);

        long startSizeBlock0 = log.size();

        LogSegment activeSegAtT0 = log.activeSegment();

        cleaner.startup();

        // advance to a time still less than maxCompactionLagMs from start
        time.sleep(maxCompactionLagMs / 2);
        Thread.sleep(5 * cleanerBackOffMs); // give cleaning thread a chance to _not_ clean
        assertEquals(startSizeBlock0, log.size(), "There should be no cleaning until the max compaction lag has passed");

        // advance to time a bit more than one maxCompactionLagMs from start
        time.sleep(maxCompactionLagMs / 2 + 1);
        long T1 = time.milliseconds();

        // write the second block of data: all zero keys
        List<Pair<Integer, Integer>> appends1 = writeKeyDups(100, 1, log, CompressionType.NONE, T1, 0, 0);

        // roll the active segment
        log.roll(Optional.empty());
        LogSegment activeSegAtT1 = log.activeSegment();
        long firstBlockCleanableSegmentOffset = activeSegAtT0.getBaseOffset();

        // the first block should get cleaned
        cleaner.awaitCleaned(new TopicPartition("log", 0), firstBlockCleanableSegmentOffset, 60000L);

        Collection<Pair<Integer, Integer>> read1 = readFromLog(log);
        long lastCleaned = cleaner.getCleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        assertTrue(lastCleaned >= firstBlockCleanableSegmentOffset,
                String.format("log cleaner should have processed at least to offset %s, but lastCleaned=%s",
                        firstBlockCleanableSegmentOffset, lastCleaned));

        //minCleanableDirtyRatio  will prevent second block of data from compacting
        assertNotEquals(appends1, read1, "log should still contain non-zero keys");

        time.sleep(maxCompactionLagMs + 1);
        // the second block should get cleaned. only zero keys left
        cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT1.getBaseOffset(), 60000L);

        Collection<Pair<Integer, Integer>> read2 = readFromLog(log);

        assertEquals(appends1, read2, "log should only contains zero keys now");

        long lastCleaned2 = cleaner.getCleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        long secondBlockCleanableSegmentOffset = activeSegAtT1.getBaseOffset();
        assertTrue(lastCleaned2 >= secondBlockCleanableSegmentOffset,
                String.format("log cleaner should have processed at least to offset %s, but lastCleaned=%s",
                        secondBlockCleanableSegmentOffset, lastCleaned2));
    }

    private Collection<Pair<Integer, Integer>> readFromLog(UnifiedLog log) throws UnsupportedEncodingException {
        List<Pair<Integer, Integer>> result = new ArrayList<>();
        for (LogSegment segment : log.logSegments()) {
            for (Record record : segment.getLog().records()) {
                Integer key = Integer.parseInt(TestUtils.readString(record.key()));
                Integer value = Integer.parseInt(TestUtils.readString(record.value()));
                result.add(Pair.of(key, value));
            }
        }
        return result;
    }

    private List<Pair<Integer, Integer>> writeKeyDups(Integer numKeys,
                                                      Integer numDups,
                                                      UnifiedLog log,
                                                      CompressionType codec,
                                                      Long timestamp,
                                                      Integer startValue,
                                                      Integer step) {
        List<Pair<Integer, Integer>> result = new ArrayList<>();
        int valCounter = startValue;
        for (int i = 0; i < numDups; i++) {
            for (Integer key = 0; key < numKeys; key++) {
                Integer curValue = valCounter;
                MemoryRecords memoryRecords = TestUtils.singletonRecords(curValue.toString().getBytes(),
                        key.toString().getBytes(),
                        codec,
                        timestamp,
                        RecordBatch.CURRENT_MAGIC_VALUE);
                log.appendAsLeader(memoryRecords, 0);
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                valCounter += step;
                result.add(Pair.of(key, curValue));
            }
        }
        return result;
    }

    @Test
    public void testIsThreadFailed() throws NoSuchAlgorithmException, IOException, InterruptedException {
        String metricName = "DeadThreadCount";
        cleaner = makeCleaner(topicPartitions, 100L, 100000);
        cleaner.startup();
        assertEquals(0, cleaner.deadThreadCount());
        // we simulate the unexpected error with an interrupt
        cleaner.getCleaners().forEach(Thread::interrupt);
        // wait until interruption is propagated to all the threads
        TestUtils.waitUntilTrue(() -> {
                    boolean result = true;
                    for (LogCleaner.CleanerThread cleanerThread : cleaner.getCleaners()) {
                        result = cleanerThread.isThreadFailed() && result;
                    }
                    return result;
                },
                "Threads didn't terminate unexpectedly",
                DEFAULT_MAX_WAIT_MS,
                100L);
        assertEquals(cleaner.getCleaners().size(), getGauge(metricName).value());
        assertEquals(cleaner.getCleaners().size(), cleaner.deadThreadCount());
    }
}
