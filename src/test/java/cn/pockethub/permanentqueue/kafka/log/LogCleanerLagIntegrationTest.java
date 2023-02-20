package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.TestUtils;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogCleanerLagIntegrationTest extends AbstractLogCleanerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogCleanerLagIntegrationTest.class);

    private int msPerHour = 60 * 60 * 1000;

    private long minCompactionLag = 1 * msPerHour;

    @BeforeEach
    public void setUp() throws Exception {
        assertTrue(minCompactionLag % 2 == 0, "compactionLag must be divisible by 2 for this test");
    }

    // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
    private MockTime time = new MockTime(1400000000000L, 1000L);
    private long cleanerBackOffMs = 200L;
    private int segmentSize = 512;

    private List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition("log", 0),
            new TopicPartition("log", 1),
            new TopicPartition("log", 2)
    );

    @ParameterizedTest
    @MethodSource({"parameters"})
    public void cleanerTest(CompressionType codec) throws IOException, NoSuchAlgorithmException, InterruptedException {
        cleaner = makeCleaner(topicPartitions,
                cleanerBackOffMs,
                minCompactionLag,
                segmentSize);
        UnifiedLog log = cleaner.getLogs().get(topicPartitions.get(0));

        // t = T0
        long T0 = time.milliseconds();
        Map<Integer, Integer> appends0 = writeDups(100, 3, log, codec, T0);
        long startSizeBlock0 = log.size();
        LOG.debug("total log size at T0: {}", startSizeBlock0);

        LogSegment activeSegAtT0 = log.activeSegment();
        LOG.debug("active segment at T0 has base offset: {}", activeSegAtT0.getBaseOffset());
        int sizeUpToActiveSegmentAtT0 = 0;
        for (LogSegment seg : log.logSegments(0L, activeSegAtT0.getBaseOffset())) {
            sizeUpToActiveSegmentAtT0 += seg.size();
        }
        LOG.debug("log size up to base offset of active segment at T0: {}", sizeUpToActiveSegmentAtT0);

        cleaner.startup();

        // T0 < t < T1
        // advance to a time still less than one compaction lag from start
        time.sleep(minCompactionLag / 2);
        // give cleaning thread a chance to _not_ clean
        Thread.sleep(5 * cleanerBackOffMs);
        assertEquals(startSizeBlock0, log.size(), "There should be no cleaning until the compaction lag has passed");

        // t = T1 > T0 + compactionLag
        // advance to time a bit more than one compaction lag from start
        time.sleep(minCompactionLag / 2 + 1);
        long T1 = time.milliseconds();

        // write another block of data
        Map<Integer, Integer> appends1 = new HashMap<>(appends0);
        appends1.putAll(writeDups(100, 3, log, codec, T1));
        long firstBlock1SegmentBaseOffset = activeSegAtT0.getBaseOffset();

        // the first block should get cleaned
        cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT0.getBaseOffset());

        // check the data is the same
        Map<Integer, Integer> read1 = readFromLog(log);
        assertEquals(appends1, read1, "Contents of the map shouldn't change.");

        int compactedSize = 0;
        for (LogSegment segment : log.logSegments(0L, activeSegAtT0.getBaseOffset())) {
            compactedSize += segment.size();
        }
        LOG.debug("after cleaning the compacted size up to active segment at T0: {}", compactedSize);
        long lastCleaned = cleaner.getCleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        assertTrue(lastCleaned >= firstBlock1SegmentBaseOffset,
                String.format("log cleaner should have processed up to offset %s, but lastCleaned=%s",
                        firstBlock1SegmentBaseOffset, lastCleaned)
        );
        assertTrue(sizeUpToActiveSegmentAtT0 > compactedSize,
                String.format("log should have been compacted: size up to offset of active segment at T0=%s compacted size=%s",
                        sizeUpToActiveSegmentAtT0, compactedSize)
        );
    }

    private Map<Integer, Integer> readFromLog(UnifiedLog log) throws UnsupportedEncodingException {
        Map<Integer, Integer> res = new HashMap<>();
        for (LogSegment segment : log.logSegments()) {
            for (Record record : segment.getLog().records()) {
                Integer key = Integer.parseInt(TestUtils.readString(record.key()));
                Integer value = Integer.parseInt(TestUtils.readString(record.value()));
                res.put(key, value);
            }
        }
        return res;
    }

    private Map<Integer, Integer> writeDups(Integer numKeys,
                                            Integer numDups,
                                            UnifiedLog log,
                                            CompressionType codec,
                                            Long timestamp) {
        Map<Integer, Integer> res = new HashMap<>();
        for (int i = 0; i < numDups; i++) {
            for (Integer key = 0; key < numKeys; key++) {
                Integer count = counter();
                log.appendAsLeader(TestUtils.singletonRecords(counter().toString().getBytes(),
                        key.toString().getBytes(),
                        codec, timestamp), 0);
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                incCounter();
                res.put(key, count);
            }
        }
        return res;
    }

    public static List<List<String>> oneParameter(){
        List<List<String>> l = new ArrayList<>();
        l.add(Arrays.asList("NONE"));
        return l;
    }

    public static Stream<Arguments> parameters(){
        return Arrays.stream(CompressionType.values()).map(Arguments::of);
    }

}
