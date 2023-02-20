package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.TestUtils;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Tag("integration")
public abstract class AbstractLogCleanerIntegrationTest extends KafkaMetricsGroup {

    private static final String alphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public LogCleaner cleaner;
    public File logDir = TestUtils.tempDir();

    private List<UnifiedLog> logs = new ArrayList<>();
    private int defaultMaxMessageSize = 128;
    private float defaultMinCleanableDirtyRatio = 0.0F;
    private long defaultMinCompactionLagMS = 0L;
    private int defaultDeleteDelay = 1000;
    private int defaultSegmentSize = 2048;
    private long defaultMaxCompactionLagMs = Long.MAX_VALUE;

    private int ctr = 0;

    public MockTime time() {
        return new MockTime();
    }

    @AfterEach
    public void teardown() throws IOException, InterruptedException {
        if (cleaner != null) {
            cleaner.shutdown();
        }
        time().getScheduler().shutdown();
        for (UnifiedLog log : logs) {
            log.close();
        }
        Utils.delete(logDir);
    }

    public Properties logConfigProperties(Properties propertyOverrides,
                                          Integer maxMessageSize,
                                          Float minCleanableDirtyRatio,
                                          Long minCompactionLagMs,
                                          Integer deleteDelay,
                                          Integer segmentSize,
                                          Long maxCompactionLagMs) {
        Properties props = new Properties();
        props.put(LogConfig.MaxMessageBytesProp, maxMessageSize);
        props.put(LogConfig.SegmentBytesProp, segmentSize);
        props.put(LogConfig.SegmentIndexBytesProp, 100 * 1024);
        props.put(LogConfig.FileDeleteDelayMsProp, deleteDelay);
        props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact);
        props.put(LogConfig.MinCleanableDirtyRatioProp, minCleanableDirtyRatio);
        props.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.toString(Long.MAX_VALUE));
        props.put(LogConfig.MinCompactionLagMsProp, minCompactionLagMs);
        props.put(LogConfig.MaxCompactionLagMsProp, maxCompactionLagMs);
        props.putAll(propertyOverrides);
        return props;
    }

    public LogCleaner makeCleaner(Collection<TopicPartition> partitions,
                                  Long backOffMs,
                                  Integer maxMessageSize) throws IOException, NoSuchAlgorithmException {
        return makeCleaner(partitions,
                defaultMinCleanableDirtyRatio,
                1,
                backOffMs,
                maxMessageSize,
                defaultMinCompactionLagMS,
                defaultDeleteDelay,
                defaultSegmentSize,
                defaultMaxCompactionLagMs,
                Optional.empty(),
                new Properties());
    }

    public LogCleaner makeCleaner(Collection<TopicPartition> partitions,
                                  Float minCleanableDirtyRatio,
                                  Long backOffMs,
                                  Long minCompactionLagMs,
                                  Integer segmentSize,
                                  Long maxCompactionLagMs) throws IOException, NoSuchAlgorithmException {
        return makeCleaner(partitions,
                minCleanableDirtyRatio,
                1,
                backOffMs,
                defaultMaxMessageSize,
                minCompactionLagMs,
                defaultDeleteDelay,
                segmentSize,
                maxCompactionLagMs,
                Optional.empty(),
                new Properties());
    }

    public LogCleaner makeCleaner(Collection<TopicPartition> partitions,
                                  Long backOffMs,
                                  Long minCompactionLagMs,
                                  Integer segmentSize) throws IOException, NoSuchAlgorithmException {
        return makeCleaner(partitions,
                defaultMinCleanableDirtyRatio,
                1,
                backOffMs,
                defaultMaxMessageSize,
                minCompactionLagMs,
                defaultDeleteDelay,
                segmentSize,
                defaultMaxCompactionLagMs,
                Optional.empty(),
                new Properties());
    }

    public LogCleaner makeCleaner(Collection<TopicPartition> partitions,
                                  Float minCleanableDirtyRatio,
                                  Integer numThreads,
                                  Long backOffMs,
                                  Integer maxMessageSize,
                                  Long minCompactionLagMs,
                                  Integer deleteDelay,
                                  Integer segmentSize,
                                  Long maxCompactionLagMs,
                                  Optional<Integer> cleanerIoBufferSize,
                                  Properties propertyOverrides) throws IOException, NoSuchAlgorithmException {
        ConcurrentMap<TopicPartition, UnifiedLog> logMap = new ConcurrentHashMap<>();
        for (TopicPartition partition : partitions) {
            File dir = new File(logDir, String.format("%s-%s", partition.topic(), partition.partition()));
            Files.createDirectories(dir.toPath());

            LogConfig logConfig = new LogConfig(logConfigProperties(propertyOverrides,
                    maxMessageSize,
                    minCleanableDirtyRatio,
                    minCompactionLagMs,
                    deleteDelay,
                    segmentSize,
                    maxCompactionLagMs));
            UnifiedLog log = UnifiedLog.apply(
                    dir,
                    logConfig,
                    0L,
                    0L,
                    time().getScheduler(),
                    new BrokerTopicStats(),
                    time(),
                    5 * 60 * 1000,
                    60 * 60 * 1000,
                    LogManager.ProducerIdExpirationCheckIntervalMs,
                    new LogDirFailureChannel(10),
                    true,
                    Optional.empty(),
                    true,
                    new ConcurrentHashMap<>());
            logMap.put(partition, log);
            this.logs.add(log);
        }

        CleanerConfig cleanerConfig = new CleanerConfig(
                numThreads,
                4 * 1024 * 1024L,
                0.9d,
                cleanerIoBufferSize.orElse(maxMessageSize / 2),
                maxMessageSize,
                Double.MAX_VALUE,
                backOffMs,
                true,
                "MD5");
        return new LogCleaner(cleanerConfig,
                Arrays.asList(logDir),
                logMap,
                new LogDirFailureChannel(1),
                time());
    }

    public Integer counter() {
        return ctr;
    }

    public void incCounter() {
        ctr += 1;
    }

    public List<Triple<Integer, String, Long>> writeDups(Integer numKeys,
                                                         Integer numDups,
                                                         UnifiedLog log,
                                                         CompressionType codec,
                                                         Integer startKey,
                                                         Byte magicValue) {
        List<Triple<Integer, String, Long>> res = new ArrayList<>();
        for (int i = 0; i < numDups; i++) {
            for (int key = startKey; key < startKey + numKeys; key++) {
                String value = counter().toString();
                MemoryRecords memoryRecords = TestUtils.singletonRecords(value.getBytes(), Integer.toString(key).getBytes(), codec, RecordBatch.NO_TIMESTAMP, magicValue);
                LogAppendInfo appendInfo = log.appendAsLeader(memoryRecords, 0);
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                incCounter();
                res.add(Triple.of(key, value, appendInfo.getFirstOffset().get().getMessageOffset()));
            }
        }
        return res;
    }

    public Pair<String, MemoryRecords> createLargeSingleMessageSet(Integer key, Byte messageFormatVersion, CompressionType codec) {

        String value = messageValue(128);
        MemoryRecords messageSet = TestUtils.singletonRecords(value.getBytes(), key.toString().getBytes(), codec, RecordBatch.NO_TIMESTAMP, messageFormatVersion);
        return Pair.of(value, messageSet);
    }

    private String messageValue(Integer length) {
        Random random = new Random(0);
        char[] charArr = new char[length];
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(alphanumericChars.length());
            charArr[i] = alphanumericChars.charAt(index);
        }
        return new String(charArr);
    }
}
