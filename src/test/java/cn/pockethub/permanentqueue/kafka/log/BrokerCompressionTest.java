package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.TestUtils;
import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.FetchDataInfo;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static cn.pockethub.permanentqueue.kafka.server.FetchIsolation.FetchLogEnd;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BrokerCompressionTest {

    private File tmpDir = TestUtils.tempDir();
    private File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private MockTime time = new MockTime(0L, 0L);
    private LogConfig logConfig = new LogConfig();

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(tmpDir);
    }

    /**
     * Test broker-side compression configuration
     */
    @ParameterizedTest
    @MethodSource({"parameters"})
    public void testBrokerSideCompression(String messageCompression, String brokerCompression) throws IOException {
        CompressionCodec messageCompressionCode = CompressionCodec.getCompressionCodec(messageCompression);
        Properties logProps = new Properties();
        logProps.put(LogConfig.CompressionTypeProp, brokerCompression);
        /*configure broker-side compression  */
        UnifiedLog log = UnifiedLog.apply(
                logDir,
                new LogConfig(logProps),
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

        /* append two messages */
        MemoryRecords memoryRecords = MemoryRecords.withRecords(CompressionType.forId(messageCompressionCode.getCodec()), 0,
                new SimpleRecord("hello".getBytes()), new SimpleRecord("there".getBytes()));
        log.appendAsLeader(memoryRecords, 0);


        if (!brokerCompression.equals("producer")) {
            CompressionCodec brokerCompressionCode = CompressionCodec.BrokerCompressionCodec.getCompressionCodec(brokerCompression);
            assertEquals(brokerCompressionCode.getCodec(), readBatch(0L, log).compressionType().id, "Compression at offset 0 should produce " + brokerCompressionCode.name());
        } else {
            assertEquals(messageCompressionCode.getCodec(), readBatch(0L, log).compressionType().id, "Compression at offset 0 should produce " + messageCompressionCode.name());
        }
    }

    private RecordBatch readBatch(Long offset, UnifiedLog log) {
        FetchDataInfo fetchInfo = log.read(offset, 4096, FetchLogEnd, true);
        return fetchInfo.getRecords().batches().iterator().next();
    }

    public static Stream<Arguments> parameters() {
        List<Arguments> res = new ArrayList<>();
        for (String brokerCompression : CompressionCodec.BrokerCompressionCodec.brokerCompressionOptions) {
            for (CompressionType messageCompression : CompressionType.values()) {
                res.add(Arguments.of(messageCompression.name, brokerCompression));
            }
        }
        return res.stream();
    }
}
