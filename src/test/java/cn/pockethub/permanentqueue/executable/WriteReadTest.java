package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.PermanentQueue;
import cn.pockethub.permanentqueue.PermanentQueueManager;
import cn.pockethub.permanentqueue.PermanentQueueManagerBuilder;
import cn.pockethub.permanentqueue.Queue;
import cn.pockethub.permanentqueue.kafka.TestUtils;
import cn.pockethub.permanentqueue.kafka.coordinator.transaction.TransactionLog;
import cn.pockethub.permanentqueue.kafka.log.*;
import cn.pockethub.permanentqueue.kafka.metadata.MockConfigRepository;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.FetchDataInfo;
import cn.pockethub.permanentqueue.kafka.server.FetchIsolation;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.builders.LogManagerBuilder;
import cn.pockethub.permanentqueue.kafka.utils.KafkaScheduler;
import com.google.common.collect.Lists;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WriteReadTest {
    private static final Logger LOG = LoggerFactory.getLogger(WriteReadTest.class);

    public static void main(String[] args) throws Throwable {
        PermanentQueueManager queueManager = new PermanentQueueManagerBuilder()
                .setLogDir(new File("/tmp/permanentQueue-test"))
                .setConfigRepository(new MockConfigRepository())
                .setInitialDefaultConfig(new LogConfig())
                .setCleanerConfig(new CleanerConfig())
                .setScheduler(new KafkaScheduler(2))
                .setBrokerTopicStats(new BrokerTopicStats())
                .build();
        queueManager.startUp();

        PermanentQueue queue = queueManager.getOrCreatePermanentQueue("test2");
        LOG.info("Initialized log at {}", queueManager.getLogDir());

        long offset = queue.write(null, "test".getBytes());
        LOG.info("Wrote message offset={}", offset);

        List<Queue.ReadEntry> read = queue.read(null);
        for (Queue.ReadEntry readEntry :read) {
            LOG.info("Read message: \"{}\" (at {})", new String(readEntry.getPayload()), readEntry.getOffset());
            //commit
            queue.markQueueOffsetCommitted(readEntry.getOffset());
        }

        queueManager.shutDown();
    }
}
