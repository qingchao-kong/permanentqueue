package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.PermanentQueue;
import cn.pockethub.permanentqueue.PermanentQueueManager;
import cn.pockethub.permanentqueue.PermanentQueueManagerBuilder;
import cn.pockethub.permanentqueue.Queue;
import cn.pockethub.permanentqueue.kafka.log.*;
import cn.pockethub.permanentqueue.kafka.metadata.MockConfigRepository;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.utils.KafkaScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

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
