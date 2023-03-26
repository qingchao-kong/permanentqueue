package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.PermanentQueue;
import cn.pockethub.permanentqueue.PermanentQueueConfig;
import cn.pockethub.permanentqueue.Queue;
import org.apache.rocketmq.common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class WriteReadTest {
    private static final Logger LOG = LoggerFactory.getLogger(WriteReadTest.class);

    public static void main(String[] args) throws Throwable {
        UUID uuid = UUID.randomUUID();
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid;
        PermanentQueueConfig config = new PermanentQueueConfig.Builder()
                .storePath(baseDir)
                .build();
        PermanentQueue permanentQueue = new PermanentQueue(config);
        permanentQueue.startUp();

        LOG.info("Initialized PermanentQueue at {}", baseDir);

        long offset = permanentQueue.write("test", "test".getBytes());
        LOG.info("Wrote message offset={}", offset);

        List<Queue.ReadEntry> read = permanentQueue.read("test", 10);
        for (Queue.ReadEntry readEntry : read) {
            LOG.info("Read message: \"{}\" (at {})", new String(readEntry.getMessageBytes()), readEntry.getOffset());
            //commit
            permanentQueue.commit("test", readEntry.getOffset());
        }

        permanentQueue.shutDown();
        Thread.sleep(10 * 1000);

        File file = new File(baseDir);
        UtilAll.deleteFile(file);
    }
}
