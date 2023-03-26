package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.PermanentQueue;
import cn.pockethub.permanentqueue.PermanentQueueConfig;
import cn.pockethub.permanentqueue.executable.AbstractStressTest.*;
import cn.pockethub.permanentqueue.Queue;
import org.apache.rocketmq.common.UtilAll;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stress test that instantiates a log and then runs continual appends against it from one thread and continual reads against it
 * from another thread and checks a few basic assertions until the user kills the process.
 */
public class OneWriterOneReaderStressTest extends AbstractStressTest{
    private static final Logger LOG = LoggerFactory.getLogger(OneWriterOneReaderStressTest.class);

    public static void main(String[] args) throws Throwable {
        UUID uuid = UUID.randomUUID();
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid;
        PermanentQueueConfig config = new PermanentQueueConfig.Builder()
                .storePath(baseDir)
                .build();
        PermanentQueue permanentQueue = new PermanentQueue(config);
        permanentQueue.startUp();

        final WriterThread writer = new WriterThread("OneWriterOneReaderStressTest", permanentQueue);
        writer.start();

        final ReaderThread reader = new ReaderThread("OneWriterOneReaderStressTest", permanentQueue);
        reader.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                running.set(false);
                try {
                    writer.join();
                    reader.join();

                    permanentQueue.shutDown();

                    File file = new File(baseDir);
                    UtilAll.deleteFile(file);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });

        while (running.get()) {
            LOG.info(System.currentTimeMillis() + "writer offset = {}, reader offset = {}", writer.getOffset(), reader.getOffset());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

//    private static class WriterThread extends WorkerThread {
//        private final String topic;
//        private final PermanentQueue permanentQueue;
//        private volatile int offset = 0;
//
//        public WriterThread(final String topic, final PermanentQueue permanentQueue) {
//            this.topic = topic;
//            this.permanentQueue = permanentQueue;
//        }
//
//        @Override
//        public void work() {
//            try {
//                long logicsOffset = permanentQueue.write(topic, (messagePrefix + this.offset).getBytes(StandardCharsets.UTF_8));
//                if (logicsOffset >= 0) {
//                    offset++;
//                    if (offset % 1000 == 0) {
////                        Thread.sleep(500);
//                    }
//                }
//            } catch (Throwable throwable) {
//                LOG.error(throwable.getMessage(), throwable);
//            }
//        }
//    }
//
//    private static class ReaderThread extends WorkerThread {
//        private final String topic;
//        private final PermanentQueue permanentQueue;
//        private volatile int offset = 0;
//
//        public ReaderThread(final String topic, final PermanentQueue permanentQueue) {
//            this.topic = topic;
//            this.permanentQueue = permanentQueue;
//        }
//
//        @Override
//        public void work() {
//            try {//todo 无消息时会阻塞等待一段时间
//                List<Queue.ReadEntry> readEntries = permanentQueue.read(topic, 1000);
//                for (Queue.ReadEntry readEntry : readEntries) {
//                    String msg = new String(readEntry.getMessageBytes(), StandardCharsets.UTF_8);
//                    Assertions.assertEquals(messagePrefix + offset, msg);
//                    permanentQueue.commit(topic, readEntry.getOffset());
//                    offset++;
//                }
//            } catch (Throwable throwable) {
//                LOG.error(throwable.getMessage(), throwable);
//            }
//        }
//    }

}
