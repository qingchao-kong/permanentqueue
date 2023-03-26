package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.*;
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
public class AbstractStressTest {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStressTest.class);
    protected static final AtomicBoolean running = new AtomicBoolean(true);
    protected static final String messagePrefix = "0123456789abcdefghijklmnopqrstuvwxyz,.':;/\\\"[]{}()-_=+`~!@#$%^&*，。/；：'、【】「」～·！@#¥%……&*（）中文验证_";

    public static void main(String[] args) throws Throwable {
        UUID uuid = UUID.randomUUID();
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid;
        PermanentQueueConfig config = new PermanentQueueConfig.Builder()
                .storePath(baseDir)
                .build();
        PermanentQueue permanentQueue = new PermanentQueue(config);
        permanentQueue.startUp();

        //topic test_A
        final WriterThread writerA_01 = new WriterThread("test_A", permanentQueue);
        writerA_01.start();
        final WriterThread writerA_02 = new WriterThread("test_A", permanentQueue);
        writerA_02.start();
        final WriterThread writerA_03 = new WriterThread("test_A", permanentQueue);
        writerA_03.start();

        final ReaderThread readerA_01 = new ReaderThread("test_A", permanentQueue);
        readerA_01.start();
        final ReaderThread readerA_02 = new ReaderThread("test_A", permanentQueue);
        readerA_02.start();
        final ReaderThread readerA_03 = new ReaderThread("test_A", permanentQueue);
        readerA_03.start();

        //topic test_B
        final WriterThread writerB_01 = new WriterThread("test_B", permanentQueue);
        writerB_01.start();
        final WriterThread writerB_02 = new WriterThread("test_B", permanentQueue);
        writerB_02.start();
        final WriterThread writerB_03 = new WriterThread("test_B", permanentQueue);
        writerB_03.start();

        final ReaderThread readerB_01 = new ReaderThread("test_B", permanentQueue);
        readerB_01.start();
        final ReaderThread readerB_02 = new ReaderThread("test_B", permanentQueue);
        readerB_02.start();
        final ReaderThread readerB_03 = new ReaderThread("test_B", permanentQueue);
        readerB_03.start();

        //topic test_C
        final WriterThread writerC_01 = new WriterThread("test_C", permanentQueue);
        writerC_01.start();
        final WriterThread writerC_02 = new WriterThread("test_C", permanentQueue);
        writerC_02.start();
        final WriterThread writerC_03 = new WriterThread("test_C", permanentQueue);
        writerC_03.start();

        final ReaderThread readerC_01 = new ReaderThread("test_C", permanentQueue);
        readerC_01.start();
        final ReaderThread readerC_02 = new ReaderThread("test_C", permanentQueue);
        readerC_02.start();
        final ReaderThread readerC_03 = new ReaderThread("test_C", permanentQueue);
        readerC_03.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                running.set(false);
                try {
                    writerA_01.join();
                    writerA_02.join();
                    writerA_03.join();
                    readerA_01.join();
                    readerA_02.join();
                    readerA_03.join();

                    writerB_01.join();
                    writerB_02.join();
                    writerB_03.join();
                    readerB_01.join();
                    readerB_02.join();
                    readerB_03.join();

                    writerC_01.join();
                    writerC_02.join();
                    writerC_03.join();
                    readerC_01.join();
                    readerC_02.join();
                    readerC_03.join();

                    permanentQueue.shutDown();

                    Thread.sleep(10*1000);
                    File file = new File(baseDir);
                    UtilAll.deleteFile(file);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });

        while (running.get()) {
            LOG.info(System.currentTimeMillis() + "writerA_01 offset = {}, readerA_01 offset = {}", writerA_01.offset, readerA_01.offset);
            LOG.info(System.currentTimeMillis() + "writerA_02 offset = {}, readerA_02 offset = {}", writerA_02.offset, readerA_02.offset);
            LOG.info(System.currentTimeMillis() + "writerA_03 offset = {}, readerA_03 offset = {}", writerA_03.offset, readerA_03.offset);

            LOG.info(System.currentTimeMillis() + "writerB_01 offset = {}, readerB_01 offset = {}", writerB_01.offset, readerB_01.offset);
            LOG.info(System.currentTimeMillis() + "writerB_02 offset = {}, readerB_02 offset = {}", writerB_02.offset, readerB_02.offset);
            LOG.info(System.currentTimeMillis() + "writerB_03 offset = {}, readerB_03 offset = {}", writerB_03.offset, readerB_03.offset);

            LOG.info(System.currentTimeMillis() + "writerC_01 offset = {}, readerB_01 offset = {}", writerB_01.offset, readerB_01.offset);
            LOG.info(System.currentTimeMillis() + "writerC_02 offset = {}, readerC_02 offset = {}", writerC_02.offset, readerC_02.offset);
            LOG.info(System.currentTimeMillis() + "writerC_03 offset = {}, readerC_03 offset = {}", writerC_03.offset, readerC_03.offset);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    protected static abstract class WorkerThread extends Thread {
        @Override
        public void run() {
            try {
                while (running.get()) {
                    work();
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                running.set(false);
            }
            LOG.info("{} exiting...", getClass().getName());
        }

        public abstract void work();
    }

    protected static class WriterThread extends WorkerThread {
        private final String topic;
        private final PermanentQueue permanentQueue;
        private volatile int offset = 0;

        public WriterThread(final String topic, final PermanentQueue permanentQueue) {
            this.topic = topic;
            this.permanentQueue = permanentQueue;
        }

        @Override
        public void work() {
            try {
                long logicsOffset = permanentQueue.write(topic, (messagePrefix + this.offset).getBytes(StandardCharsets.UTF_8));
                if (logicsOffset >= 0) {
                    offset++;
                    if (offset % 1000 == 0) {
//                        Thread.sleep(500);
                    }
                }
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }

        public int getOffset() {
            return offset;
        }
    }

    protected static class ReaderThread extends WorkerThread {
        private final String topic;
        private final PermanentQueue permanentQueue;
        private volatile int offset = 0;

        public ReaderThread(final String topic, final PermanentQueue permanentQueue) {
            this.topic = topic;
            this.permanentQueue = permanentQueue;
        }

        @Override
        public void work() {
            try {//todo 无消息时会阻塞等待一段时间
                List<Queue.ReadEntry> readEntries = permanentQueue.read(topic, 1000);
                for (Queue.ReadEntry readEntry : readEntries) {
                    String msg = new String(readEntry.getMessageBytes(), StandardCharsets.UTF_8);
                    Assertions.assertEquals(messagePrefix + offset, msg);
                    permanentQueue.commit(topic, readEntry.getOffset());
                    offset++;
                }
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }

        public int getOffset() {
            return offset;
        }
    }

}
