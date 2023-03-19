package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.*;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stress test that instantiates a log and then runs continual appends against it from one thread and continual reads against it
 * from another thread and checks a few basic assertions until the user kills the process.
 */
public class StressTest {
    private static final Logger LOG = LoggerFactory.getLogger(StressTest.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final String topic = "test";
    private static final String messagePrefix = "0123456789abcdefghijklmnopqrstuvwxyz,.':;/\\\"[]{}()-_=+`~!@#$%^&*，。/；：'、【】「」～·！@#¥%……&*（）中文验证_";

    public static void main(String[] args) throws Throwable {
        PermanentQueueManager queueManager = new PermanentQueueManager();
        queueManager.startUp();

        final WriterThread writer = new WriterThread(queueManager);
        writer.start();
        final ReaderThread reader = new ReaderThread(queueManager);
        reader.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                running.set(false);
                try {
                    writer.join();
                    reader.join();
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });

        while (running.get()) {
            LOG.info(System.currentTimeMillis() + "Reader offset = {}, writer offset = {}", reader.offset, writer.offset);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static abstract class WorkerThread extends Thread {
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

    private static class WriterThread extends WorkerThread {
        private final PermanentQueueManager queueManager;
        private volatile int offset = 0;

        public WriterThread(final PermanentQueueManager queueManager) {
            this.queueManager = queueManager;
        }

        @Override
        public void work() {
            try {
                queueManager.write(topic, (messagePrefix + offset).getBytes(StandardCharsets.UTF_8));
                offset++;
                if (offset % 1000 == 0) {
                    Thread.sleep(500);
                }
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }
    }

    private static class ReaderThread extends WorkerThread {
        private final PermanentQueueManager queueManager;
        private volatile int offset = 0;

        public ReaderThread(final PermanentQueueManager queueManager) {
            this.queueManager = queueManager;
        }

        @Override
        public void work() {
            try {
                List<PermanentQueueManager.ReadEntry> readEntries = queueManager.read("test", 100);
                for (PermanentQueueManager.ReadEntry readEntry : readEntries) {
                    String msg = new String(readEntry.getMessageBytes(), StandardCharsets.UTF_8);
                    Assertions.assertEquals(messagePrefix + offset, msg);
                    queueManager.commit(topic, readEntry.getOffset());
                    offset++;
                }
            } catch (OffsetOutOfRangeException e) {
                // this is okay
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }
    }

}
