package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.*;
import cn.pockethub.permanentqueue.kafka.utils.TestUtils;
import cn.pockethub.permanentqueue.kafka.log.CleanerConfig;
import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.metadata.MockConfigRepository;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.utils.KafkaScheduler;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
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

    public static void main(String[] args) throws Throwable {
        final File dir = TestUtils.tempDir();

        PermanentQueueManager queueManager = new PermanentQueueManagerBuilder()
                .setLogDir(dir)
                .setInitialDefaultConfig(new LogConfig())
                .setCleanerConfig(new CleanerConfig())
                .setScheduler(new KafkaScheduler(2))
                .build();
        queueManager.startUp();

        PermanentQueue queue = queueManager.getOrCreatePermanentQueue("StressTest");

        final WriterThread writer = new WriterThread(queue);
        writer.start();
        final ReaderThread reader = new ReaderThread(queue);
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
                Utils.rm(dir);
            }
        });

        while (running.get()) {
            LOG.info(System.currentTimeMillis()+"Reader offset = {}, writer offset = {}", reader.offset, writer.offset);
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
        private final PermanentQueue queue;
        private volatile int offset = 0;

        public WriterThread(final PermanentQueue queue) {
            this.queue = queue;
        }

        @Override
        public void work() {
            try {
                long writeOffset = queue.write(null, String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
                Utils.require(writeOffset == offset);
                offset += 1;
                if (offset % 1000 == 0) {
                    Thread.sleep(500);
                }
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }
    }

    private static class ReaderThread extends WorkerThread {
        private final PermanentQueue queue;
        private volatile int offset = 0;

        public ReaderThread(final PermanentQueue queue) {
            this.queue = queue;
        }

        @Override
        public void work() {
            try {
                List<Queue.ReadEntry> read = queue.read(null);
                for (Queue.ReadEntry readEntry :read) {
                    Utils.require(readEntry.getOffset() == offset,
                            "We should either read nothing or the message we asked for.");
                    offset += 1;
                    queue.markQueueOffsetCommitted(readEntry.getOffset());
                }
            } catch (OffsetOutOfRangeException e) {
                // this is okay
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }
    }

}
