package cn.pockethub.permanentqueue.other;

import cn.pockethub.permanentqueue.kafka.utils.TestUtils;
import cn.pockethub.permanentqueue.kafka.log.LogAppendInfo;
import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogManager;
import cn.pockethub.permanentqueue.kafka.log.UnifiedLog;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import cn.pockethub.permanentqueue.kafka.utils.MockTime;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static cn.pockethub.permanentqueue.Utils.require;
import static cn.pockethub.permanentqueue.kafka.server.FetchIsolation.FetchLogEnd;

/**
 * A stress test that instantiates a log and then runs continual appends against it from one thread and continual reads against it
 * from another thread and checks a few basic assertions until the user kills the process.
 */
public class StressTestLog {
    private static final Logger LOG = LoggerFactory.getLogger(StressTestLog.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws IOException {
        File dir = TestUtils.randomPartitionLogDir(TestUtils.tempDir());
        MockTime time = new MockTime();
        Properties logProperties = new Properties();
        logProperties.put(LogConfig.SegmentBytesProp, 64 * 1024 * 1024);
        logProperties.put(LogConfig.MaxMessageBytesProp, Integer.MAX_VALUE);
        logProperties.put(LogConfig.SegmentIndexBytesProp, 1024 * 1024);

        UnifiedLog log = UnifiedLog.apply(
                dir,
                new LogConfig(logProperties),
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
        WriterThread writer = new WriterThread(log);
        writer.start();
        ReaderThread reader = new ReaderThread(log);
        reader.start();

        Exit.addShutdownHook("stress-test-shutdown-hook", new Runnable() {
            @Override
            public void run() {
                running.set(false);
                try {
                    writer.join();
                    reader.join();
                    Utils.delete(dir);
                } catch (Throwable throwable) {
                    LOG.error(throwable.getMessage(), throwable);
                }
            }
        });

        while (running.get()) {
            try {
                Thread.sleep(1000);
                System.out.println(String.format("Reader offset = %d, writer offset = %d", reader.currentOffset, writer.currentOffset));
                writer.checkProgress();
                reader.checkProgress();
            } catch (Throwable throwable) {
                LOG.error(throwable.getMessage(), throwable);
            }
        }
    }

    abstract static class WorkerThread extends Thread {

        @Override
        public void run() {
            try {
                while (running.get()) {
                    work();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                running.set(false);
            }
        }

        abstract void work();

        abstract Boolean isMakingProgress();
    }

    static abstract class LogProgress extends WorkerThread {
        protected volatile long currentOffset = 0;
        private long lastOffsetCheckpointed = currentOffset;
        private long lastProgressCheckTime = System.currentTimeMillis();

        public Boolean isMakingProgress() {
            if (currentOffset > lastOffsetCheckpointed) {
                lastOffsetCheckpointed = currentOffset;
                return true;
            }

            return false;
        }

        public void checkProgress() {
            // Check if we are making progress every 500ms
            long curTime = System.currentTimeMillis();
            if ((curTime - lastProgressCheckTime) > 500) {
                require(isMakingProgress(), "Thread not making progress");
                lastProgressCheckTime = curTime;
            }
        }
    }

    static class WriterThread extends LogProgress {
        private final UnifiedLog log;

        public WriterThread(UnifiedLog log) {
            this.log = log;
        }

        @Override
        public void work() {
            LogAppendInfo logAppendInfo = log.appendAsLeader(TestUtils.singletonRecords(Long.toString(currentOffset).getBytes()), 0);
            Optional<LogOffsetMetadata> firstOffset = logAppendInfo.getFirstOffset();
            require(
                    (!firstOffset.isPresent() || (firstOffset.get().getMessageOffset() == currentOffset))
                            && logAppendInfo.getLastOffset() == currentOffset
            );
            currentOffset += 1;
            if (currentOffset % 1000 == 0) {
                try {
                    Thread.sleep(50);
                } catch (Throwable throwable) {
                    LOG.error(throwable.getMessage(), throwable);
                }
            }
        }
    }

    static class ReaderThread extends LogProgress {
        private final UnifiedLog log;

        public ReaderThread(UnifiedLog log) {
            this.log = log;
        }

        @Override
        public void work() {
            try {
                Records read = log.read(currentOffset,
                        1,
                        FetchLogEnd,
                        true).getRecords();
                if (read instanceof FileRecords
                        && read.sizeInBytes() > 0) {
                    RecordBatch first = read.batches().iterator().next();
                    require(first.lastOffset() == currentOffset, "We should either read nothing or the message we asked for.");
                    require(first.sizeInBytes() == read.sizeInBytes(), String.format("Expected %d but got %d.", first.sizeInBytes(), read.sizeInBytes()));
                    currentOffset += 1;
                } else {
                    //
                }
            } catch (OffsetOutOfRangeException e) {
                // this is okay
            }
        }
    }
}
