package cn.pockethub.permanentqueue.other;

import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogManager;
import cn.pockethub.permanentqueue.kafka.log.UnifiedLog;
import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.utils.CommandLineUtils;
import cn.pockethub.permanentqueue.kafka.utils.KafkaScheduler;
import cn.pockethub.permanentqueue.kafka.utils.Scheduler;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static cn.pockethub.permanentqueue.kafka.message.CompressionCodec.NoCompressionCodec;

public class TestLinearWriteSpeed {
    private static final Logger LOG = LoggerFactory.getLogger(TestLinearWriteSpeed.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        OptionParser parser = new OptionParser(false);
        ArgumentAcceptingOptionSpec<String> dirOpt = parser.accepts("dir", "The directory to write to.")
                .withRequiredArg()
                .describedAs("path")
                .ofType(String.class)
                .defaultsTo(System.getProperty("java.io.tmpdir"));
        ArgumentAcceptingOptionSpec<Long> bytesOpt = parser.accepts("bytes", "REQUIRED: The total number of bytes to write.")
                .withRequiredArg()
                .describedAs("num_bytes")
                .ofType(Long.class);
        ArgumentAcceptingOptionSpec<Integer> sizeOpt = parser.accepts("size", "REQUIRED: The size of each write.")
                .withRequiredArg()
                .describedAs("num_bytes")
                .ofType(Integer.class);
        ArgumentAcceptingOptionSpec<Integer> messageSizeOpt = parser.accepts("message-size", "REQUIRED: The size of each message in the message set.")
                .withRequiredArg()
                .describedAs("num_bytes")
                .ofType(Integer.class)
                .defaultsTo(1024);
        ArgumentAcceptingOptionSpec<Integer> filesOpt = parser.accepts("files", "REQUIRED: The number of logs or files.")
                .withRequiredArg()
                .describedAs("num_files")
                .ofType(Integer.class)
                .defaultsTo(1);
        ArgumentAcceptingOptionSpec<Long> reportingIntervalOpt = parser.accepts("reporting-interval", "The number of ms between updates.")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Long.class)
                .defaultsTo(1000L);
        ArgumentAcceptingOptionSpec<Integer> maxThroughputOpt = parser.accepts("max-throughput-mb", "The maximum throughput.")
                .withRequiredArg()
                .describedAs("mb")
                .ofType(Integer.class)
                .defaultsTo(Integer.MAX_VALUE);
        ArgumentAcceptingOptionSpec<Long> flushIntervalOpt = parser.accepts("flush-interval", "The number of messages between flushes")
                .withRequiredArg()
                .describedAs("message_count")
                .ofType(Long.class)
                .defaultsTo(Long.MAX_VALUE);
        ArgumentAcceptingOptionSpec<String> compressionCodecOpt = parser.accepts("compression", "The compression codec to use")
                .withRequiredArg()
                .describedAs("codec")
                .ofType(String.class)
                .defaultsTo(NoCompressionCodec.name());
        OptionSpecBuilder mmapOpt = parser.accepts("mmap", "Do writes to memory-mapped files.");
        OptionSpecBuilder channelOpt = parser.accepts("channel", "Do writes to file channels.");
        OptionSpecBuilder logOpt = parser.accepts("log", "Do writes to kafka logs.");

        OptionSet options = parser.parse(args);

        CommandLineUtils.checkRequiredArgs(parser, options, bytesOpt, sizeOpt, filesOpt);

        long bytesToWrite = options.valueOf(bytesOpt).longValue();
        int bufferSize = options.valueOf(sizeOpt).intValue();
        int numFiles = options.valueOf(filesOpt).intValue();
        long reportingInterval = options.valueOf(reportingIntervalOpt).longValue();
        String dir = options.valueOf(dirOpt);
        long maxThroughputBytes = options.valueOf(maxThroughputOpt).intValue() * 1024L * 1024L;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int messageSize = options.valueOf(messageSizeOpt).intValue();
        long flushInterval = options.valueOf(flushIntervalOpt).longValue();
        CompressionCodec compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOpt));
        Random rand = new Random();
        rand.nextBytes(buffer.array());
        int numMessages = bufferSize / (messageSize + Records.LOG_OVERHEAD);
        long createTime = System.currentTimeMillis();

        CompressionType compressionType = CompressionType.forId(compressionCodec.getCodec());
        List<SimpleRecord> records = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            records.add(new SimpleRecord(createTime, null, new byte[messageSize]));
        }
        MemoryRecords messageSet = MemoryRecords.withRecords(compressionType, records.toArray(new SimpleRecord[0]));

        Writable[] writables = new Writable[numFiles];
        KafkaScheduler scheduler = new KafkaScheduler(1);
        scheduler.startup();
        for (int i = 0; i < numFiles; i++) {
            if (options.has(mmapOpt)) {
                writables[i] = new MmapWritable(new File(dir, "kafka-test-" + i + ".dat"), bytesToWrite / numFiles, buffer);
            } else if (options.has(channelOpt)) {
                writables[i] = new ChannelWritable(new File(dir, "kafka-test-" + i + ".dat"), buffer);
            } else if (options.has(logOpt)) {
                // vary size to avoid herd effect
                int segmentSize = rand.nextInt(512) * 1024 * 1024 + 64 * 1024 * 1024;
                Properties logProperties = new Properties();
                logProperties.put(LogConfig.SegmentBytesProp, segmentSize);
                logProperties.put(LogConfig.FlushMessagesProp, flushInterval);
                writables[i] = new LogWritable(new File(dir, "kafka-test-" + i), new LogConfig(logProperties), scheduler, messageSet);
            } else {
                System.err.println("Must specify what to write to with one of --log, --channel, or --mmap");
                Exit.exit(1);
            }
        }
        bytesToWrite = (bytesToWrite / numFiles) * numFiles;

        System.out.println("%10s\t%10s\t%10s".format("mb_sec", "avg_latency", "max_latency"));

        long beginTest = System.nanoTime();
        long maxLatency = 0L;
        long totalLatency = 0L;
        long count = 0L;
        long written = 0L;
        long totalWritten = 0L;
        long lastReport = beginTest;
        while (totalWritten + bufferSize < bytesToWrite) {
            long start = System.nanoTime();
            int writeSize = writables[new Long(Math.abs((count % numFiles))).intValue()].write();
            long ellapsed = System.nanoTime() - start;
            maxLatency = Math.max(ellapsed, maxLatency);
            totalLatency += ellapsed;
            written += writeSize;
            count += 1;
            totalWritten += writeSize;
            if ((start - lastReport) / (1000.0 * 1000.0) > new Long(reportingInterval).doubleValue()) {
                double ellapsedSecs = (start - lastReport) / (1000.0 * 1000.0 * 1000.0);
                double mb = written / (1024.0 * 1024.0);
                System.out.println(String.format("%10.3f\t%10.3f\t%10.3f", mb / ellapsedSecs, totalLatency / (double) count / (1000.0 * 1000.0), maxLatency / (1000.0 * 1000.0)));
                lastReport = start;
                written = 0;
                maxLatency = 0L;
                totalLatency = 0L;
            } else if (written > maxThroughputBytes * (reportingInterval / 1000.0)) {
                // if we have written enough, just sit out this reporting interval
                long lastReportMs = lastReport / (1000 * 1000);
                long now = System.nanoTime() / (1000 * 1000);
                long sleepMs = lastReportMs + reportingInterval - now;
                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (Throwable throwable) {
                        LOG.error(throwable.getMessage(), throwable);
                    }
                }
            }
        }
        double elapsedSecs = (System.nanoTime() - beginTest) / (1000.0 * 1000.0 * 1000.0);
        System.out.println((bytesToWrite / (1024.0 * 1024.0 * elapsedSecs)) + " MB per sec");
        scheduler.shutdown();
    }

    interface Writable {
        Integer write() throws IOException;

        void close() throws IOException;
    }

    static class MmapWritable implements Writable {
        private File file;
        private Long size;
        private ByteBuffer content;

        private RandomAccessFile raf;
        private MappedByteBuffer buffer;

        public MmapWritable(File file, Long size, ByteBuffer content) throws FileNotFoundException, IOException {
            this.file = file;
            this.size = size;
            this.content = content;

            file.deleteOnExit();
            raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
        }

        @Override
        public Integer write() {
            buffer.put(content);
            content.rewind();
            return content.limit();
        }

        @Override
        public void close() throws IOException {
            raf.close();
            Utils.delete(file);
        }
    }

    static class ChannelWritable implements Writable {
        private File file;
        private ByteBuffer content;

        private FileChannel channel;

        public ChannelWritable(File file, ByteBuffer content) throws IOException {
            this.file = file;
            this.content = content;

            file.deleteOnExit();
            channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
        }

        @Override
        public Integer write() throws IOException {
            channel.write(content);
            content.rewind();
            return content.limit();
        }

        @Override
        public void close() throws IOException {
            channel.close();
            Utils.delete(file);
        }
    }

    static class LogWritable implements Writable {
        private File dir;
        private LogConfig config;
        private Scheduler scheduler;
        private MemoryRecords messages;

        private UnifiedLog log;

        public LogWritable(File dir, LogConfig config, Scheduler scheduler, MemoryRecords messages) throws IOException {
            this.dir = dir;
            this.config = config;
            this.scheduler = scheduler;
            this.messages = messages;

            Utils.delete(dir);
            log = UnifiedLog.apply(
                    dir,
                    config,
                    0L,
                    0L,
                    scheduler,
                    new BrokerTopicStats(),
                    Time.SYSTEM,
                    5 * 60 * 1000,
                    60 * 60 * 1000,
                    LogManager.ProducerIdExpirationCheckIntervalMs,
                    new LogDirFailureChannel(10),
                    true,
                    Optional.empty(),
                    true,
                    new ConcurrentHashMap<>());
        }

        @Override
        public Integer write() {
            log.appendAsLeader(messages, 0);
            return messages.sizeInBytes();
        }

        @Override
        public void close() throws IOException {
            log.close();
            Utils.delete(log.dir());
        }
    }

}
