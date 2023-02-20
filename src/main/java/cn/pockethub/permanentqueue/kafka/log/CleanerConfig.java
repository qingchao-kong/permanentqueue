package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * Configuration parameters for the log cleaner
 */
@Getter
public class CleanerConfig {
    private final int numThreads;
    private final long dedupeBufferSize;
    private final double dedupeBufferLoadFactor;
    private final int ioBuffersize;
    private final int maxMessageSize;
    private final double maxIoBytesPerSecond;
    private final long backOffMs;
    private final boolean enableCleaner;
    private final String hashAlgorithm;

    public static class Defaults {
        public static final int NUM_THREADS = 1;
        public static final long DEDUPE_BUFFER_SIZE = 4 * 1024 * 1024L;
        public static final double DEDUPE_BUFFER_LOAD_FACTOR = 0.9d;
        public static final int IO_BUFFER_SIZE = 1024 * 1024;
        public static final int MAX_MESSAGE_SIZE = 32 * 1024 * 1024;
        public static final double MAX_IO_BYTES_PER_SECOND = Double.MAX_VALUE;
        public static final long BACK_OFF_MS = 15 * 1000L;
        public static final boolean ENABLE_CLEANER = true;
        public static final String HASH_ALGORITHM = "MD5";
    }

    public CleanerConfig() {
        this(
                Defaults.NUM_THREADS,
                Defaults.DEDUPE_BUFFER_SIZE,
                Defaults.DEDUPE_BUFFER_LOAD_FACTOR,
                Defaults.IO_BUFFER_SIZE,
                Defaults.MAX_MESSAGE_SIZE,
                Defaults.MAX_IO_BYTES_PER_SECOND,
                Defaults.BACK_OFF_MS,
                Defaults.ENABLE_CLEANER,
                Defaults.HASH_ALGORITHM
        );
    }

    /**
     * @param numThreads             The number of cleaner threads to run
     * @param dedupeBufferSize       The total memory used for log deduplication
     * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param maxMessageSize         The maximum size of a message that can appear in the log
     * @param maxIoBytesPerSecond    The maximum read and write I/O that all cleaner threads are allowed to do
     * @param backOffMs              The amount of time to wait before rechecking if no logs are eligible for cleaning
     * @param enableCleaner          Allows completely disabling the log cleaner
     * @param hashAlgorithm          The hash algorithm to use in key comparison.
     */
    public CleanerConfig(final int numThreads,
                         final long dedupeBufferSize,
                         final double dedupeBufferLoadFactor,
                         final int ioBuffersize,
                         final int maxMessageSize,
                         final double maxIoBytesPerSecond,
                         final long backOffMs,
                         final boolean enableCleaner,
                         final String hashAlgorithm) {
        this.numThreads = numThreads;
        this.dedupeBufferSize = dedupeBufferSize;
        this.dedupeBufferLoadFactor = dedupeBufferLoadFactor;
        this.ioBuffersize = ioBuffersize;
        this.maxMessageSize = maxMessageSize;
        this.maxIoBytesPerSecond = maxIoBytesPerSecond;
        this.backOffMs = backOffMs;
        this.enableCleaner = enableCleaner;
        this.hashAlgorithm = hashAlgorithm;
    }
}
