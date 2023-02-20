package cn.pockethub.permanentqueue;

import cn.pockethub.permanentqueue.kafka.log.LogAppendInfo;
import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogSegment;
import cn.pockethub.permanentqueue.kafka.log.UnifiedLog;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.server.FetchDataInfo;
import cn.pockethub.permanentqueue.kafka.server.FetchIsolation;
import cn.pockethub.permanentqueue.kafka.server.KafkaConfig;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static cn.pockethub.permanentqueue.Tools.bytesToHex;
import static cn.pockethub.permanentqueue.kafka.server.FetchIsolation.FetchLogEnd;

public class PermanentQueue extends KafkaMetricsGroup implements Queue {
    private static final Logger LOG = LoggerFactory.getLogger(PermanentQueue.class);

    private static final String metricPrefix = PermanentQueue.class.getName();

    // Metric names, which should be used twice (once in metric startup and once in metric teardown).
    private static final String TIMER_WRITE_TIME = "writeTime";
    private static final String TIMER_READ_TIME = "readTime";
    public static final String METER_WRITTEN_MESSAGES = "writtenMessages";
    public static final String METER_READ_MESSAGES = "readMessages";
    private static final String METER_WRITE_DISCARDED_MESSAGES = "writeDiscardedMessages";

    private final String queueName;
    private PermanentQueueManager manager;
    private UnifiedLog unifiedLog;

    @Getter
    private final AtomicLong committedOffset;
    private long nextReadOffset;

    private final int maxSegmentSize = LogConfig.Defaults.SegmentSize;
    // Max message size should not be bigger than max segment size.
    private final int maxMessageSize = Ints.saturatedCast(maxSegmentSize);

    private final Timer writeTime;
    private final Timer readTime;
    private final Meter writtenMessages;
    private final Meter readMessages;
    private final Meter writeDiscardedMessages;

    public PermanentQueue(String queueName, PermanentQueueManager manager, UnifiedLog unifiedLog, long committedOffset, long nextReadOffset) {
        this.queueName = queueName;
        this.manager = manager;
        this.unifiedLog = unifiedLog;
        this.committedOffset = new AtomicLong(committedOffset);
        this.nextReadOffset = nextReadOffset;

        // Set up metrics
        this.writeTime = newTimer(metricPrefix + "-" + TIMER_WRITE_TIME, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, new HashMap<String, String>() {{
            put("queueName", queueName);
        }});
        this.readTime = newTimer(metricPrefix + "-" + TIMER_READ_TIME, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, new HashMap<String, String>() {{
            put("queueName", queueName);
        }});
        this.writtenMessages = newMeter(metricPrefix + "-" + METER_WRITTEN_MESSAGES, "entries", TimeUnit.SECONDS, new HashMap<String, String>() {{
            put("queueName", queueName);
        }});
        this.readMessages = newMeter(metricPrefix + "-" + METER_READ_MESSAGES, "entries", TimeUnit.SECONDS, new HashMap<String, String>() {{
            put("queueName", queueName);
        }});
        this.writeDiscardedMessages = newMeter(metricPrefix + "-" + METER_WRITE_DISCARDED_MESSAGES, "entries", TimeUnit.SECONDS, new HashMap<String, String>() {{
            put("queueName", queueName);
        }});
    }

    @Override
    public Entry createEntry(byte[] idBytes, byte[] messageBytes) {
        return new Entry(idBytes, messageBytes);
    }

    /**
     * Writes the list of entries to the permanentQueue.
     *
     * @param entries permanentQueue entries to be written
     * @return the last position written to in the permanentQueue
     */
    @Override
    public long write(List<Entry> entries) {
        TimerContext ignored = writeTime.time();
        try {
            long payloadSize = 0L;
            long messageSetSize = 0L;
            long lastWriteOffset = 0L;

            final List<SimpleRecord> records = new ArrayList<>(entries.size());
            for (final Entry entry : entries) {
                final byte[] messageBytes = entry.getMessageBytes();
                final byte[] idBytes = entry.getIdBytes();

                payloadSize += messageBytes.length;

                final SimpleRecord simpleRecord = new SimpleRecord(idBytes, messageBytes);
                // Calculate the size of the new message in the message set by including the overhead for the log entry.
                int newMessageSize = MemoryRecords.estimateSizeInBytes(RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, Collections.singletonList(simpleRecord));


                if (newMessageSize > maxMessageSize) {
                    writeDiscardedMessages.mark();
                    LOG.warn("Message with ID <{}> is too large to store in permanentQueue, skipping! (size: {} bytes / max: {} bytes)",
                            new String(idBytes, StandardCharsets.UTF_8), newMessageSize, maxMessageSize);
                    payloadSize = 0;
                    continue;
                }

                // If adding the new message to the message set would overflow the max segment size, flush the current
                // list of message to avoid a MessageSetSizeTooLargeException.
                if ((messageSetSize + newMessageSize) > maxSegmentSize) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Flushing {} bytes message set with {} messages to avoid overflowing segment with max size of {} bytes",
                                messageSetSize, records.size(), maxSegmentSize);
                    }
                    lastWriteOffset = flushMessages(records, payloadSize);
                    // Reset the messages list and size counters to start a new batch.
                    records.clear();
                    messageSetSize = 0;
                    payloadSize = 0;
                }
                records.add(simpleRecord);
                messageSetSize += newMessageSize;

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Message {} contains bytes {}", bytesToHex(idBytes), bytesToHex(messageBytes));
                }
            }

            // Flush the rest of the messages.
            if (records.size() > 0) {
                lastWriteOffset = flushMessages(records, payloadSize);
            }

            return lastWriteOffset;
        } finally {
            ignored.stop();
        }
    }

    private long flushMessages(List<SimpleRecord> records, long payloadSize) {
        if (CollectionUtils.isEmpty(records)) {
            LOG.debug("No messages to flush, not trying to write an empty message set.");
            return -1L;
        }

        MemoryRecords memoryRecords = MemoryRecords.withRecords(CompressionType.NONE, records.toArray(new SimpleRecord[0]));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to write ByteBufferMessageSet with size of {} bytes to permanentQueue", memoryRecords.sizeInBytes());
        }

        final LogAppendInfo appendInfo = unifiedLog.appendAsLeader(memoryRecords, 1);
        long lastWriteOffset = appendInfo.getLastOffset();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Wrote {} messages to permanentQueue: {} bytes (payload {} bytes), log position {} to {}",
                    records.size(), memoryRecords.sizeInBytes(), payloadSize, appendInfo.getFirstOffset(), lastWriteOffset);
        }
        writtenMessages.mark(records.size());

        return lastWriteOffset;
    }

    /**
     * Writes a single message to the permanentQueue and returns the new write position
     *
     * @param idBytes      byte array congaing the message id
     * @param messageBytes encoded message payload
     * @return the last position written to in the permanentQueue
     */
    @Override
    public long write(byte[] idBytes, byte[] messageBytes) {
        Entry entry = createEntry(idBytes, messageBytes);
        return write(Collections.singletonList(entry));
    }

    /**
     * @param maxLength The maximum number of bytes to read
     * @return
     */
    @Override
    public List<ReadEntry> read(Integer maxLength) {
        if (null == maxLength || maxLength <= 0) {
            maxLength = KafkaConfig.Defaults.FetchMaxBytes;
        }
        return read(nextReadOffset, maxLength);
    }

    private List<ReadEntry> read(long readOffset, int maxLength) {
        if (manager.isShuttingDown()) {
            return Collections.emptyList();
        }
        final List<ReadEntry> messages = new ArrayList<>();
        TimerContext ignored = readTime.time();
        try {
            final long logStartOffset = getLogStartOffset();

            if (readOffset < logStartOffset) {
                LOG.info("Read offset {} before start of log at {}, starting to read from the beginning of the permanentQueue.", readOffset, logStartOffset);
                readOffset = logStartOffset;
            }
            LOG.debug("Requesting to read a maximum of {} bytes messages from the permanentQueue, readOffset:{}", maxLength, readOffset);

            // TODO benchmark and make read-ahead strategy configurable for performance tuning
            FetchDataInfo fetchDataInfo = unifiedLog.read(readOffset, maxLength, FetchLogEnd, false);

            Iterator<Record> iterator = fetchDataInfo.getRecords().records().iterator();
            long firstOffset = Long.MIN_VALUE;
            long lastOffset = Long.MIN_VALUE;
            long totalBytes = 0;
            while (iterator.hasNext()) {
                final Record record = iterator.next();

                if (firstOffset == Long.MIN_VALUE) {
                    firstOffset = record.offset();
                }
                // always remember the last seen offset for debug purposes below
                lastOffset = record.offset();

                final byte[] payloadBytes = Utils.readBytes(record.value());
                if (LOG.isTraceEnabled()) {
                    final byte[] keyBytes = Utils.readBytes(record.key());
                    LOG.trace("Read message {} contains {}", bytesToHex(keyBytes), bytesToHex(payloadBytes));
                }
                totalBytes += payloadBytes.length;
                messages.add(new ReadEntry(payloadBytes, record.offset()));
                // remember where to read from
                nextReadOffset = record.offset() + 1;
            }
            if (messages.isEmpty()) {
                LOG.debug("No messages available to read for readOffset:{}.", readOffset);
            } else {
                LOG.debug(
                        "Read {} messages, total payload size {}, from permanentQueue, offset interval [{}, {}], requested read at {}",
                        messages.size(),
                        totalBytes,
                        firstOffset,
                        lastOffset,
                        readOffset);
            }
        } catch (OffsetOutOfRangeException e) {
            // This is fine, the reader tries to read faster than the writer committed data. Next read will get the data.
            LOG.debug("Offset out of range, no messages available starting at offset {}", readOffset);
        } catch (Exception e) {
            // the scala code does not declare the IOException in kafkaLog.read() so we can't catch it here
            // sigh.
            if (manager.isShuttingDown()) {
                LOG.debug("Caught exception during shutdown, ignoring it because we might have been blocked on a read.");
                return Collections.emptyList();
            }
            //noinspection ConstantConditions
            if (e instanceof ClosedByInterruptException) {
                LOG.debug("Interrupted while reading from permanentQueue, during shutdown this is harmless and ignored.", e);
            } else {
                throw e;
            }
        } finally {
            ignored.stop();
        }
        readMessages.mark(messages.size());
        return messages;
    }

    /**
     * Returns the first valid offset in the entire permanentQueue.
     *
     * @return first offset
     */
    public long getLogStartOffset() {
        Collection<LogSegment> logSegments = unifiedLog.logSegments();
        final LogSegment segment = Iterables.getFirst(logSegments, null);
        if (segment == null) {
            return 0;
        }
        return segment.getBaseOffset();
    }

    /**
     * Upon fully processing, and persistently storing, a batch of messages, the system should mark the message with the
     * highest offset as committed. A background job will write the last position to disk periodically.
     *
     * @param offset the offset of the latest committed message
     */
    @Override
    public void markQueueOffsetCommitted(long offset) {
        long prev;
        // the caller will not care about offsets going backwards, so we need to make sure we don't backtrack
        int i = 0;
        do {
            prev = committedOffset.get();
            // at least warn if this spins often, that would be a sign of very high contention, which should not happen
            if (++i % 10 == 0) {
                LOG.warn("Committing permanentQueue offset spins {} times now, this might be a bug. Continuing to try update.", i);
            }
        } while (!committedOffset.compareAndSet(prev, Math.max(offset, prev)));
    }
}
