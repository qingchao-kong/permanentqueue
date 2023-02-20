package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.LogCleaningAbortedException;
import cn.pockethub.permanentqueue.kafka.common.LogSegmentOffsetOverflowException;
import cn.pockethub.permanentqueue.kafka.common.function.ConsumerWithLogCleaningAbortedException;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.Throttler;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.util.*;

/**
 * This class holds the actual logic for cleaning a log
 */
@Getter
public class Cleaner {
    private final Logger LOG = LoggerFactory.getLogger(Cleaner.class);

    private Integer id;
    private OffsetMap offsetMap;
    private Integer ioBufferSize;
    private Integer maxIoBufferSize;
    private Double dupBufferLoadFactor;
    private Throttler throttler;
    private Time time;
    private ConsumerWithLogCleaningAbortedException<TopicPartition> checkDone;

    private final String logIdent;

    /* buffer used for read i/o */
    private ByteBuffer readBuffer;

    /* buffer used for write i/o */
    private ByteBuffer writeBuffer;

    private BufferSupplier decompressionBufferSupplier = BufferSupplier.create();

    /**
     * @param id                  An identifier used for logging
     * @param offsetMap           The map used for deduplication
     * @param ioBufferSize        The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
     * @param maxIoBufferSize     The maximum size of a message that can appear in the log
     * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param throttler           The throttler instance to use for limiting I/O rate.
     * @param time                The time instance
     * @param checkDone           Check if the cleaning for a partition is finished or aborted.
     */
    public Cleaner(Integer id,
                   OffsetMap offsetMap,
                   Integer ioBufferSize,
                   Integer maxIoBufferSize,
                   Double dupBufferLoadFactor,
                   Throttler throttler,
                   Time time,
                   ConsumerWithLogCleaningAbortedException<TopicPartition> checkDone) {
        this.id = id;
        this.offsetMap = offsetMap;
        this.ioBufferSize = ioBufferSize;
        this.maxIoBufferSize = maxIoBufferSize;
        this.dupBufferLoadFactor = dupBufferLoadFactor;
        this.throttler = throttler;
        this.time = time;
        this.checkDone = checkDone;

        this.logIdent = String.format("Cleaner %s: ", id);
        this.readBuffer = ByteBuffer.allocate(ioBufferSize);
        this.writeBuffer = ByteBuffer.allocate(ioBufferSize);

        assert (offsetMap.slots() * dupBufferLoadFactor > 1) :
                "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads";

    }

    /**
     * Clean the given log
     *
     * @param cleanable The log to be cleaned
     * @return The first offset not cleaned and the statistics for this round of cleaning
     */
    protected Pair<Long, CleanerStats> clean(LogToClean cleanable) throws IOException, LogCleaningAbortedException, DigestException {
        return doClean(cleanable, time.milliseconds());
    }

    protected Pair<Long, CleanerStats> doClean(LogToClean cleanable, Long currentTime) throws IOException, LogCleaningAbortedException, DigestException {
        LOG.info("Beginning cleaning of log {}", cleanable.getLog().name());

        // figure out the timestamp below which it is safe to remove delete tombstones
        // this position is defined to be a configurable time beneath the last modified time of the last clean segment
        // this timestamp is only used on the older message formats older than MAGIC_VALUE_V2
        Long legacyDeleteHorizonMs;
        Iterable<LogSegment> logSegments = cleanable.getLog().logSegments(0L, cleanable.getFirstDirtyOffset());
        if (Objects.isNull(logSegments) || !logSegments.iterator().hasNext()) {
            legacyDeleteHorizonMs = 0L;
        } else {
            LogSegment seg = CollectionUtilExt.last(logSegments);
            legacyDeleteHorizonMs = seg.getLastModified() - cleanable.getLog().config().getDeleteRetentionMs();
        }

        UnifiedLog log = cleanable.getLog();
        CleanerStats stats = new CleanerStats();

        // build the offset map
        LOG.info("Building offset map for {}...", cleanable.getLog().name());
        long upperBoundOffset = cleanable.getFirstUncleanableOffset();
        buildOffsetMap(log, cleanable.getFirstDirtyOffset(), upperBoundOffset, offsetMap, stats);
        Long endOffset = offsetMap.latestOffset() + 1;
        stats.indexDone();

        // determine the timestamp up to which the log will be cleaned
        // this is the lower of the last active segment and the compaction lag
        long cleanableHorizonMs;
        Iterable<LogSegment> unCleanableSegments = log.logSegments(0L, cleanable.getFirstUncleanableOffset());
        if (Objects.isNull(unCleanableSegments) || !unCleanableSegments.iterator().hasNext()) {
            cleanableHorizonMs = 0L;
        } else {
            cleanableHorizonMs = CollectionUtilExt.last(unCleanableSegments).getLastModified();
        }

        // group the segments and clean the groups
        LOG.info("Cleaning log {} (cleaning prior to {}, discarding tombstones prior to upper bound deletion horizon {})...",
                log.name(), new Date(cleanableHorizonMs), new Date(legacyDeleteHorizonMs));
        CleanedTransactionMetadata transactionMetadata = new CleanedTransactionMetadata();

        List<List<LogSegment>> groupedSegments = groupSegmentsBySize(log.logSegments(0L, endOffset), log.config().getSegmentSize(),
                log.config().getMaxIndexSize(), cleanable.getFirstUncleanableOffset());
        for (List<LogSegment> group : groupedSegments) {
            cleanSegments(log, group, offsetMap, currentTime, stats, transactionMetadata, legacyDeleteHorizonMs);
        }

        // record buffer utilization
        stats.setBufferUtilization(offsetMap.utilization());

        stats.allDone();

        return Pair.of(endOffset, stats);
    }

    /**
     * Clean a group of segments into a single replacement segment
     *
     * @param log                   The log being cleaned
     * @param segments              The group of segments being cleaned
     * @param map                   The offset map to use for cleaning segments
     * @param currentTime           The current time in milliseconds
     * @param stats                 Collector for cleaning statistics
     * @param transactionMetadata   State of ongoing transactions which is carried between the cleaning
     *                              of the grouped segments
     * @param legacyDeleteHorizonMs The delete horizon used for tombstones whose version is less than 2
     */
    protected void cleanSegments(UnifiedLog log,
                                 List<LogSegment> segments,
                                 OffsetMap map,
                                 Long currentTime,
                                 CleanerStats stats,
                                 CleanedTransactionMetadata transactionMetadata,
                                 Long legacyDeleteHorizonMs) throws IOException, LogCleaningAbortedException {
        // create a new segment with a suffix appended to the name of the log and indexes
        LogSegment cleaned = UnifiedLog.createNewCleanedSegment(log.dir(), log.config(), CollectionUtilExt.head(segments).getBaseOffset());
        transactionMetadata.setCleanedIndex(Optional.of(cleaned.getTxnIndex()));

        try {
            // clean segments into the new destination segment
            Iterator<LogSegment> iter = segments.iterator();
            Optional<LogSegment> currentSegmentOpt = Optional.ofNullable(iter.next());
            Map<Long, LastRecord> lastOffsetOfActiveProducers = log.lastRecordsOfActiveProducers();

            while (currentSegmentOpt.isPresent()) {
                LogSegment currentSegment = currentSegmentOpt.get();
                Optional<LogSegment> nextSegmentOpt = iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();

                // Note that it is important to collect aborted transactions from the full log segment
                // range since we need to rebuild the full transaction index for the new segment.
                long startOffset = currentSegment.getBaseOffset();
                Long upperBoundOffset = nextSegmentOpt.map(LogSegment::getBaseOffset).orElse(currentSegment.readNextOffset());
                List<AbortedTxn> abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset);
                transactionMetadata.addAbortedTransactions(abortedTransactions);

                boolean retainLegacyDeletesAndTxnMarkers = currentSegment.getLastModified() > legacyDeleteHorizonMs;
                LOG.info("Cleaning {} in log {} into {} with an upper bound deletion horizon {} computed from " +
                                "the segment last modified time of {}, {} deletes.",
                        currentSegment, log.name(), cleaned.getBaseOffset(), legacyDeleteHorizonMs,
                        currentSegment.getLastModified(), retainLegacyDeletesAndTxnMarkers ? "retaining" : "discarding");

                try {
                    cleanInto(log.topicPartition(), currentSegment.getLog(), cleaned, map, retainLegacyDeletesAndTxnMarkers, log.config().getDeleteRetentionMs(),
                            log.config().getMaxMessageSize(), transactionMetadata, lastOffsetOfActiveProducers, stats, currentTime);
                } catch (LogSegmentOffsetOverflowException e) {
                    // Split the current segment. It's also safest to abort the current cleaning process, so that we retry from
                    // scratch once the split is complete.
                    LOG.info("Caught segment overflow error during cleaning: {}", e.getMessage());
                    log.splitOverflowedSegment(currentSegment);
                    throw new LogCleaningAbortedException();
                }
                currentSegmentOpt = nextSegmentOpt;
            }

            cleaned.onBecomeInactiveSegment();
            // flush new segment to disk before swap
            cleaned.flush();

            // update the modification date to retain the last modified date of the original files
            Long modified = CollectionUtilExt.last(segments).getLastModified();
            cleaned.setLastModified(modified);

            // swap in new segment
            LOG.info("Swapping in cleaned segment {} for segment(s) {} in log {}", cleaned, segments, log);
            log.replaceSegments(Arrays.asList(cleaned), segments);
        } catch (LogCleaningAbortedException e) {
            try {
                cleaned.deleteIfExists();
            } catch (Exception deleteException) {
                e.addSuppressed(deleteException);
            } finally {
                throw e;
            }
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided
     *
     * @param topicPartition                   The topic and partition of the log segment to clean
     * @param sourceRecords                    The dirty log segment
     * @param dest                             The cleaned log segment
     * @param map                              The key=>offset mapping
     * @param retainLegacyDeletesAndTxnMarkers Should tombstones (lower than version 2) and markers be retained while cleaning this segment
     * @param deleteRetentionMs                Defines how long a tombstone should be kept as defined by log configuration
     * @param maxLogMessageSize                The maximum message size of the corresponding topic
     * @param stats                            Collector for cleaning statistics
     * @param currentTime                      The time at which the clean was initiated
     */
    protected void cleanInto(TopicPartition topicPartition,
                             FileRecords sourceRecords,
                             LogSegment dest,
                             OffsetMap map,
                             Boolean retainLegacyDeletesAndTxnMarkers,
                             Long deleteRetentionMs,
                             Integer maxLogMessageSize,
                             CleanedTransactionMetadata transactionMetadata,
                             Map<Long, LastRecord> lastRecordsOfActiveProducers,
                             CleanerStats stats,
                             Long currentTime) throws LogCleaningAbortedException, IOException {
        MemoryRecords.RecordFilter logCleanerFilter = new MemoryRecords.RecordFilter(currentTime, deleteRetentionMs) {
            private Boolean discardBatchRecords;

            @Override
            protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                // we piggy-back on the tombstone retention logic to delay deletion of transaction markers.
                // note that we will never delete a marker until all the records from that transaction are removed.

                //todo 异常处理逻辑是否正确
                boolean canDiscardBatch;
                try {
                    canDiscardBatch = shouldDiscardBatch(batch, transactionMetadata);
                } catch (IOException e) {
                    LOG.error("checkBatchRetention error", e);
                    canDiscardBatch = false;
                }

                if (batch.isControlBatch()) {
                    discardBatchRecords = canDiscardBatch && batch.deleteHorizonMs().isPresent() && batch.deleteHorizonMs().getAsLong() <= currentTime;
                } else {
                    discardBatchRecords = canDiscardBatch;
                }

                boolean isBatchLastRecordOfProducer = isBatchLastRecordOfProducer(lastRecordsOfActiveProducers, batch);

                BatchRetention batchRetention;
                if (batch.hasProducerId() && isBatchLastRecordOfProducer) {
                    batchRetention = BatchRetention.RETAIN_EMPTY;
                } else if (discardBatchRecords) {
                    batchRetention = BatchRetention.DELETE;
                } else {
                    batchRetention = BatchRetention.DELETE_EMPTY;
                }
                return new BatchRetentionResult(batchRetention, canDiscardBatch && batch.isControlBatch());
            }

            @Override
            protected boolean shouldRetainRecord(RecordBatch batch, Record record) {
                if (discardBatchRecords) {
                    // The batch is only retained to preserve producer sequence information; the records can be removed
                    return false;
                } else if (batch.isControlBatch()) {
                    return true;
                } else {
                    //todo 异常处理逻辑是否正确
                    try {
                        return Cleaner.this.shouldRetainRecord(map, retainLegacyDeletesAndTxnMarkers, batch, record, stats, currentTime);
                    } catch (DigestException e) {
                        LOG.error("shouldRetainRecord error", e);
                        return true;
                    }
                }
            }
        };

        int position = 0;
        while (position < sourceRecords.sizeInBytes()) {
            checkDone.accept(topicPartition);
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
            readBuffer.clear();
            writeBuffer.clear();

            sourceRecords.readInto(readBuffer, position);
            MemoryRecords records = MemoryRecords.readableRecords(readBuffer);
            throttler.maybeThrottle((double) records.sizeInBytes());
            MemoryRecords.FilterResult result = records.filterTo(topicPartition, logCleanerFilter, writeBuffer, maxLogMessageSize, decompressionBufferSupplier);

            stats.readMessages(result.messagesRead(), result.bytesRead());
            stats.recopyMessages(result.messagesRetained(), result.bytesRetained());

            position += result.bytesRead();

            // if any messages are to be retained, write them out
            ByteBuffer outputBuffer = result.outputBuffer();
            if (outputBuffer.position() > 0) {
                outputBuffer.flip();
                MemoryRecords retained = MemoryRecords.readableRecords(outputBuffer);
                // it's OK not to hold the Log's lock in this case, because this segment is only accessed by other threads
                // after `Log.replaceSegments` (which acquires the lock) is called
                dest.append(result.maxOffset(),
                        result.maxTimestamp(),
                        result.shallowOffsetOfMaxTimestamp(),
                        retained);
                throttler.maybeThrottle((double) outputBuffer.limit());
            }

            // if we read bytes but didn't get even one complete batch, our I/O buffer is too small, grow it and try again
            // `result.bytesRead` contains bytes from `messagesRead` and any discarded batches.
            if (readBuffer.limit() > 0 && result.bytesRead() == 0) {
                growBuffersOrFail(sourceRecords, position, maxLogMessageSize, records);
            }
        }
        restoreBuffers();
    }

    private Boolean isBatchLastRecordOfProducer(Map<Long, LastRecord> lastRecordsOfActiveProducers,
                                                RecordBatch batch) {
        // We retain the batch in order to preserve the state of active producers. There are three cases:
        // 1) The producer is no longer active, which means we can delete all records for that producer.
        // 2) The producer is still active and has a last data offset. We retain the batch that contains
        //    this offset since it also contains the last sequence number for this producer.
        // 3) The last entry in the log is a transaction marker. We retain this marker since it has the
        //    last producer epoch, which is needed to ensure fencing.
        long producerId = batch.producerId();
        if (lastRecordsOfActiveProducers.containsKey(producerId)
                && Objects.nonNull(lastRecordsOfActiveProducers.get(producerId))) {
            LastRecord lastRecord = lastRecordsOfActiveProducers.get(producerId);
            Optional<Long> lastDataOffset = lastRecord.getLastDataOffset();
            if (lastDataOffset.isPresent()) {
                return batch.lastOffset() == lastDataOffset.get();
            } else {
                return batch.isControlBatch() && batch.producerEpoch() == lastRecord.getProducerEpoch();
            }
        } else {
            return false;
        }
    }


    /**
     * Grow buffers to process next batch of records from `sourceRecords.` Buffers are doubled in size
     * up to a maximum of `maxLogMessageSize`. In some scenarios, a record could be bigger than the
     * current maximum size configured for the log. For example:
     * 1. A compacted topic using compression may contain a message set slightly larger than max.message.bytes
     * 2. max.message.bytes of a topic could have been reduced after writing larger messages
     * In these cases, grow the buffer to hold the next batch.
     */
    private void growBuffersOrFail(FileRecords sourceRecords,
                                   Integer position,
                                   Integer maxLogMessageSize,
                                   MemoryRecords memoryRecords) throws IOException {
        Integer maxSize = null;
        if (readBuffer.capacity() >= maxLogMessageSize) {
            Integer nextBatchSize = memoryRecords.firstBatchSize();
            String logDesc = String.format("log segment %s at position %s", sourceRecords.file(), position);
            if (nextBatchSize == null) {
                throw new IllegalStateException("Could not determine next batch size for " + logDesc);
            }
            if (nextBatchSize <= 0) {
                throw new IllegalStateException("Invalid batch size $nextBatchSize for " + logDesc);
            }
            if (nextBatchSize <= readBuffer.capacity()) {
                String msg = String.format("Batch size %s < buffer size %s, but not processed for %s", nextBatchSize, readBuffer.capacity(), logDesc);
                throw new IllegalStateException(msg);
            }
            int bytesLeft = new Long(sourceRecords.channel().size() - position).intValue();
            if (nextBatchSize > bytesLeft) {
                String msg = String.format("Log segment may be corrupt, batch size %s > %s bytes left in segment for %s", nextBatchSize, bytesLeft, logDesc);
                throw new CorruptRecordException(msg);
            }
            maxSize = nextBatchSize;
        } else {
            maxSize = maxLogMessageSize;
        }

        growBuffers(maxSize);
    }

    private Boolean shouldDiscardBatch(RecordBatch batch, CleanedTransactionMetadata transactionMetadata) throws IOException {
        if (batch.isControlBatch()) {
            return transactionMetadata.onControlBatchRead(batch);
        } else {
            return transactionMetadata.onBatchRead(batch);
        }
    }

    private Boolean shouldRetainRecord(OffsetMap map,
                                       Boolean retainDeletesForLegacyRecords,
                                       RecordBatch batch,
                                       Record record,
                                       CleanerStats stats,
                                       Long currentTime) throws DigestException {
        boolean pastLatestOffset = record.offset() > map.latestOffset();
        if (pastLatestOffset) {
            return true;
        }

        if (record.hasKey()) {
            ByteBuffer key = record.key();
            long foundOffset = map.get(key);
            /* First,the message must have the latest offset for the key
             * then there are two cases in which we can retain a message:
             *   1) The message has value
             *   2) The message doesn't has value but it can't be deleted now.
             */
            boolean latestOffsetForKey = record.offset() >= foundOffset;
            boolean legacyRecord = batch.magic() < RecordBatch.MAGIC_VALUE_V2;
            boolean shouldRetainDeletes = !legacyRecord ?
                    !batch.deleteHorizonMs().isPresent() || currentTime < batch.deleteHorizonMs().getAsLong()
                    : retainDeletesForLegacyRecords;
            boolean isRetainedValue = record.hasValue() || shouldRetainDeletes;
            return latestOffsetForKey && isRetainedValue;
        } else {
            stats.invalidMessage();
            return false;
        }
    }

    /**
     * Double the I/O buffer capacity
     */
    public void growBuffers(Integer maxLogMessageSize) {
        int maxBufferSize = Math.max(maxLogMessageSize, maxIoBufferSize);
        if (readBuffer.capacity() >= maxBufferSize || writeBuffer.capacity() >= maxBufferSize) {
            throw new IllegalStateException(String.format("This log contains a message larger than maximum allowable size of %s.", maxBufferSize));
        }
        int newSize = Math.min(this.readBuffer.capacity() * 2, maxBufferSize);
        LOG.info("Growing cleaner I/O buffers from {} bytes to {} bytes.", readBuffer.capacity(), newSize);
        this.readBuffer = ByteBuffer.allocate(newSize);
        this.writeBuffer = ByteBuffer.allocate(newSize);
    }

    /**
     * Restore the I/O buffer capacity to its original size
     */
    public void restoreBuffers() {
        if (this.readBuffer.capacity() > this.ioBufferSize) {
            this.readBuffer = ByteBuffer.allocate(this.ioBufferSize);
        }
        if (this.writeBuffer.capacity() > this.ioBufferSize) {
            this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize);
        }
    }

    /**
     * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single
     * destination segment. This prevents segment sizes from shrinking too much.
     *
     * @param segments     The log segments to group
     * @param maxSize      the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     * @return A list of grouped segments
     */
    protected List<List<LogSegment>> groupSegmentsBySize(Iterable<LogSegment> segments,
                                                         Integer maxSize,
                                                         Integer maxIndexSize,
                                                         Long firstUncleanableOffset) throws IOException {
        List<List<LogSegment>> grouped = new ArrayList<>();

        List<LogSegment> segs = new ArrayList<>();
        for (LogSegment seg : segments) {
            segs.add(seg);
        }

        while (!segs.isEmpty()) {
            List<LogSegment> group = Arrays.asList(segs.get(0));
            long logSize = segs.get(0).size();
            long indexSize = segs.get(0).offsetIndex().sizeInBytes();
            long timeIndexSize = segs.get(0).timeIndex().sizeInBytes();
            segs = CollectionUtilExt.tail(segs);
            while (!segs.isEmpty() &&
                    logSize + segs.get(0).size() <= maxSize &&
                    indexSize + segs.get(0).offsetIndex().sizeInBytes() <= maxIndexSize &&
                    timeIndexSize + segs.get(0).timeIndex().sizeInBytes() <= maxIndexSize &&
                    //if first segment size is 0, we don't need to do the index offset range check.
                    //this will avoid empty log left every 2^31 message.
                    (segs.get(0).size() == 0 ||
                            lastOffsetForFirstSegment(segs, firstUncleanableOffset) - CollectionUtilExt.last(group).getBaseOffset() <= Integer.MAX_VALUE)) {
                group.add(0, CollectionUtilExt.head(segs));
                logSize += CollectionUtilExt.head(segs).size();
                indexSize += CollectionUtilExt.head(segs).offsetIndex().sizeInBytes();
                timeIndexSize += CollectionUtilExt.head(segs).timeIndex().sizeInBytes();
                segs = CollectionUtilExt.tail(segs);
            }
            Collections.reverse(group);
            grouped.add(0, group);
        }
        Collections.reverse(grouped);
        return grouped;
    }

    /**
     * We want to get the last offset in the first log segment in segs.
     * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
     * scanning the segment from the last index entry.
     * Therefore, we estimate the last offset of the first log segment by using
     * the base offset of the next segment in the list.
     * If the next segment doesn't exist, first Uncleanable Offset will be used.
     *
     * @param segs - remaining segments to group.
     * @return The estimated last offset for the first segment in segs
     */
    private Long lastOffsetForFirstSegment(List<LogSegment> segs, Long firstUncleanableOffset) {
        if (segs.size() > 1) {
            /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
             * the worst case offset */
            return segs.get(1).getBaseOffset() - 1;
        } else {
            //for the last segment in the list, use the first uncleanable offset.
            return firstUncleanableOffset - 1;
        }
    }

    /**
     * Build a map of key_hash => offset for the keys in the cleanable dirty portion of the log to use in cleaning.
     *
     * @param log   The log to use
     * @param start The offset at which dirty messages begin
     * @param end   The ending offset for the map that is being built
     * @param map   The map in which to store the mappings
     * @param stats Collector for cleaning statistics
     */
    protected void buildOffsetMap(UnifiedLog log,
                                  Long start,
                                  Long end,
                                  OffsetMap map,
                                  CleanerStats stats) throws IOException, LogCleaningAbortedException, DigestException {
        map.clear();
        ArrayList<LogSegment> dirty = new ArrayList<>();
        for (LogSegment seg : log.logSegments(start, end)) {
            dirty.add(seg);
        }
        List<Long> nextSegmentStartOffsets = new ArrayList<>();
        if (!dirty.isEmpty()) {
            for (LogSegment nextSegment : dirty.subList(1, dirty.size())) {
                nextSegmentStartOffsets.add(nextSegment.getBaseOffset());
            }
            nextSegmentStartOffsets.add(end);
        }
        LOG.info("Building offset map for log {} for {} segments in offset range [{}, {}).", log.name(), dirty.size(), start, end);

        CleanedTransactionMetadata transactionMetadata = new CleanedTransactionMetadata();
        List<AbortedTxn> abortedTransactions = log.collectAbortedTransactions(start, end);
        transactionMetadata.addAbortedTransactions(abortedTransactions);

        // Add all the cleanable dirty segments. We must take at least map.slots * load_factor,
        // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
        boolean full = false;
        for (int i = 0; i < Math.min(dirty.size(), nextSegmentStartOffsets.size()) && !full; i++) {
            LogSegment segment = dirty.get(i);
            Long nextSegmentStartOffset = nextSegmentStartOffsets.get(i);
            checkDone.accept(log.topicPartition());

            full = buildOffsetMapForSegment(log.topicPartition(), segment, map, start, nextSegmentStartOffset, log.config().getMaxMessageSize(),
                    transactionMetadata, stats);
            if (full) {
                LOG.debug("Offset map is full, {} segments fully mapped, segment with base offset {} is partially mapped",
                        dirty.indexOf(segment), segment.getBaseOffset());
            }
        }
        LOG.info("Offset map for log {} complete.", log.name());
    }

    /**
     * Add the messages in the given segment to the offset map
     *
     * @param segment The segment to index
     * @param map     The map in which to store the key=>offset mapping
     * @param stats   Collector for cleaning statistics
     * @return If the map was filled whilst loading from this segment
     */
    private Boolean buildOffsetMapForSegment(TopicPartition topicPartition,
                                             LogSegment segment,
                                             OffsetMap map,
                                             Long startOffset,
                                             Long nextSegmentStartOffset,
                                             Integer maxLogMessageSize,
                                             CleanedTransactionMetadata transactionMetadata,
                                             CleanerStats stats) throws IOException, LogCleaningAbortedException, DigestException {
        int position = segment.offsetIndex().lookup(startOffset).getPosition();
        int maxDesiredMapSize = new Double(map.slots() * this.dupBufferLoadFactor).intValue();
        while (position < segment.getLog().sizeInBytes()) {
            checkDone.accept(topicPartition);
            readBuffer.clear();
            try {
                segment.getLog().readInto(readBuffer, position);
            } catch (Exception e) {
                String msg = String.format("Failed to read from segment %s of partition %s while loading offset map",
                        segment, topicPartition);
                throw new KafkaException(msg, e);
            }
            MemoryRecords records = MemoryRecords.readableRecords(readBuffer);
            throttler.maybeThrottle((double) records.sizeInBytes());

            int startPosition = position;
            for (MutableRecordBatch batch : records.batches()) {
                if (batch.isControlBatch()) {
                    transactionMetadata.onControlBatchRead(batch);
                    stats.indexMessagesRead(1);
                } else {
                    boolean isAborted = transactionMetadata.onBatchRead(batch);
                    if (isAborted) {
                        // If the batch is aborted, do not bother populating the offset map.
                        // Note that abort markers are supported in v2 and above, which means count is defined.
                        stats.indexMessagesRead(batch.countOrNull());
                    } else {
                        CloseableIterator<Record> recordsIterator = batch.streamingIterator(decompressionBufferSupplier);
                        try {
                            while (recordsIterator.hasNext()) {
                                Record record = recordsIterator.next();
                                if (record.hasKey() && record.offset() >= startOffset) {
                                    if (map.size() < maxDesiredMapSize) {
                                        map.put(record.key(), record.offset());
                                    } else {
                                        return true;
                                    }
                                }
                                stats.indexMessagesRead(1);
                            }
                        } finally {
                            recordsIterator.close();
                        }
                    }
                }

                if (batch.lastOffset() >= startOffset)
                    map.updateLatestOffset(batch.lastOffset());
            }
            int bytesRead = records.validBytes();
            position += bytesRead;
            stats.indexBytesRead(bytesRead);

            // if we didn't read even one complete message, our read buffer may be too small
            if (position == startPosition) {
                growBuffersOrFail(segment.getLog(), position, maxLogMessageSize, records);
            }
        }

        // In the case of offsets gap, fast forward to latest expected offset in this segment.
        map.updateLatestOffset(nextSegmentStartOffset - 1L);

        restoreBuffers();
        return false;
    }
}
