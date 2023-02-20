package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.LogSegmentOffsetOverflowException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaTimer;
import cn.pockethub.permanentqueue.kafka.server.FetchDataInfo;
import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import cn.pockethub.permanentqueue.kafka.server.epoch.LeaderEpochFileCache;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import cn.pockethub.permanentqueue.kafka.utils.Logging;
import lombok.Getter;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * <p/>
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 * <p/>
 * NOT thread-safe!
 */
@Getter
public class LogSegment extends Logging {

    private static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

    private final FileRecords log;
    private final LazyIndex<OffsetIndex> lazyOffsetIndex;
    private final LazyIndex<TimeIndex> lazyTimeIndex;
    private final TransactionIndex txnIndex;
    private final long baseOffset;
    private final int indexIntervalBytes;
    private final long rollJitterMs;
    private final Time time;

    private long created;

    /* the number of bytes since we last added an entry in the offset index */
    private int bytesSinceLastIndexEntry = 0;

    // The timestamp we used for time based log rolling and for ensuring max compaction delay
    // volatile for LogCleaner to see the update
    private volatile Optional<Long> rollingBasedTimestamp = Optional.empty();

    /* The maximum timestamp and offset we see so far */
    private volatile TimestampOffset _maxTimestampAndOffsetSoFar = TimestampOffset.Unknown;

    /**
     * @param log                The file records containing log entries
     * @param lazyOffsetIndex    The offset index
     * @param lazyTimeIndex      The timestamp index
     * @param txnIndex           The transaction index
     * @param baseOffset         A lower bound on the offsets in this segment
     * @param indexIntervalBytes The approximate number of bytes between entries in the index
     * @param rollJitterMs       The maximum random jitter subtracted from the scheduled segment roll time
     * @param time               The time instance
     */
    public LogSegment(final FileRecords log,
                      final LazyIndex<OffsetIndex> lazyOffsetIndex,
                      final LazyIndex<TimeIndex> lazyTimeIndex,
                      final TransactionIndex txnIndex,
                      final long baseOffset,
                      final int indexIntervalBytes,
                      final long rollJitterMs,
                      final Time time) {
        this.log = log;
        this.lazyOffsetIndex = lazyOffsetIndex;
        this.lazyTimeIndex = lazyTimeIndex;
        this.txnIndex = txnIndex;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.rollJitterMs = rollJitterMs;
        this.time = time;

        created = time.milliseconds();
    }

    public OffsetIndex offsetIndex() throws IOException {
        return lazyOffsetIndex.get();
    }

    public TimeIndex timeIndex() throws IOException {
        return lazyTimeIndex.get();
    }

    public Boolean shouldRoll(RollParams rollParams) throws IOException {
        boolean reachedRollMs = timeWaitedForRoll(rollParams.getNow(), rollParams.getMaxTimestampInMessages()) > rollParams.getMaxSegmentMs() - rollJitterMs;
        return (size() > rollParams.getMaxSegmentBytes() - rollParams.getMessagesSize())
                || (size() > 0 && reachedRollMs)
                || offsetIndex().isFull()
                || timeIndex().isFull()
                || !canConvertToRelativeOffset(rollParams.getMaxOffsetInMessages());
    }

    public void resizeIndexes(Integer size) throws IOException {
        offsetIndex().resize(size);
        timeIndex().resize(size);
    }

    public void sanityCheck(Boolean timeIndexFileNewlyCreated) throws NoSuchFileException, CorruptIndexException, IOException {
        if (lazyOffsetIndex.file().exists()) {
            // Resize the time index file to 0 if it is newly created.
            if (timeIndexFileNewlyCreated) {
                timeIndex().resize(0);
            }
            // Sanity checks for time index and offset index are skipped because
            // we will recover the segments above the recovery point in recoverLog()
            // in any case so sanity checking them here is redundant.
            txnIndex.sanityCheck();
        } else {
            throw new NoSuchFileException(String.format("Offset index file %s does not exist", lazyOffsetIndex.file().getAbsolutePath()));
        }
    }

    public void setMaxTimestampAndOffsetSoFar(TimestampOffset timestampOffset) {
        _maxTimestampAndOffsetSoFar = timestampOffset;
    }

    public TimestampOffset getMaxTimestampAndOffsetSoFar() throws IOException {
        if (_maxTimestampAndOffsetSoFar == TimestampOffset.Unknown) {
            _maxTimestampAndOffsetSoFar = timeIndex().lastEntry();
        }
        return _maxTimestampAndOffsetSoFar;
    }

    /* The maximum timestamp we see so far */
    public Long maxTimestampSoFar() throws IOException {
        return getMaxTimestampAndOffsetSoFar().getTimestamp();
    }

    public Long offsetOfMaxTimestampSoFar() throws IOException {
        return getMaxTimestampAndOffsetSoFar().getOffset();
    }

    /* Return the size in bytes of this log segment */
    public Integer size() {
        return log.sizeInBytes();
    }

    /**
     * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
     */
    public Boolean canConvertToRelativeOffset(Long offset) throws IOException {
        return offsetIndex().canAppendOffset(offset);
    }

    /**
     * Append the given messages starting with the given offset. Add
     * an entry to the index if needed.
     * <p>
     * It is assumed this method is being called from within a lock.
     *
     * @param largestOffset               The last offset in the message set
     * @param largestTimestamp            The largest timestamp in the message set.
     * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
     * @param records                     The log entries to append.
     * @return the physical position in the file of the appended records
     * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
     */
//    @nonthreadsafe
    public void append(Long largestOffset,
                       Long largestTimestamp,
                       Long shallowOffsetOfMaxTimestamp,
                       MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > 0) {
            LOG.trace("Inserting {} bytes at end offset {} at position {} with largest timestamp {} at shallow offset {}",
                    records.sizeInBytes(), largestOffset, log.sizeInBytes(), largestTimestamp, shallowOffsetOfMaxTimestamp);
            int physicalPosition = log.sizeInBytes();
            if (physicalPosition == 0) {
                rollingBasedTimestamp = Optional.of(largestTimestamp);
            }

            ensureOffsetInRange(largestOffset);

            // append the messages
            int appendedBytes = log.append(records);
            LOG.trace("Appended {} to ${log.file} at end offset {}", appendedBytes, largestOffset);
            // Update the in memory max timestamp and corresponding offset.
            if (largestTimestamp > maxTimestampSoFar()) {
                setMaxTimestampAndOffsetSoFar(new TimestampOffset(largestTimestamp, shallowOffsetOfMaxTimestamp));
            }
            // append an entry to the index (if needed)
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                offsetIndex().append(largestOffset, physicalPosition);
                timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), false);
                bytesSinceLastIndexEntry = 0;
            }
            bytesSinceLastIndexEntry += records.sizeInBytes();
        }
    }

    private void ensureOffsetInRange(Long offset) throws IOException {
        if (!canConvertToRelativeOffset(offset)) {
            throw new LogSegmentOffsetOverflowException(this, offset);
        }
    }

    private Integer appendChunkFromFile(FileRecords records, Integer position, BufferSupplier bufferSupplier) throws IOException {
        int bytesToAppend = 0;
        long maxTimestamp = Long.MIN_VALUE;
        long offsetOfMaxTimestamp = Long.MIN_VALUE;
        long maxOffset = Long.MIN_VALUE;
        ByteBuffer readBuffer = bufferSupplier.get(1024 * 1024);

        // find all batches that are valid to be appended to the current log segment and
        // determine the maximum offset and timestamp
        for (FileLogInputStream.FileChannelRecordBatch batch : records.batchesFrom(position)) {
            if (!canAppend(batch, bytesToAppend, readBuffer)) {
                break;
            }
            if (batch.maxTimestamp() > maxTimestamp) {
                maxTimestamp = batch.maxTimestamp();
                offsetOfMaxTimestamp = batch.lastOffset();
            }
            maxOffset = batch.lastOffset();
            bytesToAppend += batch.sizeInBytes();
        }

        if (bytesToAppend > 0) {
            // Grow buffer if needed to ensure we copy at least one batch
            if (readBuffer.capacity() < bytesToAppend) {
                readBuffer = bufferSupplier.get(bytesToAppend);
            }

            readBuffer.limit(bytesToAppend);
            records.readInto(readBuffer, position);

            append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer));
        }

        bufferSupplier.release(readBuffer);
        return bytesToAppend;
    }

    private Boolean canAppend(RecordBatch batch, int bytesToAppend, ByteBuffer readBuffer) throws IOException {
        return canConvertToRelativeOffset(batch.lastOffset()) &&
                (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes() < readBuffer.capacity());
    }

    /**
     * Append records from a file beginning at the given position until either the end of the file
     * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
     *
     * @return the number of bytes appended to the log (may be less than the size of the input if an
     * offset is encountered which would overflow this segment)
     */
    public Integer appendFromFile(FileRecords records, Integer start) throws IOException {
        int position = start;
        BufferSupplier bufferSupplier = new BufferSupplier.GrowableBufferSupplier();
        while (position < start + records.sizeInBytes()) {
            int bytesAppended = appendChunkFromFile(records, position, bufferSupplier);
            if (bytesAppended == 0) {
                return position - start;
            }
            position += bytesAppended;
        }
        return position - start;
    }

    //    @nonthreadsafe
    public void updateTxnIndex(CompletedTxn completedTxn, Long lastStableOffset) throws IOException {
        if (completedTxn.getIsAborted()) {
            LOG.trace("Writing aborted transaction {} to transaction index, last stable offset is {}", completedTxn, lastStableOffset);
            txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset));
        }
    }

    private void updateProducerState(ProducerStateManager producerStateManager, RecordBatch batch) throws IOException {
        if (batch.hasProducerId()) {
            long producerId = batch.producerId();
            ProducerAppendInfo appendInfo = producerStateManager.prepareUpdate(producerId, AppendOrigin.Replication);
            Optional<CompletedTxn> maybeCompletedTxn = appendInfo.append(batch, Optional.empty());
            producerStateManager.update(appendInfo);
            if (maybeCompletedTxn.isPresent()) {
                CompletedTxn completedTxn = maybeCompletedTxn.get();
                Long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                updateTxnIndex(completedTxn, lastStableOffset);
                producerStateManager.completeTxn(completedTxn);
            }
        }
        producerStateManager.updateMapEndOffset(batch.lastOffset() + 1);
    }

    /**
     * Find the physical file position for the first message with offset >= the requested offset.
     * <p>
     * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
     * in the file higher than the greatest-lower-bound from the index.
     *
     * @param offset               The offset we want to translate
     * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
     *                             when omitted, the search will begin at the position in the offset index.
     * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
     * message or null if no message meets this criteria.
     */
//    @threadsafe
    protected FileRecords.LogOffsetPosition translateOffset(Long offset, Integer startingFilePosition) throws IOException {
        OffsetPosition mapping = offsetIndex().lookup(offset);
        return log.searchForOffsetWithSize(offset, Math.max(mapping.getPosition(), startingFilePosition));
    }

    public FetchDataInfo read(Long startOffset,
                              Integer maxSize) throws IOException {
        return read(startOffset, maxSize, (long) size(), false);
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
     * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     *
     * @param startOffset   A lower bound on the first offset to include in the message set we read
     * @param maxSize       The maximum number of bytes to include in the message set we read
     * @param maxPosition   The maximum position in the log segment that should be exposed for read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
     * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
     * or null if the startOffset is larger than the largest offset in this log
     */
//    @ThreadSafe
    public FetchDataInfo read(Long startOffset,
                              Integer maxSize,
                              Long maxPosition,
                              Boolean minOneMessage) throws IOException {
        if (maxSize < 0) {
            String msg = String.format("Invalid max size %s for log read from segment %s", maxSize, log);
            throw new IllegalArgumentException(msg);
        }

        FileRecords.LogOffsetPosition startOffsetAndSize = translateOffset(startOffset, 0);

        // if the start position is already off the end of the log, return null
        if (startOffsetAndSize == null) {
            return null;
        }

        int startPosition = startOffsetAndSize.position;
        LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition);

        int adjustedMaxSize = minOneMessage ? Math.max(maxSize, startOffsetAndSize.size) : maxSize;

        // return a log segment but with zero size in the case below
        if (adjustedMaxSize == 0) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY);
        }

        // calculate the length of the message set to read based on whether or not they gave us a maxOffset
        int fetchSize = Math.min(new Long(maxPosition - startPosition).intValue(), adjustedMaxSize);

        return new FetchDataInfo(offsetMetadata,
                log.slice(startPosition, fetchSize),
                adjustedMaxSize < startOffsetAndSize.size,
                Optional.empty()
        );
    }

    public Optional<Long> fetchUpperBoundOffset(OffsetPosition startOffsetPosition, Integer fetchSize) throws IOException {
        return offsetIndex().fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(OffsetPosition::getOffset);
    }

    /**
     * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
     * from the end of the log and index.
     *
     * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
     *                             the transaction index.
     * @param leaderEpochCache     Optionally a cache for updating the leader epoch during recovery.
     * @return The number of bytes truncated from the log
     * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
     */
//    @nonthreadsafe
    public Integer recover(ProducerStateManager producerStateManager, Optional<LeaderEpochFileCache> leaderEpochCache) throws IOException {
        offsetIndex().reset();
        timeIndex().reset();
        txnIndex.reset();
        int validBytes = 0;
        int lastIndexEntry = 0;
        setMaxTimestampAndOffsetSoFar(TimestampOffset.Unknown);
        try {
            for (FileLogInputStream.FileChannelRecordBatch batch : log.batches()) {
                batch.ensureValid();
                ensureOffsetInRange(batch.lastOffset());

                // The max timestamp is exposed at the batch level, so no need to iterate the records
                if (batch.maxTimestamp() > maxTimestampSoFar()) {
                    setMaxTimestampAndOffsetSoFar(new TimestampOffset(batch.maxTimestamp(), batch.lastOffset()));
                }

                // Build offset index
                if (validBytes - lastIndexEntry > indexIntervalBytes) {
                    offsetIndex().append(batch.lastOffset(), validBytes);
                    timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), false);
                    lastIndexEntry = validBytes;
                }
                validBytes += batch.sizeInBytes();

                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    if (leaderEpochCache.isPresent()) {
                        LeaderEpochFileCache cache = leaderEpochCache.get();
                        if (batch.partitionLeaderEpoch() >= 0
                                && (!cache.latestEpoch().isPresent() || batch.partitionLeaderEpoch() > cache.latestEpoch().get())) {
                            cache.assign(batch.partitionLeaderEpoch(), batch.baseOffset());
                        }
                    }
                    updateProducerState(producerStateManager, batch);
                }
            }
        } catch (CorruptRecordException | InvalidRecordException e) {
            LOG.warn("Found invalid messages in log segment {} at byte offset {}: {}. {}",
                    log.file().getAbsolutePath(), validBytes, e.getMessage(), e.getCause());
        }
        int truncated = log.sizeInBytes() - validBytes;
        if (truncated > 0) {
            LOG.debug("Truncated {} invalid bytes at the end of segment {} during recovery", truncated, log.file().getAbsoluteFile());
        }

        log.truncateTo(validBytes);
        offsetIndex().trimToValidSize();
        // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
        timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), true);
        timeIndex().trimToValidSize();
        return truncated;
    }

    private void loadLargestTimestamp() throws IOException {
        // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
        TimestampOffset lastTimeIndexEntry = timeIndex().lastEntry();
        setMaxTimestampAndOffsetSoFar(lastTimeIndexEntry);

        OffsetPosition offsetPosition = offsetIndex().lookup(lastTimeIndexEntry.getOffset());
        // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
        FileRecords.TimestampAndOffset maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.getPosition());
        if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.getTimestamp()) {
            setMaxTimestampAndOffsetSoFar(new TimestampOffset(maxTimestampOffsetAfterLastEntry.timestamp, maxTimestampOffsetAfterLastEntry.offset));
        }
    }

    /**
     * Check whether the last offset of the last batch in this segment overflows the indexes.
     */
    public Boolean hasOverflow() throws IOException {
        long nextOffset = readNextOffset();
        return nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1);
    }

    public TxnIndexSearchResult collectAbortedTxns(Long fetchOffset, Long upperBoundOffset) throws IOException {
        return txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset);
    }

    @Override
    public String toString() {
        long largestRecordTs = 0;
        try {
            Optional<Long> optional = largestRecordTimestamp();
            if (optional.isPresent()) {
                largestRecordTs = optional.get();
            }
        } catch (Throwable e) {

        }
        return "LogSegment(baseOffset=" + baseOffset +
                ", size=" + size() +
                ", lastModifiedTime=" + getLastModified() +
                ", largestRecordTimestamp=" + largestRecordTs +
                ")";
    }

    /**
     * Truncate off all index and log entries with offsets >= the given offset.
     * If the given offset is larger than the largest message in this segment, do nothing.
     *
     * @param offset The offset to truncate to
     * @return The number of log bytes truncated
     */
//    @nonthreadsafe
    public Integer truncateTo(Long offset) throws IOException {
        // Do offset translation before truncating the index to avoid needless scanning
        // in case we truncate the full index
        FileRecords.LogOffsetPosition mapping = translateOffset(offset, 0);
        offsetIndex().truncateTo(offset);
        timeIndex().truncateTo(offset);
        txnIndex.truncateTo(offset);

        // After truncation, reset and allocate more space for the (new currently active) index
        offsetIndex().resize(offsetIndex().getMaxIndexSize());
        timeIndex().resize(timeIndex().getMaxIndexSize());

        int bytesTruncated = mapping == null ? 0 : log.truncateTo(mapping.position);
        if (log.sizeInBytes() == 0) {
            created = time.milliseconds();
            rollingBasedTimestamp = Optional.empty();
        }

        bytesSinceLastIndexEntry = 0;
        if (maxTimestampSoFar() >= 0) {
            loadLargestTimestamp();
        }
        return bytesTruncated;
    }

    /**
     * Calculate the offset that would be used for the next message to be append to this segment.
     * Note that this is expensive.
     */
//    @threadsafe
    public Long readNextOffset() throws IOException {
        FetchDataInfo fetchData = read(offsetIndex().lastOffset(), log.sizeInBytes());
        if (fetchData == null) {
            return baseOffset;
        } else {
            return CollectionUtilExt.lastOptional(fetchData.getRecords().batches())
                    .map(RecordBatch::nextOffset)
                    .orElse(baseOffset);
        }
    }

    /**
     * Flush this log segment to disk
     */
//    @threadsafe
    public void flush() throws IOException {
        LogFlushStats.logFlushTimer.time(() -> {
            log.flush();
            offsetIndex().flush();
            timeIndex().flush();
            txnIndex.flush();
            return null;
        });
    }

    /**
     * Update the directory reference for the log and indices in this segment. This would typically be called after a
     * directory is renamed.
     */
    public void updateParentDir(File dir) {
        log.updateParentDir(dir);
        lazyOffsetIndex.updateParentDir(dir);
        lazyTimeIndex.updateParentDir(dir);
        txnIndex.updateParentDir(dir);
    }

    /**
     * Change the suffix for the index and log files for this log segment
     * IOException from this method should be handled by the caller
     */
    public void changeFileSuffixes(String oldSuffix, String newSuffix) throws IOException {
        log.renameTo(new File(CoreUtils.replaceSuffix(log.file().getPath(), oldSuffix, newSuffix)));
        lazyOffsetIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyOffsetIndex.file().getPath(), oldSuffix, newSuffix)));
        lazyTimeIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyTimeIndex.file().getPath(), oldSuffix, newSuffix)));
        txnIndex.renameTo(new File(CoreUtils.replaceSuffix(txnIndex.file().getPath(), oldSuffix, newSuffix)));
    }

    public Boolean hasSuffix(String suffix) {
        return log.file().getName().endsWith(suffix) &&
                lazyOffsetIndex.file().getName().endsWith(suffix) &&
                lazyTimeIndex.file().getName().endsWith(suffix) &&
                txnIndex.file().getName().endsWith(suffix);
    }

    /**
     * Append the largest time index entry to the time index and trim the log and indexes.
     * <p>
     * The time index entry appended will be used to decide when to delete the segment.
     */
    public void onBecomeInactiveSegment() throws IOException {
        timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), true);
        offsetIndex().trimToValidSize();
        timeIndex().trimToValidSize();
        log.trim();
    }

    /**
     * If not previously loaded,
     * load the timestamp of the first message into memory.
     */
    private void loadFirstBatchTimestamp() {
        if (!rollingBasedTimestamp.isPresent()) {
            Iterator<FileLogInputStream.FileChannelRecordBatch> iter = log.batches().iterator();
            if (iter.hasNext()) {
                rollingBasedTimestamp = Optional.of(iter.next().maxTimestamp());
            }
        }
    }

    /**
     * The time this segment has waited to be rolled.
     * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
     * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
     * segment rolling time.
     * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
     * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
     * segment rolling time.
     */
    public Long timeWaitedForRoll(Long now, Long messageTimestamp) {
        // Load the timestamp of the first message into memory
        loadFirstBatchTimestamp();
        if (rollingBasedTimestamp.isPresent() && rollingBasedTimestamp.get() >= 0) {
            Long t = rollingBasedTimestamp.get();
            return messageTimestamp - t;
        } else {
            return now - created;
        }
    }

    /**
     * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
     */
    public Long getFirstBatchTimestamp() {
        loadFirstBatchTimestamp();
        if (rollingBasedTimestamp.isPresent() && rollingBasedTimestamp.get() >= 0) {
            return rollingBasedTimestamp.get();
        } else {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Search the message offset based on timestamp and offset.
     * <p>
     * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
     * <p>
     * - If all the messages in the segment have smaller offsets, return None
     * - If all the messages in the segment have smaller timestamps, return None
     * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
     * the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
     * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
     * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
     * <p>
     * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
     * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
     * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
     * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
     *
     * @param timestamp      The timestamp to search for.
     * @param startingOffset The starting offset to search.
     * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
     */
    public Optional<FileRecords.TimestampAndOffset> findOffsetByTimestamp(Long timestamp, Long startingOffset) throws IOException {
        // Get the index entry with a timestamp less than or equal to the target timestamp
        TimestampOffset timestampOffset = timeIndex().lookup(timestamp);
        int position = offsetIndex().lookup(Math.max(timestampOffset.getOffset(), startingOffset)).getPosition();

        // Search the timestamp
        return Optional.of(log.searchForTimestamp(timestamp, position, startingOffset));
    }

    /**
     * Close this log segment
     */
    public void close() {
        if (_maxTimestampAndOffsetSoFar != TimestampOffset.Unknown) {
            CoreUtils.swallow(() -> timeIndex().maybeAppend(maxTimestampSoFar(), offsetOfMaxTimestampSoFar(), true), this);
        }
        CoreUtils.swallow(() -> lazyOffsetIndex.close(), this);
        CoreUtils.swallow(() -> lazyTimeIndex.close(), this);
        CoreUtils.swallow(() -> log.close(), this);
        CoreUtils.swallow(() -> txnIndex.close(), this);
    }

    /**
     * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
     */
    public void closeHandlers() {
        CoreUtils.swallow(() -> lazyOffsetIndex.closeHandler(), this);
        CoreUtils.swallow(() -> lazyTimeIndex.closeHandler(), this);
        CoreUtils.swallow(() -> log.closeHandlers(), this);
        CoreUtils.swallow(() -> txnIndex.close(), this);
    }

    /**
     * Delete this log segment from the filesystem.
     */
    public void deleteIfExists() throws IOException {
        CoreUtils.tryAll(Arrays.asList(
                () -> delete(() -> log.deleteIfExists(), "log", log.file(), true),
                () -> delete(() -> lazyOffsetIndex.deleteIfExists(), "offset index", lazyOffsetIndex.file(), true),
                () -> delete(() -> lazyTimeIndex.deleteIfExists(), "time index", lazyTimeIndex.file(), true),
                () -> delete(() -> txnIndex.deleteIfExists(), "transaction index", txnIndex.file(), false)
        ));
    }

    private void delete(SupplierWithIOException<Boolean> delete, String fileType, File file, Boolean logIfMissing) throws IOException {
        try {
            if (delete.get())
                LOG.info("Deleted {} {}.", fileType, file.getAbsolutePath());
            else if (logIfMissing) {
                LOG.info("Failed to delete {} {} because it does not exist.", fileType, file.getAbsolutePath());
            }
        } catch (IOException e) {
            String msg = String.format("Delete of %s %s failed.", fileType, file.getAbsolutePath());
            throw new IOException(msg, e);
        }
    }

    public Boolean deleted() {
        return !log.file().exists() && !lazyOffsetIndex.file().exists() && !lazyTimeIndex.file().exists() && !txnIndex.file().exists();
    }

    /**
     * The last modified time of this log segment as a unix time stamp
     */
    public Long getLastModified() {
        return log.file().lastModified();
    }

    /**
     * The largest timestamp this segment contains, if maxTimestampSoFar >= 0, otherwise None.
     */
    public Optional<Long> largestRecordTimestamp() throws IOException {
        return (maxTimestampSoFar() >= 0) ? Optional.of(maxTimestampSoFar()) : Optional.empty();
    }

    /**
     * The largest timestamp this segment contains.
     */
    public Long largestTimestamp() throws IOException {
        return (maxTimestampSoFar() >= 0) ? maxTimestampSoFar() : getLastModified();
    }

    /**
     * Change the last modified time for this log segment
     */
    public void setLastModified(Long ms) throws IOException {
        FileTime fileTime = FileTime.fromMillis(ms);
        Files.setLastModifiedTime(log.file().toPath(), fileTime);
        Files.setLastModifiedTime(lazyOffsetIndex.file().toPath(), fileTime);
        Files.setLastModifiedTime(lazyTimeIndex.file().toPath(), fileTime);
    }

    public static LogSegment open(File dir, Long baseOffset, LogConfig config, Time time, Boolean fileAlreadyExists,
                                  Integer initFileSize, Boolean preallocate, String fileSuffix) throws IOException {
        Integer maxIndexSize = config.getMaxIndexSize();
        return new LogSegment(
                FileRecords.open(UnifiedLog.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
                LazyIndex.forOffset(UnifiedLog.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize, true),
                LazyIndex.forTime(UnifiedLog.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize, true),
                new TransactionIndex(baseOffset, UnifiedLog.transactionIndexFile(dir, baseOffset, fileSuffix)),
                baseOffset,
                config.getIndexInterval(),
                config.randomSegmentJitter(),
                time);
    }

    public static void deleteIfExists(File dir, Long baseOffset, String fileSuffix) throws IOException {
        UnifiedLog.deleteFileIfExists(UnifiedLog.offsetIndexFile(dir, baseOffset, fileSuffix));
        UnifiedLog.deleteFileIfExists(UnifiedLog.timeIndexFile(dir, baseOffset, fileSuffix));
        UnifiedLog.deleteFileIfExists(UnifiedLog.transactionIndexFile(dir, baseOffset, fileSuffix));
        UnifiedLog.deleteFileIfExists(UnifiedLog.logFile(dir, baseOffset, fileSuffix));
    }

    static class LogFlushStats extends KafkaMetricsGroup {
        public static final KafkaTimer logFlushTimer = new KafkaTimer(newTimerStatic("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, new HashMap<>(), LogFlushStats.class));
    }
}
