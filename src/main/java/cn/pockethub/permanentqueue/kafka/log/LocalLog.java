package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.function.BiFunctionWithIOException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.server.FetchDataInfo;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.Scheduler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LocalLog extends KafkaMetricsGroup {

    private static final Logger LOG = LoggerFactory.getLogger(LocalLog.class);

    /**
     * a log file
     */
    protected static final String LogFileSuffix = ".log";

    /**
     * an index file
     */
    protected static final String IndexFileSuffix = ".index";

    /**
     * a time index file
     */
    protected static final String TimeIndexFileSuffix = ".timeindex";

    /**
     * an (aborted) txn index
     */
    protected static final String TxnIndexFileSuffix = ".txnindex";

    /**
     * a file that is scheduled to be deleted
     */
    protected static final String DeletedFileSuffix = ".deleted";

    /**
     * A temporary file that is being used for log cleaning
     */
    protected static final String CleanedFileSuffix = ".cleaned";

    /**
     * A temporary file used when swapping files into the log
     */
    protected static final String SwapFileSuffix = ".swap";

    /**
     * a directory that is scheduled to be deleted
     */
    protected static final String DeleteDirSuffix = "-delete";

    /**
     * a directory that is used for future partition
     */
    protected static final String FutureDirSuffix = "-future";

    protected static final Pattern DeleteDirPattern = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + DeleteDirSuffix);
    protected static final Pattern FutureDirPattern = Pattern.compile("^(\\S+)-(\\S+)\\.(\\S+)" + FutureDirSuffix);

    protected static final Long UnknownOffset = -1L;


    private volatile File _dir;
    protected volatile LogConfig config;
    protected LogSegments segments;
    protected volatile Long recoveryPoint;
    private volatile LogOffsetMetadata nextOffsetMetadata;
    protected Scheduler scheduler;
    protected Time time;
    protected TopicPartition topicPartition;
    protected LogDirFailureChannel logDirFailureChannel;

    private final String logIdent;

    // The memory mapped buffer for index files of this log will be closed with either delete() or closeHandlers()
    // After memory mapped buffer is closed, no disk IO operation should be performed for this log.
    private volatile Boolean isMemoryMappedBufferClosed = false;

    // Cache value of parent directory to avoid allocations in hot paths like ReplicaManager.checkpointHighWatermarks
    private volatile String _parentDir;

    // Last time the log was flushed
    private AtomicLong lastFlushedTime;

    protected File dir() {
        return _dir;
    }

    protected String name() {
        return dir().getName();
    }

    protected String parentDir() {
        return _parentDir;
    }

    protected File parentDirFile() {
        return new File(_parentDir);
    }

    protected Boolean isFuture() {
        return dir().getName().endsWith(LocalLog.FutureDirSuffix);
    }

    public LocalLog(File _dir,
                    LogConfig config,
                    LogSegments segments,
                    Long recoveryPoint,
                    LogOffsetMetadata nextOffsetMetadata,
                    Scheduler scheduler,
                    Time time,
                    TopicPartition topicPartition,
                    LogDirFailureChannel logDirFailureChannel) {

        this._dir = _dir;
        this.config = config;
        this.segments = segments;
        this.recoveryPoint = recoveryPoint;
        this.nextOffsetMetadata = nextOffsetMetadata;
        this.scheduler = scheduler;
        this.time = time;
        this.topicPartition = topicPartition;
        this.logDirFailureChannel = logDirFailureChannel;

        this.logIdent = String.format("[LocalLog partition=%s, dir=%s] ", topicPartition, dir().getParent());

        this._parentDir = dir().getParent();

        this.lastFlushedTime = new AtomicLong(time.milliseconds());
    }

//    @Override
//    public String logIdent() {
//        return logIdent;
//    }

    private <T> T maybeHandleIOException(String msg, SupplierWithIOException<T> fun) {
        return LocalLog.maybeHandleIOException(logDirFailureChannel, parentDir(), msg, fun);
    }

    /**
     * Rename the directory of the log
     *
     * @param name the new dir name
     * @throws KafkaStorageException if rename fails
     */
    protected Boolean renameDir(String name) {
        String msg = String.format("Error while renaming dir for %s in log dir %s", topicPartition, dir().getParent());
        return maybeHandleIOException(msg, new SupplierWithIOException<Boolean>() {
            @Override
            public Boolean get() throws IOException {
                File renamedDir = new File(dir().getParent(), name);
                Utils.atomicMoveWithFallback(dir().toPath(), renamedDir.toPath());
                if (!renamedDir.equals(dir())) {
                    _dir = renamedDir;
                    _parentDir = renamedDir.getParent();
                    segments.updateParentDir(renamedDir);
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Update the existing configuration to the new provided configuration.
     *
     * @param newConfig the new configuration to be updated to
     */
    protected void updateConfig(LogConfig newConfig) {
        LogConfig oldConfig = config;
        config = newConfig;
        RecordVersion oldRecordVersion = oldConfig.recordVersion();
        RecordVersion newRecordVersion = newConfig.recordVersion();
        if (newRecordVersion.precedes(oldRecordVersion)) {
            LOG.warn("Record format version has been downgraded from {} to {}.", oldRecordVersion, newRecordVersion);
        }
    }

    protected void checkIfMemoryMappedBufferClosed() {
        if (isMemoryMappedBufferClosed) {
            String msg = String.format("The memory mapped buffer for log of %s is already closed", topicPartition);
            throw new KafkaStorageException(msg);
        }
    }

    protected void updateRecoveryPoint(Long newRecoveryPoint) {
        recoveryPoint = newRecoveryPoint;
    }

    /**
     * Update recoveryPoint to provided offset and mark the log as flushed, if the offset is greater
     * than the existing recoveryPoint.
     *
     * @param offset the offset to be updated
     */
    protected void markFlushed(Long offset) {
        checkIfMemoryMappedBufferClosed();
        if (offset > recoveryPoint) {
            updateRecoveryPoint(offset);
            lastFlushedTime.set(time.milliseconds());
        }
    }

    /**
     * The number of messages appended to the log since the last flush
     */
    protected Long unflushedMessages() {
        return logEndOffset() - recoveryPoint;
    }

    /**
     * Flush local log segments for all offsets up to offset-1.
     * Does not update the recovery point.
     *
     * @param offset The offset to flush up to (non-inclusive)
     */
    protected void flush(Long offset) throws IOException {
        Long currentRecoveryPoint = recoveryPoint;
        if (currentRecoveryPoint <= offset) {
            Collection<LogSegment> segmentsToFlush = segments.values(currentRecoveryPoint, offset);
            for (LogSegment segment : segmentsToFlush) {
                segment.flush();
            }
            // If there are any new segments, we need to flush the parent directory for crash consistency.
            for (LogSegment segment : segmentsToFlush) {
                if (segment.getBaseOffset() >= currentRecoveryPoint) {
                    Utils.flushDir(dir().toPath());
                    break;
                }
            }
        }
    }

    /**
     * The time this log is last known to have been fully flushed to disk
     */
    protected Long lastFlushTime() {
        return lastFlushedTime.get();
    }

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    protected LogOffsetMetadata logEndOffsetMetadata() {
        return nextOffsetMetadata;
    }

    /**
     * The offset of the next message that will be appended to the log
     */
    protected Long logEndOffset() {
        return nextOffsetMetadata.getMessageOffset();
    }

    /**
     * Update end offset of the log, and update the recoveryPoint.
     *
     * @param endOffset the new end offset of the log
     */
    protected void updateLogEndOffset(Long endOffset) {
        nextOffsetMetadata = new LogOffsetMetadata(endOffset, segments.activeSegment().getBaseOffset(), segments.activeSegment().size());
        if (recoveryPoint > endOffset) {
            updateRecoveryPoint(endOffset);
        }
    }

    /**
     * Close file handlers used by log but don't write to disk.
     * This is called if the log directory is offline.
     */
    protected void closeHandlers() {
        segments.closeHandlers();
        isMemoryMappedBufferClosed = true;
    }

    /**
     * Closes the segments of the log.
     */
    protected void close() {
        String msg = String.format("Error while renaming dir for %s in dir %s", topicPartition, dir().getParent());
        maybeHandleIOException(msg, new SupplierWithIOException<Void>() {
            @Override
            public Void get() {
                checkIfMemoryMappedBufferClosed();
                segments.close();
                return null;
            }
        });
    }

    /**
     * Completely delete this log directory with no delay.
     */
    protected void deleteEmptyDir() {
        String msg = String.format("Error while deleting dir for %s in dir %s", topicPartition, dir().getParent());
        maybeHandleIOException(msg, new SupplierWithIOException<Void>() {
            @Override
            public Void get() throws IOException {
                if (segments.nonEmpty()) {
                    String msg = String.format("Can not delete directory when %s segments are still present", segments.numberOfSegments());
                    throw new IllegalStateException(msg);
                }
                if (!isMemoryMappedBufferClosed) {
                    String msg = String.format("Can not delete directory when memory mapped buffer for log of %s is still open.", topicPartition);
                    throw new IllegalStateException(msg);
                }
                Utils.delete(dir());
                return null;
            }
        });
    }

    /**
     * Completely delete all segments with no delay.
     *
     * @return the deleted segments
     */
    protected Collection<LogSegment> deleteAllSegments() {
        String msg = String.format("Error while deleting all segments for %s in dir %s", topicPartition, dir().getParent());
        return maybeHandleIOException(msg, () -> deleteAllSegmentsFun());
    }

    private Collection<LogSegment> deleteAllSegmentsFun() throws IOException {
        List<LogSegment> deletableSegments = new ArrayList<>(segments.values());
        removeAndDeleteSegments(segments.values(), false, new SegmentDeletionReason.LogDeletion(this));
        isMemoryMappedBufferClosed = true;
        return deletableSegments;
    }

    /**
     * Find segments starting from the oldest until the user-supplied predicate is false.
     * A final segment that is empty will never be returned.
     *
     * @param predicate A function that takes in a candidate log segment, the next higher segment
     *                  (if there is one). It returns true iff the segment is deletable.
     * @return the segments ready to be deleted
     */
    protected Collection<LogSegment> deletableSegments(BiFunctionWithIOException<LogSegment, Optional<LogSegment>, Boolean> predicate) throws IOException {
        if (segments.isEmpty()) {
            return new HashSet<>();
        } else {
            List<LogSegment> deletable = new ArrayList<>();
            Iterator<LogSegment> segmentsIterator = segments.values().iterator();
            Optional<LogSegment> segmentOpt = nextOption(segmentsIterator);
            while (segmentOpt.isPresent()) {
                LogSegment segment = segmentOpt.get();
                Optional<LogSegment> nextSegmentOpt = nextOption(segmentsIterator);
                boolean isLastSegmentAndEmpty = !nextSegmentOpt.isPresent() && segment.size() == 0;
                if (predicate.apply(segment, nextSegmentOpt) && !isLastSegmentAndEmpty) {
                    deletable.add(segment);
                    segmentOpt = nextSegmentOpt;
                } else {
                    segmentOpt = Optional.empty();
                }
            }
            return deletable;
        }
    }

    /**
     * This method deletes the given log segments by doing the following for each of them:
     * - It removes the segment from the segment map so that it will no longer be used for reads.
     * - It renames the index and log files by appending .deleted to the respective file name
     * - It can either schedule an asynchronous delete operation to occur in the future or perform the deletion synchronously
     * <p>
     * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
     * physically deleting a file while it is being read.
     * <p>
     * This method does not convert IOException to KafkaStorageException, the immediate caller
     * is expected to catch and handle IOException.
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     * @param asyncDelete      Whether the segment files should be deleted asynchronously
     * @param reason           The reason for the segment deletion
     */
    protected void removeAndDeleteSegments(Collection<LogSegment> segmentsToDelete,
                                           Boolean asyncDelete,
                                           SegmentDeletionReason reason) throws IOException {
        if (CollectionUtils.isNotEmpty(segmentsToDelete)) {
            // Most callers hold an iterator into the `segments` collection and `removeAndDeleteSegment` mutates it by
            // removing the deleted segment, we should force materialization of the iterator here, so that results of the
            // iteration remain valid and deterministic. We should also pass only the materialized view of the
            // iterator to the logic that actually deletes the segments.
            List<LogSegment> toDelete = new ArrayList<>(segmentsToDelete);
            reason.logReason(toDelete);
            for (LogSegment segment : toDelete) {
                segments.remove(segment.getBaseOffset());
            }
            LocalLog.deleteSegmentFiles(toDelete, asyncDelete, dir(), topicPartition, config, scheduler, logDirFailureChannel, logIdent);
        }
    }

    /**
     * This method deletes the given segment and creates a new segment with the given new base offset. It ensures an
     * active segment exists in the log at all times during this process.
     * <p>
     * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
     * physically deleting a file while it is being read.
     * <p>
     * This method does not convert IOException to KafkaStorageException, the immediate caller
     * is expected to catch and handle IOException.
     *
     * @param newOffset       The base offset of the new segment
     * @param segmentToDelete The old active segment to schedule for deletion
     * @param asyncDelete     Whether the segment files should be deleted asynchronously
     * @param reason          The reason for the segment deletion
     */
    protected LogSegment createAndDeleteSegment(Long newOffset,
                                                LogSegment segmentToDelete,
                                                Boolean asyncDelete,
                                                SegmentDeletionReason reason) throws IOException {
        if (newOffset == segmentToDelete.getBaseOffset()) {
            segmentToDelete.changeFileSuffixes("", DeletedFileSuffix);
        }

        LogSegment newSegment = LogSegment.open(dir(),
                newOffset,
                config,
                time,
                false,
                config.initFileSize(),
                config.getPreallocate(),
                "");
        segments.add(newSegment);

        reason.logReason(Arrays.asList(segmentToDelete));
        if (newOffset != segmentToDelete.getBaseOffset()) {
            segments.remove(segmentToDelete.getBaseOffset());
        }
        LocalLog.deleteSegmentFiles(Arrays.asList(segmentToDelete), asyncDelete, dir(), topicPartition, config, scheduler, logDirFailureChannel, logIdent);

        return newSegment;
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * If the message offset is out of range, throw an OffsetOutOfRangeException
     */
    protected LogOffsetMetadata convertToOffsetMetadataOrThrow(Long offset) throws OffsetOutOfRangeException {
        FetchDataInfo fetchDataInfo = read(offset,
                1,
                false,
                nextOffsetMetadata,
                false);
        return fetchDataInfo.getFetchOffsetMetadata();
    }

    /**
     * Read messages from the log.
     *
     * @param startOffset        The offset to begin reading at
     * @param maxLength          The maximum number of bytes to read
     * @param minOneMessage      If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
     * @param maxOffsetMetadata  The metadata of the maximum offset to be fetched
     * @param includeAbortedTxns If true, aborted transactions are included
     * @return The fetch data information including fetch starting offset metadata and messages read.
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset
     */
    public FetchDataInfo read(Long startOffset,
                              Integer maxLength,
                              Boolean minOneMessage,
                              LogOffsetMetadata maxOffsetMetadata,
                              Boolean includeAbortedTxns) throws OffsetOutOfRangeException {
        return maybeHandleIOException(String.format("Exception while reading from %s in dir %s", topicPartition, dir().getParent()),
                () -> {
                    LOG.trace("Reading maximum {} bytes at offset {} from log with total length {} bytes",
                            maxLength, startOffset, segments.sizeInBytes());

                    LogOffsetMetadata endOffsetMetadata = nextOffsetMetadata;
                    long endOffset = endOffsetMetadata.getMessageOffset();
                    Optional<LogSegment> segmentOpt = segments.floorSegment(startOffset);

                    // return error on attempt to read beyond the log end offset
                    if (startOffset > endOffset || !segmentOpt.isPresent()) {
                        String msg = String.format("Received request for offset %s for partition %s, but we only have log segments upto %s.",
                                startOffset, topicPartition, endOffset);
                        throw new OffsetOutOfRangeException(msg);
                    }

                    if (startOffset == maxOffsetMetadata.getMessageOffset()) {
                        return emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns);
                    } else if (startOffset > maxOffsetMetadata.getMessageOffset()) {
                        return emptyFetchDataInfo(convertToOffsetMetadataOrThrow(startOffset), includeAbortedTxns);
                    } else {
                        // Do the read on the segment with a base offset less than the target offset
                        // but if that segment doesn't contain any messages with an offset greater than that
                        // continue to read from successive segments until we get some messages or we reach the end of the log
                        FetchDataInfo fetchDataInfo = null;
                        while (fetchDataInfo == null && segmentOpt.isPresent()) {
                            LogSegment segment = segmentOpt.get();
                            long baseOffset = segment.getBaseOffset();

                            // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
                            int maxPosition = maxOffsetMetadata.getSegmentBaseOffset() == segment.getBaseOffset() ? maxOffsetMetadata.getRelativePositionInSegment() : segment.size();

                            fetchDataInfo = segment.read(startOffset, maxLength, (long) maxPosition, minOneMessage);
                            if (fetchDataInfo != null) {
                                if (includeAbortedTxns) {
                                    fetchDataInfo = addAbortedTransactions(startOffset, segment, fetchDataInfo);
                                }
                            } else {
                                segmentOpt = segments.higherSegment(baseOffset);
                            }
                        }

                        if (fetchDataInfo != null) {
                            return fetchDataInfo;
                        } else {
                            // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
                            // this can happen when all messages with offset larger than start offsets have been deleted.
                            // In this case, we will return the empty set with log end offset metadata
                            return new FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY);
                        }
                    }
                }
        );
    }

    protected void append(Long lastOffset, Long largestTimestamp, Long shallowOffsetOfMaxTimestamp, MemoryRecords records) throws IOException {
        segments.activeSegment().append(lastOffset,
                largestTimestamp,
                shallowOffsetOfMaxTimestamp,
                records);
        updateLogEndOffset(lastOffset + 1);
    }

    private FetchDataInfo addAbortedTransactions(Long startOffset, LogSegment segment, FetchDataInfo fetchInfo) throws IOException {
        int fetchSize = fetchInfo.getRecords().sizeInBytes();
        OffsetPosition startOffsetPosition = new OffsetPosition(fetchInfo.getFetchOffsetMetadata().getMessageOffset(),
                fetchInfo.getFetchOffsetMetadata().getRelativePositionInSegment());
        long upperBoundOffset = segment.fetchUpperBoundOffset(startOffsetPosition, fetchSize)
                .orElse(segments.higherSegment(segment.getBaseOffset()).map(LogSegment::getBaseOffset).orElse(logEndOffset()));

        List<FetchResponseData.AbortedTransaction> abortedTransactions = new ArrayList<>();
        collectAbortedTransactions(startOffset, upperBoundOffset, segment, new Consumer<List<AbortedTxn>>() {
                    @Override
                    public void accept(List<AbortedTxn> abortedTxns) {
                        List<FetchResponseData.AbortedTransaction> mapped = abortedTxns.stream()
                                .map(AbortedTxn::asAbortedTransaction)
                                .collect(Collectors.toList());
                        abortedTransactions.addAll(mapped);
                    }
                }
        );

        return new FetchDataInfo(fetchInfo.getFetchOffsetMetadata(),
                fetchInfo.getRecords(),
                fetchInfo.getFirstEntryIncomplete(),
                Optional.of(abortedTransactions));
    }

    private void accumulator(List<FetchResponseData.AbortedTransaction> abortedTransactions,
                             List<AbortedTxn> abortedTxns) {
        List<FetchResponseData.AbortedTransaction> mapped = abortedTxns.stream()
                .map(AbortedTxn::asAbortedTransaction)
                .collect(Collectors.toList());
        abortedTransactions.addAll(mapped);
    }

    private void collectAbortedTransactions(Long startOffset, Long upperBoundOffset,
                                            LogSegment startingSegment,
                                            Consumer<List<AbortedTxn>> accumulator) throws IOException {
        Iterator<LogSegment> higherSegments = segments.higherSegments(startingSegment.getBaseOffset()).iterator();
        Optional<LogSegment> segmentEntryOpt = Optional.of(startingSegment);
        while (segmentEntryOpt.isPresent()) {
            LogSegment segment = segmentEntryOpt.get();
            TxnIndexSearchResult searchResult = segment.collectAbortedTxns(startOffset, upperBoundOffset);
            accumulator.accept(searchResult.getAbortedTransactions());
            if (searchResult.getIsComplete()) {
                return;
            }
            segmentEntryOpt = nextOption(higherSegments);
        }
    }

    protected List<AbortedTxn> collectAbortedTransactions(Long logStartOffset, Long baseOffset, Long upperBoundOffset) throws IOException {
        Optional<LogSegment> segmentEntry = segments.floorSegment(baseOffset);
        List<AbortedTxn> allAbortedTxns = new ArrayList<>();
        if (segmentEntry.isPresent()) {
            LogSegment segment = segmentEntry.get();
            collectAbortedTransactions(logStartOffset, upperBoundOffset, segment, new Consumer<List<AbortedTxn>>() {
                @Override
                public void accept(List<AbortedTxn> abortedTxns) {
                    allAbortedTxns.addAll(abortedTxns);
                }
            });
        }
        return allAbortedTxns;
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *
     * @param expectedNextOffset The expected next offset after the segment is rolled
     * @return The newly rolled segment
     */
    protected LogSegment roll(Optional<Long> expectedNextOffset) {
        return maybeHandleIOException(String.format("Error while rolling log segment for %s in dir %s", topicPartition, dir().getParent()),
                new SupplierWithIOException<LogSegment>() {
                    @Override
                    public LogSegment get() throws IOException {
                        return rollInner(expectedNextOffset);
                    }
                }
        );
    }

    private LogSegment rollInner(Optional<Long> expectedNextOffset) throws IOException {
        long start = time.hiResClockMs();
        checkIfMemoryMappedBufferClosed();
        Long newOffset = Math.max(expectedNextOffset.orElse(0L), logEndOffset());
        File logFile = LocalLog.logFile(dir(), newOffset, "");
        LogSegment activeSegment = segments.activeSegment();
        if (segments.contains(newOffset)) {
            // segment with the same base offset already exists and loaded
            if (activeSegment.getBaseOffset() == newOffset && activeSegment.size() == 0) {
                // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an
                // active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
                LOG.warn("Trying to roll a new log segment with start offset {} =max(provided offset = {}, LEO = {}) while it already " +
                                "exists and is active with size 0. Size of time index: {}, size of offset index: {}.",
                        newOffset, expectedNextOffset, logEndOffset(), activeSegment.timeIndex().entries(), activeSegment.offsetIndex().entries());
                LogSegment newSegment = createAndDeleteSegment(newOffset, activeSegment, true, new SegmentDeletionReason.LogRoll(this));
                updateLogEndOffset(nextOffsetMetadata.getMessageOffset());
                LOG.info("Rolled new log segment at offset {} in {} ms.", newOffset, time.hiResClockMs() - start);
                return newSegment;
            } else {
                String msg = String.format("Trying to roll a new log segment for topic partition %s with start offset %s =max(provided offset = %s, LEO = %s) while it already exists. Existing segment is %s.",
                        topicPartition, newOffset, expectedNextOffset, logEndOffset(), segments.get(newOffset));
                throw new KafkaException(msg);
            }
        } else if (!segments.isEmpty() && newOffset < activeSegment.getBaseOffset()) {
            String msg = String.format("Trying to roll a new log segment for topic partition %s with " +
                            "start offset %s =max(provided offset = %s, LEO = %s) lower than start offset of the active segment %s",
                    topicPartition, newOffset, expectedNextOffset, logEndOffset(), activeSegment);
            throw new KafkaException(msg);
        } else {
            File offsetIdxFile = offsetIndexFile(dir(), newOffset, "");
            File timeIdxFile = timeIndexFile(dir(), newOffset, "");
            File txnIdxFile = transactionIndexFile(dir(), newOffset, "");

            for (File file : Arrays.asList(logFile, offsetIdxFile, timeIdxFile, txnIdxFile)) {
                if (file.exists()) {
                    LOG.warn("Newly rolled segment file {} already exists; deleting it first", file.getAbsolutePath());
                    Files.delete(file.toPath());
                }
            }

            Optional<LogSegment> lastSegment = segments.lastSegment();
            if (lastSegment.isPresent()) {
                lastSegment.get().onBecomeInactiveSegment();
            }
        }

        LogSegment newSegment = LogSegment.open(dir(),
                newOffset,
                config,
                time,
                false,
                config.initFileSize(),
                config.getPreallocate(),
                "");
        segments.add(newSegment);

        // We need to update the segment base offset and append position data of the metadata when log rolls.
        // The next offset should not change.
        updateLogEndOffset(nextOffsetMetadata.getMessageOffset());

        LOG.info("Rolled new log segment at offset {} in {} ms.", newOffset, time.hiResClockMs() - start);

        return newSegment;
    }

    /**
     * Delete all data in the local log and start at the new offset.
     *
     * @param newOffset The new offset to start the log with
     * @return the list of segments that were scheduled for deletion
     */
    protected Iterable<LogSegment> truncateFullyAndStartAt(Long newOffset) {
        return maybeHandleIOException(String.format("Error while truncating the entire log for %s in dir %s", topicPartition, dir().getParent()),
                () -> {
                    LOG.debug("Truncate and start at offset {}", newOffset);
                    checkIfMemoryMappedBufferClosed();
                    List<LogSegment> segmentsToDelete = new ArrayList<>(segments.values());

                    if (CollectionUtils.isNotEmpty(segmentsToDelete)) {
                        removeAndDeleteSegments(CollectionUtilExt.dropRight(segmentsToDelete, 1), true, new SegmentDeletionReason.LogTruncation(this));
                        // Use createAndDeleteSegment() to create new segment first and then delete the old last segment to prevent missing
                        // active segment during the deletion process
                        createAndDeleteSegment(newOffset, CollectionUtilExt.last(segmentsToDelete), true, new SegmentDeletionReason.LogTruncation(this));
                    }

                    updateLogEndOffset(newOffset);

                    return segmentsToDelete;
                }
        );
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     * @return the list of segments that were scheduled for deletion
     */
    protected Collection<LogSegment> truncateTo(Long targetOffset) throws IOException {
        List<LogSegment> deletableSegments = segments.filter(segment -> segment.getBaseOffset() > targetOffset);
        removeAndDeleteSegments(deletableSegments, true, new SegmentDeletionReason.LogTruncation(this));
        segments.activeSegment().truncateTo(targetOffset);
        updateLogEndOffset(targetOffset);
        return deletableSegments;
    }


    /*----------------------------static---------------------------*/

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    protected static String filenamePrefixFromOffset(Long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Construct a log file name in the given dir with the given base offset and the given suffix
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
     */
    protected static File logFile(File dir, Long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix);
    }

    /**
     * Return a directory name to rename the log directory to for async deletion.
     * The name will be in the following format: "topic-partitionId.uniqueId-delete".
     * If the topic name is too long, it will be truncated to prevent the total name
     * from exceeding 255 characters.
     */
    protected static String logDeleteDirName(TopicPartition topicPartition) {
        String uniqueId = UUID.randomUUID().toString().replaceAll("-", "");
        String suffix = String.format("-%s.%s%s", topicPartition.partition(), uniqueId, DeleteDirSuffix);
        int prefixLength = Math.min(topicPartition.topic().length(), 255 - suffix.length());
        return topicPartition.topic().substring(0, prefixLength) + suffix;
    }

    /**
     * Return a future directory name for the given topic partition. The name will be in the following
     * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
     */
    protected static String logFutureDirName(TopicPartition topicPartition) {
        return logDirNameWithSuffix(topicPartition, FutureDirSuffix);
    }

    protected static String logDirNameWithSuffix(TopicPartition topicPartition, String suffix) {
        String uniqueId = UUID.randomUUID().toString().replaceAll("-", "");
        return logDirName(topicPartition) + "." + uniqueId + suffix;
    }

    /**
     * Return a directory name for the given topic partition. The name will be in the following
     * format: topic-partition where topic, partition are variables.
     */
    protected static String logDirName(TopicPartition topicPartition) {
        return topicPartition.topic() + "-" + topicPartition.partition();
    }

    /**
     * Construct an index file name in the given dir using the given base offset and the given suffix
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
     */
    protected static File offsetIndexFile(File dir, Long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix);
    }

    /**
     * Construct a time index file name in the given dir using the given base offset and the given suffix
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
     */
    protected static File timeIndexFile(File dir, Long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix);
    }

    /**
     * Construct a transaction index file name in the given dir using the given base offset and the given suffix
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
     */
    protected static File transactionIndexFile(File dir, Long offset, String suffix) {
        return new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix);
    }

    protected static Long offsetFromFileName(String filename) {
        return Long.parseLong(filename.substring(0, filename.indexOf('.')));
    }

    protected static Long offsetFromFile(File file) {
        return offsetFromFileName(file.getName());
    }

    /**
     * Parse the topic and partition out of the directory name of a log
     */
    protected static TopicPartition parseTopicPartitionName(File dir) {
        if (dir == null) {
            throw new KafkaException("dir should not be null");
        }

        String dirName = dir.getName();
        if (StringUtils.isBlank(dirName) || !dirName.contains("-")) {
            throw new KafkaException("Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
                    "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
                    "Kafka's log directories (and children) should only contain Kafka topic data.");
        }
        if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches() ||
                dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches()) {
            throw new KafkaException("Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
                    "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
                    "Kafka's log directories (and children) should only contain Kafka topic data.");
        }

        String name = (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) ?
                dirName.substring(0, dirName.lastIndexOf('.')) : dirName;

        int index = name.lastIndexOf('-');
        String topic = name.substring(0, index);
        String partitionString = name.substring(index + 1);
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(partitionString)) {
            throw new KafkaException("Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
                    "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
                    "Kafka's log directories (and children) should only contain Kafka topic data.");
        }

        try {
            int partition = Integer.parseInt(partitionString);
            return new TopicPartition(topic, partition);
        } catch (NumberFormatException e) {
            throw new KafkaException("Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
                    "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
                    "Kafka's log directories (and children) should only contain Kafka topic data.");
        }
    }

    protected static Boolean isIndexFile(File file) {
        String filename = file.getName();
        return filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix);
    }

    protected static Boolean isLogFile(File file) {
        return file.getPath().endsWith(LogFileSuffix);
    }

    /**
     * Invokes the provided function and handles any IOException raised by the function by marking the
     * provided directory offline.
     *
     * @param logDirFailureChannel Used to asynchronously handle log directory failure.
     * @param logDir               The log directory to be marked offline during an IOException.
     * @param errorMsg             The error message to be used when marking the log directory offline.
     * @param fun                  The function to be executed.
     * @return The value returned by the function after a successful invocation
     */
    protected static <T> T maybeHandleIOException(LogDirFailureChannel logDirFailureChannel,
                                                  String logDir,
                                                  String errorMsg,
                                                  SupplierWithIOException<T> fun) {
        if (logDirFailureChannel.hasOfflineLogDir(logDir)) {
            throw new KafkaStorageException("The log dir " + logDir + " is already offline due to a previous IO exception.");
        }
        try {
            return fun.get();
        } catch (IOException e) {
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, errorMsg, e);
            throw new KafkaStorageException(errorMsg, e);
        }
    }

    /**
     * Split a segment into one or more segments such that there is no offset overflow in any of them. The
     * resulting segments will contain the exact same messages that are present in the input segment. On successful
     * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
     * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
     * <p>
     * Note that this method assumes we have already determined that the segment passed in contains records that cause
     * offset overflow.
     * <p>
     * The split logic overloads the use of .clean files that LogCleaner typically uses to make the process of replacing
     * the input segment with multiple new segments atomic and recoverable in the event of a crash. See replaceSegments
     * and completeSwapOperations for the implementation to make this operation recoverable on crashes.</p>
     *
     * @param segment              Segment to split
     * @param existingSegments     The existing segments of the log
     * @param dir                  The directory in which the log will reside
     * @param topicPartition       The topic
     * @param config               The log configuration settings
     * @param scheduler            The thread pool scheduler used for background actions
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param logPrefix            The logging prefix
     * @return List of new segments that replace the input segment
     */
    protected static SplitSegmentResult splitOverflowedSegment(LogSegment segment,
                                                               LogSegments existingSegments,
                                                               File dir,
                                                               TopicPartition topicPartition,
                                                               LogConfig config,
                                                               Scheduler scheduler,
                                                               LogDirFailureChannel logDirFailureChannel,
                                                               String logPrefix) throws IOException {
        assert isLogFile(segment.getLog().file()) : String.format("Cannot split file %s", segment.getLog().file().getAbsoluteFile());
        assert segment.hasOverflow()
                : String.format("Split operation is only permitted for segments with overflow, and the problem path is %s",
                segment.getLog().file().getAbsoluteFile());

        LOG.info("{}Splitting overflowed segment {}", logPrefix, segment);

        List<LogSegment> newSegments = new ArrayList<>();
        try {
            int position = 0;
            FileRecords sourceRecords = segment.getLog();

            while (position < sourceRecords.sizeInBytes()) {
                FileLogInputStream.FileChannelRecordBatch firstBatch = CollectionUtilExt.head(sourceRecords.batchesFrom(position));
                LogSegment newSegment = createNewCleanedSegment(dir, config, firstBatch.baseOffset());
                newSegments.add(newSegment);

                int bytesAppended = newSegment.appendFromFile(sourceRecords, position);
                if (bytesAppended == 0) {
                    throw new IllegalStateException(String.format("Failed to append records from position %s in %s", position, segment));
                }

                position += bytesAppended;
            }

            // prepare new segments
            int totalSizeOfNewSegments = 0;
            for (LogSegment splitSegment : newSegments) {
                splitSegment.onBecomeInactiveSegment();
                splitSegment.flush();
                splitSegment.setLastModified(segment.getLastModified());
                totalSizeOfNewSegments += splitSegment.getLog().sizeInBytes();
            }
            // size of all the new segments combined must equal size of the original segment
            if (totalSizeOfNewSegments != segment.getLog().sizeInBytes()) {
                String msg = String.format("Inconsistent segment sizes after split before: %s after: %s",
                        segment.getLog().sizeInBytes(), totalSizeOfNewSegments);
                throw new IllegalStateException(msg);
            }

            // replace old segment with new ones
            LOG.info("{}Replacing overflowed segment {} with split segments {}", logPrefix, segment, newSegments);
            List<LogSegment> newSegmentsToAdd = new ArrayList<>(newSegments);
            Iterable<LogSegment> deletedSegments = LocalLog.replaceSegments(existingSegments,
                    newSegmentsToAdd,
                    Arrays.asList(segment),
                    dir,
                    topicPartition,
                    config,
                    scheduler,
                    logDirFailureChannel,
                    logPrefix,
                    false);
            return new SplitSegmentResult(deletedSegments, newSegmentsToAdd);
        } catch (Exception e) {
            for (LogSegment splitSegment : newSegments) {
                splitSegment.close();
                splitSegment.deleteIfExists();
            }
            throw e;
        }
    }

    /**
     * Swap one or more new segment in place and delete one or more existing segments in a crash-safe
     * manner. The old segments will be asynchronously deleted.
     * <p>
     * This method does not need to convert IOException to KafkaStorageException because it is either
     * called before all logs are loaded or the caller will catch and handle IOException
     * <p>
     * The sequence of operations is:
     * <p>
     * - Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments() on
     * the Log instance. If broker crashes at this point, the clean-and-swap operation is aborted and
     * the .cleaned files are deleted on recovery in LogLoader.
     * - New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
     * clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
     * in LogLoader. We detect this situation by maintaining a specific order in which files are renamed
     * from .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery,
     * all .swap files whose offset is greater than the minimum-offset .clean file are deleted.
     * - If the broker crashes after all new segments were renamed to .swap, the operation is completed,
     * the swap operation is resumed on recovery as described in the next step.
     * - Old segment files are renamed to .deleted and asynchronous delete is scheduled. If the broker
     * crashes, any .deleted files left behind are deleted on recovery in LogLoader.
     * replaceSegments() is then invoked to complete the swap with newSegment recreated from the
     * .swap file and oldSegments containing segments which were not renamed before the crash.
     * - Swap segment(s) are renamed to replace the existing segments, completing this operation.
     * If the broker crashes, any .deleted files which may be left behind are deleted
     * on recovery in LogLoader.
     *
     * @param existingSegments     The existing segments of the log
     * @param newSegments          The new log segment to add to the log
     * @param oldSegments          The old log segments to delete from the log
     * @param dir                  The directory in which the log will reside
     * @param topicPartition       The topic
     * @param config               The log configuration settings
     * @param scheduler            The thread pool scheduler used for background actions
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param logPrefix            The logging prefix
     * @param isRecoveredSwapFile  true if the new segment was created from a swap file during recovery after a crash
     */
    protected static Iterable<LogSegment> replaceSegments(LogSegments existingSegments,
                                                          List<LogSegment> newSegments,
                                                          List<LogSegment> oldSegments,
                                                          File dir,
                                                          TopicPartition topicPartition,
                                                          LogConfig config,
                                                          Scheduler scheduler,
                                                          LogDirFailureChannel logDirFailureChannel,
                                                          String logPrefix,
                                                          Boolean isRecoveredSwapFile) throws IOException {
        TreeSet<LogSegment> sortedNewSegments = new TreeSet<>((o1, o2) -> (int) (o1.getBaseOffset() - o2.getBaseOffset()));
        sortedNewSegments.addAll(newSegments);
        // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
        // but before this method is executed. We want to filter out those segments to avoid calling deleteSegmentFiles()
        // multiple times for the same segment.
        TreeSet<LogSegment> sortedOldSegments = new TreeSet<>((o1, o2) -> (int) (o1.getBaseOffset() - o2.getBaseOffset()));
        for (LogSegment seg : oldSegments) {
            if (existingSegments.contains(seg.getBaseOffset())) {
                sortedOldSegments.add(seg);
            }
        }

        // need to do this in two phases to be crash safe AND do the delete asynchronously
        // if we crash in the middle of this we complete the swap in loadSegments()
        if (!isRecoveredSwapFile) {
            for (LogSegment seg : sortedNewSegments.descendingSet()) {
                seg.changeFileSuffixes(CleanedFileSuffix, SwapFileSuffix);
            }
        }
        for (LogSegment seg : sortedNewSegments.descendingSet()) {
            existingSegments.add(seg);
        }
        Set<Long> newSegmentBaseOffsets = sortedNewSegments.stream().map(LogSegment::getBaseOffset).collect(Collectors.toSet());

        // delete the old files
        List<LogSegment> deletedNotReplaced = new ArrayList<>();
        for (LogSegment seg : sortedOldSegments) {
            if (seg.getBaseOffset() != CollectionUtilExt.head(sortedNewSegments).getBaseOffset()) {
                existingSegments.remove(seg.getBaseOffset());
            }
            deleteSegmentFiles(
                    Collections.singletonList(seg),
                    true,
                    dir,
                    topicPartition,
                    config,
                    scheduler,
                    logDirFailureChannel,
                    logPrefix);
            if (!newSegmentBaseOffsets.contains(seg.getBaseOffset())) {
                deletedNotReplaced.add(seg);
            }

        }

        // okay we are safe now, remove the swap suffix
        for (LogSegment seg : sortedNewSegments) {
            seg.changeFileSuffixes(SwapFileSuffix, "");
        }
        Utils.flushDir(dir.toPath());
        return deletedNotReplaced;
    }

    /**
     * Perform physical deletion of the index and log files for the given segment.
     * Prior to the deletion, the index and log files are renamed by appending .deleted to the
     * respective file name. Allows these files to be optionally deleted asynchronously.
     * <p>
     * This method assumes that the file exists. It does not need to convert IOException
     * (thrown from changeFileSuffixes) to KafkaStorageException because it is either called before
     * all logs are loaded or the caller will catch and handle IOException.
     *
     * @param segmentsToDelete     The segments to be deleted
     * @param asyncDelete          If true, the deletion of the segments is done asynchronously
     * @param dir                  The directory in which the log will reside
     * @param topicPartition       The topic
     * @param config               The log configuration settings
     * @param scheduler            The thread pool scheduler used for background actions
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param logPrefix            The logging prefix
     * @throws IOException if the file can't be renamed and still exists
     */
    protected static void deleteSegmentFiles(Collection<LogSegment> segmentsToDelete,
                                             Boolean asyncDelete,
                                             File dir,
                                             TopicPartition topicPartition,
                                             LogConfig config,
                                             Scheduler scheduler,
                                             LogDirFailureChannel logDirFailureChannel,
                                             String logPrefix) throws IOException {
        for (LogSegment segment : segmentsToDelete) {
            if (!segment.hasSuffix(DeletedFileSuffix)) {
                segment.changeFileSuffixes("", DeletedFileSuffix);
            }
        }

        if (asyncDelete) {
            scheduler.schedule("delete-file",
                    () -> deleteSegments(logPrefix, segmentsToDelete, dir, logDirFailureChannel, topicPartition),
                    config.getFileDeleteDelayMs(),
                    -1, TimeUnit.MILLISECONDS);
        } else {
            deleteSegments(logPrefix, segmentsToDelete, dir, logDirFailureChannel, topicPartition);
        }
    }

    private static void deleteSegments(String logPrefix,
                                       Iterable<LogSegment> segmentsToDelete,
                                       File dir,
                                       LogDirFailureChannel logDirFailureChannel,
                                       TopicPartition topicPartition) {
        LOG.info("{}Deleting segment files {}", logPrefix, CollectionUtilExt.mkString(segmentsToDelete, ","));
        String parentDir = dir.getParent();
        String msg = String.format("Error while deleting segments for %s in dir %s", topicPartition, parentDir);
        maybeHandleIOException(logDirFailureChannel, parentDir, msg, new SupplierWithIOException<Void>() {
            @Override
            public Void get() throws IOException {
                for (LogSegment segment : segmentsToDelete) {
                    segment.deleteIfExists();
                }
                return null;
            }
        });
    }

    protected static FetchDataInfo emptyFetchDataInfo(LogOffsetMetadata fetchOffsetMetadata,
                                                      Boolean includeAbortedTxns) {
        Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions = includeAbortedTxns ? Optional.of(new ArrayList<>()) : Optional.empty();
        return new FetchDataInfo(fetchOffsetMetadata,
                MemoryRecords.EMPTY,
                false,
                abortedTransactions);
    }

    protected static LogSegment createNewCleanedSegment(File dir, LogConfig logConfig, Long baseOffset) throws IOException {
        LogSegment.deleteIfExists(dir, baseOffset, CleanedFileSuffix);
        return LogSegment.open(dir,
                baseOffset,
                logConfig,
                Time.SYSTEM,
                false,
                logConfig.initFileSize(),
                logConfig.getPreallocate(),
                CleanedFileSuffix);
    }

    /**
     * Wraps the value of iterator.next() in an option.
     * Note: this facility is a part of the Iterator class starting from scala v2.13.
     *
     * @param iterator
     * @return Some(iterator.next) if a next element exists, None otherwise.
     * @tparam T the type of object held within the iterator
     */
    protected static <T> Optional<T> nextOption(Iterator<T> iterator) {
        if (iterator.hasNext()) {
            return Optional.of(iterator.next());
        } else {
            return Optional.empty();
        }
    }

}

