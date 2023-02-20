package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.LogSegmentOffsetOverflowException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import cn.pockethub.permanentqueue.kafka.server.epoch.LeaderEpochFileCache;
import cn.pockethub.permanentqueue.kafka.snapshot.Snapshots;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import cn.pockethub.permanentqueue.kafka.utils.Scheduler;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static cn.pockethub.permanentqueue.kafka.log.UnifiedLog.*;
import static cn.pockethub.permanentqueue.kafka.log.UnifiedLog.isIndexFile;

public class LogLoader {
    private static final Logger LOG = LoggerFactory.getLogger(LogLoader.class);

    /**
     * Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
     * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
     * avoided by passing in the recovery point, however finding the correct position to do this
     * requires accessing the offset index which may not be safe in an unclean shutdown.
     * For more information see the discussion in PR#2104
     */
    public static final String CleanShutdownFile = ".kafka_cleanshutdown";

    private File dir;
    private TopicPartition topicPartition;
    private LogConfig config;
    private Scheduler scheduler;
    private Time time;
    private LogDirFailureChannel logDirFailureChannel;
    private Boolean hadCleanShutdown;
    private LogSegments segments;
    private Long logStartOffsetCheckpoint;
    private Long recoveryPointCheckpoint;
    private Optional<LeaderEpochFileCache> leaderEpochCache;
    private ProducerStateManager producerStateManager;
    private ConcurrentMap<String, Integer> numRemainingSegments;

    private String logIdent;


    public LogLoader(File dir,
                     TopicPartition topicPartition,
                     LogConfig config,
                     Scheduler scheduler,
                     Time time,
                     LogDirFailureChannel logDirFailureChannel,
                     Boolean hadCleanShutdown,
                     LogSegments segments,
                     Long logStartOffsetCheckpoint,
                     Long recoveryPointCheckpoint,
                     Optional<LeaderEpochFileCache> leaderEpochCache,
                     ProducerStateManager producerStateManager,
                     ConcurrentMap<String, Integer> numRemainingSegments) {
        this.dir = dir;
        this.topicPartition = topicPartition;
        this.config = config;
        this.scheduler = scheduler;
        this.time = time;
        this.logDirFailureChannel = logDirFailureChannel;
        this.hadCleanShutdown = hadCleanShutdown;
        this.segments = segments;
        this.logStartOffsetCheckpoint = logStartOffsetCheckpoint;
        this.recoveryPointCheckpoint = recoveryPointCheckpoint;
        this.leaderEpochCache = leaderEpochCache;
        this.producerStateManager = producerStateManager;
        this.numRemainingSegments = numRemainingSegments;

        this.logIdent = String.format("[LogLoader partition=%s, dir=%s] ", topicPartition, dir.getParent());
    }

    /**
     * Load the log segments from the log files on disk, and returns the components of the loaded log.
     * Additionally, it also suitably updates the provided LeaderEpochFileCache and ProducerStateManager
     * to reflect the contents of the loaded log.
     * <p>
     * In the context of the calling thread, this function does not need to convert IOException to
     * KafkaStorageException because it is only called before all logs are loaded.
     *
     * @return the offsets of the Log successfully loaded from disk
     * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
     *                                           overflow index offset
     */
    public LoadedLogOffsets load() throws IOException{
        // First pass: through the files in the log directory and remove any temporary files
        // and find any interrupted swap operations
        Set<File> swapFiles = removeTempFilesAndCollectSwapFiles();

        // The remaining valid swap files must come from compaction or segment split operation. We can
        // simply rename them to regular segment files. But, before renaming, we should figure out which
        // segments are compacted/split and delete these segment files: this is done by calculating
        // min/maxSwapFileOffset.
        // We store segments that require renaming in this code block, and do the actual renaming later.
        Long minSwapFileOffset = Long.MAX_VALUE;
        Long maxSwapFileOffset = Long.MIN_VALUE;
        for (File f : swapFiles) {
            if (!UnifiedLog.isLogFile(new File(CoreUtils.replaceSuffix(f.getPath(), SwapFileSuffix, "")))) {
                continue;
            }
            Long baseOffset = offsetFromFile(f);
            LogSegment segment = LogSegment.open(f.getParentFile(),
                    baseOffset,
                    config,
                    time,
                    false,
                    0,
                    false,
                    SwapFileSuffix);
            LOG.info("Found log file {} from interrupted swap operation, which is recoverable from {} files by renaming.",
                    f.getPath(), UnifiedLog.SwapFileSuffix);
            minSwapFileOffset = Math.min(segment.getBaseOffset(), minSwapFileOffset);
            maxSwapFileOffset = Math.max(segment.readNextOffset(), maxSwapFileOffset);
        }

        // Second pass: delete segments that are between minSwapFileOffset and maxSwapFileOffset. As
        // discussed above, these segments were compacted or split but haven't been renamed to .delete
        // before shutting down the broker.
        File[] files = dir.listFiles();
        if (Objects.nonNull(files)) {
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }

                try {
                    if (!file.getName().endsWith(SwapFileSuffix)) {
                        long offset = offsetFromFile(file);
                        if (offset >= minSwapFileOffset && offset < maxSwapFileOffset) {
                            LOG.info("Deleting segment files {} that is compacted but has not been deleted yet.", file.getName());
                            file.delete();
                        }
                    }
                } catch (StringIndexOutOfBoundsException | NumberFormatException e) {
                    // offsetFromFile with files that do not include an offset in the file name
                }
            }
        }

        // Third pass: rename all swap files.
        files = dir.listFiles();
        if (Objects.nonNull(files)) {
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }

                if (file.getName().endsWith(SwapFileSuffix)) {
                    LOG.info("Recovering file {} by renaming from {} files.", file.getName(), UnifiedLog.SwapFileSuffix);
                    file.renameTo(new File(CoreUtils.replaceSuffix(file.getPath(), SwapFileSuffix, "")));
                }
            }
        }

        // Fourth pass: load all the log and index files.
        // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
        // this happens, restart loading segment files from scratch.
        retryOnOffsetOverflow(() -> {
            // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
            // loading of segments. In that case, we also need to close all segments that could have been left open in previous
            // call to loadSegmentFiles().
            segments.close();
            segments.clear();
            loadSegmentFiles();
            return null;
        });

        Pair<Long, Long> pair = null;
        if (!dir.getAbsolutePath().endsWith(UnifiedLog.DeleteDirSuffix)) {
            Pair<Long, Long> tmpPair = retryOnOffsetOverflow(this::recoverLog);
            segments.lastSegment().get().resizeIndexes(config.getMaxIndexSize());
            pair = tmpPair;
        } else {
            if (segments.isEmpty()) {
                segments.add(LogSegment.open(dir, 0L, config, time, false, config.initFileSize(), false, ""));
                pair = Pair.of(0L, 0L);
            }
        }
        long newRecoveryPoint = pair.getKey();
        long nextOffset = pair.getValue();

        if (leaderEpochCache.isPresent()) {
            LeaderEpochFileCache cache = leaderEpochCache.get();
            cache.truncateFromEnd(nextOffset);
        }
        long newLogStartOffset = Math.max(logStartOffsetCheckpoint, segments.firstSegment().get().getBaseOffset());
        // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
        if (leaderEpochCache.isPresent()) {
            LeaderEpochFileCache cache = leaderEpochCache.get();
            cache.truncateFromStart(logStartOffsetCheckpoint);
        }

        // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
        // from scratch.
        if (!producerStateManager.isEmpty()) {
            throw new IllegalStateException("Producer state must be empty during log initialization");
        }

        // Reload all snapshots into the ProducerStateManager cache, the intermediate ProducerStateManager used
        // during log recovery may have deleted some files without the LogLoader.producerStateManager instance witnessing the
        // deletion.
        producerStateManager.removeStraySnapshots(ImmutableList.copyOf(segments.baseOffsets()));
        UnifiedLog.rebuildProducerState(producerStateManager,
                segments,
                newLogStartOffset,
                nextOffset,
                config.recordVersion(),
                time,
                 hadCleanShutdown,
                logIdent);
        LogSegment activeSegment = segments.lastSegment().get();
        return new LoadedLogOffsets(
                newLogStartOffset,
                newRecoveryPoint,
                new LogOffsetMetadata(nextOffset, activeSegment.getBaseOffset(), activeSegment.size()));
    }

    /**
     * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
     * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
     * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
     * by this method.
     *
     * @return Set of .swap files that are valid to be swapped in as segment files and index files
     */
    private Set<File> removeTempFilesAndCollectSwapFiles() throws IOException{

        Set<File> swapFiles = new HashSet<>();
        Set<File> cleanedFiles = new HashSet<>();
        long minCleanedFileOffset = Long.MAX_VALUE;

        File[] files = dir.listFiles();
        if (Objects.nonNull(files)) {
            for (File file : files) {
                if (!file.canRead()) {
                    throw new IOException("Could not read file " + file);
                }
                String filename = file.getName();

                // Delete stray files marked for deletion, but skip KRaft snapshots.
                // These are handled in the recovery logic in `KafkaMetadataLog`.
                if (filename.endsWith(DeletedFileSuffix) && !filename.endsWith(Snapshots.DELETE_SUFFIX)) {
                    LOG.debug("Deleting stray temporary file {}", file.getAbsolutePath());
                    Files.deleteIfExists(file.toPath());
                } else if (filename.endsWith(CleanedFileSuffix)) {
                    minCleanedFileOffset = Math.min(offsetFromFile(file), minCleanedFileOffset);
                    cleanedFiles.add(file);
                } else if (filename.endsWith(SwapFileSuffix)) {
                    swapFiles.add(file);
                }
            }
        }

        // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
        // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
        // for more details about the split operation.
        Set<File> invalidSwapFiles = new HashSet<>();
        Set<File> validSwapFiles = new HashSet<>();
        for (File file : swapFiles) {
            if (offsetFromFile(file) >= minCleanedFileOffset) {
                invalidSwapFiles.add(file);
            } else {
                validSwapFiles.add(file);
            }
        }
        for (File file : invalidSwapFiles) {
            LOG.debug("Deleting invalid swap file {} minCleanedFileOffset: {}", file.getAbsoluteFile(), minCleanedFileOffset);
            Files.deleteIfExists(file.toPath());
        }

        // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
        for (File file :cleanedFiles) {
            LOG.debug("Deleting stray .clean file {}", file.getAbsolutePath());
            Files.deleteIfExists(file.toPath());
        }

        return validSwapFiles;
    }

    /**
     * Retries the provided function only whenever an LogSegmentOffsetOverflowException is raised by
     * it during execution. Before every retry, the overflowed segment is split into one or more segments
     * such that there is no offset overflow in any of them.
     *
     * @param fn The function to be executed
     * @return The value returned by the function, if successful
     * @throws Exception whenever the executed function throws any exception other than
     *                   LogSegmentOffsetOverflowException, the same exception is raised to the caller
     */
    private <T> T retryOnOffsetOverflow(SupplierWithIOException<T> fn) throws IOException, IllegalStateException{
        while (true) {
            try {
                return fn.get();
            } catch (LogSegmentOffsetOverflowException e) {
                LOG.info("Caught segment overflow error: {}. Split segment and retry.", e.getMessage());
                SplitSegmentResult result = UnifiedLog.splitOverflowedSegment(
                        e.getSegment(),
                        segments,
                        dir,
                        topicPartition,
                        config,
                        scheduler,
                        logDirFailureChannel,
                        logIdent);
                deleteProducerSnapshotsAsync(result.getDeletedSegments());
            }catch (Throwable throwable){
                throw new IllegalStateException(throwable);
            }
        }
    }

    /**
     * Loads segments from disk into the provided params.segments.
     * <p>
     * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded.
     * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
     * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
     * caller is responsible for closing them appropriately, if needed.
     *
     * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
     */
    private void loadSegmentFiles() throws IOException{
        // load segments in ascending order because transactional data from one segment may depend on the
        // segments that come before it
        File[] files = dir.listFiles();
        if (Objects.nonNull(files)) {
            Arrays.sort(files, Comparator.comparing(File::getName));
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }

                if (isIndexFile(file)) {
                    // if it is an index file, make sure it has a corresponding .log file
                    long offset = offsetFromFile(file);
                    File logFile = UnifiedLog.logFile(dir, offset, "");
                    if (!logFile.exists()) {
                        LOG.warn("Found an orphaned index file {}, with no corresponding log file.", file.getAbsolutePath());
                        Files.deleteIfExists(file.toPath());
                    }
                } else if (isLogFile(file)) {
                    // if it's a log file, load the corresponding log segment
                    long baseOffset = offsetFromFile(file);
                    boolean timeIndexFileNewlyCreated = !UnifiedLog.timeIndexFile(dir, baseOffset, "").exists();
                    LogSegment segment = LogSegment.open(dir, baseOffset, config, time, true, 0, false, "");

                    try {
                        segment.sanityCheck(timeIndexFileNewlyCreated);
                    } catch (NoSuchFileException e) {
                        if (hadCleanShutdown || segment.getBaseOffset() < recoveryPointCheckpoint) {
                            LOG.error("Could not find offset index file corresponding to log file {}, recovering segment and rebuilding index files...",
                                    segment.getLog().file().getAbsolutePath());
                        }
                        recoverSegment(segment);
                    } catch (CorruptIndexException e) {
                        LOG.warn("Found a corrupted index file corresponding to log file" +
                                " {} due to {}, recovering segment and" +
                                " rebuilding index files...", segment.getLog().file().getAbsolutePath(), e.getMessage());
                        recoverSegment(segment);
                    }
                    segments.add(segment);
                }
            }
        }
    }

    /**
     * Just recovers the given segment, without adding it to the provided params.segments.
     *
     * @param segment Segment to recover
     * @return The number of bytes truncated from the segment
     * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
     */
    private int recoverSegment(LogSegment segment) throws IOException{
        ProducerStateManager producerStateManager = new ProducerStateManager(
                topicPartition,
                dir,
                this.producerStateManager.getMaxTransactionTimeoutMs(),
                this.producerStateManager.getMaxProducerIdExpirationMs(),
                time);
        UnifiedLog.rebuildProducerState(
                producerStateManager,
                segments,
                logStartOffsetCheckpoint,
                segment.getBaseOffset(),
                config.recordVersion(),
                time,
                 false,
                logIdent);
        int bytesTruncated = segment.recover(producerStateManager, leaderEpochCache);
        // once we have recovered the segment's data, take a snapshot to ensure that we won't
        // need to reload the same segment again while recovering another segment.
        producerStateManager.takeSnapshot();
        return bytesTruncated;
    }

    /**
     * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
     * active segment, and returns the updated recovery point and next offset after recovery. Along
     * the way, the method suitably updates the LeaderEpochFileCache or ProducerStateManager inside
     * the provided LogComponents.
     * <p>
     * This method does not need to convert IOException to KafkaStorageException because it is only
     * called before all logs are loaded.
     *
     * @return a tuple containing (newRecoveryPoint, nextOffset).
     * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
     */
    protected Pair<Long, Long> recoverLog() throws IOException{
        // If we have the clean shutdown marker, skip recovery.
        if (!hadCleanShutdown) {
            Collection<LogSegment> unflushed = segments.values(recoveryPointCheckpoint, Long.MAX_VALUE);
            int numUnflushed = unflushed.size();
            Iterator<LogSegment> unflushedIter = unflushed.iterator();
            boolean truncated = false;
            int numFlushed = 0;
            String threadName = Thread.currentThread().getName();
            numRemainingSegments.put(threadName, numUnflushed);

            while (unflushedIter.hasNext() && !truncated) {
                LogSegment segment = unflushedIter.next();
                LOG.info("Recovering unflushed segment {}. {}/{} recovered for {}.",
                        segment.getBaseOffset(), numFlushed, numUnflushed, topicPartition);

                int truncatedBytes = 0;
                try {
                    truncatedBytes = recoverSegment(segment);
                } catch (InvalidOffsetException e) {
                    long startOffset = segment.getBaseOffset();
                    LOG.warn("Found invalid offset during recovery. Deleting the" +
                            " corrupt segment and creating an empty one with starting offset {}", startOffset);
                    segment.truncateTo(startOffset);
                }
                if (truncatedBytes > 0) {
                    // we had an invalid message, delete all remaining log
                    LOG.warn("Corruption found in segment {}, truncating to offset {}", segment.getBaseOffset(), segment.readNextOffset());
                    List<LogSegment> segmentsToDelete = new ArrayList<>();
                    while (unflushedIter.hasNext()) {
                        segmentsToDelete.add(unflushedIter.next());
                    }
                    removeAndDeleteSegmentsAsync(segmentsToDelete);
                    truncated = true;
                    // segment is truncated, so set remaining segments to 0
                    numRemainingSegments.put(threadName, 0);
                } else {
                    numFlushed += 1;
                    numRemainingSegments.put(threadName, numUnflushed - numFlushed);
                }
            }
        }

        Optional<Long> logEndOffsetOption = deleteSegmentsIfLogStartGreaterThanLogEnd();

        if (segments.isEmpty()) {
            // no existing segments, create a new mutable segment beginning at logStartOffset
            segments.add(
                    LogSegment.open(dir,
                            logStartOffsetCheckpoint,
                            config,
                            time,
                            false,
                            config.initFileSize(),
                            config.getPreallocate(),
                            ""));
        }

        // Update the recovery point if there was a clean shutdown and did not perform any changes to
        // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
        // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
        // the recovery point when the log is flushed. If we advanced the recovery point here, we could
        // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
        // point and before we flush the segment.
        if (hadCleanShutdown && logEndOffsetOption.isPresent()) {
            Long logEndOffset = logEndOffsetOption.get();
            return Pair.of(logEndOffset, logEndOffset);
        } else {
            long logEndOffset = logEndOffsetOption.orElse(segments.lastSegment().get().readNextOffset());
            return Pair.of(Math.min(recoveryPointCheckpoint, logEndOffset), logEndOffset);
        }
    }

    /**
     * return the log end offset if valid
     *
     * @return
     */
    private Optional<Long> deleteSegmentsIfLogStartGreaterThanLogEnd()throws IOException {
        if (segments.nonEmpty()) {
            long logEndOffset = segments.lastSegment().get().readNextOffset();
            if (logEndOffset >= logStartOffsetCheckpoint) {
                return Optional.of(logEndOffset);
            } else {
                LOG.warn("Deleting all segments because logEndOffset ({}) is smaller than logStartOffset {}. " +
                        "This could happen if segment files were deleted from the file system.", logEndOffset, logStartOffsetCheckpoint);
                removeAndDeleteSegmentsAsync(segments.values());
                if (leaderEpochCache.isPresent()) {
                    leaderEpochCache.get().clearAndFlush();
                }
                producerStateManager.truncateFullyAndStartAt(logStartOffsetCheckpoint);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * This method deletes the given log segments and the associated producer snapshots, by doing the
     * following for each of them:
     * - It removes the segment from the segment map so that it will no longer be used for reads.
     * - It schedules asynchronous deletion of the segments that allows reads to happen concurrently without
     * synchronization and without the possibility of physically deleting a file while it is being
     * read.
     * <p>
     * This method does not need to convert IOException to KafkaStorageException because it is either
     * called before all logs are loaded or the immediate caller will catch and handle IOException
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     */
    private void removeAndDeleteSegmentsAsync(Collection<LogSegment> segmentsToDelete) throws IOException{
        if (CollectionUtils.isNotEmpty(segmentsToDelete)) {
            // Most callers hold an iterator into the `params.segments` collection and
            // `removeAndDeleteSegmentAsync` mutates it by removing the deleted segment. Therefore,
            // we should force materialization of the iterator here, so that results of the iteration
            // remain valid and deterministic. We should also pass only the materialized view of the
            // iterator to the logic that deletes the segments.
            ImmutableList<LogSegment> toDelete = ImmutableList.copyOf(segmentsToDelete);
            LOG.info("Deleting segments as part of log recovery: {}",
                    toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(",")));
            for (LogSegment segment : toDelete) {
                segments.remove(segment.getBaseOffset());
            }
            UnifiedLog.deleteSegmentFiles(
                    toDelete,
                    true,
                    dir,
                    topicPartition,
                    config,
                    scheduler,
                    logDirFailureChannel,
                    logIdent);
            deleteProducerSnapshotsAsync(segmentsToDelete);
        }
    }

    private void deleteProducerSnapshotsAsync(Iterable<LogSegment> segments) throws IOException{
        UnifiedLog.deleteProducerSnapshots(segments,
                producerStateManager,
                true,
                scheduler,
                config,
                logDirFailureChannel,
                dir.getParent(),
                topicPartition);
    }

    @Getter
    class LoadedLogOffsets {
        private Long logStartOffset;
        private Long recoveryPoint;
        private LogOffsetMetadata nextOffsetMetadata;

        public LoadedLogOffsets(Long logStartOffset,
                                Long recoveryPoint,
                                LogOffsetMetadata nextOffsetMetadata) {
            this.logStartOffset = logStartOffset;
            this.recoveryPoint = recoveryPoint;
            this.nextOffsetMetadata = nextOffsetMetadata;
        }
    }
}
