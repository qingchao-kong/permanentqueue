package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * Maintains a mapping from ProducerIds to metadata about the last appended entries (e.g.
 * epoch, sequence number, last offset, etc.)
 * <p>
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 * <p>
 * As long as a producer id is contained in the map, the corresponding producer can continue to write data.
 * However, producer ids can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given producer id is retained in the log provided it hasn't expired due to
 * age. This ensures that producer ids will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 */
@Getter
public class ProducerStateManager {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerStateManager.class);

    public static Long LateTransactionBufferMs = 5 * 60 * 1000L;

    private static Short ProducerSnapshotVersion = 1;
    private static String VersionField = "version";
    private static String CrcField = "crc";
    private static String ProducerIdField = "producer_id";
    private static String LastSequenceField = "last_sequence";
    private static String ProducerEpochField = "epoch";
    private static String LastOffsetField = "last_offset";
    private static String OffsetDeltaField = "offset_delta";
    private static String TimestampField = "timestamp";
    private static String ProducerEntriesField = "producer_entries";
    private static String CoordinatorEpochField = "coordinator_epoch";
    private static String CurrentTxnFirstOffsetField = "current_txn_first_offset";

    private static Integer VersionOffset = 0;
    private static Integer CrcOffset = VersionOffset + 2;
    private static Integer ProducerEntriesOffset = CrcOffset + 4;

    public static Schema ProducerSnapshotEntrySchema = new Schema(
            new Field(ProducerIdField, Type.INT64, "The producer ID"),
            new Field(ProducerEpochField, Type.INT16, "Current epoch of the producer"),
            new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
            new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
            new Field(OffsetDeltaField, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
            new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
            new Field(CoordinatorEpochField, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
            new Field(CurrentTxnFirstOffsetField, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"));
    public static Schema PidSnapshotMapSchema = new Schema(
            new Field(VersionField, Type.INT16, "Version of the snapshot file"),
            new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
            new Field(ProducerEntriesField, new ArrayOf(ProducerSnapshotEntrySchema), "The entries in the producer table"));


    private TopicPartition topicPartition;
    private File _logDir;
    private Integer maxTransactionTimeoutMs;
    private Integer maxProducerIdExpirationMs;
    private Time time;

    private String logIdent;

    private ConcurrentSkipListMap<Long, SnapshotFile> snapshots;

    private Map<Long, ProducerStateEntry> producers = new HashMap<>();
    private Long lastMapOffset = 0L;
    private Long lastSnapOffset = 0L;

    // Keep track of the last timestamp from the oldest transaction. This is used
    // to detect (approximately) when a transaction has been left hanging on a partition.
    // We make the field volatile so that it can be safely accessed without a lock.
    private volatile Long oldestTxnLastTimestamp = -1L;

    // ongoing transactions sorted by the first offset of the transaction
    private TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();

    // completed transactions whose markers are at offsets above the high watermark
    private TreeMap<Long, TxnMetadata> unreplicatedTxns = new TreeMap<>();

    public ProducerStateManager(TopicPartition topicPartition,
                                File _logDir,
                                Integer maxTransactionTimeoutMs,
                                Integer maxProducerIdExpirationMs,
                                Time time) {
        this.topicPartition = topicPartition;
        this._logDir = _logDir;
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        this.maxProducerIdExpirationMs = maxProducerIdExpirationMs;
        this.time = time;

        this.logIdent = String.format("[ProducerStateManager partition=%s] ", topicPartition);

        this.snapshots = loadSnapshots();
    }

    //    @threadsafe
    public Boolean hasLateTransaction(Long currentTimeMs) {
        Long lastTimestamp = oldestTxnLastTimestamp;
        return lastTimestamp > 0 && (currentTimeMs - lastTimestamp) > maxTransactionTimeoutMs + ProducerStateManager.LateTransactionBufferMs;
    }

    /**
     * Load producer state snapshots by scanning the _logDir.
     */
    private ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() {
        ConcurrentSkipListMap<Long, SnapshotFile> tm = new ConcurrentSkipListMap<>();
        for (SnapshotFile f : listSnapshotFiles(_logDir)) {
            tm.put(f.getOffset(), f);
        }
        return tm;
    }

    /**
     * Scans the log directory, gathering all producer state snapshot files. Snapshot files which do not have an offset
     * corresponding to one of the provided offsets in segmentBaseOffsets will be removed, except in the case that there
     * is a snapshot file at a higher offset than any offset in segmentBaseOffsets.
     * <p>
     * The goal here is to remove any snapshot files which do not have an associated segment file, but not to remove the
     * largest stray snapshot file which was emitted during clean shutdown.
     */
    protected void removeStraySnapshots(List<Long> segmentBaseOffsets) throws IOException {
        Optional<Long> maxSegmentBaseOffset = CollectionUtils.isEmpty(segmentBaseOffsets) ? Optional.empty() : Optional.of(Collections.max(segmentBaseOffsets));
        Set<Long> baseOffsets = new HashSet<>(segmentBaseOffsets);
        Optional<SnapshotFile> latestStraySnapshot = Optional.empty();

        ConcurrentSkipListMap<Long, SnapshotFile> ss = loadSnapshots();
        for (SnapshotFile snapshot : ss.values()) {
            long key = snapshot.getOffset();
            if (latestStraySnapshot.isPresent()) {
                SnapshotFile prev = latestStraySnapshot.get();
                if (!baseOffsets.contains(key)) {
                    // this snapshot is now the largest stray snapshot.
                    prev.deleteIfExists();
                    ss.remove(prev.getOffset());
                    latestStraySnapshot = Optional.of(snapshot);
                }
            } else {
                if (!baseOffsets.contains(key)) {
                    latestStraySnapshot = Optional.of(snapshot);
                }
            }
        }

        // Check to see if the latestStraySnapshot is larger than the largest segment base offset, if it is not,
        // delete the largestStraySnapshot.
        if (latestStraySnapshot.isPresent() && maxSegmentBaseOffset.isPresent()) {
            Long strayOffset = latestStraySnapshot.get().getOffset();
            Long maxOffset = maxSegmentBaseOffset.get();
            if (strayOffset < maxOffset) {
                SnapshotFile snapshotFile = ss.remove(strayOffset);
                if (Objects.nonNull(snapshotFile)) {
                    snapshotFile.deleteIfExists();
                }
            }
        }

        this.snapshots = ss;
    }

    /**
     * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
     * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
     * marker written at a higher offset than the current high watermark).
     */
    public Optional<LogOffsetMetadata> firstUnstableOffset() {
        Optional<LogOffsetMetadata> unreplicatedFirstOffset = Optional.ofNullable(unreplicatedTxns.firstEntry()).map(entry -> entry.getValue().getFirstOffset());
        Optional<LogOffsetMetadata> undecidedFirstOffset = Optional.ofNullable(ongoingTxns.firstEntry()).map(entry -> entry.getValue().getFirstOffset());
        if (!unreplicatedFirstOffset.isPresent()) {
            return undecidedFirstOffset;
        } else if (!undecidedFirstOffset.isPresent()) {
            return unreplicatedFirstOffset;
        } else if (undecidedFirstOffset.get().getMessageOffset() < unreplicatedFirstOffset.get().getMessageOffset()) {
            return undecidedFirstOffset;
        } else {
            return unreplicatedFirstOffset;
        }
    }

    /**
     * Acknowledge all transactions which have been completed before a given offset. This allows the LSO
     * to advance to the next unstable offset.
     */
    public void onHighWatermarkUpdated(Long highWatermark) {
        removeUnreplicatedTransactions(highWatermark);
    }

    /**
     * The first undecided offset is the earliest transactional message which has not yet been committed
     * or aborted. Unlike [[firstUnstableOffset]], this does not reflect the state of replication (i.e.
     * whether a completed transaction marker is beyond the high watermark).
     */
    protected Optional<Long> firstUndecidedOffset() {
        return Optional.ofNullable(ongoingTxns.firstEntry()).map(entry -> entry.getValue().getFirstOffset().getMessageOffset());
    }

    /**
     * Returns the last offset of this map
     */
    public Long mapEndOffset() {
        return lastMapOffset;
    }

    /**
     * Get a copy of the active producers
     */
    public Map<Long, ProducerStateEntry> activeProducers() {
        return producers;
    }

    public Boolean isEmpty() {
        return MapUtils.isEmpty(producers) && MapUtils.isEmpty(unreplicatedTxns);
    }

    private void loadFromSnapshot(Long logStartOffset, Long currentTime) throws IOException {
        while (true) {
            Optional<SnapshotFile> optional = latestSnapshotFile();
            if (optional.isPresent()) {
                SnapshotFile snapshot = optional.get();
                try {
                    LOG.info("Loading producer state from snapshot file '{}'", snapshot);
                    List<ProducerStateEntry> loadedProducers = new ArrayList<>();
                    for (ProducerStateEntry producerEntry : readSnapshot(snapshot.getFile())) {
                        if (!isProducerExpired(currentTime, producerEntry)) {
                            loadedProducers.add(producerEntry);
                        }
                    }
                    loadedProducers.forEach(this::loadProducerEntry);
                    lastSnapOffset = snapshot.getOffset();
                    lastMapOffset = lastSnapOffset;
                    updateOldestTxnTimestamp();
                    return;
                } catch (CorruptSnapshotException e) {
                    LOG.warn("Failed to load producer snapshot from '{}': {}", snapshot.getFile(), e.getMessage());
                    removeAndDeleteSnapshot(snapshot.getOffset());
                }
            } else {
                lastSnapOffset = logStartOffset;
                lastMapOffset = logStartOffset;
                return;
            }
        }
    }

    // visible for testing
    protected void loadProducerEntry(ProducerStateEntry entry) {
        long producerId = entry.getProducerId();
        producers.put(producerId, entry);
        Optional<Long> currentTxnFirstOffset = entry.getCurrentTxnFirstOffset();
        if (currentTxnFirstOffset.isPresent()) {
            Long offset = currentTxnFirstOffset.get();
            ongoingTxns.put(offset, new TxnMetadata(producerId, offset));
        }
    }

    private Boolean isProducerExpired(Long currentTimeMs, ProducerStateEntry producerState) {
        return !producerState.getCurrentTxnFirstOffset().isPresent() && currentTimeMs - producerState.getLastTimestamp() >= maxProducerIdExpirationMs;
    }

    /**
     * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
     */
    public void removeExpiredProducers(Long currentTimeMs) {
        Set<Long> toRemove = new HashSet<>();
        for (Map.Entry<Long, ProducerStateEntry> entry : producers.entrySet()) {
            if (isProducerExpired(currentTimeMs, entry.getValue())) {
                toRemove.add(entry.getKey());
            }
        }
        HashMap<Long, ProducerStateEntry> tmpProducers = new HashMap<>(producers);
        for (Long l : toRemove) {
            tmpProducers.remove(l);
        }
        producers = tmpProducers;
    }

    /**
     * Truncate the producer id mapping to the given offset range and reload the entries from the most recent
     * snapshot in range (if there is one). We delete snapshot files prior to the logStartOffset but do not remove
     * producer state from the map. This means that in-memory and on-disk state can diverge, and in the case of
     * broker failover or unclean shutdown, any in-memory state not persisted in the snapshots will be lost, which
     * would lead to UNKNOWN_PRODUCER_ID errors. Note that the log end offset is assumed to be less than or equal
     * to the high watermark.
     */
    public void truncateAndReload(Long logStartOffset, Long logEndOffset, Long currentTimeMs) throws IOException {
        // remove all out of range snapshots
        for (SnapshotFile snapshot : snapshots.values()) {
            if (snapshot.getOffset() > logEndOffset || snapshot.getOffset() <= logStartOffset) {
                removeAndDeleteSnapshot(snapshot.getOffset());
            }
        }

        if (!Objects.equals(logEndOffset, mapEndOffset())) {
            producers.clear();
            ongoingTxns.clear();
            updateOldestTxnTimestamp();

            // since we assume that the offset is less than or equal to the high watermark, it is
            // safe to clear the unreplicated transactions
            unreplicatedTxns.clear();
            loadFromSnapshot(logStartOffset, currentTimeMs);
        } else {
            onLogStartOffsetIncremented(logStartOffset);
        }
    }

    public ProducerAppendInfo prepareUpdate(Long producerId, AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElseGet(() -> ProducerStateEntry.empty(producerId));
        return new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin);
    }

    /**
     * Update the mapping with the given append information
     */
    public void update(ProducerAppendInfo appendInfo) {
        if (appendInfo.getProducerId() == RecordBatch.NO_PRODUCER_ID) {
            String msg = String.format("Invalid producer id %s passed to update for partition %s",
                    appendInfo.getProducerId(), topicPartition);
            throw new IllegalArgumentException(msg);
        }

        LOG.trace("Updated producer {} state to $appendInfo", appendInfo.getProducerId());
        ProducerStateEntry updatedEntry = appendInfo.toEntry();
        ProducerStateEntry currentEntry = producers.get(appendInfo.getProducerId());
        if (Objects.nonNull(currentEntry)) {
            currentEntry.update(updatedEntry);
        } else {
            producers.put(appendInfo.getProducerId(), updatedEntry);
        }

        for (TxnMetadata txn : appendInfo.startedTransactions()) {
            ongoingTxns.put(txn.getFirstOffset().getMessageOffset(), txn);
        }

        updateOldestTxnTimestamp();
    }

    private void updateOldestTxnTimestamp() {
        Map.Entry<Long, TxnMetadata> firstEntry = ongoingTxns.firstEntry();
        if (firstEntry == null) {
            oldestTxnLastTimestamp = -1L;
        } else {
            TxnMetadata oldestTxnMetadata = firstEntry.getValue();
            ProducerStateEntry entry = producers.get(oldestTxnMetadata.getProducerId());
            if (Objects.nonNull(entry)) {
                oldestTxnLastTimestamp = entry.getLastTimestamp();
            } else {
                oldestTxnLastTimestamp = -1L;
            }
        }
    }

    public void updateMapEndOffset(Long lastOffset) {
        lastMapOffset = lastOffset;
    }

    /**
     * Get the last written entry for the given producer id.
     */
    public Optional<ProducerStateEntry> lastEntry(Long producerId) {
        return Optional.ofNullable(producers.get(producerId));
    }

    /**
     * Take a snapshot at the current end offset if one does not already exist.
     */
    public void takeSnapshot() throws IOException {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            SnapshotFile snapshotFile = SnapshotFile.apply(UnifiedLog.producerSnapshotFile(_logDir, lastMapOffset));
            long start = time.hiResClockMs();
            writeSnapshot(snapshotFile.getFile(), producers);
            LOG.info("Wrote producer snapshot at offset {} with {} producer ids in {} ms.",
                    lastMapOffset, producers.size(), time.hiResClockMs() - start);

            snapshots.put(snapshotFile.getOffset(), snapshotFile);

            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset;
        }
    }

    /**
     * Update the parentDir for this ProducerStateManager and all of the snapshot files which it manages.
     */
    public void updateParentDir(File parentDir) {
        _logDir = parentDir;
        snapshots.forEach((k, v) -> v.updateParentDir(parentDir));
    }

    /**
     * Get the last offset (exclusive) of the latest snapshot file.
     */
    public Optional<Long> latestSnapshotOffset() {
        return latestSnapshotFile().map(SnapshotFile::getOffset);
    }

    /**
     * Get the last offset (exclusive) of the oldest snapshot file.
     */
    public Optional<Long> oldestSnapshotOffset() {
        return oldestSnapshotFile().map(SnapshotFile::getOffset);
    }

    /**
     * Visible for testing
     */
    protected Optional<SnapshotFile> snapshotFileForOffset(Long offset) {
        return Optional.ofNullable(snapshots.get(offset));
    }

    /**
     * Remove any unreplicated transactions lower than the provided logStartOffset and bring the lastMapOffset forward
     * if necessary.
     */
    public void onLogStartOffsetIncremented(Long logStartOffset) {
        removeUnreplicatedTransactions(logStartOffset);

        if (lastMapOffset < logStartOffset) {
            lastMapOffset = logStartOffset;
        }

        lastSnapOffset = latestSnapshotOffset().orElse(logStartOffset);
    }

    private void removeUnreplicatedTransactions(Long offset) {
        Iterator<Map.Entry<Long, TxnMetadata>> iterator = unreplicatedTxns.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, TxnMetadata> txnEntry = iterator.next();
            Optional<Long> lastOffset = txnEntry.getValue().getLastOffset();
            if (lastOffset.isPresent() && lastOffset.get() < offset) {
                iterator.remove();
            }
        }
    }

    /**
     * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
     */
    public void truncateFullyAndStartAt(Long offset) throws IOException {
        producers.clear();
        ongoingTxns.clear();
        unreplicatedTxns.clear();
        for (SnapshotFile snapshot : snapshots.values()) {
            removeAndDeleteSnapshot(snapshot.getOffset());
        }
        lastSnapOffset = 0L;
        lastMapOffset = offset;
        updateOldestTxnTimestamp();
    }

    /**
     * Compute the last stable offset of a completed transaction, but do not yet mark the transaction complete.
     * That will be done in `completeTxn` below. This is used to compute the LSO that will be appended to the
     * transaction index, but the completion must be done only after successfully appending to the index.
     */
    public Long lastStableOffset(CompletedTxn completedTxn) {
        Optional<TxnMetadata> nextIncompleteTxn = Optional.empty();
        for (TxnMetadata txn : ongoingTxns.values()) {
            if (!txn.getProducerId().equals(completedTxn.getProducerId())) {
                nextIncompleteTxn = Optional.of(txn);
            }
        }
        return nextIncompleteTxn.map(txn -> txn.getFirstOffset().getMessageOffset()).orElse(completedTxn.getLastOffset() + 1);
    }

    /**
     * Mark a transaction as completed. We will still await advancement of the high watermark before
     * advancing the first unstable offset.
     */
    public void completeTxn(CompletedTxn completedTxn) {
        TxnMetadata txnMetadata = ongoingTxns.remove(completedTxn.getFirstOffset());
        if (txnMetadata == null) {
            String msg = String.format("Attempted to complete transaction %s on partition %s which was not started",
                    completedTxn, topicPartition);
            throw new IllegalArgumentException(msg);
        }

        txnMetadata.setLastOffset(Optional.of(completedTxn.getLastOffset()));
        unreplicatedTxns.put(completedTxn.getFirstOffset(), txnMetadata);
        updateOldestTxnTimestamp();
    }

    //    @threadsafe
    public void deleteSnapshotsBefore(Long offset) throws IOException {
        for (SnapshotFile snapshot : snapshots.subMap(0L, offset).values()) {
            removeAndDeleteSnapshot(snapshot.getOffset());
        }
    }

    private Optional<SnapshotFile> oldestSnapshotFile() {
        return Optional.ofNullable(snapshots.firstEntry()).map(Map.Entry::getValue);
    }

    private Optional<SnapshotFile> latestSnapshotFile() {
        return Optional.ofNullable(snapshots.lastEntry()).map(Map.Entry::getValue);
    }

    /**
     * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
     * ProducerStateManager, and deletes the backing snapshot file.
     */
    private void removeAndDeleteSnapshot(Long snapshotOffset) throws IOException {
        SnapshotFile remove = snapshots.remove(snapshotOffset);
        if (Objects.nonNull(remove)) {
            remove.deleteIfExists();
        }
    }

    /**
     * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
     * ProducerStateManager, and renames the backing snapshot file to have the Log.DeletionSuffix.
     * <p>
     * Note: This method is safe to use with async deletes. If a race occurs and the snapshot file
     * is deleted without this ProducerStateManager instance knowing, the resulting exception on
     * SnapshotFile rename will be ignored and None will be returned.
     */
    protected Optional<SnapshotFile> removeAndMarkSnapshotForDeletion(Long snapshotOffset) throws IOException {
        Optional<SnapshotFile> optional = Optional.ofNullable(snapshots.remove(snapshotOffset));
        if (optional.isPresent()) {
            SnapshotFile snapshot = optional.get();
            // If the file cannot be renamed, it likely means that the file was deleted already.
            // This can happen due to the way we construct an intermediate producer state manager
            // during log recovery, and use it to issue deletions prior to creating the "real"
            // producer state manager.
            //
            // In any case, removeAndMarkSnapshotForDeletion is intended to be used for snapshot file
            // deletion, so ignoring the exception here just means that the intended operation was
            // already completed.
            try {
                snapshot.renameTo(UnifiedLog.DeletedFileSuffix);
                return Optional.ofNullable(snapshot);
            } catch (NoSuchFileException e) {
                LOG.info("Failed to rename producer state snapshot {} with deletion suffix because it was already deleted",
                        snapshot.getFile().getAbsoluteFile());
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    /*static method*/
    public Iterable<ProducerStateEntry> readSnapshot(File file) throws IOException {
        try {
            byte[] buffer = Files.readAllBytes(file.toPath());
            Struct struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer));

            Short version = struct.getShort(VersionField);
            if (version != ProducerSnapshotVersion) {
                String msg = String.format("Snapshot contained an unknown file version %s", version);
                throw new CorruptSnapshotException(msg);
            }

            long crc = struct.getUnsignedInt(CrcField);
            long computedCrc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.length - ProducerEntriesOffset);
            if (crc != computedCrc) {
                String msg = String.format("Snapshot is corrupt (CRC is no longer valid). Stored crc: %s. Computed crc: %s",
                        crc, computedCrc);
                throw new CorruptSnapshotException(msg);
            }

            return Arrays.stream(struct.getArray(ProducerEntriesField)).map(producerEntryObj -> {
                Struct producerEntryStruct = (Struct) producerEntryObj;
                Long producerId = producerEntryStruct.getLong(ProducerIdField);
                Short producerEpoch = producerEntryStruct.getShort(ProducerEpochField);
                Integer seq = producerEntryStruct.getInt(LastSequenceField);
                Long offset = producerEntryStruct.getLong(LastOffsetField);
                Long timestamp = producerEntryStruct.getLong(TimestampField);
                Integer offsetDelta = producerEntryStruct.getInt(OffsetDeltaField);
                Integer coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField);
                Long currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField);
                Queue<BatchMetadata> lastAppendedDataBatches = new ArrayDeque<>();
                if (offset >= 0) {
                    lastAppendedDataBatches.add(new BatchMetadata(seq, offset, offsetDelta, timestamp));
                }

                ProducerStateEntry newEntry = new ProducerStateEntry(producerId,
                        lastAppendedDataBatches,
                        producerEpoch,
                        coordinatorEpoch,
                        timestamp,
                        currentTxnFirstOffset >= 0 ? Optional.of(currentTxnFirstOffset) : Optional.empty());
                return newEntry;
            }).collect(Collectors.toList());
        } catch (SchemaException e) {
            throw new CorruptSnapshotException(String.format("Snapshot failed schema validation: %s", e.getMessage()));
        }
    }

    private void writeSnapshot(File file, Map<Long, ProducerStateEntry> entries) throws IOException {
        Struct struct = new Struct(PidSnapshotMapSchema);
        struct.set(VersionField, ProducerSnapshotVersion);
        // we'll fill this after writing the entries
        struct.set(CrcField, 0L);
        Object[] entriesArray = new Object[entries.size()];
        int i = 0;
        for (Map.Entry<Long, ProducerStateEntry> mapEntry : entries.entrySet()) {
            Long producerId = mapEntry.getKey();
            ProducerStateEntry entry = mapEntry.getValue();
            Struct producerEntryStruct = struct.instance(ProducerEntriesField);
            producerEntryStruct.set(ProducerIdField, producerId)
                    .set(ProducerEpochField, entry.getProducerEpoch())
                    .set(LastSequenceField, entry.lastSeq())
                    .set(LastOffsetField, entry.lastDataOffset())
                    .set(OffsetDeltaField, entry.lastOffsetDelta())
                    .set(TimestampField, entry.getLastTimestamp())
                    .set(CoordinatorEpochField, entry.getCoordinatorEpoch())
                    .set(CurrentTxnFirstOffsetField, entry.getCurrentTxnFirstOffset().orElse(-1L));
            entriesArray[i] = producerEntryStruct;
            i++;
        }
        struct.set(ProducerEntriesField, entriesArray);

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        // now fill in the CRC
        long crc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset);
        ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc);

        FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            fileChannel.write(buffer);
            fileChannel.force(true);
        } finally {
            fileChannel.close();
        }
    }

    private Boolean isSnapshotFile(File file) {
        return file.getName().endsWith(UnifiedLog.ProducerSnapshotFileSuffix);
    }

    // visible for testing
    protected List<SnapshotFile> listSnapshotFiles(File dir) {
        if (dir.exists() && dir.isDirectory()) {
            return Optional.ofNullable(dir.listFiles())
                    .map(files -> Arrays.stream(files)
                            .filter(f -> f.isFile() && isSnapshotFile(f))
                            .map(SnapshotFile::apply)
                            .collect(Collectors.toList())
                    ).orElse(new ArrayList<>(0));
        } else {
            return new ArrayList<>(0);
        }
    }
}

class CorruptSnapshotException extends KafkaException {
    public CorruptSnapshotException(String msg) {
        super(msg);
    }
}

@Getter
class BatchMetadata {

    private Integer lastSeq;
    private Long lastOffset;
    private Integer offsetDelta;
    private Long timestamp;

    public BatchMetadata(Integer lastSeq, Long lastOffset, Integer offsetDelta, Long timestamp) {
        this.lastSeq = lastSeq;
        this.lastOffset = lastOffset;
        this.offsetDelta = offsetDelta;
        this.timestamp = timestamp;
    }

    public Integer firstSeq() {
        return DefaultRecordBatch.decrementSequence(lastSeq, offsetDelta);
    }

    public Long firstOffset() {
        return lastOffset - offsetDelta;
    }

    @Override
    public String toString() {
        return String.format("BatchMetadata(" +
                "firstSeq=%s, " +
                "lastSeq=%s, " +
                "firstOffset=%s, " +
                "lastOffset=%s, " +
                "timestamp=%s)", firstSeq(), lastSeq, firstOffset(), lastOffset, timestamp);
    }
}
