package cn.pockethub.permanentqueue.kafka.server.epoch;

import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithThrowable;
import cn.pockethub.permanentqueue.kafka.server.checkpoints.LeaderEpochCheckpoint;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import cn.pockethub.permanentqueue.kafka.utils.Logging;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

public class LeaderEpochFileCache extends Logging {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderEpochFileCache.class);

    private TopicPartition topicPartition;
    private LeaderEpochCheckpoint checkpoint;
    private String logIdent;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private TreeMap<Integer, EpochEntry> epochs = new TreeMap<>();

    public LeaderEpochFileCache(TopicPartition topicPartition,
                                LeaderEpochCheckpoint checkpoint) {
        this.topicPartition = topicPartition;
        this.checkpoint = checkpoint;

        this.logIdent = String.format("[LeaderEpochCache %s] ", topicPartition);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            for (EpochEntry epochEntry : checkpoint.read()) {
                assign(epochEntry);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Assigns the supplied Leader Epoch to the supplied Offset
     * Once the epoch is assigned it cannot be reassigned
     */
    public void assign(Integer epoch, Long startOffset) {
        EpochEntry entry = new EpochEntry(epoch, startOffset);
        if (assign(entry)) {
            LOG.debug("Appended new epoch entry {}. Cache now contains {} entries.", entry, epochs.size());
            flush();
        }
    }

    private Boolean assign(EpochEntry entry) {
        if (entry.getEpoch() < 0 || entry.getStartOffset() < 0) {
            throw new IllegalArgumentException("Received invalid partition leader epoch entry " + entry);
        }

        // Check whether the append is needed before acquiring the write lock
        // in order to avoid contention with readers in the common case
        if (!isUpdateNeeded(entry)) {
            return false;
        }

        return CoreUtils.inWriteLock(lock, new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                if (isUpdateNeeded(entry)) {
                    maybeTruncateNonMonotonicEntries(entry);
                    epochs.put(entry.getEpoch(), entry);
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    private Boolean isUpdateNeeded(EpochEntry entry) {
        Optional<EpochEntry> epochEntryOptional = latestEntry();
        if (epochEntryOptional.isPresent()) {
            EpochEntry lastEntry = epochEntryOptional.get();
            return !Objects.equals(entry.getEpoch(), lastEntry.getEpoch())
                    || entry.getStartOffset() < lastEntry.getStartOffset();
        } else {
            return true;
        }
    }

    /**
     * Remove any entries which violate monotonicity prior to appending a new entry
     */
    private void maybeTruncateNonMonotonicEntries(EpochEntry newEntry) {
        Collection<EpochEntry> removedEpochs = removeFromEnd(entry ->
                entry.getEpoch() >= newEntry.getEpoch()
                        || entry.getStartOffset() >= newEntry.getStartOffset()
        );

        if (removedEpochs.size() > 1
                || (!removedEpochs.isEmpty() && !Objects.equals(CollectionUtilExt.head(removedEpochs).getStartOffset(), newEntry.getStartOffset()))) {

            // Only log a warning if there were non-trivial removals. If the start offset of the new entry
            // matches the start offset of the removed epoch, then no data has been written and the truncation
            // is expected.
            LOG.warn("New epoch entry {} caused truncation of conflicting entries {}. " +
                    "Cache now contains {} entries.", newEntry, removedEpochs, epochs.size());
        }
    }

    private Collection<EpochEntry> removeFromEnd(Predicate<EpochEntry> predicate) {
        return removeWhileMatching(epochs.descendingMap().entrySet().iterator(), predicate);
    }

    private Collection<EpochEntry> removeFromStart(Predicate<EpochEntry> predicate) {
        return removeWhileMatching(epochs.entrySet().iterator(), predicate);
    }

    private Collection<EpochEntry> removeWhileMatching(Iterator<Map.Entry<Integer, EpochEntry>> iterator,
                                                       Predicate<EpochEntry> predicate) {
        ArrayList removedEpochs = new ArrayList<EpochEntry>();

        while (iterator.hasNext()) {
            EpochEntry entry = iterator.next().getValue();
            if (predicate.test(entry)) {
                removedEpochs.add(entry);
                iterator.remove();
            } else {
                return removedEpochs;
            }
        }

        return removedEpochs;
    }

    public Boolean nonEmpty() {
        return CoreUtils.inReadLock(lock, new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                return !epochs.isEmpty();
            }
        });
    }

    public Optional<EpochEntry> latestEntry() {
        return CoreUtils.inReadLock(lock, new Supplier<Optional<EpochEntry>>() {
            @Override
            public Optional<EpochEntry> get() {
                return Optional.ofNullable(epochs.lastEntry()).map(Map.Entry::getValue);
            }
        });
    }

    /**
     * Returns the current Leader Epoch if one exists. This is the latest epoch
     * which has messages assigned to it.
     */
    public Optional<Integer> latestEpoch() {
        return latestEntry().map(EpochEntry::getEpoch);
    }

    public Optional<Integer> previousEpoch() throws Throwable {
        return CoreUtils.inReadLockWithThrowable(lock, new SupplierWithThrowable<Optional<Integer>>() {
            @Override
            public Optional<Integer> get() throws Throwable {
                return latestEntry()
                        .flatMap(entry -> Optional.ofNullable(epochs.lowerEntry(entry.getEpoch())))
                        .map(Map.Entry::getKey);
            }
        });
    }

    /**
     * Get the earliest cached entry if one exists.
     */
    public Optional<EpochEntry> earliestEntry() throws Throwable {
        return CoreUtils.inReadLockWithThrowable(lock, new SupplierWithThrowable<Optional<EpochEntry>>() {
            @Override
            public Optional<EpochEntry> get() {
                return Optional.ofNullable(epochs.firstEntry()).map(Map.Entry::getValue);
            }
        });
    }

    /**
     * Returns the Leader Epoch and the End Offset for a requested Leader Epoch.
     * <p>
     * The Leader Epoch returned is the largest epoch less than or equal to the requested Leader
     * Epoch. The End Offset is the end offset of this epoch, which is defined as the start offset
     * of the first Leader Epoch larger than the Leader Epoch requested, or else the Log End
     * Offset if the latest epoch was requested.
     * <p>
     * During the upgrade phase, where there are existing messages may not have a leader epoch,
     * if requestedEpoch is < the first epoch cached, UNDEFINED_EPOCH_OFFSET will be returned
     * so that the follower falls back to High Water Mark.
     *
     * @param requestedEpoch requested leader epoch
     * @param logEndOffset   the existing Log End Offset
     * @return found leader epoch and end offset
     */
    public Pair<Integer, Long> endOffsetFor(Integer requestedEpoch, Long logEndOffset) throws Throwable {
        return CoreUtils.inReadLockWithThrowable(lock, new SupplierWithThrowable<Pair<Integer, Long>>() {
            @Override
            public Pair<Integer, Long> get() throws Throwable {
                Pair<Integer, Long> epochAndOffset;
                if (requestedEpoch == UNDEFINED_EPOCH) {
                    // This may happen if a bootstrapping follower sends a request with undefined epoch or
                    // a follower is on the older message format where leader epochs are not recorded
                    epochAndOffset = Pair.of(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
                } else {
                    Optional<Integer> latestEpochOptional = latestEpoch();
                    if (latestEpochOptional.isPresent() && latestEpochOptional.get().equals(requestedEpoch)) {
                        // For the leader, the latest epoch is always the current leader epoch that is still being written to.
                        // Followers should not have any reason to query for the end offset of the current epoch, but a consumer
                        // might if it is verifying its committed offset following a group rebalance. In this case, we return
                        // the current log end offset which makes the truncation check work as expected.
                        epochAndOffset = Pair.of(requestedEpoch, logEndOffset);
                    } else {
                        Map.Entry<Integer, EpochEntry> higherEntry = epochs.higherEntry(requestedEpoch);
                        if (higherEntry == null) {
                            // The requested epoch is larger than any known epoch. This case should never be hit because
                            // the latest cached epoch is always the largest.
                            epochAndOffset = Pair.of(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
                        } else {
                            Map.Entry<Integer, EpochEntry> floorEntry = epochs.floorEntry(requestedEpoch);
                            if (floorEntry == null) {
                                // The requested epoch is smaller than any known epoch, so we return the start offset of the first
                                // known epoch which is larger than it. This may be inaccurate as there could have been
                                // epochs in between, but the point is that the data has already been removed from the log
                                // and we want to ensure that the follower can replicate correctly beginning from the leader's
                                // start offset.
                                epochAndOffset = Pair.of(requestedEpoch, higherEntry.getValue().getStartOffset());
                            } else {
                                // We have at least one previous epoch and one subsequent epoch. The result is the first
                                // prior epoch and the starting offset of the first subsequent epoch.
                                epochAndOffset = Pair.of(floorEntry.getValue().getEpoch(), higherEntry.getValue().getStartOffset());
                            }
                        }
                    }
                }
                LOG.trace("Processed end offset request for epoch {} and returning epoch {} " +
                                "with end offset {} from epoch cache of size {}",
                        requestedEpoch, epochAndOffset.getKey(), epochAndOffset.getValue(), epochs.size());
                return epochAndOffset;
            }
        });
    }

    /**
     * Removes all epoch entries from the store with start offsets greater than or equal to the passed offset.
     */
    public void truncateFromEnd(Long endOffset) {
        CoreUtils.inWriteLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                Optional<EpochEntry> latestEntryOptional = latestEntry();
                if (endOffset >= 0
                        && latestEntryOptional.isPresent() && latestEntryOptional.get().getStartOffset() >= endOffset) {
                    Collection<EpochEntry> removedEntries = removeFromEnd(new Predicate<EpochEntry>() {
                        @Override
                        public boolean test(EpochEntry epochEntry) {
                            return latestEntryOptional.get().getStartOffset() >= endOffset;
                        }
                    });

                    flush();

                    LOG.debug("Cleared entries {} from epoch cache after truncating to end offset {}, leaving {} entries in the cache.",
                            removedEntries.stream().map(EpochEntry::toString).collect(Collectors.joining(", ")),
                            endOffset, epochs.size());
                }
                return null;
            }
        });
    }

    /**
     * Clears old epoch entries. This method searches for the oldest epoch < offset, updates the saved epoch offset to
     * be offset, then clears any previous epoch entries.
     * <p>
     * This method is exclusive: so truncateFromStart(6) will retain an entry at offset 6.
     *
     * @param startOffset the offset to clear up to
     */
    public void truncateFromStart(Long startOffset) {
        CoreUtils.inWriteLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                Collection<EpochEntry> removedEntries = removeFromStart(entry -> entry.getStartOffset() <= startOffset);
                CollectionUtilExt.lastOptional(removedEntries)
                        .ifPresent(firstBeforeStartOffset -> {
                                    EpochEntry updatedFirstEntry = new EpochEntry(firstBeforeStartOffset.getEpoch(), startOffset);
                                    epochs.put(updatedFirstEntry.getEpoch(), updatedFirstEntry);

                                    flush();

                                    LOG.debug("Cleared entries $removedEntries and rewrote first entry $updatedFirstEntry after " +
                                            "truncating to start offset $startOffset, leaving ${epochs.size} in the cache.");
                                }
                        );
                return null;
            }
        });
    }

    /**
     * Delete all entries.
     */
    public void clearAndFlush() {
        CoreUtils.inWriteLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                epochs.clear();
                flush();
                return null;
            }
        });
    }

    public void clear() {
        CoreUtils.inWriteLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                epochs.clear();
                return null;
            }
        });
    }

    // Visible for testing
    public Collection<EpochEntry> epochEntries() {
        return epochs.values();
    }

    private void flush() {
        checkpoint.write(epochs.values());
    }
}
