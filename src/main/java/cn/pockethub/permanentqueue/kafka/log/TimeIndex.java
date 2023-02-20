package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * An index that maps from the timestamp to the logical offsets of the messages in a segment. This index might be
 * sparse, i.e. it may not hold an entry for all the messages in the segment.
 * <p>
 * The index is stored in a file that is preallocated to hold a fixed maximum amount of 12-byte time index entries.
 * The file format is a series of time index entries. The physical format is a 8 bytes timestamp and a 4 bytes "relative"
 * offset used in the [[OffsetIndex]]. A time index entry (TIMESTAMP, OFFSET) means that the biggest timestamp seen
 * before OFFSET is TIMESTAMP. i.e. Any message whose timestamp is greater than TIMESTAMP must come after OFFSET.
 * <p>
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 * <p>
 * The timestamps in the same time index file are guaranteed to be monotonically increasing.
 * <p>
 * The index supports timestamp lookup for a memory map of this file. The lookup is done using a binary search to find
 * the offset of the message whose indexed timestamp is closest but smaller or equals to the target timestamp.
 * <p>
 * Time index files can be opened in two ways: either as an empty, mutable index that allows appending or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * <p>
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 */
// Avoid shadowing mutable file in AbstractIndex
public class TimeIndex extends AbstractIndex {
    private static final Logger LOG = LoggerFactory.getLogger(TimeIndex.class);

    private volatile TimestampOffset _lastEntry;


    public TimeIndex(File _file,
                     Long baseOffset,
                     Integer maxIndexSize,
                     Boolean writable) throws IOException {
        super(_file, baseOffset, maxIndexSize, writable);

        this._lastEntry = lastEntryFromIndexFile();

        LOG.debug("Loaded index file {} with maxEntries = {}, maxIndexSize = {}, entries = {}, lastOffset = {}, file position = {}",
                file().getAbsolutePath(), maxEntries(), maxIndexSize, _entries, _lastEntry, mmap.position());
    }


    @Override
    public Integer entrySize() {
        return 12;
    }

    // We override the full check to reserve the last time index entry slot for the on roll call.
    @Override
    public Boolean isFull() {
        return entries() >= maxEntries() - 1;
    }

    private Long timestamp(ByteBuffer buffer, Integer n) {
        return buffer.getLong(n * entrySize());
    }

    private Integer relativeOffset(ByteBuffer buffer, Integer n) {
        return buffer.getInt(n * entrySize() + 8);
    }

    public TimestampOffset lastEntry() {
        return _lastEntry;
    }

    /**
     * Read the last entry from the index file. This operation involves disk access.
     */
    private TimestampOffset lastEntryFromIndexFile() {
        return CoreUtils.inLock(lock, new Supplier<TimestampOffset>() {
            @Override
            public TimestampOffset get() {
                if (_entries == 0) {
                    return new TimestampOffset(RecordBatch.NO_TIMESTAMP, getBaseOffset());
                } else {
                    return parseEntry(mmap, _entries - 1);
                }
            }
        });
    }

    /**
     * Get the nth timestamp mapping from the time index
     *
     * @param n The entry number in the time index
     * @return The timestamp/offset pair at that entry
     */
    public TimestampOffset entry(Integer n) {
        return maybeLock(lock, new Supplier<TimestampOffset>() {
            @Override
            public TimestampOffset get() {
                if (n >= _entries) {
                    String msg = String.format("Attempt to fetch the %sth entry from  time index %s " +
                            "which has size %s.", n, file().getAbsolutePath(), _entries);
                    throw new IllegalArgumentException(msg);
                }
                return parseEntry(mmap, n);
            }
        });
    }

    @Override
    public TimestampOffset parseEntry(ByteBuffer buffer, Integer n) {
        return new TimestampOffset(timestamp(buffer, n), getBaseOffset() + relativeOffset(buffer, n));
    }

    /**
     * Attempt to append a time index entry to the time index.
     * The new entry is appended only if both the timestamp and offset are greater than the last appended timestamp and
     * the last appended offset.
     *
     * @param timestamp     The timestamp of the new time index entry
     * @param offset        The offset of the new time index entry
     * @param skipFullCheck To skip checking whether the segment is full or not. We only skip the check when the segment
     *                      gets rolled or the segment is closed.
     */
    public void maybeAppend(Long timestamp, Long offset, Boolean skipFullCheck) {
        lock.lock();
        try {
            if (!skipFullCheck) {
                assert !isFull() : "Attempt to append to a full time index (size = " + _entries + ").";
            }
            // We do not throw exception when the offset equals to the offset of last entry. That means we are trying
            // to insert the same time index entry as the last entry.
            // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion
            // because that could happen in the following two scenarios:
            // 1. A log segment is closed.
            // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled.
            if (_entries != 0 && offset < lastEntry().getOffset()) {
                String msg = String.format("Attempt to append an offset (%s) to slot %s no larger than" +
                        " the last offset appended (%s) to %s.", offset, _entries, lastEntry().getOffset(), file().getAbsolutePath());
                throw new InvalidOffsetException(msg);
            }
            if (_entries != 0 && timestamp < lastEntry().getTimestamp()) {
                String msg = String.format("Attempt to append a timestamp (%s) to slot %s no larger" +
                        " than the last timestamp appended (%s) to %s.", timestamp, _entries, lastEntry().getTimestamp(), file().getAbsolutePath());
                throw new IllegalStateException(msg);
            }
            // We only append to the time index when the timestamp is greater than the last inserted timestamp.
            // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time
            // index will be empty.
            if (timestamp > lastEntry().getTimestamp()) {
                LOG.trace("Adding index entry {} => {} to {}.", timestamp, offset, file().getAbsolutePath());
                mmap.putLong(timestamp);
                mmap.putInt(relativeOffset(offset));
                _entries += 1;
                _lastEntry = new TimestampOffset(timestamp, offset);
                String msg = String.format("%s entries but file position in index is %s.", _entries, mmap.position());
                assert _entries * entrySize() == mmap.position() : msg;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find the time index entry whose timestamp is less than or equal to the given timestamp.
     * If the target timestamp is smaller than the least timestamp in the time index, (NoTimestamp, baseOffset) is
     * returned.
     *
     * @param targetTimestamp The timestamp to look up.
     * @return The time index entry found.
     */
    public TimestampOffset lookup(Long targetTimestamp) {
        return maybeLock(lock, new Supplier<TimestampOffset>() {
            @Override
            public TimestampOffset get() {
                ByteBuffer idx = mmap.duplicate();
                int slot = largestLowerBoundSlotFor(idx, targetTimestamp, IndexSearchType.KEY);
                if (slot == -1) {
                    return new TimestampOffset(RecordBatch.NO_TIMESTAMP, getBaseOffset());
                } else {
                    return parseEntry(idx, slot);
                }
            }
        });
    }

    @Override
    public void truncate() {
        truncateToEntries(0);
    }

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    @Override
    public void truncateTo(Long offset) {
        CoreUtils.inLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                ByteBuffer idx = mmap.duplicate();
                int slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.VALUE);

                /* There are 3 cases for choosing the new size
                 * 1) if there is no entry in the index <= the offset, delete everything
                 * 2) if there is an entry for this exact offset, delete it and everything larger than it
                 * 3) if there is no entry for this offset, delete everything larger than the next smallest
                 */
                int newEntries;
                if (slot < 0) {
                    newEntries = 0;
                } else if (relativeOffset(idx, slot) == offset - getBaseOffset()) {
                    newEntries = slot;
                } else {
                    newEntries = slot + 1;
                }
                truncateToEntries(newEntries);
                return null;
            }
        });
    }

    @Override
    public Boolean resize(Integer newSize) throws IOException {
        return CoreUtils.inLockWithIOException(lock, new SupplierWithIOException<Boolean>() {
            @Override
            public Boolean get() throws IOException {
                return innerResize(newSize);
            }
        });
    }

    private Boolean innerResize(Integer newSize) throws IOException {
        if (super.resize(newSize)) {
            _lastEntry = lastEntryFromIndexFile();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(Integer entries) {
        CoreUtils.inLock(lock, new Supplier<Void>() {
            @Override
            public Void get()  {
                _entries = entries;
                mmap.position(_entries * entrySize());
                _lastEntry = lastEntryFromIndexFile();
                LOG.debug("Truncated index {} to {} entries; position is now {} and last entry is now {}",
                        file().getAbsolutePath(), entries, mmap.position(), _lastEntry);
                return null;
            }
        });
    }

    @Override
    public void sanityCheck() {
        long lastTimestamp = lastEntry().getTimestamp();
        long lastOffset = lastEntry().getOffset();
        if (_entries != 0 && lastTimestamp < timestamp(mmap, 0)) {
            String msg = String.format("Corrupt time index found, time index file (%s) has " +
                            "non-zero size but the last timestamp is %s which is less than the first timestamp %s",
                    file().getAbsolutePath(), lastTimestamp, timestamp(mmap, 0));
            throw new CorruptIndexException(msg);
        }
        if (_entries != 0 && lastOffset < getBaseOffset()) {
            String msg = String.format("Corrupt time index found, time index file (%s) has " +
                            "non-zero size but the last offset is %s which is less than the first offset %s",
                    file().getAbsolutePath(), lastOffset, getBaseOffset());
            throw new CorruptIndexException(msg);
        }
        if (length() % entrySize() != 0) {
            String msg = String.format("Time index file %s is corrupt, found %s bytes " +
                            "which is neither positive nor a multiple of %s.",
                    file().getAbsolutePath(), length(), entrySize());
            throw new CorruptIndexException(msg);
        }
    }
}
