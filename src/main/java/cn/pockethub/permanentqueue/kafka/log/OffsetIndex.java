package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.IndexOffsetOverflowException;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * <p/>
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * <p/>
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * <p/>
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * <p/>
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * <p/>
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * <p/>
 * The frequency of entries is up to the user of this class.
 * <p/>
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public class OffsetIndex extends AbstractIndex {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetIndex.class);

    /* the last offset in the index */
    private Long _lastOffset = lastEntry().getOffset();

    public OffsetIndex(final File _file, final long baseOffset) throws IOException {
        this(_file, baseOffset, -1, true);
    }

    public OffsetIndex(final File _file, final long baseOffset, final int maxIndexSize, final Boolean writable) throws IOException {
        super(_file, baseOffset, maxIndexSize, writable);

        LOG.debug("Loaded index file {} with maxEntries = {}, maxIndexSize = {}, entries = {}, lastOffset = {}, file position = {}",
                file().getAbsolutePath(), maxEntries(), maxIndexSize, _entries, _lastOffset, mmap.position());
    }

    @Override
    public Integer entrySize() {
        return 8;
    }

    /**
     * The last entry in the index
     */
    private OffsetPosition lastEntry() {
        return CoreUtils.inLock(lock, new Supplier<OffsetPosition>() {
            @Override
            public OffsetPosition get() {
                if (_entries == 0) {
                    return new OffsetPosition(getBaseOffset(), 0);
                } else {
                    return parseEntry(mmap, _entries - 1);
                }
            }
        });
    }

    public Long lastOffset() {
        return _lastOffset;
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and its corresponding physical file position.
     *
     * @param targetOffset The offset to look up.
     * @return The offset found and the corresponding file position for this offset
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(Long targetOffset) {
        return maybeLock(lock, new Supplier<OffsetPosition>() {
            @Override
            public OffsetPosition get() {
                ByteBuffer idx = mmap.duplicate();
                int slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY);
                if (slot == -1) {
                    return new OffsetPosition(getBaseOffset(), 0);
                } else {
                    return parseEntry(idx, slot);
                }
            }
        });
    }

    /**
     * Find an upper bound offset for the given fetch starting position and size. This is an offset which
     * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
     * such offset.
     */
    public Optional<OffsetPosition> fetchUpperBoundOffset(OffsetPosition fetchOffset, Integer fetchSize) {
        return maybeLock(lock, new Supplier<Optional<OffsetPosition>>() {
            @Override
            public Optional<OffsetPosition> get() {
                ByteBuffer idx = mmap.duplicate();
                int slot = smallestUpperBoundSlotFor(idx, (long) (fetchOffset.getPosition() + fetchSize), IndexSearchType.VALUE);
                if (slot == -1) {
                    return Optional.empty();
                } else {
                    return Optional.of(parseEntry(idx, slot));
                }
            }
        });
    }

    private Integer relativeOffset(ByteBuffer buffer, Integer n) {
        return buffer.getInt(n * entrySize());
    }

    private Integer physical(ByteBuffer buffer, Integer n) {
        return buffer.getInt(n * entrySize() + 4);
    }

    @Override
    protected OffsetPosition parseEntry(ByteBuffer buffer, Integer n) {
        return new OffsetPosition(getBaseOffset() + relativeOffset(buffer, n), physical(buffer, n));
    }

    /**
     * Get the nth offset mapping from the index
     *
     * @param n The entry number in the index
     * @return The offset/position pair at that entry
     */
    public OffsetPosition entry(Integer n) {
        return maybeLock(lock, new Supplier<OffsetPosition>() {
            @Override
            public OffsetPosition get() {
                if (n >= _entries) {
                    String msg = String.format("Attempt to fetch the %sth entry from index %s, which has size %s.",
                            n, file().getAbsolutePath(), _entries);
                    throw new IllegalArgumentException(msg);
                }
                return parseEntry(mmap, n);
            }
        });
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     *
     * @throws IndexOffsetOverflowException if the offset causes index offset to overflow
     * @throws InvalidOffsetException       if provided offset is not larger than the last offset
     */
    public void append(Long offset, Integer position) {
        CoreUtils.inLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                assert !isFull() : "Attempt to append to a full index (size = " + _entries + ").";
                if (_entries == 0 || offset > _lastOffset) {
                    LOG.trace("Adding index entry {} => {} to {}", offset, position, file().getAbsolutePath());
                    mmap.putInt(relativeOffset(offset));
                    mmap.putInt(position);
                    _entries += 1;
                    _lastOffset = offset;
                    String msg = String.format("%s entries but file position in index is %s.", entries(), mmap.position());
                    assert _entries * entrySize() == mmap.position() : msg;
                } else {
                    String msg = String.format("Attempt to append an offset (%s) to position %s no larger than" +
                                    " the last offset appended (%s) to %s.",
                            offset, entries(), _lastOffset, file().getAbsolutePath());
                    throw new InvalidOffsetException(msg);
                }
                return null;
            }
        });
    }

    @Override
    public void truncate() {
        truncateToEntries(0);
    }

    @Override
    public void truncateTo(Long offset) {
        CoreUtils.inLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                ByteBuffer idx = mmap.duplicate();
                int slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY);

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

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(Integer entries) {
        CoreUtils.inLock(lock, new Supplier<Void>() {
            @Override
            public Void get() {
                _entries = entries;
                mmap.position(_entries * entrySize());
                _lastOffset = lastEntry().getOffset();
                LOG.debug("Truncated index {} to {} entries; position is now {} and last offset is now {}",
                        file().getAbsolutePath(), entries, mmap.position(), _lastOffset);
                return null;
            }
        });
    }

    @Override
    public void sanityCheck() {
        if (_entries != 0 && _lastOffset < getBaseOffset()) {
            String msg = String.format("Corrupt index found, index file (%s) has non-zero size " +
                            "but the last offset is %s which is less than the base offset %s.",
                    file().getAbsolutePath(), _lastOffset, getBaseOffset());
            throw new CorruptIndexException(msg);
        }
        if (length() % entrySize() != 0) {
            String msg = String.format("Index file %s is corrupt, found %s bytes which is " +
                            "neither positive nor a multiple of %s.",
                    file().getAbsolutePath(), length(), entrySize());
            throw new CorruptIndexException(msg);
        }
    }
}
