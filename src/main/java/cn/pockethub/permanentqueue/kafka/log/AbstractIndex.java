package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.IndexOffsetOverflowException;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import cn.pockethub.permanentqueue.kafka.utils.Logging;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.OperatingSystem;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The abstract index class which holds entry format agnostic methods.
 */
@Getter
public abstract class AbstractIndex extends Logging {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractIndex.class);

    private volatile File _file;
    private Long baseOffset;
    private Integer maxIndexSize;
    private Boolean writable;

    // Length of the index file
    private volatile Long _length = null;

    protected ReentrantLock lock = new ReentrantLock();

    protected volatile MappedByteBuffer mmap;

    /**
     * The maximum number of entries this index can hold
     */
    protected volatile Integer _maxEntries;

    /**
     * The number of entries in this index
     */
    protected volatile Integer _entries;

    /**
     * @param _file        The index file
     * @param baseOffset   the base offset of the segment that this index is corresponding to.
     * @param maxIndexSize The maximum index size in bytes.
     * @param writable
     */
    public AbstractIndex(File _file,
                         Long baseOffset,
                         Integer maxIndexSize,
                         Boolean writable) throws IOException {
        this._file = _file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;
        this.writable = writable;

        this.mmap = initMMap();
        this._maxEntries = mmap.limit() / entrySize();
        this._entries = mmap.position() / entrySize();

    }

    protected abstract Integer entrySize();

    private MappedByteBuffer initMMap() throws IOException {
        boolean newlyCreated = file().createNewFile();
        RandomAccessFile raf = (writable) ? new RandomAccessFile(file(), "rw") : new RandomAccessFile(file(), "r");
        try {
            /* pre-allocate the file if necessary */
            if (newlyCreated) {
                if (maxIndexSize < entrySize()) {
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                }
                raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize()));
            }

            /* memory-map the file */
            _length = raf.length();
            MappedByteBuffer idx;
            if (writable) {
                idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, _length);
            } else {
                idx = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, _length);
            }
            /* set the position in the index for the next entry */
            if (newlyCreated) {
                idx.position(0);
            } else {
                // if this is a pre-existing index, assume it is valid and set position to last entry
                idx.position(roundDownToExactMultiple(idx.limit(), entrySize()));
            }
            return idx;
        } finally {
            CoreUtils.swallow(raf::close, this);
        }
    }

    /*
   Kafka mmaps index files into memory, and all the read / write operations of the index is through OS page cache. This
   avoids blocked disk I/O in most cases.

   To the extent of our knowledge, all the modern operating systems use LRU policy or its variants to manage page
   cache. Kafka always appends to the end of the index file, and almost all the index lookups (typically from in-sync
   followers or consumers) are very close to the end of the index. So, the LRU cache replacement policy should work very
   well with Kafka's index access pattern.

   However, when looking up index, the standard binary search algorithm is not cache friendly, and can cause unnecessary
   page faults (the thread is blocked to wait for reading some index entries from hard disk, as those entries are not
   cached in the page cache).

   For example, in an index with 13 pages, to lookup an entry in the last page (page #12), the standard binary search
   algorithm will read index entries in page #0, 6, 9, 11, and 12.
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12 |
   steps:       |1| | | | | |3| | |4|  |5 |2/6|
   In each page, there are hundreds log entries, corresponding to hundreds to thousands of kafka messages. When the
   index gradually growing from the 1st entry in page #12 to the last entry in page #12, all the write (append)
   operations are in page #12, and all the in-sync follower / consumer lookups read page #0,6,9,11,12. As these pages
   are always used in each in-sync lookup, we can assume these pages are fairly recently used, and are very likely to be
   in the page cache. When the index grows to page #13, the pages needed in a in-sync lookup change to #0, 7, 10, 12,
   and 13:
   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13 |
   steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|
   Page #7 and page #10 have not been used for a very long time. They are much less likely to be in the page cache, than
   the other pages. The 1st lookup, after the 1st index entry in page #13 is appended, is likely to have to read page #7
   and page #10 from disk (page fault), which can take up to more than a second. In our test, this can cause the
   at-least-once produce latency to jump to about 1 second from a few ms.

   Here, we use a more cache-friendly lookup algorithm:
   if (target > indexEntry[end - N]) // if the target is in the last N entries of the index
      binarySearch(end - N, end)
   else
      binarySearch(begin, end - N)

   If possible, we only look up in the last N entries of the index. By choosing a proper constant N, all the in-sync
   lookups should go to the 1st branch. We call the last N entries the "warm" section. As we frequently look up in this
   relatively small section, the pages containing this section are more likely to be in the page cache.

   We set N (_warmEntries) to 8192, because
   1. This number is small enough to guarantee all the pages of the "warm" section is touched in every warm-section
      lookup. So that, the entire warm section is really "warm".
      When doing warm-section lookup, following 3 entries are always touched: indexEntry(end), indexEntry(end-N),
      and indexEntry((end*2 -N)/2). If page size >= 4096, all the warm-section pages (3 or fewer) are touched, when we
      touch those 3 entries. As of 2018, 4096 is the smallest page size for all the processors (x86-32, x86-64, MIPS,
      SPARC, Power, ARM etc.).
   2. This number is large enough to guarantee most of the in-sync lookups are in the warm-section. With default Kafka
      settings, 8KB index corresponds to about 4MB (offset index) or 2.7MB (time index) log messages.

   We can't set make N (_warmEntries) to be larger than 8192, as there is no simple way to guarantee all the "warm"
   section pages are really warm (touched in every lookup) on a typical 4KB-page host.

   In there future, we may use a backend thread to periodically touch the entire warm section. So that, we can
   1) support larger warm section
   2) make sure the warm section of low QPS topic-partitions are really warm.
 */
    protected Integer _warmEntries() {
        return 8192 / entrySize();
    }

    /**
     * True iff there are no more slots available in this index
     */
    public Boolean isFull() {
        return _entries >= _maxEntries;
    }

    public File file() {
        return _file;
    }

    public Integer maxEntries() {
        return _maxEntries;
    }

    public Integer entries() {
        return _entries;
    }

    public Long length() {
        return _length;
    }

    public void updateParentDir(File parentDir) {
        _file = new File(parentDir, file().getName());
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     *
     * @param newSize new size of the index file
     * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
     */
    public Boolean resize(Integer newSize) throws IOException {
        return CoreUtils.inLockWithIOException(lock, () -> resizeInner(newSize));
    }

    private Boolean resizeInner(Integer newSize) throws IOException {
        int roundedNewSize = roundDownToExactMultiple(newSize, entrySize());

        if (_length == roundedNewSize) {
            LOG.debug("Index {} was not resized because it already has size {}", file().getAbsolutePath(), roundedNewSize);
            return false;
        } else {
            RandomAccessFile raf = new RandomAccessFile(file(), "rw");
            try {
                int position = mmap.position();

                /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
                if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS) {
                    safeForceUnmap();
                }
                raf.setLength(roundedNewSize);
                _length = (long) roundedNewSize;
                mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                _maxEntries = mmap.limit() / entrySize();
                mmap.position(position);
                LOG.debug("Resized {} to {}, position is {} and limit is {}",
                        file().getAbsolutePath(), roundedNewSize, mmap.position(), mmap.limit());
                return true;
            } finally {
                CoreUtils.swallow(raf::close, this);
            }
        }
    }

    /**
     * Rename the file that backs this offset index
     *
     * @throws IOException if rename fails
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file().toPath(), f.toPath(), false);
        } finally {
            _file = f;
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        CoreUtils.inLock(lock, () -> mmap.force());
    }

    /**
     * Delete this index file.
     *
     * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
     * not exist
     * @throws IOException if deletion fails due to an I/O error
     */
    public Boolean deleteIfExists() throws IOException {
        closeHandler();
        return Files.deleteIfExists(file().toPath());
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() throws IOException {
        CoreUtils.inLockWithIOException(lock, () -> resize(entrySize() * _entries));
    }

    /**
     * The number of bytes actually used by this index
     */
    public Integer sizeInBytes() {
        return entrySize() * _entries;
    }

    /**
     * Close the index
     */
    public void close() throws IOException {
        trimToValidSize();
        closeHandler();
    }

    public void closeHandler()  {
        // On JVM, a memory mapping is typically unmapped by garbage collector.
        // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
        // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
        // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
        CoreUtils.inLock(lock, () -> {
            safeForceUnmap();
            return null;
        });
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems
     *
     * @throws CorruptIndexException if any problems are found
     */
    public abstract void sanityCheck() throws CorruptIndexException;

    /**
     * Remove all the entries from the index.
     */
    protected abstract void truncate();

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public abstract void truncateTo(Long offset);

    /**
     * Remove all the entries from the index and resize the index to the max index size.
     */
    public void reset() throws IOException {
        truncate();
        resize(maxIndexSize);
    }

    /**
     * Get offset relative to base offset of this index
     *
     * @throws IndexOffsetOverflowException
     */
    public Integer relativeOffset(Long offset) {
        Optional<Integer> relativeOffset = toRelative(offset);
        if (!relativeOffset.isPresent()) {
            String msg = String.format("Integer overflow for offset: %s (%s)", offset, file().getAbsoluteFile());
            throw new IndexOffsetOverflowException(msg);
        }
        return relativeOffset.get();
    }

    /**
     * Check if a particular offset is valid to be appended to this index.
     *
     * @param offset The offset to check
     * @return true if this offset is valid to be appended to this index; false otherwise
     */
    public Boolean canAppendOffset(Long offset) {
        return toRelative(offset).isPresent();
    }

    protected void safeForceUnmap() {
        if (mmap != null) {
            try {
                forceUnmap();
            } catch (Throwable t) {
                LOG.error("Error unmapping index {}", file(), t);
            }
        }
    }

    /**
     * Forcefully free the buffer's mmap.
     */
    protected void forceUnmap() throws IOException {
        try {
            ByteBufferUnmapper.unmap(file().getAbsolutePath(), mmap);
        } finally {
            // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
            mmap = null;
        }
    }

    /**
     * Execute the given function in a lock only if we are running on windows or z/OS. We do this
     * because Windows or z/OS won't let us resize a file while it is mmapped. As a result we have to force unmap it
     * and this requires synchronizing reads.
     */
    protected <T> T maybeLock(Lock lock, java.util.function.Supplier<T> fun) {
        if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS) {
            lock.lock();
        }
        try {
            return fun.get();
        } finally {
            if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS) {
                lock.unlock();
            }
        }
    }

    /**
     * To parse an entry in the index.
     *
     * @param buffer the buffer of this memory mapped index.
     * @param n      the slot
     * @return the index entry stored in the given slot.
     */
    protected abstract IndexEntry parseEntry(ByteBuffer buffer, Integer n);

    /**
     * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
     * The comparison is made using the `IndexEntry.compareTo()` method.
     *
     * @param idx    The index buffer
     * @param target The index key to look for
     * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
     */
    protected Integer largestLowerBoundSlotFor(ByteBuffer idx, Long target, IndexSearchType searchEntity) {
        return indexSlotRangeFor(idx, target, searchEntity).getKey();
    }

    /**
     * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
     */
    protected Integer smallestUpperBoundSlotFor(ByteBuffer idx, Long target, IndexSearchType searchEntity) {
        return indexSlotRangeFor(idx, target, searchEntity).getValue();
    }

    /**
     * Lookup lower and upper bounds for the given target.
     */
    private Pair<Integer, Integer> indexSlotRangeFor(ByteBuffer idx, Long target, IndexSearchType searchEntity) {
        // check if the index is empty
        if (_entries == 0) {
            return Pair.of(-1, -1);
        }


        int firstHotEntry = Math.max(0, _entries - 1 - _warmEntries());
        // check if the target offset is in the warm section of the index
        if (compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
            return binarySearch(firstHotEntry, _entries - 1, idx, target, searchEntity);
        }

        // check if the target offset is smaller than the least offset
        if (compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0) {
            return Pair.of(-1, 0);
        }

        return binarySearch(0, firstHotEntry, idx, target, searchEntity);
    }

    private Pair<Integer, Integer> binarySearch(Integer begin, Integer end, ByteBuffer idx, Long target, IndexSearchType searchEntity) {
        // binary search for the entry
        int lo = begin;
        int hi = end;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            IndexEntry found = parseEntry(idx, mid);
            int compareResult = compareIndexEntry(found, target, searchEntity);
            if (compareResult > 0) {
                hi = mid - 1;
            } else if (compareResult < 0) {
                lo = mid;
            } else {
                return Pair.of(mid, mid);
            }
        }
        return Pair.of(lo, (lo == _entries - 1) ? -1 : lo + 1);
    }

    private Integer compareIndexEntry(IndexEntry indexEntry, Long target, IndexSearchType searchEntity) {
        switch (searchEntity) {
            case KEY:
                return Long.compare(indexEntry.indexKey(), target);
            case VALUE:
                return Long.compare(indexEntry.indexValue(), target);
            default:
                return null;
        }
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundDownToExactMultiple(67, 8) == 64
     */
    private Integer roundDownToExactMultiple(Integer number, Integer factor) {
        return factor * (number / factor);
    }

    private Optional<Integer> toRelative(Long offset) {
        Long relativeOffset = offset - baseOffset;
        if (relativeOffset < 0 || relativeOffset > Integer.MAX_VALUE) {
            return Optional.empty();
        } else {
            return Optional.of(relativeOffset.intValue());
        }
    }

    public enum IndexSearchType {
        KEY, VALUE;
    }

}
