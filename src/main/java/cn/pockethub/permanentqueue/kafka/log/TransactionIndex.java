package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.function.Iterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static cn.pockethub.permanentqueue.kafka.log.AbortedTxn.CurrentVersion;

/**
 * The transaction index maintains metadata about the aborted transactions for each segment. This includes
 * the start and end offsets for the aborted transactions and the last stable offset (LSO) at the time of
 * the abort. This index is used to find the aborted transactions in the range of a given fetch request at
 * the READ_COMMITTED isolation level.
 * <p>
 * There is at most one transaction index for each log segment. The entries correspond to the transactions
 * whose commit markers were written in the corresponding log segment. Note, however, that individual transactions
 * may span multiple segments. Recovering the index therefore requires scanning the earlier segments in
 * order to find the start of the transactions.
 */
//@nonthreadsafe
public class TransactionIndex {

    private Long startOffset;
    private volatile File _file;

    // note that the file is not created until we need it
    private volatile Optional<FileChannel> maybeChannel = Optional.empty();
    private Optional<Long> lastOffset = Optional.empty();

    public TransactionIndex(Long startOffset, File _file) throws IOException {
        this.startOffset = startOffset;
        this._file = _file;

        if (_file.exists()) {
            openChannel();
        }
    }

    public void append(AbortedTxn abortedTxn) throws IOException {
        lastOffset.ifPresent(offset -> {
            if (offset >= abortedTxn.lastOffset()) {
                String msg = String.format("The last offset of appended transactions must increase sequentially, but " +
                                "%s is not greater than current last offset %s of index %s",
                        abortedTxn.lastOffset(), offset, file().getAbsolutePath());
                throw new IllegalArgumentException(msg);
            }
        });
        lastOffset = Optional.of(abortedTxn.lastOffset());
        Utils.writeFully(channel(), abortedTxn.getBuffer().duplicate());
    }

    public void flush() throws IOException{
        if (maybeChannel.isPresent()) {
            maybeChannel.get().force(true);
        }
    }

    public File file() {
        return _file;
    }

    public void updateParentDir(File parentDir) {
        _file = new File(parentDir, file().getName());
    }

    /**
     * Delete this index.
     *
     * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
     * not exist
     * @throws IOException if deletion fails due to an I/O error
     */
    public Boolean deleteIfExists() throws IOException {
        close();
        return Files.deleteIfExists(file().toPath());
    }

    private FileChannel channel() throws IOException {
        if (maybeChannel.isPresent()) {
            return maybeChannel.get();
        } else {
            return openChannel();
        }
    }

    private FileChannel openChannel() throws IOException {
        FileChannel channel = FileChannel.open(file().toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        maybeChannel = Optional.of(channel);
        channel.position(channel.size());
        return channel;
    }

    /**
     * Remove all the entries from the index. Unlike `AbstractIndex`, this index is not resized ahead of time.
     */
    public void reset() throws IOException {
        if (maybeChannel.isPresent()) {
            maybeChannel.get().truncate(0);
        }
        lastOffset = Optional.empty();
    }

    public void close() throws IOException {
        if (maybeChannel.isPresent()) {
            maybeChannel.get().close();
        }
        maybeChannel = Optional.empty();
    }

    public void renameTo(File f) throws IOException {
        try {
            if (file().exists()) {
                Utils.atomicMoveWithFallback(file().toPath(), f.toPath(), false);
            }
        } finally {
            _file = f;
        }
    }

    public void truncateTo(Long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(AbortedTxn.TotalSize);
        Optional<Long> newLastOffset = Optional.empty();
        Iterator<Pair<AbortedTxn, Integer>> iterator = iterator(() -> buffer);
        while (iterator.hasNext()) {
            Pair<AbortedTxn, Integer> next = iterator.next();
            AbortedTxn abortedTxn = next.getKey();
            Integer position = next.getValue();
            if (abortedTxn.lastOffset() >= offset) {
                channel().truncate(position);
                lastOffset = newLastOffset;
                return;
            }
            newLastOffset = Optional.of(abortedTxn.lastOffset());
        }
    }

    private Iterator<Pair<AbortedTxn, Integer>> iterator() {
        return iterator(() -> ByteBuffer.allocate(AbortedTxn.TotalSize));
    }

    private Iterator<Pair<AbortedTxn, Integer>> iterator(final Supplier<ByteBuffer> allocate) {
        if (maybeChannel.isPresent()) {
            FileChannel channel = maybeChannel.get();
            final int[] positionArr = new int[]{0};

            return new Iterator<Pair<AbortedTxn, Integer>>() {
                @Override
                public boolean hasNext()throws IOException {
                    return channel.position() - positionArr[0] >= AbortedTxn.TotalSize;
                }

                @Override
                public Pair<AbortedTxn, Integer> next() {
                    try {
                        ByteBuffer buffer = allocate.get();
                        Utils.readFully(channel, buffer, positionArr[0]);
                        buffer.flip();

                        AbortedTxn abortedTxn = new AbortedTxn(buffer);
                        if (abortedTxn.version() > CurrentVersion) {
                            String msg = String.format("Unexpected aborted transaction version %s in transaction index %s, current version is %s",
                                    abortedTxn.version(), file().getAbsolutePath(), CurrentVersion);
                            throw new KafkaException(msg);
                        }
                        Pair<AbortedTxn, Integer> nextEntry = Pair.of(abortedTxn, positionArr[0]);
                        positionArr[0] += AbortedTxn.TotalSize;
                        return nextEntry;
                    } catch (IOException e) {
                        // We received an unexpected error reading from the index file. We propagate this as an
                        // UNKNOWN error to the consumer, which will cause it to retry the fetch.
                        String msg = String.format("Failed to read from the transaction index %s", file().getAbsolutePath());
                        throw new KafkaException(msg, e);
                    }
                }
            };
        } else {
            return new Iterator<Pair<AbortedTxn, Integer>>(){
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Pair<AbortedTxn, Integer> next() {
                    return null;
                }
            };
        }
    }

    public List<AbortedTxn> allAbortedTxns()throws Throwable {
        List<AbortedTxn> abortedTxns = new ArrayList<>();
        Iterator<Pair<AbortedTxn, Integer>> iterator = iterator(null);
        while (iterator.hasNext()) {
            abortedTxns.add(iterator.next().getKey());
        }
        return abortedTxns;
    }

    /**
     * Collect all aborted transactions which overlap with a given fetch range.
     *
     * @param fetchOffset      Inclusive first offset of the fetch range
     * @param upperBoundOffset Exclusive last offset in the fetch range
     * @return An object containing the aborted transactions and whether the search needs to continue
     * into the next log segment.
     */
    public TxnIndexSearchResult collectAbortedTxns(Long fetchOffset, Long upperBoundOffset) throws IOException{
        List<AbortedTxn> abortedTransactions = new ArrayList<>();
        Iterator<Pair<AbortedTxn, Integer>> iterator = iterator(null);
        while (iterator.hasNext()) {
            AbortedTxn abortedTxn = iterator.next().getKey();
            if (abortedTxn.lastOffset() >= fetchOffset && abortedTxn.firstOffset() < upperBoundOffset) {
                abortedTransactions.add(abortedTxn);
            }

            if (abortedTxn.lastStableOffset() >= upperBoundOffset) {
                return new TxnIndexSearchResult(abortedTransactions, true);
            }
        }
        return new TxnIndexSearchResult(abortedTransactions, false);
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems.
     *
     * @throws CorruptIndexException if any problems are found.
     */
    public void sanityCheck() throws CorruptIndexException,IOException{
        ByteBuffer buffer = ByteBuffer.allocate(AbortedTxn.TotalSize);
        Iterator<Pair<AbortedTxn, Integer>> iterator = iterator(() -> buffer);
        while (iterator.hasNext()) {
            AbortedTxn abortedTxn = iterator.next().getKey();
            if (abortedTxn.lastOffset() < startOffset) {
                String msg = String.format("Last offset of aborted transaction %s in index %s is less than start offset %s",
                        abortedTxn, file().getAbsolutePath(), startOffset);
                throw new CorruptIndexException(msg);
            }
        }
    }

}
