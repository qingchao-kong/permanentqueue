package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.function.FunctionWithIOException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import lombok.Getter;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A wrapper over an `AbstractIndex` instance that provides a mechanism to defer loading
 * (i.e. memory mapping) the underlying index until it is accessed for the first time via the
 * `get` method.
 * <p>
 * In addition, this class exposes a number of methods (e.g. updateParentDir, renameTo, close,
 * etc.) that provide the desired behavior without causing the index to be loaded. If the index
 * had previously been loaded, the methods in this class simply delegate to the relevant method in
 * the index.
 * <p>
 * This is an important optimization with regards to broker start-up and shutdown time if it has a
 * large number of segments.
 * <p>
 * Methods of this class are thread safe. Make sure to check `AbstractIndex` subclasses
 * documentation to establish their thread safety.
 */
//@threadsafe
public class LazyIndex<T extends AbstractIndex> {
    private volatile IndexWrapper indexWrapper;
    private FunctionWithIOException<File, T> loadIndex;

    private ReentrantLock lock = new ReentrantLock();

    /**
     * @param indexWrapper
     * @param loadIndex    A function that takes a `File` pointing to an index and returns a loaded
     *                     `AbstractIndex` instance.
     */
    private LazyIndex(IndexWrapper indexWrapper, FunctionWithIOException<File, T> loadIndex) {
        this.indexWrapper = indexWrapper;
        this.loadIndex = loadIndex;
    }

    public File file() {
        return indexWrapper.file();
    }

    public T get() throws IOException {
        if (indexWrapper instanceof IndexValue) {
            IndexValue indexValue = (IndexValue) indexWrapper;
            return (T) indexValue.getIndex();
        } else if (indexWrapper instanceof IndexFile) {
            return CoreUtils.inLockWithIOException(lock, new SupplierWithIOException<T>() {
                        @Override
                        public T get() throws IOException {
                            if (indexWrapper instanceof IndexValue) {
                                IndexValue indexValue = (IndexValue) indexWrapper;
                                return (T) indexValue.getIndex();
                            } else if (indexWrapper instanceof IndexFile) {
                                IndexFile indexFile = (IndexFile) indexWrapper;
                                IndexValue indexValue = new IndexValue(loadIndex.apply(indexFile.file()));
                                indexWrapper = indexValue;
                                return (T) indexValue.getIndex();
                            } else {
                                return null;
                            }
                        }
                    }
            );
        } else {
            return null;
        }
    }

    public void updateParentDir(File parentDir) {
        CoreUtils.inLock(lock, () -> {
            indexWrapper.updateParentDir(parentDir);
            return null;
        });
    }

    public void renameTo(File f) throws IOException {
        CoreUtils.inLockWithIOException(lock, () -> {
            indexWrapper.renameTo(f);
            return null;
        });
    }

    public Boolean deleteIfExists() throws IOException {
        return CoreUtils.inLockWithIOException(lock, () -> indexWrapper.deleteIfExists());
    }

    public void close() throws Throwable {
        CoreUtils.inLockWithThrowable(lock, () -> {
            indexWrapper.close();
            return null;
        });
    }

    public void closeHandler() throws Throwable {
        CoreUtils.inLockWithThrowable(lock, () -> {
            indexWrapper.closeHandler();
            return null;
        });
    }

    public static LazyIndex<OffsetIndex> forOffset(File file, Long baseOffset, Integer maxIndexSize, Boolean writable) {
        return new LazyIndex(new IndexFile(file), new FunctionWithIOException<File, AbstractIndex>() {
            @Override
            public AbstractIndex apply(File file) throws IOException {
                return new OffsetIndex(file, baseOffset, maxIndexSize, writable);
            }
        });
    }

    public static LazyIndex<TimeIndex> forTime(File file, Long baseOffset, Integer maxIndexSize, Boolean writable) {
        return new LazyIndex(new IndexFile(file), new FunctionWithIOException<File, AbstractIndex>() {
            @Override
            public AbstractIndex apply(File file) throws IOException {
                return new TimeIndex(file, baseOffset, maxIndexSize, writable);
            }
        });
    }

    private interface IndexWrapper {

        File file();

        void updateParentDir(File f);

        void renameTo(File f) throws IOException;

        Boolean deleteIfExists() throws IOException;

        void close() throws IOException;

        void closeHandler();

    }

    static class IndexFile implements IndexWrapper {
        private volatile File _file;

        public IndexFile(File _file) {
            this._file = _file;
        }

        @Override
        public File file() {
            return _file;
        }

        @Override
        public void updateParentDir(File parentDir) {
            _file = new File(parentDir, file().getName());
        }

        @Override
        public void renameTo(File f) throws IOException {
            try {
                Utils.atomicMoveWithFallback(file().toPath(), f.toPath(), false);
            } catch (NoSuchFileException e) {
                if (file().exists()) {
                    throw e;
                }
            } finally {
                _file = f;
            }
        }

        @Override
        public Boolean deleteIfExists() throws IOException {
            return Files.deleteIfExists(file().toPath());
        }

        @Override
        public void close() {

        }

        @Override
        public void closeHandler() {

        }
    }

    @Getter
    class IndexValue<T extends AbstractIndex> implements IndexWrapper {
        private T index;

        public IndexValue(T index) {
            this.index = index;
        }

        @Override
        public File file() {
            return index.file();
        }

        @Override
        public void updateParentDir(File parentDir) {
            index.updateParentDir(parentDir);
        }

        @Override
        public void renameTo(File f) throws IOException {
            index.renameTo(f);
        }

        @Override
        public Boolean deleteIfExists() throws IOException {
            return index.deleteIfExists();
        }

        @Override
        public void close() throws IOException {
            index.close();
        }

        @Override
        public void closeHandler() {
            index.closeHandler();
        }
    }
}
