package cn.pockethub.permanentqueue.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

/**
 * A file lock a la flock/funlock
 * <p/>
 * The given path will be created and opened if it doesn't exist.
 */
public class FileLock {
    private static final Logger LOG = LoggerFactory.getLogger(FileLock.class);

    private final File file;
    private final FileChannel channel;
    private java.nio.channels.FileLock flock = null;

    public FileLock(final File file) throws IOException {
        file.createNewFile(); // create the file if it doesn't exist

        this.file = file;
        this.channel = new RandomAccessFile(file, "rw").getChannel();
    }

    /**
     * Lock the file or throw an exception if the lock is already held
     */
    public void lock() throws IOException {
        synchronized (this) {
            LOG.trace("Acquiring lock on " + file.getAbsolutePath());
            flock = channel.lock();
        }
    }

    /**
     * Try to lock the file and return true if the locking succeeds
     */
    public boolean tryLock() throws IOException {
        synchronized (this) {
            LOG.trace("Acquiring lock on " + file.getAbsolutePath());
            try {
                // weirdly this method will return null if the lock is held by another
                // process, but will throw an exception if the lock is held by this process
                // so we have to handle both cases
                flock = channel.tryLock();
                return flock != null;
            } catch (OverlappingFileLockException e) {
                return false;
            }
        }
    }

    /**
     * Unlock the lock if it is held
     */
    public void unlock() throws IOException {
        synchronized (this) {
            LOG.trace("Releasing lock on " + file.getAbsolutePath());
            if (flock != null) {
                flock.release();
            }
        }
    }

    /**
     * Destroy this lock, closing the associated FileChannel
     */
    public void destroy() throws IOException {
        synchronized (this) {
            unlock();
            channel.close();
        }
    }

    public File getFile() {
        return file;
    }
}
