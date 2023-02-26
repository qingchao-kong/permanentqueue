package cn.pockethub.permanentqueue.kafka.server.checkpoints;

import org.apache.kafka.common.utils.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class represents a utility to capture a checkpoint in a file. It writes down to the file in the below format.
 *
 * ========= File beginning =========
 * version: int
 * entries-count: int
 * entry-as-string-on-each-line
 * ========= File end ===============
 *
 * Each entry is represented as a string on each line in the checkpoint file. {@link EntryFormatter} is used
 * to convert the entry into a string and vice versa.
 *
 * @param <T> entry type.
 */
public class CheckpointFile<T> {

    private final int version;
    private final EntryFormatter<T> formatter;
    private final Object lock = new Object();
    private final Path absolutePath;
    private final Path tempPath;

    public CheckpointFile(File file,
                          int version,
                          EntryFormatter<T> formatter) throws IOException {
        this.version = version;
        this.formatter = formatter;
        try {
            // Create the file if it does not exist.
            Files.createFile(file.toPath());
        } catch (FileAlreadyExistsException ex) {
            // Ignore if file already exists.
        }
        absolutePath = file.toPath().toAbsolutePath();
        tempPath = Paths.get(absolutePath.toString() + ".tmp");
    }

    public void write(Collection<T> entries) throws IOException {
        synchronized (lock) {
            // write to temp file and then swap with the existing file
            try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
                // Write the version
                writer.write(Integer.toString(version));
                writer.newLine();

                // Write the entries count
                writer.write(Integer.toString(entries.size()));
                writer.newLine();

                // Write each entry on a new line.
                for (T entry : entries) {
                    writer.write(formatter.toString(entry));
                    writer.newLine();
                }

                writer.flush();
                fileOutputStream.getFD().sync();
            }

            Utils.atomicMoveWithFallback(tempPath, absolutePath);
        }
    }

    public List<T> read() throws IOException {
        synchronized (lock) {
            try (BufferedReader reader = Files.newBufferedReader(absolutePath)) {
                CheckpointReadBuffer<T> checkpointBuffer = new CheckpointReadBuffer<>(absolutePath.toString(), reader, version, formatter);
                return checkpointBuffer.read();
            }
        }
    }

    private static class CheckpointReadBuffer<T> {

        private final String location;
        private final BufferedReader reader;
        private final int version;
        private final EntryFormatter<T> formatter;

        CheckpointReadBuffer(String location,
                             BufferedReader reader,
                             int version,
                             EntryFormatter<T> formatter) {
            this.location = location;
            this.reader = reader;
            this.version = version;
            this.formatter = formatter;
        }

        List<T> read() throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return Collections.emptyList();
            }

            int readVersion = toInt(line);
            if (readVersion != version) {
                throw new IOException("Unrecognised version:" + readVersion + ", expected version: " + version
                        + " in checkpoint file at: " + location);
            }

            line = reader.readLine();
            if (line == null) {
                return Collections.emptyList();
            }
            int expectedSize = toInt(line);
            List<T> entries = new ArrayList<>(expectedSize);
            line = reader.readLine();
            while (line != null) {
                Optional<T> maybeEntry = formatter.fromString(line);
                if (!maybeEntry.isPresent()) {
                    throw buildMalformedLineException(line);
                }
                entries.add(maybeEntry.get());
                line = reader.readLine();
            }

            if (entries.size() != expectedSize) {
                throw new IOException("Expected [" + expectedSize + "] entries in checkpoint file ["
                        + location + "], but found only [" + entries.size() + "]");
            }

            return entries;
        }

        private int toInt(String line) throws IOException {
            try {
                return Integer.parseInt(line);
            } catch (NumberFormatException e) {
                throw buildMalformedLineException(line);
            }
        }

        private IOException buildMalformedLineException(String line) {
            return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line));
        }
    }

    /**
     * This is used to convert the given entry of type {@code T} into a string and vice versa.
     *
     * @param <T> entry type
     */
    public interface EntryFormatter<T> {

        /**
         * @param entry entry to be converted into string.
         * @return String representation of the given entry.
         */
        String toString(T entry);

        /**
         * @param value string representation of an entry.
         * @return entry converted from the given string representation if possible. {@link Optional#empty()} represents
         * that the given string representation could not be converted into an entry.
         */
        Optional<T> fromString(String value);
    }
}
