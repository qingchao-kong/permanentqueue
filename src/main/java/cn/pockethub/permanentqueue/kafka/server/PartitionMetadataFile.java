package cn.pockethub.permanentqueue.kafka.server;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class PartitionMetadataFile {
    private static final String PartitionMetadataFilename = "partition.metadata";
    private static final Pattern WhiteSpacesPattern = Pattern.compile(":\\s+");
    private static final int CurrentVersion = 0;

    private File file;
    private LogDirFailureChannel logDirFailureChannel;

    private Path path;
    private Path tempPath;
    private Object lock = new Object();
    private String logDir;
    private volatile Optional<Uuid> dirtyTopicIdOpt = Optional.empty();

    public PartitionMetadataFile(File file, LogDirFailureChannel logDirFailureChannel) {
        this.file = file;
        this.logDirFailureChannel = logDirFailureChannel;

        this.path = file.toPath().toAbsolutePath();
        this.tempPath = Paths.get(path.toString() + ".tmp");
        this.logDir = file.getParentFile().getParent();
    }

    /**
     * Records the topic ID that will be flushed to disk.
     */
    public void record(Uuid topicId) {
        // Topic IDs should not differ, but we defensively check here to fail earlier in the case that the IDs somehow differ.
        dirtyTopicIdOpt.ifPresent(dirtyTopicId -> {
            if (dirtyTopicId != topicId) {
                String msg = String.format("Tried to record topic ID %s to file " +
                        "but had already recorded %s", topicId, dirtyTopicId);
                throw new InconsistentTopicIdException(msg);
            }
        });
        dirtyTopicIdOpt = Optional.ofNullable(topicId);
    }

    public void maybeFlush() {
        // We check dirtyTopicId first to avoid having to take the lock unnecessarily in the frequently called log append path
        dirtyTopicIdOpt.ifPresent(uuid -> {
            synchronized (this) {
                dirtyTopicIdOpt.ifPresent(topicId -> {
                    try {
                        // write to temp file and then swap with the existing file
                        FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8));
                        try {
                            writer.write(PartitionMetadataFileFormatter.toFile(new PartitionMetadata(CurrentVersion, topicId)));
                            writer.flush();
                            fileOutputStream.getFD().sync();
                        } finally {
                            writer.close();
                        }

                        Utils.atomicMoveWithFallback(tempPath, path);
                    } catch (IOException e) {
                        String msg = String.format("Error while writing to partition metadata file %s", file.getAbsolutePath());
                        logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
                        throw new KafkaStorageException(msg, e);
                    }
                    dirtyTopicIdOpt = Optional.empty();
                });
            }
        });
    }

    public PartitionMetadata read() {
        synchronized (lock) {
            try {
                BufferedReader reader = Files.newBufferedReader(path);
                try {
                    PartitionMetadataReadBuffer partitionBuffer = new PartitionMetadataReadBuffer(file.getAbsolutePath(), reader);
                    return partitionBuffer.read();
                } finally {
                    reader.close();
                }
            } catch (IOException e) {
                String msg = String.format("Error while reading partition metadata file %s", file.getAbsolutePath());
                logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
                throw new KafkaStorageException(msg, e);
            }
        }
    }

    public Boolean exists() {
        return file.exists();
    }

    public void delete() throws IOException {
        Files.delete(file.toPath());
    }

    @Override
    public String toString() {
        return String.format("PartitionMetadataFile(path=%s)", path);
    }

    public static File newFile(File dir) {
        return new File(dir, PartitionMetadataFilename);
    }

    static class PartitionMetadataFileFormatter {
        public static String toFile(PartitionMetadata data) {
            return String.format("version: %s\ntopic_id: %s", data.getVersion(), data.getTopicId());
        }
    }

    static class PartitionMetadataReadBuffer {

        private final String location;
        private final BufferedReader reader;

        public PartitionMetadataReadBuffer(String location, BufferedReader reader) {
            this.location = location;
            this.reader = reader;
        }

        private IOException malformedLineException(String line) {
            return new IOException(String.format("Malformed line in checkpoint file (%s): '%s'", location, line));
        }

        public PartitionMetadata read() throws IOException {
            String line = null;
            Uuid metadataTopicId = null;
            try {
                line = reader.readLine();
                String[] split = WhiteSpacesPattern.split(line);
                if (Objects.nonNull(split) && split.length == 2) {
                    String version = split[1];
                    if (Integer.parseInt(version) == CurrentVersion) {
                        line = reader.readLine();
                        split = WhiteSpacesPattern.split(line);
                        if (Objects.nonNull(split) && split.length == 2) {
                            String topicId = split[1];
                            metadataTopicId = Uuid.fromString(topicId);
                        } else {
                            throw malformedLineException(line);
                        }
                        if (metadataTopicId.equals(Uuid.ZERO_UUID)) {
                            throw new IOException(String.format("Invalid topic ID in partition metadata file (%s)", location));
                        }
                        return new PartitionMetadata(CurrentVersion, metadataTopicId);
                    } else {
                        throw new IOException(String.format("Unrecognized version of partition metadata file (%s): %s", location, version));
                    }
                } else {
                    throw malformedLineException(line);
                }
            } catch (NumberFormatException e) {
                throw malformedLineException(line);
            }
        }
    }
}
