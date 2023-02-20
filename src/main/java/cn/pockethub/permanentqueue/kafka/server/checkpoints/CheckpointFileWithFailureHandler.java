package cn.pockethub.permanentqueue.kafka.server.checkpoints;

import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.errors.KafkaStorageException;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class CheckpointFileWithFailureHandler<T> {

    private File file;
    private Integer version;
    private CheckpointFile.EntryFormatter<T> formatter;
    private LogDirFailureChannel logDirFailureChannel;
    private String logDir;

    private CheckpointFile<T> checkpointFile;


    public CheckpointFileWithFailureHandler(File file,
                                            Integer version,
                                            CheckpointFile.EntryFormatter<T> formatter,
                                            LogDirFailureChannel logDirFailureChannel,
                                            String logDir) throws IOException{
        this.file = file;
        this.version = version;
        this.formatter = formatter;
        this.logDirFailureChannel = logDirFailureChannel;
        this.logDir = logDir;

        this.checkpointFile = new CheckpointFile<T>(file, version, formatter);
    }

    public void write(Iterable<T> entries) {
        try {
            checkpointFile.write(ImmutableList.copyOf(entries));
        } catch (IOException e) {
            String msg = "Error while writing to checkpoint file " + file.getAbsolutePath();
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
            throw new KafkaStorageException(msg, e);
        }
    }

    public Collection<T> read() {
        try {
            return checkpointFile.read();
        } catch (IOException e) {
            String msg = "Error while reading checkpoint file " + file.getAbsolutePath();
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
            throw new KafkaStorageException(msg, e);
        }
    }
}
