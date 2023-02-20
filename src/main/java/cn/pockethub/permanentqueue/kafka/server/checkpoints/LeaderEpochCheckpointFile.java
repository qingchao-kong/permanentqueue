package cn.pockethub.permanentqueue.kafka.server.checkpoints;

import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.epoch.EpochEntry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class LeaderEpochCheckpointFile implements LeaderEpochCheckpoint {

    private static final String LeaderEpochCheckpointFilename = "leader-epoch-checkpoint";
    private static final Pattern WhiteSpacesPattern = Pattern.compile("\\s+");
    private static final int CurrentVersion = 0;

    private File file;
    private LogDirFailureChannel logDirFailureChannel;

    private CheckpointFileWithFailureHandler checkpoint;

    public LeaderEpochCheckpointFile(File file) throws IOException {
        this(file, null);
    }

    public LeaderEpochCheckpointFile(File file, LogDirFailureChannel logDirFailureChannel) throws IOException {
        this.file = file;
        this.logDirFailureChannel = logDirFailureChannel;

        this.checkpoint = new CheckpointFileWithFailureHandler<EpochEntry>(file, CurrentVersion, new Formatter(), logDirFailureChannel, file.getParentFile().getParent());
    }

    @Override
    public void write(Iterable<EpochEntry> epochs) {
        checkpoint.write(epochs);
    }

    @Override
    public List<EpochEntry> read() {
        return new ArrayList<>(checkpoint.read());
    }


    public static File newFile(File dir) {
        return new File(dir, LeaderEpochCheckpointFilename);
    }

    static class Formatter implements CheckpointFile.EntryFormatter<EpochEntry> {

        @Override
        public String toString(EpochEntry entry) {
            return String.format("%s %s", entry.getEpoch(), entry.getStartOffset());
        }

        @Override
        public Optional<EpochEntry> fromString(String line) {
            String[] split = WhiteSpacesPattern.split(line);
            if (split.length == 2) {
                String epoch = split[0];
                String offset = split[1];
                return Optional.of(new EpochEntry(Integer.parseInt(epoch), Long.parseLong(offset)));
            } else {
                return Optional.empty();
            }
        }

    }
}
