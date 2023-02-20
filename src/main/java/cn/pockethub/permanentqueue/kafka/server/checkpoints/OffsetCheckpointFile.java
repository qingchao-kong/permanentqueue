package cn.pockethub.permanentqueue.kafka.server.checkpoints;

import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Getter
public class OffsetCheckpointFile {
    private static final Pattern WhiteSpacesPattern = Pattern.compile("\\s+");
    protected static final int CurrentVersion = 0;

    private File file;
    private LogDirFailureChannel logDirFailureChannel;

    public final CheckpointFileWithFailureHandler<Map.Entry<TopicPartition, Long>> checkpoint;

    public OffsetCheckpointFile(final File file) throws IOException {
        this(file, null);
    }

    public OffsetCheckpointFile(final File file, LogDirFailureChannel logDirFailureChannel) throws IOException {
        this.file = file;
        this.logDirFailureChannel = logDirFailureChannel;
        this.checkpoint = new CheckpointFileWithFailureHandler(file, OffsetCheckpointFile.CurrentVersion,
                new Formatter(), logDirFailureChannel, file.getParent());
    }

    public void write(Map<TopicPartition, Long> offsets)throws IOException {
        checkpoint.write(offsets.entrySet());
    }

    public Map<TopicPartition, Long> read() {
        return checkpoint.read().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static class Formatter implements CheckpointFile.EntryFormatter<Map.Entry<TopicPartition, Long>> {
        @Override
        public String toString(Map.Entry<TopicPartition, Long> entry) {
            return String.format("%s %s %s", entry.getKey().topic(), entry.getKey().partition(), entry.getValue());
        }

        @Override
        public Optional<Map.Entry<TopicPartition, Long>> fromString(String line) {
            String[] split = WhiteSpacesPattern.split(line);
            if (ArrayUtils.isNotEmpty(split) && split.length == 3) {
                String topic = split[0];
                int partition = Integer.parseInt(split[1]);
                Long offset = Long.parseLong(split[2]);
                return Optional.of(Pair.of(new TopicPartition(topic, partition), offset));
            } else {
                return Optional.empty();
            }
        }
    }
}
