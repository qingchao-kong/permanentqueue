package cn.pockethub.permanentqueue.kafka.server;

import lombok.Getter;
import org.apache.kafka.common.Uuid;

@Getter
public class PartitionMetadata {
    private Integer version;
    private Uuid topicId;

    public PartitionMetadata(Integer version, Uuid topicId) {
        this.version = version;
        this.topicId = topicId;
    }
}
