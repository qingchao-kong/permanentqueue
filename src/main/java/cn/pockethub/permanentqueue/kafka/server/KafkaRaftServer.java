package cn.pockethub.permanentqueue.kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;

public class KafkaRaftServer {

    public static final String MetadataTopic = Topic.METADATA_TOPIC_NAME;
    public static final TopicPartition MetadataPartition = Topic.METADATA_TOPIC_PARTITION;
    public static final Uuid MetadataTopicId = Uuid.METADATA_TOPIC_ID;

    public enum ProcessRole{
        BrokerRole,
        ControllerRole,
        ;
    }
}
