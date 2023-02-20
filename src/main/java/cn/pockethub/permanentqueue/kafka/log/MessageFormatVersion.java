package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.server.KafkaConfig;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import org.apache.kafka.common.record.RecordVersion;

import static cn.pockethub.permanentqueue.kafka.log.LogConfig.shouldIgnoreMessageFormatVersion;
import static cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion.IBP_3_0_IV1;

public class MessageFormatVersion {
    private String messageFormatVersionString;
    private String interBrokerProtocolVersionString;

    private MetadataVersion messageFormatVersion;
    private MetadataVersion interBrokerProtocolVersion;

    public MessageFormatVersion(String messageFormatVersionString, String interBrokerProtocolVersionString) {
        this.messageFormatVersionString = messageFormatVersionString;
        this.interBrokerProtocolVersionString = interBrokerProtocolVersionString;

        this.messageFormatVersion =MetadataVersion.fromVersionString(messageFormatVersionString);
        this.interBrokerProtocolVersion =MetadataVersion.fromVersionString(interBrokerProtocolVersionString);
    }

    public Boolean shouldIgnore() {
        return shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion);
    }

    public Boolean shouldWarn() {
        return interBrokerProtocolVersion.isAtLeast(IBP_3_0_IV1) && messageFormatVersion.highestSupportedRecordVersion().precedes(RecordVersion.V2);
    }

//    @nowarn("cat=deprecation")
    public String topicWarningMessage(String topicName) {
        return String.format("Topic configuration %s with value `%s` is ignored for `%s` because the inter-broker protocol version `%s` is " +
                        "greater or equal than 3.0. This configuration is deprecated and it will be removed in Apache Kafka 4.0.",
                LogConfig.MessageFormatVersionProp, messageFormatVersionString, topicName, interBrokerProtocolVersionString);
    }

//    @nowarn("cat=deprecation")
    public String brokerWarningMessage() {
        return String.format("Broker configuration %s with value %s is ignored " +
                        "because the inter-broker protocol version `%s` is greater or equal than 3.0. " +
                        "This configuration is deprecated and it will be removed in Apache Kafka 4.0.",
                KafkaConfig.LogMessageFormatVersionProp, messageFormatVersionString, interBrokerProtocolVersionString);
    }
}
