package cn.pockethub.permanentqueue.kafka.server;

import org.apache.kafka.common.config.ConfigResource;

import java.util.Properties;

public interface ConfigRepository {

    /**
     * Return a copy of the topic configuration for the given topic.  Future changes will not be reflected.
     *
     * @param topicName the name of the topic for which the configuration will be returned
     * @return a copy of the topic configuration for the given topic
     */
    default Properties topicConfig(String topicName) {
        return config(new ConfigResource(ConfigResource.Type.TOPIC, topicName));
    }

    /**
     * Return a copy of the broker configuration for the given broker.  Future changes will not be reflected.
     *
     * @param brokerId the id of the broker for which configuration will be returned
     * @return a copy of the broker configuration for the given broker
     */
    default Properties brokerConfig(Integer brokerId) {
        return config(new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString()));
    }

    /**
     * Return a copy of the configuration for the given resource.  Future changes will not be reflected.
     *
     * @param configResource the resource for which the configuration will be returned
     * @return a copy of the configuration for the given resource
     */
    Properties config(ConfigResource configResource);
}
