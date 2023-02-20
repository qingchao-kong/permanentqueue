package cn.pockethub.permanentqueue.kafka.metadata;

import cn.pockethub.permanentqueue.kafka.server.ConfigRepository;
import org.apache.kafka.common.config.ConfigResource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

public class MockConfigRepository implements ConfigRepository {

    Map<ConfigResource, Properties> configs = new HashMap<>();

    @Override
    public Properties config(ConfigResource configResource) {
        return configs.getOrDefault(configResource, new Properties());
    }

    public void setConfig(ConfigResource configResource, String key, String value) {
        synchronized (configs) {
            Properties properties = configs.getOrDefault(configResource, new Properties());
            Properties newProperties = new Properties();
            newProperties.putAll(properties);
            if (value == null) {
                newProperties.remove(key);
            } else {
                newProperties.put(key, value);
            }
            configs.put(configResource, newProperties);
        }
    }

    public void setTopicConfig(String topicName, String key, String value) {
        synchronized (configs) {
            setConfig(new ConfigResource(TOPIC, topicName), key, value);
        }
    }

    public static MockConfigRepository forTopic(String topic, String key, String value) {
        Properties properties = new Properties();
        properties.put(key, value);
        return forTopic(topic, properties);
    }

    public static MockConfigRepository forTopic(String topic, Properties properties) {
        MockConfigRepository repository = new MockConfigRepository();
        repository.configs.put(new ConfigResource(TOPIC, topic), properties);
        return repository;
    }
}
