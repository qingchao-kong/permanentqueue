package cn.pockethub.permanentqueue.kafka.server;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

public interface BrokerReconfigurable {

    Set<String> reconfigurableConfigs();

    void validateReconfiguration(KafkaConfig newConfig);

    void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig)throws InterruptedException, NoSuchAlgorithmException, IOException;
}
