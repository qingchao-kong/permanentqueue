package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogManager;
import cn.pockethub.permanentqueue.kafka.log.UnifiedLog;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class DynamicLogConfig implements BrokerReconfigurable{

    private LogManager logManager;
    private KafkaBroker server;

    public DynamicLogConfig(LogManager logManager, KafkaBroker server){
        this.logManager=logManager;
        this.server=server;
    }

    // Exclude message.format.version for now since we need to check that the version
    // is supported on all brokers in the cluster.
//    @nowarn("cat=deprecation")
    public static final Set<String> ExcludedConfigs = new HashSet<>(Arrays.asList(KafkaConfig.LogMessageFormatVersionProp));

    public static final Set<String> ReconfigurableConfigs;
    static {
        ReconfigurableConfigs=new HashSet<>(LogConfig.TopicConfigSynonyms.values());
        ReconfigurableConfigs.removeAll(ExcludedConfigs);
    }
    public static final Map<String,String> KafkaConfigToLogConfigName;
    static {
        KafkaConfigToLogConfigName=new HashMap<>();
        for (Map.Entry<String, String> entry :LogConfig.TopicConfigSynonyms.entrySet()) {
            KafkaConfigToLogConfigName.put(entry.getValue(),entry.getKey());
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return DynamicLogConfig.ReconfigurableConfigs;
    }

    @Override
    public void validateReconfiguration(KafkaConfig newConfig) {
        // For update of topic config overrides, only config names and types are validated
        // Names and types have already been validated. For consistency with topic config
        // validation, no additional validation is performed.
    }

    @Override
    public void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig) throws InterruptedException, NoSuchAlgorithmException,IOException {
//        LogConfig originalLogConfig = logManager.getCurrentDefaultConfig();
//        boolean originalUncleanLeaderElectionEnable = originalLogConfig.getUncleanLeaderElectionEnable();
//        Map<String,Object> newBrokerDefaults = new HashMap<>(originalLogConfig.originals());
//        for (Map.Entry<String, ?> entry :newConfig.valuesFromThisConfig().entrySet()) {
//            String k = entry.getKey();
//            Object v = entry.getValue();
//            if (ReconfigurableConfigs.contains(k)) {
//                if (KafkaConfigToLogConfigName.containsKey(k)
//                        &&Objects.nonNull(KafkaConfigToLogConfigName.get(k))) {
//                    String configName = KafkaConfigToLogConfigName.get(k);
//                    if (Objects.isNull(v)) {
//                        newBrokerDefaults.remove(configName);
//                    }else {
//                        newBrokerDefaults.put(configName, v);
//                    }
//                }
//            }
//        }
//
//        logManager.reconfigureDefaultLogConfig(new LogConfig(new HashMap<>(newBrokerDefaults)));
//
//        updateLogsConfig(newBrokerDefaults);
//
//        if (logManager.getCurrentDefaultConfig().getUncleanLeaderElectionEnable() && !originalUncleanLeaderElectionEnable) {
//            if(server instanceof KafkaServer){
//                ((KafkaServer)server).kafkaController().enableDefaultUncleanLeaderElection();
//            }
//        }
    }

    private void updateLogsConfig(Map<String, Object> newBrokerDefaults)throws Throwable {
        logManager.brokerConfigUpdated();
        for (UnifiedLog log :logManager.allLogs()) {
            Map<Object, Object> props = new HashMap<>(newBrokerDefaults);
            for (Map.Entry<String, Object>entry:log.config().originals().entrySet()){
                if (log.config().getOverriddenConfigs().contains(entry.getKey())) {
                    props.put(entry.getKey(),entry.getValue());
                }
            }

            LogConfig logConfig =new LogConfig(props, log.config().getOverriddenConfigs());
            log.updateConfig(logConfig);
        }
    }

}
