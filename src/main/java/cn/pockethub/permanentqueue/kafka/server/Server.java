package cn.pockethub.permanentqueue.kafka.server;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Server {

//    void startup();

//    void shutdown();

//    void awaitShutdown();

    class Static {
        public static String MetricsPrefix = "kafka.server";
        public static String ClusterIdLabel = "kafka.cluster.id";
        public static String BrokerIdLabel = "kafka.broker.id";
        public static String NodeIdLabel = "kafka.node.id";

        public static Metrics initializeMetrics(KafkaConfig config,
                                                Time time,
                                                String clusterId) {
            KafkaMetricsContext metricsContext = createKafkaMetricsContext(config, clusterId);
            return buildMetrics(config, time, metricsContext);
        }

        private static Metrics buildMetrics(KafkaConfig config,
                                            Time time,
                                            KafkaMetricsContext metricsContext) {
            List<MetricsReporter> defaultReporters = initializeDefaultReporters(config);
            MetricConfig metricConfig = buildMetricsConfig(config);
            return new Metrics(metricConfig, defaultReporters, time, true, metricsContext);
        }

        public static MetricConfig buildMetricsConfig(KafkaConfig kafkaConfig) {
            return new MetricConfig()
                    .samples(kafkaConfig.metricNumSamples)
                    .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
                    .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS);
        }

        protected static KafkaMetricsContext createKafkaMetricsContext(KafkaConfig config, String clusterId) {
            Map<String, Object> contextLabels = new HashMap<>();
            contextLabels.put(ClusterIdLabel, clusterId);

            if (config.usesSelfManagedQuorum()) {
                contextLabels.put(NodeIdLabel, config.nodeId.toString());
            } else {
                contextLabels.put(BrokerIdLabel, config.brokerId.toString());
            }

            contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            return new KafkaMetricsContext(MetricsPrefix, contextLabels);
        }

        private static List<MetricsReporter> initializeDefaultReporters(KafkaConfig config) {
            JmxReporter jmxReporter = new JmxReporter();
            jmxReporter.configure(config.originals());

            List<MetricsReporter> reporters = new ArrayList<>();
            reporters.add(jmxReporter);
            return reporters;
        }
    }
}
