package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.List;
import java.util.Map;

public abstract class KafkaBroker extends KafkaMetricsGroup {
    //properties for MetricsContext
    public static final String MetricsTypeName = "KafkaServer";

    /**
     * The log message that we print when the broker has been successfully started.
     * The ducktape system tests look for a line matching the regex 'Kafka\s*Server.*started'
     * to know when the broker is started, so it is best not to change this message -- but if
     * you do change it, be sure to make it match that regex or the system tests will fail.
     */
    public static final String STARTED_MESSAGE = "Kafka Server started";

    protected static void notifyClusterListeners(String clusterId,
                                                 List<Object> clusterListeners) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        clusterResourceListeners.maybeAddAll(clusterListeners);
        clusterResourceListeners.onUpdate(new ClusterResource(clusterId));
    }

    protected static void notifyMetricsReporters(String clusterId,
                                                 KafkaConfig config,
                                                 List<Object> metricsReporters) {
        KafkaMetricsContext metricsContext = Server.Static.createKafkaMetricsContext(config, clusterId);
        for (Object x : metricsReporters) {
            if (x instanceof MetricsReporter) {
                ((MetricsReporter)x).contextChange(metricsContext);
            }else {
                //do nothing
            }
        }
    }


//    private LinuxIoMetricsCollector linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", Time.SYSTEM, logger.underlying());

    public KafkaBroker() {
//        newGauge("BrokerState", new Gauge<Byte>() {
//            @Override
//            public Byte value() {
//                return brokerState().value();
//            }
//        }, new HashMap<>());
//        newGauge("ClusterId", new Gauge<String>() {
//            @Override
//            public String value() {
//                return clusterId();
//            }
//        }, new HashMap<>());
//        newGauge("yammer-metrics-count", new Gauge<Integer>() {
//            @Override
//            public Integer value() {
//                return KafkaYammerMetrics.defaultRegistry().allMetrics().size();
//            }
//        }, new HashMap<>());

//        if (linuxIoMetricsCollector.usable()) {
//            newGauge("linux-disk-read-bytes", new Gauge<Long>() {
//                @Override
//                public Long value() {
//                    return linuxIoMetricsCollector.readBytes();
//                }
//            }, new HashMap<>());
//            newGauge("linux-disk-write-bytes", new Gauge<Long>() {
//                @Override
//                public Long value() {
//                    return linuxIoMetricsCollector.writeBytes();
//                }
//            }, new HashMap<>());
//        }
    }

//    abstract Optional<Authorizer> authorizer();

//    abstract BrokerState brokerState();

//    abstract String clusterId();

//    abstract KafkaConfig config();

//    abstract KafkaRequestHandlerPool dataPlaneRequestHandlerPool();

//    abstract KafkaApis dataPlaneRequestProcessor();

//    abstract KafkaScheduler kafkaScheduler();

//    abstract KafkaYammerMetrics kafkaYammerMetrics();

//    abstract LogManager logManager();

//    abstract Metrics metrics();

//    abstract QuotaFactory.QuotaManagers quotaManagers();
//
//    abstract ReplicaManager replicaManager();
//
//    abstract SocketServer socketServer();

//    abstract MetadataCache metadataCache();

//    abstract GroupCoordinator groupCoordinator();
//
//    abstract Integer boundPort(listenerName:ListenerName);

//    abstract void startup();

//    abstract void awaitShutdown();

//    abstract void shutdown();

//    abstract BrokerTopicStats brokerTopicStats();

//    abstract CredentialProvider credentialProvider();
//
//    abstract BrokerToControllerChannelManager clientToControllerChannelManager();

    // For backwards compatibility, we need to keep older metrics tied
    // to their original name when this class was named `KafkaServer`
    @Override
    public MetricName metricName(String name, Map<String, String> metricTags) {
        return explicitMetricName(Server.Static.MetricsPrefix, KafkaBroker.MetricsTypeName, name, metricTags);
    }


}
