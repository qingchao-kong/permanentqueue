package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import com.yammer.metrics.core.Meter;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class BrokerTopicMetrics extends KafkaMetricsGroup {

    private Optional<String> name;

    private Map<String, String> tags;

    // an internal map for "lazy initialization" of certain metrics
    private Map<String, MeterWrapper> metricTypeMap = new HashMap<>();

    public BrokerTopicMetrics(Optional<String> name) {
        this.name = name;

        if (!name.isPresent()) {
            tags = new HashMap<>();
        } else {
            String topic = name.get();
            tags = new HashMap<String, String>() {{
                put("topic", topic);
            }};
        }

        metricTypeMap.putAll(new HashMap<String, MeterWrapper>() {{
            put(BrokerTopicStats.MessagesInPerSec, new MeterWrapper(BrokerTopicStats.MessagesInPerSec, "messages"));
            put(BrokerTopicStats.BytesInPerSec, new MeterWrapper(BrokerTopicStats.BytesInPerSec, "bytes"));
            put(BrokerTopicStats.BytesOutPerSec, new MeterWrapper(BrokerTopicStats.BytesOutPerSec, "bytes"));
            put(BrokerTopicStats.BytesRejectedPerSec, new MeterWrapper(BrokerTopicStats.BytesRejectedPerSec, "bytes"));
            put(BrokerTopicStats.FailedProduceRequestsPerSec, new MeterWrapper(BrokerTopicStats.FailedProduceRequestsPerSec, "requests"));
            put(BrokerTopicStats.FailedFetchRequestsPerSec, new MeterWrapper(BrokerTopicStats.FailedFetchRequestsPerSec, "requests"));
            put(BrokerTopicStats.TotalProduceRequestsPerSec, new MeterWrapper(BrokerTopicStats.TotalProduceRequestsPerSec, "requests"));
            put(BrokerTopicStats.TotalFetchRequestsPerSec, new MeterWrapper(BrokerTopicStats.TotalFetchRequestsPerSec, "requests"));
            put(BrokerTopicStats.FetchMessageConversionsPerSec, new MeterWrapper(BrokerTopicStats.FetchMessageConversionsPerSec, "requests"));
            put(BrokerTopicStats.ProduceMessageConversionsPerSec, new MeterWrapper(BrokerTopicStats.ProduceMessageConversionsPerSec, "requests"));
            put(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec, new MeterWrapper(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec, "requests"));
            put(BrokerTopicStats.InvalidMagicNumberRecordsPerSec, new MeterWrapper(BrokerTopicStats.InvalidMagicNumberRecordsPerSec, "requests"));
            put(BrokerTopicStats.InvalidMessageCrcRecordsPerSec, new MeterWrapper(BrokerTopicStats.InvalidMessageCrcRecordsPerSec, "requests"));
            put(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec, new MeterWrapper(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec, "requests"));
        }});
        if (!name.isPresent()) {
            metricTypeMap.put(BrokerTopicStats.ReplicationBytesInPerSec, new MeterWrapper(BrokerTopicStats.ReplicationBytesInPerSec, "bytes"));
            metricTypeMap.put(BrokerTopicStats.ReplicationBytesOutPerSec, new MeterWrapper(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes"));
            metricTypeMap.put(BrokerTopicStats.ReassignmentBytesInPerSec, new MeterWrapper(BrokerTopicStats.ReassignmentBytesInPerSec, "bytes"));
            metricTypeMap.put(BrokerTopicStats.ReassignmentBytesOutPerSec, new MeterWrapper(BrokerTopicStats.ReassignmentBytesOutPerSec, "bytes"));
        }
    }

    // used for testing only
    public Map<String, MeterWrapper> metricMap() {
        return metricTypeMap;
    }

    public Meter messagesInRate(){
        return metricTypeMap.get(BrokerTopicStats.MessagesInPerSec).meter();
    }

    public Meter bytesInRate(){
        return metricTypeMap.get(BrokerTopicStats.BytesInPerSec).meter();
    }

    public Meter bytesOutRate(){
        return metricTypeMap.get(BrokerTopicStats.BytesOutPerSec).meter();
    }

    public Meter bytesRejectedRate(){
        return metricTypeMap.get(BrokerTopicStats.BytesRejectedPerSec).meter();
    }

    protected Optional<Meter> replicationBytesInRate(){
            if (!name.isPresent()) {
                return Optional.of(metricTypeMap.get(BrokerTopicStats.ReplicationBytesInPerSec).meter());
            }
            else {
                return Optional.empty();
            }
    }

    protected Optional<Meter> replicationBytesOutRate() {
        if (!name.isPresent()) {
            return Optional.of(metricTypeMap.get(BrokerTopicStats.ReplicationBytesOutPerSec).meter());
        }
        else {
            return Optional.empty();
        }
    }

    protected Optional<Meter> reassignmentBytesInPerSec(){
        if (!name.isPresent()) {
            return Optional.of(metricTypeMap.get(BrokerTopicStats.ReassignmentBytesInPerSec).meter());
        } else {
            return Optional.empty();
        }
    }

    protected Optional<Meter> reassignmentBytesOutPerSec(){
            if (!name.isPresent()) {
                return Optional.of(metricTypeMap.get(BrokerTopicStats.ReassignmentBytesOutPerSec).meter());
            }
            else {
                return Optional.empty();
            }
    }

    public Meter failedProduceRequestRate() {
        return metricTypeMap.get(BrokerTopicStats.FailedProduceRequestsPerSec).meter();
    }

    public Meter failedFetchRequestRate(){
        return metricTypeMap.get(BrokerTopicStats.FailedFetchRequestsPerSec).meter();
    }

    public Meter totalProduceRequestRate() {
        return metricTypeMap.get(BrokerTopicStats.TotalProduceRequestsPerSec).meter();
    }

    public Meter totalFetchRequestRate() {
        return metricTypeMap.get(BrokerTopicStats.TotalFetchRequestsPerSec).meter();
    }

    public Meter fetchMessageConversionsRate() {
        return metricTypeMap.get(BrokerTopicStats.FetchMessageConversionsPerSec).meter();
    }

    public Meter produceMessageConversionsRate() {
        return metricTypeMap.get(BrokerTopicStats.ProduceMessageConversionsPerSec).meter();
    }

    public Meter noKeyCompactedTopicRecordsPerSec() {
        return metricTypeMap.get(BrokerTopicStats.NoKeyCompactedTopicRecordsPerSec).meter();
    }

    public Meter invalidMagicNumberRecordsPerSec() {
        return metricTypeMap.get(BrokerTopicStats.InvalidMagicNumberRecordsPerSec).meter();
    }

    public Meter invalidMessageCrcRecordsPerSec() {
        return metricTypeMap.get(BrokerTopicStats.InvalidMessageCrcRecordsPerSec).meter();
    }

    public Meter invalidOffsetOrSequenceRecordsPerSec() {
        return metricTypeMap.get(BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec).meter();
    }

    public void closeMetric(String metricType){
        MeterWrapper meter = metricTypeMap.get(metricType);
        if (meter != null) {
            meter.close();
        }
    }

    public void close(){
        for (MeterWrapper metaWrapper :metricTypeMap.values()) {
            metaWrapper.close();
        }
    }

    public class MeterWrapper {

        private String metricType;
        private String eventType;


        private volatile Meter lazyMeter;
        private Object meterLock = new Object();

        public MeterWrapper(String metricType, String eventType) {
            this.metricType = metricType;
            this.eventType = eventType;

            if (MapUtils.isEmpty(tags)) {
                // greedily initialize the general topic metrics
                meter();
            }
        }

        public Meter meter() {
            Meter meter = lazyMeter;
            if (meter == null) {
                synchronized (meterLock) {
                    meter = lazyMeter;
                    if (meter == null) {
                        meter = newMeter(metricType, eventType, TimeUnit.SECONDS, tags);
                        lazyMeter = meter;
                    }
                }
            }
            return meter;
        }

        public void close() {
            synchronized (meterLock) {
                if (lazyMeter != null) {
                    removeMetric(metricType, tags);
                    lazyMeter = null;
                }
            }
        }
    }

}
