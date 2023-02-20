package cn.pockethub.permanentqueue.kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BrokerTopicStats {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerTopicStats.class);

    /*static*/
    public static final String MessagesInPerSec = "MessagesInPerSec";
    public static final String BytesInPerSec = "BytesInPerSec";
    public static final String BytesOutPerSec = "BytesOutPerSec";
    public static final String BytesRejectedPerSec = "BytesRejectedPerSec";
    public static final String ReplicationBytesInPerSec = "ReplicationBytesInPerSec";
    public static final String ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec";
    public static final String FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec";
    public static final String FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec";
    public static final String TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec";
    public static final String TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec";
    public static final String FetchMessageConversionsPerSec = "FetchMessageConversionsPerSec";
    public static final String ProduceMessageConversionsPerSec = "ProduceMessageConversionsPerSec";
    public static final String ReassignmentBytesInPerSec = "ReassignmentBytesInPerSec";
    public static final String ReassignmentBytesOutPerSec = "ReassignmentBytesOutPerSec";

    // These following topics are for LogValidator for better debugging on failed records
    public static final String NoKeyCompactedTopicRecordsPerSec = "NoKeyCompactedTopicRecordsPerSec";
    public static final String InvalidMagicNumberRecordsPerSec = "InvalidMagicNumberRecordsPerSec";
    public static final String InvalidMessageCrcRecordsPerSec = "InvalidMessageCrcRecordsPerSec";
    public static final String InvalidOffsetOrSequenceRecordsPerSec = "InvalidOffsetOrSequenceRecordsPerSec";

    private static BrokerTopicMetrics valueFactory(String k) {
        return new BrokerTopicMetrics(Optional.of(k));
    }
    /*static*/

    /*非static*/
    private final Map<String, BrokerTopicMetrics> stats = new HashMap<>();
    public BrokerTopicMetrics allTopicsStats = new BrokerTopicMetrics(Optional.empty());

    public BrokerTopicMetrics topicStats(final String topic) {
        return stats.computeIfAbsent(topic, k -> valueFactory(topic));
    }

    public void updateReplicationBytesIn(Long value) {
        allTopicsStats.replicationBytesInRate().ifPresent(meter -> meter.mark(value));
    }

    private void updateReplicationBytesOut(Long value) {
        allTopicsStats.replicationBytesOutRate().ifPresent(metric -> metric.mark(value));
    }

    public void updateReassignmentBytesIn(Long value) {
        allTopicsStats.reassignmentBytesInPerSec().ifPresent(metric -> metric.mark(value));
    }

    public void updateReassignmentBytesOut(Long value) {
        allTopicsStats.reassignmentBytesOutPerSec().ifPresent(meter -> meter.mark(value));
    }

    // This method only removes metrics only used for leader
    public void removeOldLeaderMetrics(String topic) {
        BrokerTopicMetrics topicMetrics = topicStats(topic);
        if (topicMetrics != null) {
            topicMetrics.closeMetric(BrokerTopicStats.MessagesInPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.BytesInPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.BytesRejectedPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.FailedProduceRequestsPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.TotalProduceRequestsPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.ProduceMessageConversionsPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesOutPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.ReassignmentBytesOutPerSec);
        }
    }

    // This method only removes metrics only used for follower
    public void removeOldFollowerMetrics(String topic) {
        BrokerTopicMetrics topicMetrics = topicStats(topic);
        if (topicMetrics != null) {
            topicMetrics.closeMetric(BrokerTopicStats.ReplicationBytesInPerSec);
            topicMetrics.closeMetric(BrokerTopicStats.ReassignmentBytesInPerSec);
        }
    }

    public void removeMetrics(String topic) {
        BrokerTopicMetrics metrics = stats.remove(topic);
        if (metrics != null) {
            metrics.close();
        }
    }

    public void updateBytesOut(String topic, Boolean isFollower, Boolean isReassignment, Long value) {
        if (isFollower) {
            if (isReassignment) {
                updateReassignmentBytesOut(value);
            }
            updateReplicationBytesOut(value);
        } else {
            topicStats(topic).bytesOutRate().mark(value);
            allTopicsStats.bytesOutRate().mark(value);
        }
    }

    public void close() {
        allTopicsStats.close();
        for (BrokerTopicMetrics brokerTopicMetrics : stats.values()) {
            brokerTopicMetrics.close();
        }

        LOG.info("Broker and topic stats closed");
    }
}
