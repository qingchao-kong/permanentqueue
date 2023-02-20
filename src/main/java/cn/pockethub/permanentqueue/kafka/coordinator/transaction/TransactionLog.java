package cn.pockethub.permanentqueue.kafka.coordinator.transaction;


import org.apache.kafka.common.record.CompressionType;

public class TransactionLog {

    // log-level config default values and enforced values
    public static final int DefaultNumPartitions = 50;
    public static final int DefaultSegmentBytes = 100 * 1024 * 1024;
    public static final short DefaultReplicationFactor = 3;
    public static final int DefaultMinInSyncReplicas = 2;
    public static final int DefaultLoadBufferSize = 5 * 1024 * 1024;

    // enforce always using
    //  1. cleanup policy = compact
    //  2. compression = none
    //  3. unclean leader election = disabled
    //  4. required acks = -1 when writing
    public static final CompressionType EnforcedCompressionType = CompressionType.NONE;
    public static final int EnforcedRequiredAcks = -1;
}
