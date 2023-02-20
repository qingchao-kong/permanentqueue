package cn.pockethub.permanentqueue.kafka.coordinator.group;

import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;

public class OffsetConfig {

    public static final int DefaultMaxMetadataSize = 4096;
    public static final int DefaultLoadBufferSize = 5 * 1024 * 1024;
    public static final long DefaultOffsetRetentionMs = 24 * 60 * 60 * 1000L;
    public static final long DefaultOffsetsRetentionCheckIntervalMs = 600000L;
    public static final int DefaultOffsetsTopicNumPartitions = 50;
    public static final int DefaultOffsetsTopicSegmentBytes = 100 * 1024 * 1024;
    public static final short DefaultOffsetsTopicReplicationFactor = 3;
    public static final CompressionCodec DefaultOffsetsTopicCompressionCodec = CompressionCodec.NoCompressionCodec;
    public static final int DefaultOffsetCommitTimeoutMs = 5000;
    public static final short DefaultOffsetCommitRequiredAcks = -1;
}
