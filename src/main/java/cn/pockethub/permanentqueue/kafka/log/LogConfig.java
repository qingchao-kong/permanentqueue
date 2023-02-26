package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.metadata.ConfigSynonym;
import cn.pockethub.permanentqueue.kafka.server.KafkaConfig;
import cn.pockethub.permanentqueue.kafka.server.TopicConfigHandler;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersionValidator;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.Utils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.*;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

@Getter
public class LogConfig extends AbstractConfig {

    public static class Defaults {
        public static final int SegmentSize = KafkaConfig.Defaults.LogSegmentBytes;
        public static final long SegmentMs = KafkaConfig.Defaults.LogRollHours * 60 * 60 * 1000L;
        public static final long SegmentJitterMs = KafkaConfig.Defaults.LogRollJitterHours * 60 * 60 * 1000L;
        public static final long FlushInterval = KafkaConfig.Defaults.LogFlushIntervalMessages;
        public static final long FlushMs = KafkaConfig.Defaults.LogFlushSchedulerIntervalMs;
        public static final long RetentionSize = KafkaConfig.Defaults.LogRetentionBytes;
        public static final long RetentionMs = KafkaConfig.Defaults.LogRetentionHours * 60 * 60 * 1000L;
        public static final boolean RemoteLogStorageEnable = false;
        // It indicates the value to be derived from RetentionSize
        public static final int LocalRetentionBytes = -2;
        // It indicates the value to be derived from RetentionMs
        public static final int LocalRetentionMs = -2;
        public static final int MaxMessageSize = KafkaConfig.Defaults.MessageMaxBytes;
        public static final int MaxIndexSize = KafkaConfig.Defaults.LogIndexSizeMaxBytes;
        public static final int IndexInterval = KafkaConfig.Defaults.LogIndexIntervalBytes;
        public static final long FileDeleteDelayMs = KafkaConfig.Defaults.LogDeleteDelayMs;
        public static final long DeleteRetentionMs = KafkaConfig.Defaults.LogCleanerDeleteRetentionMs;
        public static final long MinCompactionLagMs = KafkaConfig.Defaults.LogCleanerMinCompactionLagMs;
        public static final long MaxCompactionLagMs = KafkaConfig.Defaults.LogCleanerMaxCompactionLagMs;
        public static final double MinCleanableDirtyRatio = KafkaConfig.Defaults.LogCleanerMinCleanRatio;
        public static final String CleanupPolicy = KafkaConfig.Defaults.LogCleanupPolicy;
        public static final boolean UncleanLeaderElectionEnable = KafkaConfig.Defaults.UncleanLeaderElectionEnable;
        public static final int MinInSyncReplicas = KafkaConfig.Defaults.MinInSyncReplicas;
        public static final String CompressionType = KafkaConfig.Defaults.CompressionType;
        public static final boolean PreAllocateEnable = KafkaConfig.Defaults.LogPreAllocateEnable;
        ;
        /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */;
        @Deprecated
        public static final String MessageFormatVersion = KafkaConfig.Defaults.LogMessageFormatVersion;
        ;
        public static final String MessageTimestampType = KafkaConfig.Defaults.LogMessageTimestampType;
        public static final long MessageTimestampDifferenceMaxMs = KafkaConfig.Defaults.LogMessageTimestampDifferenceMaxMs;
        public static final List<String> LeaderReplicationThrottledReplicas = new ArrayList<>();
        public static final List<String> FollowerReplicationThrottledReplicas = new ArrayList<>();
        public static final int MaxIdMapSnapshots = KafkaConfig.Defaults.MaxIdMapSnapshots;
        public static final boolean MessageDownConversionEnable = KafkaConfig.Defaults.MessageDownConversionEnable;
    }

    /*static字段*/
    public static final String SegmentBytesProp = TopicConfig.SEGMENT_BYTES_CONFIG;
    public static final String SegmentMsProp = TopicConfig.SEGMENT_MS_CONFIG;
    public static final String SegmentJitterMsProp = TopicConfig.SEGMENT_JITTER_MS_CONFIG;
    public static final String SegmentIndexBytesProp = TopicConfig.SEGMENT_INDEX_BYTES_CONFIG;
    public static final String FlushMessagesProp = TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG;
    public static final String FlushMsProp = TopicConfig.FLUSH_MS_CONFIG;
    public static final String RetentionBytesProp = TopicConfig.RETENTION_BYTES_CONFIG;
    public static final String RetentionMsProp = TopicConfig.RETENTION_MS_CONFIG;
    public static final String RemoteLogStorageEnableProp = TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
    public static final String LocalLogRetentionMsProp = TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG;
    public static final String LocalLogRetentionBytesProp = TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG;
    public static final String MaxMessageBytesProp = TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
    public static final String IndexIntervalBytesProp = TopicConfig.INDEX_INTERVAL_BYTES_CONFIG;
    public static final String DeleteRetentionMsProp = TopicConfig.DELETE_RETENTION_MS_CONFIG;
    public static final String MinCompactionLagMsProp = TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG;
    public static final String MaxCompactionLagMsProp = TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG;
    public static final String FileDeleteDelayMsProp = TopicConfig.FILE_DELETE_DELAY_MS_CONFIG;
    public static final String MinCleanableDirtyRatioProp = TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG;
    public static final String CleanupPolicyProp = TopicConfig.CLEANUP_POLICY_CONFIG;
    public static final String Delete = TopicConfig.CLEANUP_POLICY_DELETE;
    public static final String Compact = TopicConfig.CLEANUP_POLICY_COMPACT;
    public static final String UncleanLeaderElectionEnableProp = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;
    public static final String MinInSyncReplicasProp = TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
    public static final String CompressionTypeProp = TopicConfig.COMPRESSION_TYPE_CONFIG;
    public static final String PreAllocateEnableProp = TopicConfig.PREALLOCATE_CONFIG;
    ;
    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */;
    //    @deprecated("3.0") @nowarn("cat=deprecation")                                                                                                      ;
    public static final String MessageFormatVersionProp = TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;
    public static final String MessageTimestampTypeProp = TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG;
    public static final String MessageTimestampDifferenceMaxMsProp = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG;
    public static final String MessageDownConversionEnableProp = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG;
    ;
    // Leave these out of TopicConfig for now as they are replication quota configs                                                                    ;
    public static final String LeaderReplicationThrottledReplicasProp = "leader.replication.throttled.replicas";
    public static final String FollowerReplicationThrottledReplicasProp = "follower.replication.throttled.replicas";
    ;
    public static final String SegmentSizeDoc = TopicConfig.SEGMENT_BYTES_DOC;
    public static final String SegmentMsDoc = TopicConfig.SEGMENT_MS_DOC;
    public static final String SegmentJitterMsDoc = TopicConfig.SEGMENT_JITTER_MS_DOC;
    public static final String MaxIndexSizeDoc = TopicConfig.SEGMENT_INDEX_BYTES_DOC;
    public static final String FlushIntervalDoc = TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC;
    public static final String FlushMsDoc = TopicConfig.FLUSH_MS_DOC;
    public static final String RetentionSizeDoc = TopicConfig.RETENTION_BYTES_DOC;
    public static final String RetentionMsDoc = TopicConfig.RETENTION_MS_DOC;
    public static final String RemoteLogStorageEnableDoc = TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC;
    public static final String LocalLogRetentionMsDoc = TopicConfig.LOCAL_LOG_RETENTION_MS_DOC;
    public static final String LocalLogRetentionBytesDoc = TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC;
    public static final String MaxMessageSizeDoc = TopicConfig.MAX_MESSAGE_BYTES_DOC;
    public static final String IndexIntervalDoc = TopicConfig.INDEX_INTERVAL_BYTES_DOC;
    public static final String FileDeleteDelayMsDoc = TopicConfig.FILE_DELETE_DELAY_MS_DOC;
    public static final String DeleteRetentionMsDoc = TopicConfig.DELETE_RETENTION_MS_DOC;
    public static final String MinCompactionLagMsDoc = TopicConfig.MIN_COMPACTION_LAG_MS_DOC;
    public static final String MaxCompactionLagMsDoc = TopicConfig.MAX_COMPACTION_LAG_MS_DOC;
    public static final String MinCleanableRatioDoc = TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC;
    public static final String CompactDoc = TopicConfig.CLEANUP_POLICY_DOC;
    public static final String UncleanLeaderElectionEnableDoc = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC;
    public static final String MinInSyncReplicasDoc = TopicConfig.MIN_IN_SYNC_REPLICAS_DOC;
    public static final String CompressionTypeDoc = TopicConfig.COMPRESSION_TYPE_DOC;
    public static final String PreAllocateEnableDoc = TopicConfig.PREALLOCATE_DOC;
    ;
    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */;
    //    @deprecated("3.0") @nowarn("cat=deprecation")                                                                                                      ;
    public static final String MessageFormatVersionDoc = TopicConfig.MESSAGE_FORMAT_VERSION_DOC;
    ;
    public static final String MessageTimestampTypeDoc = TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC;
    public static final String MessageTimestampDifferenceMaxMsDoc = TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC;
    public static final String MessageDownConversionEnableDoc = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC;

    public static final String LeaderReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on " +
            "the leader side. The list should describe a set of replicas in the form " +
            "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
            "all replicas for this topic.";
    public static final String FollowerReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on " +
            "the follower side. The list should describe a set of " + "replicas in the form " +
            "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
            "all replicas for this topic.";

    protected static final String ServerDefaultHeaderName = "Server Default Property";

    public static final Set<String> configsWithNoServerDefaults = new HashSet<>(Arrays.asList(RemoteLogStorageEnableProp,
            LocalLogRetentionMsProp,
            LocalLogRetentionBytesProp));

    public static LogConfigDef configDef = new LogConfigDef()
            .define(SegmentBytesProp, INT, Defaults.SegmentSize, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), MEDIUM,
                    SegmentSizeDoc, KafkaConfig.LogSegmentBytesProp)
            .define(SegmentMsProp, LONG, Defaults.SegmentMs, atLeast(1), MEDIUM, SegmentMsDoc,
                    KafkaConfig.LogRollTimeMillisProp)
            .define(SegmentJitterMsProp, LONG, Defaults.SegmentJitterMs, atLeast(0), MEDIUM, SegmentJitterMsDoc,
                    KafkaConfig.LogRollTimeJitterMillisProp)
            .define(SegmentIndexBytesProp, INT, Defaults.MaxIndexSize, atLeast(4), MEDIUM, MaxIndexSizeDoc,
                    KafkaConfig.LogIndexSizeMaxBytesProp)
            .define(FlushMessagesProp, LONG, Defaults.FlushInterval, atLeast(1), MEDIUM, FlushIntervalDoc,
                    KafkaConfig.LogFlushIntervalMessagesProp)
            .define(FlushMsProp, LONG, Defaults.FlushMs, atLeast(0), MEDIUM, FlushMsDoc,
                    KafkaConfig.LogFlushIntervalMsProp)
            // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
            .define(RetentionBytesProp, LONG, Defaults.RetentionSize, MEDIUM, RetentionSizeDoc,
                    KafkaConfig.LogRetentionBytesProp)
            // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
            .define(RetentionMsProp, LONG, Defaults.RetentionMs, atLeast(-1), MEDIUM, RetentionMsDoc,
                    KafkaConfig.LogRetentionTimeMillisProp)
            .define(MaxMessageBytesProp, INT, Defaults.MaxMessageSize, atLeast(0), MEDIUM, MaxMessageSizeDoc,
                    KafkaConfig.MessageMaxBytesProp)
            .define(IndexIntervalBytesProp, INT, Defaults.IndexInterval, atLeast(0), MEDIUM, IndexIntervalDoc,
                    KafkaConfig.LogIndexIntervalBytesProp)
            .define(DeleteRetentionMsProp, LONG, Defaults.DeleteRetentionMs, atLeast(0), MEDIUM,
                    DeleteRetentionMsDoc, KafkaConfig.LogCleanerDeleteRetentionMsProp)
            .define(MinCompactionLagMsProp, LONG, Defaults.MinCompactionLagMs, atLeast(0), MEDIUM, MinCompactionLagMsDoc,
                    KafkaConfig.LogCleanerMinCompactionLagMsProp)
            .define(MaxCompactionLagMsProp, LONG, Defaults.MaxCompactionLagMs, atLeast(1), MEDIUM, MaxCompactionLagMsDoc,
                    KafkaConfig.LogCleanerMaxCompactionLagMsProp)
            .define(FileDeleteDelayMsProp, LONG, Defaults.FileDeleteDelayMs, atLeast(0), MEDIUM, FileDeleteDelayMsDoc,
                    KafkaConfig.LogDeleteDelayMsProp)
            .define(MinCleanableDirtyRatioProp, DOUBLE, Defaults.MinCleanableDirtyRatio, between(0, 1), MEDIUM,
                    MinCleanableRatioDoc, KafkaConfig.LogCleanerMinCleanRatioProp)
            .define(CleanupPolicyProp, LIST, Defaults.CleanupPolicy, ConfigDef.ValidList.in(LogConfig.Compact, LogConfig.Delete), MEDIUM, CompactDoc,
                    KafkaConfig.LogCleanupPolicyProp)
            .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable,
                    MEDIUM, UncleanLeaderElectionEnableDoc, KafkaConfig.UncleanLeaderElectionEnableProp)
            .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), MEDIUM, MinInSyncReplicasDoc,
                    KafkaConfig.MinInSyncReplicasProp)
            .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(CompressionCodec.BrokerCompressionCodec.brokerCompressionOptions.toArray(new String[0])),
                    MEDIUM, CompressionTypeDoc, KafkaConfig.CompressionTypeProp)
            .define(PreAllocateEnableProp, BOOLEAN, Defaults.PreAllocateEnable, MEDIUM, PreAllocateEnableDoc,
                    KafkaConfig.LogPreAllocateProp)
            .define(MessageFormatVersionProp, STRING, Defaults.MessageFormatVersion, new MetadataVersionValidator(), MEDIUM, MessageFormatVersionDoc,
                    KafkaConfig.LogMessageFormatVersionProp)
            .define(MessageTimestampTypeProp, STRING, Defaults.MessageTimestampType, in("CreateTime", "LogAppendTime"), MEDIUM, MessageTimestampTypeDoc,
                    KafkaConfig.LogMessageTimestampTypeProp)
            .define(MessageTimestampDifferenceMaxMsProp, LONG, Defaults.MessageTimestampDifferenceMaxMs,
                    atLeast(0), MEDIUM, MessageTimestampDifferenceMaxMsDoc, KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)
            .define(LeaderReplicationThrottledReplicasProp, LIST, Defaults.LeaderReplicationThrottledReplicas, new TopicConfigHandler.ThrottledReplicaListValidator(), MEDIUM,
                    LeaderReplicationThrottledReplicasDoc, LeaderReplicationThrottledReplicasProp)
            .define(FollowerReplicationThrottledReplicasProp, LIST, Defaults.FollowerReplicationThrottledReplicas, new TopicConfigHandler.ThrottledReplicaListValidator(), MEDIUM,
                    FollowerReplicationThrottledReplicasDoc, FollowerReplicationThrottledReplicasProp)
            .define(MessageDownConversionEnableProp, BOOLEAN, Defaults.MessageDownConversionEnable, LOW,
                    MessageDownConversionEnableDoc, KafkaConfig.LogMessageDownConversionEnableProp);


    static {
        // RemoteLogStorageEnableProp, LocalLogRetentionMsProp, LocalLogRetentionBytesProp do not have server default
        // config names.
        // This define method is not overridden in LogConfig as these configs do not have server defaults yet.
        configDef.defineInternal(RemoteLogStorageEnableProp, BOOLEAN, Defaults.RemoteLogStorageEnable, null, MEDIUM, RemoteLogStorageEnableDoc)
                .defineInternal(LocalLogRetentionMsProp, LONG, Defaults.LocalRetentionMs, atLeast(-2), MEDIUM, LocalLogRetentionMsDoc)
                .defineInternal(LocalLogRetentionBytesProp, LONG, Defaults.LocalRetentionBytes, atLeast(-2), MEDIUM, LocalLogRetentionBytesDoc);
    }

    /**
     * Maps topic configurations to their equivalent broker configurations.
     * <p>
     * Topics can be configured either by setting their dynamic topic configurations, or by
     * setting equivalent broker configurations. For historical reasons, the equivalent broker
     * configurations have different names. This table maps each topic configuration to its
     * equivalent broker configurations.
     * <p>
     * In some cases, the equivalent broker configurations must be transformed before they
     * can be used. For example, log.roll.hours must be converted to milliseconds before it
     * can be used as the value of segment.ms.
     * <p>
     * The broker configurations will be used in the order specified here. In other words, if
     * both the first and the second synonyms are configured, we will use only the value of
     * the first synonym and ignore the second.
     */
//    @nowarn("cat=deprecation")
    public static Map<String, List<ConfigSynonym>> AllTopicConfigSynonyms = new HashMap<String, List<ConfigSynonym>>() {{
        put(SegmentBytesProp, Arrays.asList(new ConfigSynonym(KafkaConfig.LogSegmentBytesProp)));
        put(SegmentMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogRollTimeMillisProp),
                new ConfigSynonym(KafkaConfig.LogRollTimeHoursProp, ConfigSynonym.HOURS_TO_MILLISECONDS)));
        put(SegmentJitterMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogRollTimeJitterMillisProp),
                new ConfigSynonym(KafkaConfig.LogRollTimeJitterHoursProp, ConfigSynonym.HOURS_TO_MILLISECONDS)));
        put(SegmentIndexBytesProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogIndexSizeMaxBytesProp)));
        put(FlushMessagesProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogFlushIntervalMessagesProp)));
        put(FlushMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogFlushIntervalMsProp),
                new ConfigSynonym(KafkaConfig.LogFlushSchedulerIntervalMsProp)));
        put(RetentionBytesProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogRetentionBytesProp)));
        put(RetentionMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogRetentionTimeMillisProp),
                new ConfigSynonym(KafkaConfig.LogRetentionTimeMinutesProp, ConfigSynonym.MINUTES_TO_MILLISECONDS),
                new ConfigSynonym(KafkaConfig.LogRetentionTimeHoursProp, ConfigSynonym.HOURS_TO_MILLISECONDS)));
        put(MaxMessageBytesProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.MessageMaxBytesProp)));
        put(IndexIntervalBytesProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogIndexIntervalBytesProp)));
        put(DeleteRetentionMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogCleanerDeleteRetentionMsProp)));
        put(MinCompactionLagMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogCleanerMinCompactionLagMsProp)));
        put(MaxCompactionLagMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogCleanerMaxCompactionLagMsProp)));
        put(FileDeleteDelayMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogDeleteDelayMsProp)));
        put(MinCleanableDirtyRatioProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogCleanerMinCleanRatioProp)));
        put(CleanupPolicyProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogCleanupPolicyProp)));
        put(UncleanLeaderElectionEnableProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.UncleanLeaderElectionEnableProp)));
        put(MinInSyncReplicasProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.MinInSyncReplicasProp)));
        put(CompressionTypeProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.CompressionTypeProp)));
        put(PreAllocateEnableProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogPreAllocateProp)));
        put(MessageFormatVersionProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogMessageFormatVersionProp)));
        put(MessageTimestampTypeProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogMessageTimestampTypeProp)));
        put(MessageTimestampDifferenceMaxMsProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)));
        put(MessageDownConversionEnableProp, Arrays.asList(
                new ConfigSynonym(KafkaConfig.LogMessageDownConversionEnableProp)));
    }};

    /**
     * Map topic config to the broker config with highest priority. Some of these have additional synonyms
     * that can be obtained using [[kafka.server.DynamicBrokerConfig#brokerConfigSynonyms]]
     * or using [[AllTopicConfigSynonyms]]
     */
    public static Map<String, String> TopicConfigSynonyms = AllTopicConfigSynonyms.entrySet()
            .stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(0).name()));
    /*static字段*/

    /*非static字段*/
    private Map<Object, Object> props;
    private Set<String> overriddenConfigs;

    /**
     * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
     * should also go in [[LogConfig.extractLogConfigMap()]].
     */
    private Integer segmentSize = getInt(LogConfig.SegmentBytesProp);
    private Long segmentMs = getLong(LogConfig.SegmentMsProp);
    private Long segmentJitterMs = getLong(LogConfig.SegmentJitterMsProp);
    private Integer maxIndexSize = getInt(LogConfig.SegmentIndexBytesProp);
    private Long flushInterval = getLong(LogConfig.FlushMessagesProp);
    private Long flushMs = getLong(LogConfig.FlushMsProp);
    private Long retentionSize = getLong(LogConfig.RetentionBytesProp);
    private Long retentionMs = getLong(LogConfig.RetentionMsProp);
    private Integer maxMessageSize = getInt(LogConfig.MaxMessageBytesProp);
    private Integer indexInterval = getInt(LogConfig.IndexIntervalBytesProp);
    private Long fileDeleteDelayMs = getLong(LogConfig.FileDeleteDelayMsProp);
    private Long deleteRetentionMs = getLong(LogConfig.DeleteRetentionMsProp);
    private Long compactionLagMs = getLong(LogConfig.MinCompactionLagMsProp);
    private Long maxCompactionLagMs = getLong(LogConfig.MaxCompactionLagMsProp);
    private Double minCleanableRatio = getDouble(LogConfig.MinCleanableDirtyRatioProp);
    private Boolean compact = getList(LogConfig.CleanupPolicyProp)
            .stream()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet())
            .contains(LogConfig.Compact);
    private Boolean delete = getList(LogConfig.CleanupPolicyProp)
            .stream()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet())
            .contains(LogConfig.Delete);
    private Boolean uncleanLeaderElectionEnable = getBoolean(LogConfig.UncleanLeaderElectionEnableProp);
    private Integer minInSyncReplicas = getInt(LogConfig.MinInSyncReplicasProp);
    private String compressionType = getString(LogConfig.CompressionTypeProp).toLowerCase(Locale.ROOT);
    private Boolean preallocate = getBoolean(LogConfig.PreAllocateEnableProp);

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    @Deprecated
    private MetadataVersion messageFormatVersion = MetadataVersion.fromVersionString(getString(LogConfig.MessageFormatVersionProp));

    private TimestampType messageTimestampType = TimestampType.forName(getString(LogConfig.MessageTimestampTypeProp));
    private Long messageTimestampDifferenceMaxMs = getLong(LogConfig.MessageTimestampDifferenceMaxMsProp);
    private List<String> LeaderReplicationThrottledReplicas = getList(LogConfig.LeaderReplicationThrottledReplicasProp);
    private List<String> FollowerReplicationThrottledReplicas = getList(LogConfig.FollowerReplicationThrottledReplicasProp);
    private Boolean messageDownConversionEnable = getBoolean(LogConfig.MessageDownConversionEnableProp);

    private RemoteLogConfig _remoteLogConfig = new RemoteLogConfig();

    private Random random = new Random();
    /*非static字段*/

    public LogConfig() {
        this(new Properties(), new HashSet<>());
    }

    public LogConfig(Map<Object, Object> props) {
        this(props, new HashSet<>());
    }

    public LogConfig(Map<Object, Object> props, Set<String> overriddenConfigs) {
        super(LogConfig.configDef, props, false);
        this.props = props;
        this.overriddenConfigs = overriddenConfigs;
    }

    public RecordVersion recordVersion() {
        return messageFormatVersion.highestSupportedRecordVersion();
    }

    public Long randomSegmentJitter() {
        if (segmentJitterMs == 0) {
            return 0L;
        } else {
            return Utils.abs(random.nextInt()) % Math.min(segmentJitterMs, segmentMs);
        }
    }

    public Long maxSegmentMs() {
        if (compact && maxCompactionLagMs > 0) {
            return Math.min(maxCompactionLagMs, segmentMs);
        } else {
            return segmentMs;
        }
    }

    public Integer initFileSize() {
        if (preallocate) {
            return segmentSize;
        } else {
            return 0;
        }
    }

    public String overriddenConfigsAsLoggableString() {
        Map<String, Object> overriddenTopicProps = new HashMap<>();
        this.props.forEach((k, v) -> {
            if (this.overriddenConfigs.contains(k)) {
                overriddenTopicProps.put(k.toString(), v);
            }
        });
        return ConfigUtils.configMapToRedactedString(overriddenTopicProps, configDef);
    }


    /*static方法*/
    // Package private for testing, return a copy since it's a mutable global variable
    protected static LogConfigDef configDefCopy() {
        return new LogConfigDef(configDef);
    }

    public static LogConfig apply() {
        return new LogConfig(new HashMap<>());
    }

    public static List<String> configNames() {
        List<String> names = new ArrayList<>(configDef.names());
        Collections.sort(names);
        return names;
    }

    public static Optional<String> serverConfigName(String configName) {
        return configDef.serverConfigName(configName);
    }

    public static Optional<ConfigDef.Type> configType(String configName) {
        return Optional.ofNullable(configDef.configKeys().get(configName))
                .map(ConfigDef.ConfigKey::type);
    }

    /**
     * Create a log config instance using the given properties and defaults
     */
    public static LogConfig fromProps(Map<String, Object> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        Set<String> overriddenKeys = overrides.keySet().stream().map(key -> (String) key).collect(Collectors.toSet());
        return new LogConfig(props, overriddenKeys);
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        List<String> names = configNames();
        for (Object name : props.keySet()) {
            if (!names.contains(name.toString())) {
                throw new InvalidConfigurationException("Unknown topic config name: " + name);
            }
        }
    }

    protected static Map<String, ConfigDef.ConfigKey> configKeys() {
        return configDef.configKeys();
    }

    public static void validateValues(Map<Object, Object> props) {
        Long minCompactionLag = (Long) props.get(MinCompactionLagMsProp);
        Long maxCompactionLag = (Long) props.get(MaxCompactionLagMsProp);
        if (minCompactionLag > maxCompactionLag) {
            String msg = String.format("conflict topic config setting %s (%s) > %s (%s)",
                    MinCompactionLagMsProp, minCompactionLag, MaxCompactionLagMsProp, maxCompactionLag);
            throw new InvalidConfigurationException(msg);
        }
    }

    /**
     * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
     */
    public static void validate(Properties props) {
        validateNames(props);
        Map<String, Object> valueMaps = configDef.parse(props);
        validateValues(new HashMap<>(valueMaps));
    }

    /**
     * Copy the subset of properties that are relevant to Logs. The individual properties
     * are listed here since the names are slightly different in each Config class...
     */
//    @nowarn("cat=deprecation")
    public static Map<String, Object> extractLogConfigMap(KafkaConfig kafkaConfig) {
        Map<String, Object> logProps = new HashMap<>();
        logProps.put(SegmentBytesProp, kafkaConfig.logSegmentBytes());
        logProps.put(SegmentMsProp, kafkaConfig.logRollTimeMillis());
        logProps.put(SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis());
        logProps.put(SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes());
        logProps.put(FlushMessagesProp, kafkaConfig.logFlushIntervalMessages());
        logProps.put(FlushMsProp, kafkaConfig.logFlushIntervalMs());
        logProps.put(RetentionBytesProp, kafkaConfig.logRetentionBytes());
        logProps.put(RetentionMsProp, kafkaConfig.logRetentionTimeMillis());
        logProps.put(MaxMessageBytesProp, kafkaConfig.messageMaxBytes());
        logProps.put(IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes());
        logProps.put(DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs());
        logProps.put(MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs());
        logProps.put(MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs());
        logProps.put(FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs());
        logProps.put(MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio());
        logProps.put(CleanupPolicyProp, kafkaConfig.logCleanupPolicy());
        logProps.put(MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas());
        logProps.put(CompressionTypeProp, kafkaConfig.compressionType());
        logProps.put(UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable());
        logProps.put(PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable());
        logProps.put(MessageFormatVersionProp, kafkaConfig.getLogMessageFormatVersion().version());
        logProps.put(MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType().name);
        logProps.put(MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs())
        ;
        logProps.put(MessageDownConversionEnableProp, kafkaConfig.logMessageDownConversionEnable());
        return logProps;
    }
    public static Boolean shouldIgnoreMessageFormatVersion(MetadataVersion interBrokerProtocolVersion) {
        return interBrokerProtocolVersion.isAtLeast(MetadataVersion.IBP_3_0_IV1);
    }

    static class LogConfigDef extends ConfigDef {
        private final Map<String, String> serverDefaultConfigNames = new HashMap<>();

        public LogConfigDef() {
            this(new ConfigDef());
        }

        public LogConfigDef(ConfigDef base) {
            super(base);

            if (base instanceof LogConfigDef) {
                serverDefaultConfigNames.putAll(((LogConfigDef) base).serverDefaultConfigNames);
            }
        }

        public LogConfigDef define(String name, Type defType, Object defaultValue, Validator validator,
                                   Importance importance, String doc, String serverDefaultConfigName) {
            super.define(name, defType, defaultValue, validator, importance, doc);
            serverDefaultConfigNames.put(name, serverDefaultConfigName);
            return this;
        }

        public LogConfigDef define(String name, Type defType, Object defaultValue, Importance importance,
                                   String documentation, String serverDefaultConfigName) {
            super.define(name, defType, defaultValue, importance, documentation);
            serverDefaultConfigNames.put(name, serverDefaultConfigName);
            return this;
        }

        public LogConfigDef define(String name, Type defType, Importance importance, String documentation,
                                   String serverDefaultConfigName) {
            super.define(name, defType, importance, documentation);
            serverDefaultConfigNames.put(name, serverDefaultConfigName);
            return this;
        }

        @Override
        public List<String> headers() {
            return Arrays.asList("Name", "Description", "Type", "Default", "Valid Values", ServerDefaultHeaderName, "Importance");
        }

        @Override
        public String getConfigValue(ConfigKey key, String headerName) {
            if (ServerDefaultHeaderName.equals(headerName)) {
                return serverDefaultConfigNames.getOrDefault(key.name, null);
            } else {
                return super.getConfigValue(key, headerName);
            }
        }

        public Optional<String> serverConfigName(String configName) {
            return Optional.ofNullable(serverDefaultConfigNames.get(configName));
        }
    }

    class RemoteLogConfig {
        private Boolean remoteStorageEnable;
        private Long localRetentionMs;
        private Long localRetentionBytes;

        public RemoteLogConfig() {
            this.remoteStorageEnable = getBoolean(LogConfig.RemoteLogStorageEnableProp);
            initLocalRetentionMs();
            initLocalRetentionBytes();
        }

        private void initLocalRetentionMs() {
            long localLogRetentionMs = getLong(LogConfig.LocalLogRetentionMsProp);
            // -2 indicates to derive value from retentionMs property.
            if (localLogRetentionMs == -2) {
                this.localRetentionMs = retentionMs;
            } else {
                // Added validation here to check the effective value should not be more than RetentionMs.
                if (localLogRetentionMs == -1 && retentionMs != -1) {
                    String msg = String.format("Value must not be -1 as %s value is set as %s.", LogConfig.RetentionMsProp, retentionMs);
                    throw new ConfigException(LogConfig.LocalLogRetentionMsProp, localLogRetentionMs, msg);
                }

                if (localLogRetentionMs > retentionMs) {
                    String msg = String.format("Value must not be more than property: %s value.", LogConfig.RetentionMsProp);
                    throw new ConfigException(LogConfig.LocalLogRetentionMsProp, localLogRetentionMs, msg);
                }

                this.localRetentionMs = localLogRetentionMs;
            }
        }

        private void initLocalRetentionBytes() {
            long localLogRetentionBytes = getLong(LogConfig.LocalLogRetentionBytesProp);

            // -2 indicates to derive value from retentionSize property.
            if (localLogRetentionBytes == -2) {
                this.localRetentionBytes = retentionSize;
            } else {
                // Added validation here to check the effective value should not be more than RetentionBytes.
                if (localLogRetentionBytes == -1 && retentionSize != -1) {
                    String msg = String.format("Value must not be -1 as %s value is set as %s.", LogConfig.RetentionBytesProp, retentionSize);
                    throw new ConfigException(LogConfig.LocalLogRetentionBytesProp, localLogRetentionBytes, msg);
                }

                if (localLogRetentionBytes > retentionSize) {
                    String msg = String.format("Value must not be more than property: %s value.", LogConfig.RetentionBytesProp);
                    throw new ConfigException(LogConfig.LocalLogRetentionBytesProp, localLogRetentionBytes, msg);
                }

                this.localRetentionBytes = localLogRetentionBytes;
            }
        }

    }
}
