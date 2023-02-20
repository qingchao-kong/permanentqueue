package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.cluster.EndPoint;
import cn.pockethub.permanentqueue.kafka.coordinator.group.OffsetConfig;
import cn.pockethub.permanentqueue.kafka.coordinator.transaction.TransactionLog;
import cn.pockethub.permanentqueue.kafka.coordinator.transaction.TransactionStateManager;
import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.raft.RaftConfig;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersionValidator;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.CoreUtils;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.config.ConfigDef.*;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static cn.pockethub.permanentqueue.kafka.server.KafkaRaftServer.ProcessRole.BrokerRole;
import static cn.pockethub.permanentqueue.kafka.server.KafkaRaftServer.ProcessRole.ControllerRole;
import static org.apache.kafka.common.config.ConfigDef.Importance.*;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.*;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

@Getter
public class KafkaConfig extends AbstractConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfig.class);

    public static class Defaults {
        /*------------------- Zookeeper Configuration -------------------*/
        public static final int ZkSessionTimeoutMs = 18000;
        public static final boolean ZkEnableSecureAcls = false;
        public static final int ZkMaxInFlightRequests = 10;
        public static final boolean ZkSslClientEnable = false;
        public static final String ZkSslProtocol = "TLSv1.2";
        public static final String ZkSslEndpointIdentificationAlgorithm = "HTTPS";
        public static final boolean ZkSslCrlEnable = false;
        public static final boolean ZkSslOcspEnable = false;

        /*------------------- General Configuration -------------------*/
        public static final boolean BrokerIdGenerationEnable = true;
        public static final int MaxReservedBrokerId = 1000;
        public static final int BrokerId = -1;
        public static final int MessageMaxBytes = 1024 * 1024 + Records.LOG_OVERHEAD;
        public static final int NumNetworkThreads = 3;
        public static final int NumIoThreads = 8;
        public static final int BackgroundThreads = 10;

        public static final int QueuedMaxRequests = 500;
        public static final int QueuedMaxRequestBytes = -1;
        public static final int InitialBrokerRegistrationTimeoutMs = 60000;
        public static final int BrokerHeartbeatIntervalMs = 2000;
        public static final int BrokerSessionTimeoutMs = 9000;
        public static final int MetadataSnapshotMaxNewRecordBytes = 20 * 1024 * 1024;
        public static final int MetadataMaxIdleIntervalMs = 500;

        /**
         * KRaft mode configs
         */
        public static final int EmptyNodeId = -1;

        /*------------------- Authorizer Configuration -------------------*/
        public static final String AuthorizerClassName = "";

        /*------------------- Socket Server Configuration -------------------*/
        public static final String Listeners = "PLAINTEXT://:9092";
        public static final String ListenerSecurityProtocolMap;

        static {
            StringBuilder builder = new StringBuilder();
            Iterator<Map.Entry<ListenerName, SecurityProtocol>> iterator = EndPoint.DefaultSecurityProtocolMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ListenerName, SecurityProtocol> next = iterator.next();
                ListenerName listenerName = next.getKey();
                SecurityProtocol securityProtocol = next.getValue();
                builder.append(listenerName.value()).append(":").append(securityProtocol.name);
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
            ListenerSecurityProtocolMap = builder.toString();
        }

        public static final int SocketSendBufferBytes = 100 * 1024;
        public static final int SocketReceiveBufferBytes = 100 * 1024;
        public static final int SocketRequestMaxBytes = 100 * 1024 * 1024;
        public static final int SocketListenBacklogSize = 50;
        public static final int MaxConnectionsPerIp = Integer.MAX_VALUE;
        public static final String MaxConnectionsPerIpOverrides = "";
        public static final int MaxConnections = Integer.MAX_VALUE;
        public static final int MaxConnectionCreationRate = Integer.MAX_VALUE;
        public static final long ConnectionsMaxIdleMs = 10 * 60 * 1000L;
        public static final int RequestTimeoutMs = 30000;
        //        val ConnectionSetupTimeoutMs = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS;
//        val ConnectionSetupTimeoutMaxMs = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
        public static final long ConnectionSetupTimeoutMs = 10 * 1000L;
        public static final long ConnectionSetupTimeoutMaxMs = 30 * 1000L;
        public static final int FailedAuthenticationDelayMs = 100;

        /*------------------- Log Configuration -------------------*/
        public static final int NumPartitions = 1;
        public static final String LogDir = "/tmp/kafka-logs";
        public static final int LogSegmentBytes = 1 * 1024 * 1024 * 1024;
        public static final int LogRollHours = 24 * 7;
        public static final int LogRollJitterHours = 0;
        public static final int LogRetentionHours = 24 * 7;

        public static final long LogRetentionBytes = -1L;
        public static final long LogCleanupIntervalMs = 5 * 60 * 1000L;
        public static final String Delete = "delete";
        public static final String Compact = "compact";
        public static final String LogCleanupPolicy = Delete;
        public static final int LogCleanerThreads = 1;
        public static final Double LogCleanerIoMaxBytesPerSecond = Double.MAX_VALUE;
        public static final long LogCleanerDedupeBufferSize = 128 * 1024 * 1024L;
        public static final int LogCleanerIoBufferSize = 512 * 1024;
        public static final double LogCleanerDedupeBufferLoadFactor = 0.9d;
        public static final int LogCleanerBackoffMs = 15 * 1000;
        public static final double LogCleanerMinCleanRatio = 0.5d;
        public static final boolean LogCleanerEnable = true;
        public static final long LogCleanerDeleteRetentionMs = 24 * 60 * 60 * 1000L;
        public static final long LogCleanerMinCompactionLagMs = 0L;
        public static final long LogCleanerMaxCompactionLagMs = Long.MAX_VALUE;
        public static final int LogIndexSizeMaxBytes = 10 * 1024 * 1024;
        public static final int LogIndexIntervalBytes = 4096;
        public static final long LogFlushIntervalMessages = Long.MAX_VALUE;
        public static final int LogDeleteDelayMs = 60000;
        public static final long LogFlushSchedulerIntervalMs = Long.MAX_VALUE;
        public static final int LogFlushOffsetCheckpointIntervalMs = 60000;
        public static final int LogFlushStartOffsetCheckpointIntervalMs = 60000;
        public static final boolean LogPreAllocateEnable = false;

        /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
//        @Deprecated
        public static final String LogMessageFormatVersion = MetadataVersion.IBP_3_0_IV1.version();

        public static final String LogMessageTimestampType = "CreateTime";
        public static final long LogMessageTimestampDifferenceMaxMs = Long.MAX_VALUE;
        public static final int NumRecoveryThreadsPerDataDir = 1;
        public static final boolean AutoCreateTopicsEnable = true;
        public static final int MinInSyncReplicas = 1;
        public static final boolean MessageDownConversionEnable = true;

        /*------------------- Replication configuration -------------------*/
        public static final int ControllerSocketTimeoutMs = RequestTimeoutMs;
        public static final int ControllerMessageQueueSize = Integer.MAX_VALUE;
        public static final int DefaultReplicationFactor = 1;
        public static final long ReplicaLagTimeMaxMs = 30000L;
        public static final int ReplicaSocketTimeoutMs = 30 * 1000;
        public static final int ReplicaSocketReceiveBufferBytes = 64 * 1024;
        public static final int ReplicaFetchMaxBytes = 1024 * 1024;
        public static final int ReplicaFetchWaitMaxMs = 500;
        public static final int ReplicaFetchMinBytes = 1;
        public static final int ReplicaFetchResponseMaxBytes = 10 * 1024 * 1024;
        public static final int NumReplicaFetchers = 1;
        public static final int ReplicaFetchBackoffMs = 1000;
        public static final long ReplicaHighWatermarkCheckpointIntervalMs = 5000L;
        public static final int FetchPurgatoryPurgeIntervalRequests = 1000;
        public static final int ProducerPurgatoryPurgeIntervalRequests = 1000;
        public static final int DeleteRecordsPurgatoryPurgeIntervalRequests = 1;
        public static final boolean AutoLeaderRebalanceEnable = true;
        public static final int LeaderImbalancePerBrokerPercentage = 10;
        public static final int LeaderImbalanceCheckIntervalSeconds = 300;
        public static final boolean UncleanLeaderElectionEnable = false;
        public static final String InterBrokerSecurityProtocol = SecurityProtocol.PLAINTEXT.name;
        public static final String InterBrokerProtocolVersion = MetadataVersion.latest().version();

        /*------------------- Controlled shutdown configuration -------------------*/
        public static final int ControlledShutdownMaxRetries = 3;
        public static final int ControlledShutdownRetryBackoffMs = 5000;
        public static final boolean ControlledShutdownEnable = true;

        /*------------------- Group coordinator configuration -------------------*/
        public static final int GroupMinSessionTimeoutMs = 6000;
        public static final int GroupMaxSessionTimeoutMs = 1800000;
        public static final int GroupInitialRebalanceDelayMs = 3000;
        public static final int GroupMaxSize = Integer.MAX_VALUE;

        /*------------------- Offset management configuration -------------------*/
        public static final int OffsetMetadataMaxSize = OffsetConfig.DefaultMaxMetadataSize;
        public static final int OffsetsLoadBufferSize = OffsetConfig.DefaultLoadBufferSize;
        public static final short OffsetsTopicReplicationFactor = OffsetConfig.DefaultOffsetsTopicReplicationFactor;
        public static final int OffsetsTopicPartitions = OffsetConfig.DefaultOffsetsTopicNumPartitions;
        public static final int OffsetsTopicSegmentBytes = OffsetConfig.DefaultOffsetsTopicSegmentBytes;
        public static final int OffsetsTopicCompressionCodec = OffsetConfig.DefaultOffsetsTopicCompressionCodec.getCodec();
        public static final int OffsetsRetentionMinutes = 7 * 24 * 60;
        public static final long OffsetsRetentionCheckIntervalMs = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs;
        public static final int OffsetCommitTimeoutMs = OffsetConfig.DefaultOffsetCommitTimeoutMs;
        public static final short OffsetCommitRequiredAcks = OffsetConfig.DefaultOffsetCommitRequiredAcks;

        /*------------------- Transaction management configuration -------------------*/
        public static final int TransactionalIdExpirationMs = TransactionStateManager.DefaultTransactionalIdExpirationMs;
        public static final int TransactionsMaxTimeoutMs = TransactionStateManager.DefaultTransactionsMaxTimeoutMs;
        public static final int TransactionsTopicMinISR = TransactionLog.DefaultMinInSyncReplicas;
        public static final int TransactionsLoadBufferSize = TransactionLog.DefaultLoadBufferSize;
        public static final short TransactionsTopicReplicationFactor = TransactionLog.DefaultReplicationFactor;
        public static final int TransactionsTopicPartitions = TransactionLog.DefaultNumPartitions;
        public static final int TransactionsTopicSegmentBytes = TransactionLog.DefaultSegmentBytes;
        public static final int TransactionsAbortTimedOutTransactionsCleanupIntervalMS = TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs;
        public static final int TransactionsRemoveExpiredTransactionsCleanupIntervalMS = TransactionStateManager.DefaultRemoveExpiredTransactionalIdsIntervalMs;

        /*------------------- Fetch Configuration ------------------*/
        public static final int MaxIncrementalFetchSessionCacheSlots = 1000;
        public static final int FetchMaxBytes = 55 * 1024 * 1024;

        /*------------------- Quota Configuration -------------------*/
        public static final int NumQuotaSamples = ClientQuotaManagerConfig.DefaultNumQuotaSamples;
        public static final int QuotaWindowSizeSeconds = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds;
        public static final int NumReplicationQuotaSamples = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples;
        public static final int ReplicationQuotaWindowSizeSeconds = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds;
        public static final int NumAlterLogDirsReplicationQuotaSamples = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples;
        public static final int AlterLogDirsReplicationQuotaWindowSizeSeconds = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds;
        public static final int NumControllerQuotaSamples = ClientQuotaManagerConfig.DefaultNumQuotaSamples;
        public static final int ControllerQuotaWindowSizeSeconds = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds;

        /*------------------- Transaction Configuration -------------------*/
        public static final int TransactionalIdExpirationMsDefault = 604800000;

        public static final boolean DeleteTopicEnable = true;

        public static final String CompressionType = CompressionCodec.ProducerCompressionCodec.getName();

        public static final int MaxIdMapSnapshots = 2;

        /*------------------- Kafka Metrics Configuration -------------------*/
        public static final int MetricNumSamples = 2;
        public static final int MetricSampleWindowMs = 30000;
        public static final String MetricReporterClasses = "";
        public static final String MetricRecordingLevel = Sensor.RecordingLevel.INFO.toString();

        /*------------------- Kafka Yammer Metrics Reporter Configuration -------------------*/
        public static final String KafkaMetricReporterClasses = "";
        public static final int KafkaMetricsPollingIntervalSeconds = 10;

        /*------------------- SSL configuration -------------------*/
        public static final String SslProtocol = SslConfigs.DEFAULT_SSL_PROTOCOL;
        public static final String SslEnabledProtocols = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS;
        public static final String SslKeystoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE;
        public static final String SslTruststoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;
        public static final String SslKeyManagerAlgorithm = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM;
        public static final String SslTrustManagerAlgorithm = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM;
        public static final String SslEndpointIdentificationAlgorithm = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;
        public static final String SslClientAuthentication = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT);
        public static final List<String> SslClientAuthenticationValidValues = SslClientAuth.VALUES.stream().map(v -> v.toString().toLowerCase(Locale.ROOT)).collect(Collectors.toList());
        public static final String SslPrincipalMappingRules = BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES;

        /*------------------- General Security configuration -------------------*/
        public static final long ConnectionsMaxReauthMsDefault = 0L;
        public static final int DefaultServerMaxMaxReceiveSize = BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE;
        public static final Class<DefaultKafkaPrincipalBuilder> DefaultPrincipalSerde = DefaultKafkaPrincipalBuilder.class;

        /*------------------- Sasl configuration -------------------*/
        public static final String SaslMechanismInterBrokerProtocol = SaslConfigs.DEFAULT_SASL_MECHANISM;
        public static final List<String> SaslEnabledMechanisms = BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS;
        public static final String SaslKerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD;
        public static final double SaslKerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;
        public static final double SaslKerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER;
        public static final long SaslKerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN;
        public static final List<String> SaslKerberosPrincipalToLocalRules = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES;
        public static final double SaslLoginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR;
        public static final double SaslLoginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER;
        public static final short SaslLoginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
        public static final short SaslLoginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS;
        public static final long SaslLoginRetryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
        public static final long SaslLoginRetryBackoffMs = SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS;
        public static final String SaslOAuthBearerScopeClaimName = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
        public static final String SaslOAuthBearerSubClaimName = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME;
        public static final long SaslOAuthBearerJwksEndpointRefreshMs = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
        public static final long SaslOAuthBearerJwksEndpointRetryBackoffMaxMs = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
        public static final long SaslOAuthBearerJwksEndpointRetryBackoffMs = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
        public static final int SaslOAuthBearerClockSkewSeconds = SaslConfigs.DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;

        /*------------------- Delegation Token configuration -------------------*/
        public static final long DelegationTokenMaxLifeTimeMsDefault = 7 * 24 * 60 * 60 * 1000L;
        public static final long DelegationTokenExpiryTimeMsDefault = 24 * 60 * 60 * 1000L;
        public static final long DelegationTokenExpiryCheckIntervalMsDefault = 1 * 60 * 60 * 1000L;

        /*------------------- Password encryption configuration for dynamic configs -------------------*/
        public static final String PasswordEncoderCipherAlgorithm = "AES/CBC/PKCS5Padding";
        public static final int PasswordEncoderKeyLength = 128;
        public static final int PasswordEncoderIterations = 4096;

        /*------------------- Raft Quorum Configuration -------------------*/
        public static final List<String> QuorumVoters = RaftConfig.DEFAULT_QUORUM_VOTERS;
        public static final int QuorumElectionTimeoutMs = RaftConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS;
        public static final int QuorumFetchTimeoutMs = RaftConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS;
        public static final int QuorumElectionBackoffMs = RaftConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS;
        public static final int QuorumLingerMs = RaftConfig.DEFAULT_QUORUM_LINGER_MS;
        public static final int QuorumRequestTimeoutMs = RaftConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS;
        public static final int QuorumRetryBackoffMs = RaftConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS;
    }

    private static final String LogConfigPrefix = "log.";

    /*静态成员*/
    /*----------- Zookeeper Configuration ----------------*/
    public static final String ZkConnectProp = "zookeeper.connect";
    public static final String ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms";
    public static final String ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms";
    public static final String ZkEnableSecureAclsProp = "zookeeper.set.acl";
    public static final String ZkMaxInFlightRequestsProp = "zookeeper.max.in.flight.requests";
    public static final String ZkSslClientEnableProp = "zookeeper.ssl.client.enable";
    public static final String ZkClientCnxnSocketProp = "zookeeper.clientCnxnSocket";
    public static final String ZkSslKeyStoreLocationProp = "zookeeper.ssl.keystore.location";
    public static final String ZkSslKeyStorePasswordProp = "zookeeper.ssl.keystore.password";
    public static final String ZkSslKeyStoreTypeProp = "zookeeper.ssl.keystore.type";
    public static final String ZkSslTrustStoreLocationProp = "zookeeper.ssl.truststore.location";
    public static final String ZkSslTrustStorePasswordProp = "zookeeper.ssl.truststore.password";
    public static final String ZkSslTrustStoreTypeProp = "zookeeper.ssl.truststore.type";
    public static final String ZkSslProtocolProp = "zookeeper.ssl.protocol";
    public static final String ZkSslEnabledProtocolsProp = "zookeeper.ssl.enabled.protocols";
    public static final String ZkSslCipherSuitesProp = "zookeeper.ssl.cipher.suites";
    public static final String ZkSslEndpointIdentificationAlgorithmProp = "zookeeper.ssl.endpoint.identification.algorithm";
    public static final String ZkSslCrlEnableProp = "zookeeper.ssl.crl.enable";
    public static final String ZkSslOcspEnableProp = "zookeeper.ssl.ocsp.enable";

    // a map from the Kafka config to the corresponding ZooKeeper Java system property
    protected static Map<String, String> ZkSslConfigToSystemPropertyMap = new HashMap<String, String>() {{
        put(ZkSslClientEnableProp, ZKClientConfig.SECURE_CLIENT);
        put(ZkClientCnxnSocketProp, ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
        put(ZkSslKeyStoreLocationProp, "zookeeper.ssl.keyStore.location");
        put(ZkSslKeyStorePasswordProp, "zookeeper.ssl.keyStore.password");
        put(ZkSslKeyStoreTypeProp, "zookeeper.ssl.keyStore.type");
        put(ZkSslTrustStoreLocationProp, "zookeeper.ssl.trustStore.location");
        put(ZkSslTrustStorePasswordProp, "zookeeper.ssl.trustStore.password");
        put(ZkSslTrustStoreTypeProp, "zookeeper.ssl.trustStore.type");
        put(ZkSslProtocolProp, "zookeeper.ssl.protocol");
        put(ZkSslEnabledProtocolsProp, "zookeeper.ssl.enabledProtocols");
        put(ZkSslCipherSuitesProp, "zookeeper.ssl.ciphersuites");
        put(ZkSslEndpointIdentificationAlgorithmProp, "zookeeper.ssl.hostnameVerification");
        put(ZkSslCrlEnableProp, "zookeeper.ssl.crl");
        put(ZkSslOcspEnableProp, "zookeeper.ssl.ocsp");
    }};

    protected static Optional<String> zooKeeperClientProperty(ZKClientConfig clientConfig, String kafkaPropName) {
        return Optional.ofNullable(clientConfig.getProperty(ZkSslConfigToSystemPropertyMap.get(kafkaPropName)));
    }

    //
    protected static void setZooKeeperClientProperty(ZKClientConfig clientConfig, String kafkaPropName, Object kafkaPropValue) {
        String value;
        switch (kafkaPropName) {
            case ZkSslEndpointIdentificationAlgorithmProp:
                value = Boolean.toString(kafkaPropValue.toString().equalsIgnoreCase("HTTPS"));
                break;
            case ZkSslEnabledProtocolsProp:
            case ZkSslCipherSuitesProp:
                if (kafkaPropValue instanceof List) {
                    value = ((List<?>) kafkaPropValue).stream().map(Object::toString).collect(Collectors.joining(","));
                } else {
                    value = kafkaPropValue.toString();
                }
                break;
            default:
                value = kafkaPropValue.toString();
        }
        clientConfig.setProperty(ZkSslConfigToSystemPropertyMap.get(kafkaPropName), value);
    }
//
//    // For ZooKeeper TLS client authentication to be enabled the client must (at a minimum) configure itself as using TLS
//    // with both a client connection socket and a key store location explicitly set.
//    protected static Boolean zkTlsClientAuthEnabled(zkClientConfig: ZKClientConfig) {
//      return   zooKeeperClientProperty(zkClientConfig, ZkSslClientEnableProp).contains("true") &&
//                zooKeeperClientProperty(zkClientConfig, ZkClientCnxnSocketProp).isDefined &&
//                zooKeeperClientProperty(zkClientConfig, ZkSslKeyStoreLocationProp).isDefined
//    }

    /*------------ General Configuration -------------------*/
    public static final String BrokerIdGenerationEnableProp = "broker.id.generation.enable";
    public static final String MaxReservedBrokerIdProp = "reserved.broker.max.id";
    public static final String BrokerIdProp = "broker.id";
    public static final String MessageMaxBytesProp = "message.max.bytes";
    public static final String NumNetworkThreadsProp = "num.network.threads";
    public static final String NumIoThreadsProp = "num.io.threads";
    public static final String BackgroundThreadsProp = "background.threads";
    public static final String NumReplicaAlterLogDirsThreadsProp = "num.replica.alter.log.dirs.threads";
    public static final String QueuedMaxRequestsProp = "queued.max.requests";
    public static final String QueuedMaxBytesProp = "queued.max.request.bytes";
    public static final String RequestTimeoutMsProp = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String ConnectionSetupTimeoutMsProp = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;
    public static final String ConnectionSetupTimeoutMaxMsProp = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /*------------------- KRaft mode configs -------------------*/
    public static final String ProcessRolesProp = "process.roles";
    public static final String InitialBrokerRegistrationTimeoutMsProp = "initial.broker.registration.timeout.ms";
    public static final String BrokerHeartbeatIntervalMsProp = "broker.heartbeat.interval.ms";
    public static final String BrokerSessionTimeoutMsProp = "broker.session.timeout.ms";
    public static final String NodeIdProp = "node.id";
    public static final String MetadataLogDirProp = "metadata.log.dir";
    public static final String MetadataSnapshotMaxNewRecordBytesProp = "metadata.log.max.record.bytes.between.snapshots";
    public static final String ControllerListenerNamesProp = "controller.listener.names";
    public static final String SaslMechanismControllerProtocolProp = "sasl.mechanism.controller.protocol";
    public static final String MetadataLogSegmentMinBytesProp = "metadata.log.segment.min.bytes";
    public static final String MetadataLogSegmentBytesProp = "metadata.log.segment.bytes";
    public static final String MetadataLogSegmentMillisProp = "metadata.log.segment.ms";
    public static final String MetadataMaxRetentionBytesProp = "metadata.max.retention.bytes";
    public static final String MetadataMaxRetentionMillisProp = "metadata.max.retention.ms";
    public static final String QuorumVotersProp = RaftConfig.QUORUM_VOTERS_CONFIG;
    public static final String MetadataMaxIdleIntervalMsProp = "metadata.max.idle.interval.ms";

    /*------------------- Authorizer Configuration -------------------*/
    public static final String AuthorizerClassNameProp = "authorizer.class.name";
    public static final String EarlyStartListenersProp = "early.start.listeners";

    /*------------------- Socket Server Configuration -------------------*/
    public static final String ListenersProp = "listeners";
    public static final String AdvertisedListenersProp = "advertised.listeners";
    public static final String ListenerSecurityProtocolMapProp = "listener.security.protocol.map";
    public static final String ControlPlaneListenerNameProp = "control.plane.listener.name";
    public static final String SocketSendBufferBytesProp = "socket.send.buffer.bytes";
    public static final String SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes";
    public static final String SocketRequestMaxBytesProp = "socket.request.max.bytes";
    public static final String SocketListenBacklogSizeProp = "socket.listen.backlog.size";
    public static final String MaxConnectionsPerIpProp = "max.connections.per.ip";
    public static final String MaxConnectionsPerIpOverridesProp = "max.connections.per.ip.overrides";
    public static final String MaxConnectionsProp = "max.connections";
    public static final String MaxConnectionCreationRateProp = "max.connection.creation.rate";
    public static final String ConnectionsMaxIdleMsProp = "connections.max.idle.ms";
    public static final String FailedAuthenticationDelayMsProp = "connection.failed.authentication.delay.ms";

    /*------------------- rack configuration -------------------*/
    public static final String RackProp = "broker.rack";

    /*------------------- Log Configuration -------------------*/
    public static final String NumPartitionsProp = "num.partitions";
    public static final String LogDirsProp = LogConfigPrefix + "dirs";
    public static final String LogDirProp = LogConfigPrefix + "dir";
    public static final String LogSegmentBytesProp = LogConfigPrefix + "segment.bytes";

    public static final String LogRollTimeMillisProp = LogConfigPrefix + "roll.ms";
    public static final String LogRollTimeHoursProp = LogConfigPrefix + "roll.hours";

    public static final String LogRollTimeJitterMillisProp = LogConfigPrefix + "roll.jitter.ms";
    public static final String LogRollTimeJitterHoursProp = LogConfigPrefix + "roll.jitter.hours";

    public static final String LogRetentionTimeMillisProp = LogConfigPrefix + "retention.ms";
    public static final String LogRetentionTimeMinutesProp = LogConfigPrefix + "retention.minutes";
    public static final String LogRetentionTimeHoursProp = LogConfigPrefix + "retention.hours";

    public static final String LogRetentionBytesProp = LogConfigPrefix + "retention.bytes";
    public static final String LogCleanupIntervalMsProp = LogConfigPrefix + "retention.check.interval.ms";
    public static final String LogCleanupPolicyProp = LogConfigPrefix + "cleanup.policy";
    public static final String LogCleanerThreadsProp = LogConfigPrefix + "cleaner.threads";
    public static final String LogCleanerIoMaxBytesPerSecondProp = LogConfigPrefix + "cleaner.io.max.bytes.per.second";
    public static final String LogCleanerDedupeBufferSizeProp = LogConfigPrefix + "cleaner.dedupe.buffer.size";
    public static final String LogCleanerIoBufferSizeProp = LogConfigPrefix + "cleaner.io.buffer.size";
    public static final String LogCleanerDedupeBufferLoadFactorProp = LogConfigPrefix + "cleaner.io.buffer.load.factor";
    public static final String LogCleanerBackoffMsProp = LogConfigPrefix + "cleaner.backoff.ms";
    public static final String LogCleanerMinCleanRatioProp = LogConfigPrefix + "cleaner.min.cleanable.ratio";
    public static final String LogCleanerEnableProp = LogConfigPrefix + "cleaner.enable";
    public static final String LogCleanerDeleteRetentionMsProp = LogConfigPrefix + "cleaner.delete.retention.ms";
    public static final String LogCleanerMinCompactionLagMsProp = LogConfigPrefix + "cleaner.min.compaction.lag.ms";
    public static final String LogCleanerMaxCompactionLagMsProp = LogConfigPrefix + "cleaner.max.compaction.lag.ms";
    public static final String LogIndexSizeMaxBytesProp = LogConfigPrefix + "index.size.max.bytes";
    public static final String LogIndexIntervalBytesProp = LogConfigPrefix + "index.interval.bytes";
    public static final String LogFlushIntervalMessagesProp = LogConfigPrefix + "flush.interval.messages";
    public static final String LogDeleteDelayMsProp = LogConfigPrefix + "segment.delete.delay.ms";
    public static final String LogFlushSchedulerIntervalMsProp = LogConfigPrefix + "flush.scheduler.interval.ms";
    public static final String LogFlushIntervalMsProp = LogConfigPrefix + "flush.interval.ms";
    public static final String LogFlushOffsetCheckpointIntervalMsProp = LogConfigPrefix + "flush.offset.checkpoint.interval.ms";
    public static final String LogFlushStartOffsetCheckpointIntervalMsProp = LogConfigPrefix + "flush.start.offset.checkpoint.interval.ms";
    public static final String LogPreAllocateProp = LogConfigPrefix + "preallocate";

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    @Deprecated
    public static final String LogMessageFormatVersionProp = LogConfigPrefix + "message.format.version";

    public static final String LogMessageTimestampTypeProp = LogConfigPrefix + "message.timestamp.type";
    public static final String LogMessageTimestampDifferenceMaxMsProp = LogConfigPrefix + "message.timestamp.difference.max.ms";
    public static final String LogMaxIdMapSnapshotsProp = LogConfigPrefix + "max.id.map.snapshots";
    public static final String NumRecoveryThreadsPerDataDirProp = "num.recovery.threads.per.data.dir";
    public static final String AutoCreateTopicsEnableProp = "auto.create.topics.enable";
    public static final String MinInSyncReplicasProp = "min.insync.replicas";
    public static final String CreateTopicPolicyClassNameProp = "create.topic.policy.class.name";
    public static final String AlterConfigPolicyClassNameProp = "alter.config.policy.class.name";
    public static final String LogMessageDownConversionEnableProp = LogConfigPrefix + "message.downconversion.enable";

    /*------------------- Replication configuration -------------------*/
    public static final String ControllerSocketTimeoutMsProp = "controller.socket.timeout.ms";
    public static final String DefaultReplicationFactorProp = "default.replication.factor";
    public static final String ReplicaLagTimeMaxMsProp = "replica.lag.time.max.ms";
    public static final String ReplicaSocketTimeoutMsProp = "replica.socket.timeout.ms";
    public static final String ReplicaSocketReceiveBufferBytesProp = "replica.socket.receive.buffer.bytes";
    public static final String ReplicaFetchMaxBytesProp = "replica.fetch.max.bytes";
    public static final String ReplicaFetchWaitMaxMsProp = "replica.fetch.wait.max.ms";
    public static final String ReplicaFetchMinBytesProp = "replica.fetch.min.bytes";
    public static final String ReplicaFetchResponseMaxBytesProp = "replica.fetch.response.max.bytes";
    public static final String ReplicaFetchBackoffMsProp = "replica.fetch.backoff.ms";
    public static final String NumReplicaFetchersProp = "num.replica.fetchers";
    public static final String ReplicaHighWatermarkCheckpointIntervalMsProp = "replica.high.watermark.checkpoint.interval.ms";
    public static final String FetchPurgatoryPurgeIntervalRequestsProp = "fetch.purgatory.purge.interval.requests";
    public static final String ProducerPurgatoryPurgeIntervalRequestsProp = "producer.purgatory.purge.interval.requests";
    public static final String DeleteRecordsPurgatoryPurgeIntervalRequestsProp = "delete.records.purgatory.purge.interval.requests";
    public static final String AutoLeaderRebalanceEnableProp = "auto.leader.rebalance.enable";
    public static final String LeaderImbalancePerBrokerPercentageProp = "leader.imbalance.per.broker.percentage";
    public static final String LeaderImbalanceCheckIntervalSecondsProp = "leader.imbalance.check.interval.seconds";
    public static final String UncleanLeaderElectionEnableProp = "unclean.leader.election.enable";
    public static final String InterBrokerSecurityProtocolProp = "security.inter.broker.protocol";
    public static final String InterBrokerProtocolVersionProp = "inter.broker.protocol.version";
    public static final String InterBrokerListenerNameProp = "inter.broker.listener.name";
    public static final String ReplicaSelectorClassProp = "replica.selector.class";

    /*------------------- Controlled shutdown configuration -------------------*/
    public static final String ControlledShutdownMaxRetriesProp = "controlled.shutdown.max.retries";
    public static final String ControlledShutdownRetryBackoffMsProp = "controlled.shutdown.retry.backoff.ms";
    public static final String ControlledShutdownEnableProp = "controlled.shutdown.enable";

    /*------------------- Group coordinator configuration -------------------*/
    public static final String GroupMinSessionTimeoutMsProp = "group.min.session.timeout.ms";
    public static final String GroupMaxSessionTimeoutMsProp = "group.max.session.timeout.ms";
    public static final String GroupInitialRebalanceDelayMsProp = "group.initial.rebalance.delay.ms";
    public static final String GroupMaxSizeProp = "group.max.size";

    /*------------------- Offset management configuration -------------------*/
    public static final String OffsetMetadataMaxSizeProp = "offset.metadata.max.bytes";
    public static final String OffsetsLoadBufferSizeProp = "offsets.load.buffer.size";
    public static final String OffsetsTopicReplicationFactorProp = "offsets.topic.replication.factor";
    public static final String OffsetsTopicPartitionsProp = "offsets.topic.num.partitions";
    public static final String OffsetsTopicSegmentBytesProp = "offsets.topic.segment.bytes";
    public static final String OffsetsTopicCompressionCodecProp = "offsets.topic.compression.codec";
    public static final String OffsetsRetentionMinutesProp = "offsets.retention.minutes";
    public static final String OffsetsRetentionCheckIntervalMsProp = "offsets.retention.check.interval.ms";
    public static final String OffsetCommitTimeoutMsProp = "offsets.commit.timeout.ms";
    public static final String OffsetCommitRequiredAcksProp = "offsets.commit.required.acks";

    /*------------------- Transaction management configuration -------------------*/
    public static final String TransactionalIdExpirationMsProp = "transactional.id.expiration.ms";
    public static final String TransactionsMaxTimeoutMsProp = "transaction.max.timeout.ms";
    public static final String TransactionsTopicMinISRProp = "transaction.state.log.min.isr";
    public static final String TransactionsLoadBufferSizeProp = "transaction.state.log.load.buffer.size";
    public static final String TransactionsTopicPartitionsProp = "transaction.state.log.num.partitions";
    public static final String TransactionsTopicSegmentBytesProp = "transaction.state.log.segment.bytes";
    public static final String TransactionsTopicReplicationFactorProp = "transaction.state.log.replication.factor";
    public static final String TransactionsAbortTimedOutTransactionCleanupIntervalMsProp = "transaction.abort.timed.out.transaction.cleanup.interval.ms";
    public static final String TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp = "transaction.remove.expired.transaction.cleanup.interval.ms";

    /*------------------- Fetch Configuration ------------------*/
    public static final String MaxIncrementalFetchSessionCacheSlots = "max.incremental.fetch.session.cache.slots";
    public static final String FetchMaxBytes = "fetch.max.bytes";

    /*------------------- Quota Configuration -------------------*/
    public static final String NumQuotaSamplesProp = "quota.window.num";
    public static final String NumReplicationQuotaSamplesProp = "replication.quota.window.num";
    public static final String NumAlterLogDirsReplicationQuotaSamplesProp = "alter.log.dirs.replication.quota.window.num";
    public static final String NumControllerQuotaSamplesProp = "controller.quota.window.num";
    public static final String QuotaWindowSizeSecondsProp = "quota.window.size.seconds";
    public static final String ReplicationQuotaWindowSizeSecondsProp = "replication.quota.window.size.seconds";
    public static final String AlterLogDirsReplicationQuotaWindowSizeSecondsProp = "alter.log.dirs.replication.quota.window.size.seconds";
    public static final String ControllerQuotaWindowSizeSecondsProp = "controller.quota.window.size.seconds";
    public static final String ClientQuotaCallbackClassProp = "client.quota.callback.class";

    public static final String DeleteTopicEnableProp = "delete.topic.enable";
    public static final String CompressionTypeProp = "compression.type";

    /*----------------- Kafka Metrics Configuration ------------------*/
    public static final String MetricSampleWindowMsProp = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final String MetricNumSamplesProp = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final String MetricReporterClassesProp = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    public static final String MetricRecordingLevelProp = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /*-------------- Kafka Yammer Metrics Reporters Configuration ---------------*/
    public static final String KafkaMetricsReporterClassesProp = "kafka.metrics.reporters";
    public static final String KafkaMetricsPollingIntervalSecondsProp = "kafka.metrics.polling.interval.secs";

    /*------------------- Common Security Configuration -------------------*/

    public static final String PrincipalBuilderClassProp = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG;
    public static final String ConnectionsMaxReauthMsProp = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS;
    public static final String SaslServerMaxReceiveSizeProp = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG;
    public static final String securityProviderClassProp = SecurityConfig.SECURITY_PROVIDERS_CONFIG;

    /*------------------ SSL Configuration ------------------***/

    public static final String SslProtocolProp = SslConfigs.SSL_PROTOCOL_CONFIG;
    public static final String SslProviderProp = SslConfigs.SSL_PROVIDER_CONFIG;
    public static final String SslCipherSuitesProp = SslConfigs.SSL_CIPHER_SUITES_CONFIG;
    public static final String SslEnabledProtocolsProp = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
    public static final String SslKeystoreTypeProp = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    public static final String SslKeystoreLocationProp = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
    public static final String SslKeystorePasswordProp = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
    public static final String SslKeyPasswordProp = SslConfigs.SSL_KEY_PASSWORD_CONFIG;
    public static final String SslKeystoreKeyProp = SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
    public static final String SslKeystoreCertificateChainProp = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
    public static final String SslTruststoreTypeProp = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
    public static final String SslTruststoreLocationProp = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
    public static final String SslTruststorePasswordProp = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
    public static final String SslTruststoreCertificatesProp = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
    public static final String SslKeyManagerAlgorithmProp = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
    public static final String SslTrustManagerAlgorithmProp = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
    public static final String SslEndpointIdentificationAlgorithmProp = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
    public static final String SslSecureRandomImplementationProp = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
    public static final String SslClientAuthProp = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG;
    public static final String SslPrincipalMappingRulesProp = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG;
    public static final String SslEngineFactoryClassProp = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;

    /*------------------ SASL Configuration ------------------*/

    public static final String SaslMechanismInterBrokerProtocolProp = "sasl.mechanism.inter.broker.protocol";
    public static final String SaslJaasConfigProp = SaslConfigs.SASL_JAAS_CONFIG;
    public static final String SaslEnabledMechanismsProp = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG;
    public static final String SaslServerCallbackHandlerClassProp = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS;
    public static final String SaslClientCallbackHandlerClassProp = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
    public static final String SaslLoginClassProp = SaslConfigs.SASL_LOGIN_CLASS;
    public static final String SaslLoginCallbackHandlerClassProp = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
    public static final String SaslKerberosServiceNameProp = SaslConfigs.SASL_KERBEROS_SERVICE_NAME;
    public static final String SaslKerberosKinitCmdProp = SaslConfigs.SASL_KERBEROS_KINIT_CMD;
    public static final String SaslKerberosTicketRenewWindowFactorProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR;
    public static final String SaslKerberosTicketRenewJitterProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER;
    public static final String SaslKerberosMinTimeBeforeReloginProp = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN;
    public static final String SaslKerberosPrincipalToLocalRulesProp = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG;
    public static final String SaslLoginRefreshWindowFactorProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR;
    public static final String SaslLoginRefreshWindowJitterProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER;
    public static final String SaslLoginRefreshMinPeriodSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS;
    public static final String SaslLoginRefreshBufferSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS;

    public static final String SaslLoginConnectTimeoutMsProp = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
    public static final String SaslLoginReadTimeoutMsProp = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
    public static final String SaslLoginRetryBackoffMaxMsProp = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
    public static final String SaslLoginRetryBackoffMsProp = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
    public static final String SaslOAuthBearerScopeClaimNameProp = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
    public static final String SaslOAuthBearerSubClaimNameProp = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
    public static final String SaslOAuthBearerTokenEndpointUrlProp = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
    public static final String SaslOAuthBearerJwksEndpointUrlProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
    public static final String SaslOAuthBearerJwksEndpointRefreshMsProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
    public static final String SaslOAuthBearerJwksEndpointRetryBackoffMaxMsProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
    public static final String SaslOAuthBearerJwksEndpointRetryBackoffMsProp = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
    public static final String SaslOAuthBearerClockSkewSecondsProp = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
    public static final String SaslOAuthBearerExpectedAudienceProp = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
    public static final String SaslOAuthBearerExpectedIssuerProp = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;

    /*------------------ Delegation Token Configuration ---------------------*/
    public static final String DelegationTokenSecretKeyAliasProp = "delegation.token.master.key";
    public static final String DelegationTokenSecretKeyProp = "delegation.token.secret.key";
    public static final String DelegationTokenMaxLifeTimeProp = "delegation.token.max.lifetime.ms";
    public static final String DelegationTokenExpiryTimeMsProp = "delegation.token.expiry.time.ms";
    public static final String DelegationTokenExpiryCheckIntervalMsProp = "delegation.token.expiry.check.interval.ms";

    /*------------------ Password encryption configuration for dynamic configs ------------------*/
    public static final String PasswordEncoderSecretProp = "password.encoder.secret";
    public static final String PasswordEncoderOldSecretProp = "password.encoder.old.secret";
    public static final String PasswordEncoderKeyFactoryAlgorithmProp = "password.encoder.keyfactory.algorithm";
    public static final String PasswordEncoderCipherAlgorithmProp = "password.encoder.cipher.algorithm";
    public static final String PasswordEncoderKeyLengthProp = "password.encoder.key.length";
    public static final String PasswordEncoderIterationsProp = "password.encoder.iterations";

    /* Documentation */
    /*--------------- Zookeeper Configuration ---------------*/
    public static String ZkConnectDoc = "Specifies the ZooKeeper connection string in the form <code>hostname:port</code> where host and port are the " +
            "host and port of a ZooKeeper server. To allow connecting through other ZooKeeper nodes when that ZooKeeper machine is " +
            "down you can also specify multiple hosts in the form <code>hostname1:port1,hostname2:port2,hostname3:port3</code>.\n" +
            "The server can also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. " +
            "For example to give a chroot path of <code>/chroot/path</code> you would give the connection string as <code>hostname1:port1,hostname2:port2,hostname3:port3/chroot/path</code>.";
    public static String ZkSessionTimeoutMsDoc = "Zookeeper session timeout";
    public static String ZkConnectionTimeoutMsDoc = "The max time that the client waits to establish a connection to zookeeper. If not set, the value in " + ZkSessionTimeoutMsProp + " is used";
    public static String ZkEnableSecureAclsDoc = "Set client to use secure ACLs";
    public static String ZkMaxInFlightRequestsDoc = "The maximum number of unacknowledged requests the client will send to Zookeeper before blocking.";

    public static String ZkSslClientEnableDoc;

    static {
        ArrayList<String> keys = new ArrayList<>(ZkSslConfigToSystemPropertyMap.keySet());
        Collections.sort(keys);
        StringBuilder builder = new StringBuilder("<code>");
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            builder.append(iterator.next());
            if (iterator.hasNext()) {
                builder.append("</code>, <code>");
            }
        }
        builder.append("</code>");

        ZkSslClientEnableDoc = String.format("Set client to use TLS when connecting to ZooKeeper." +
                        " An explicit value overrides any value set via the <code>zookeeper.client.secure</code> system property (note the different name)." +
                        " Defaults to false if neither is set; when true, <code>%s</code> must be set (typically to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code>); other values to set may include %s",
                ZkClientCnxnSocketProp, builder);
    }

    public static String ZkClientCnxnSocketDoc = String.format("Typically set to <code>org.apache.zookeeper.ClientCnxnSocketNetty</code> when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the same-named <code>%s</code> system property.", ZkSslConfigToSystemPropertyMap.get(ZkClientCnxnSocketProp));
    public static String ZkSslKeyStoreLocationDoc = String.format("Keystore location when using a client-side certificate with TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase).", ZkSslConfigToSystemPropertyMap.get(ZkSslKeyStoreLocationProp));
    public static String ZkSslKeyStorePasswordDoc = String.format("Keystore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
                    " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
                    " Note that ZooKeeper does not support a key password different from the keystore password, so be sure to set the key password in the keystore to be identical to the keystore password; otherwise the connection attempt to Zookeeper will fail.",
            ZkSslConfigToSystemPropertyMap.get(ZkSslKeyStorePasswordProp));
    public static String ZkSslKeyStoreTypeDoc = String.format("Keystore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
                    " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
                    " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the keystore.",
            ZkSslConfigToSystemPropertyMap.get(ZkSslKeyStoreTypeProp));
    public static String ZkSslTrustStoreLocationDoc = String.format("Truststore location when using TLS connectivity to ZooKeeper." +
                    " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase).",
            ZkSslConfigToSystemPropertyMap.get(ZkSslTrustStoreLocationProp));
    public static String ZkSslTrustStorePasswordDoc = String.format("Truststore password when using TLS connectivity to ZooKeeper." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase).", ZkSslConfigToSystemPropertyMap.get(ZkSslTrustStorePasswordProp));
    public static String ZkSslTrustStoreTypeDoc = String.format("Truststore type when using TLS connectivity to ZooKeeper." +
                    " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
                    " The default value of <code>null</code> means the type will be auto-detected based on the filename extension of the truststore.",
            ZkSslConfigToSystemPropertyMap.get(ZkSslTrustStoreTypeProp));
    public static String ZkSslProtocolDoc = String.format("Specifies the protocol to be used in ZooKeeper TLS negotiation." +
            " An explicit value overrides any value set via the same-named <code>%s</code> system property.", ZkSslConfigToSystemPropertyMap.get(ZkSslProtocolProp));
    public static String ZkSslEnabledProtocolsDoc = String.format("Specifies the enabled protocol(s) in ZooKeeper TLS negotiation (csv)." +
                    " Overrides any explicit value set via the <code>%s</code> system property (note the camelCase)." +
                    " The default value of <code>null</code> means the enabled protocol will be the value of the <code>%s</code> configuration property.",
            ZkSslConfigToSystemPropertyMap.get(ZkSslEnabledProtocolsProp), KafkaConfig.ZkSslProtocolProp);
    public static String ZkSslCipherSuitesDoc = String.format("Specifies the enabled cipher suites to be used in ZooKeeper TLS negotiation (csv)." +
                    " Overrides any explicit value set via the <code>%s</code> system property (note the single word \"ciphersuites\")." +
                    " The default value of <code>null</code> means the list of enabled cipher suites is determined by the Java runtime being used.",
            ZkSslConfigToSystemPropertyMap.get(ZkSslCipherSuitesProp));
    public static String ZkSslEndpointIdentificationAlgorithmDoc = String.format("Specifies whether to enable hostname verification in the ZooKeeper TLS negotiation process," +
                    "with (case-insensitively) \"https\" meaning ZooKeeper hostname verification is enabled and an explicit blank value meaning it is disabled (disabling it is only recommended for testing purposes)." +
                    " An explicit value overrides any \"true\" or \"false\" value set via the <code>%s</code> system property (note the different name and values; true implies https and false implies blank).",
            ZkSslConfigToSystemPropertyMap.get(ZkSslEndpointIdentificationAlgorithmProp));
    public static String ZkSslCrlEnableDoc = String.format("Specifies whether to enable Certificate Revocation List in the ZooKeeper TLS protocols." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the shorter name).", ZkSslConfigToSystemPropertyMap.get(ZkSslCrlEnableProp));
    public static String ZkSslOcspEnableDoc = String.format("Specifies whether to enable Online Certificate Status Protocol in the ZooKeeper TLS protocols." +
            " Overrides any explicit value set via the <code>%s</code> system property (note the shorter name).", ZkSslConfigToSystemPropertyMap.get(ZkSslOcspEnableProp));
    /*------------------ General Configuration ***********/
    public static String BrokerIdGenerationEnableDoc = String.format("Enable automatic broker id generation on the server. When enabled the value configured for %s should be reviewed.",
            MaxReservedBrokerIdProp);
    public static String MaxReservedBrokerIdDoc = "Max number that can be used for a broker.id";
    public static String BrokerIdDoc = "The broker id for this server. If unset, a unique broker id will be generated." +
            "To avoid conflicts between zookeeper generated broker id's and user configured broker id's, generated broker ids " +
            "start from " + MaxReservedBrokerIdProp + " + 1.";
    public static String MessageMaxBytesDoc = String.format("%s This can be set per topic with the topic level <code>%s</code> config.",
            TopicConfig.MAX_MESSAGE_BYTES_DOC, TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
    public static String NumNetworkThreadsDoc = "The number of threads that the server uses for receiving requests from the network and sending responses to the network";
    public static String NumIoThreadsDoc = "The number of threads that the server uses for processing requests, which may include disk I/O";
    public static String NumReplicaAlterLogDirsThreadsDoc = "The number of threads that can move replicas between log directories, which may include disk I/O";
    public static String BackgroundThreadsDoc = "The number of threads to use for various background processing tasks";
    public static String QueuedMaxRequestsDoc = "The number of queued requests allowed for data-plane, before blocking the network threads";
    public static String QueuedMaxRequestBytesDoc = "The number of queued bytes allowed before no more requests are read";
    public static String RequestTimeoutMsDoc = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    public static String ConnectionSetupTimeoutMsDoc = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC;
    public static String ConnectionSetupTimeoutMaxMsDoc = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC;

    /**
     * KRaft mode configs
     */
    public static String ProcessRolesDoc = "The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both. " +
            "This configuration is only applicable for clusters in KRaft (Kafka Raft) mode (instead of ZooKeeper). Leave this config undefined or empty for Zookeeper clusters.";
    public static String InitialBrokerRegistrationTimeoutMsDoc = "When initially registering with the controller quorum, the number of milliseconds to wait before declaring failure and exiting the broker process.";
    public static String BrokerHeartbeatIntervalMsDoc = "The length of time in milliseconds between broker heartbeats. Used when running in KRaft mode.";
    public static String BrokerSessionTimeoutMsDoc = "The length of time in milliseconds that a broker lease lasts if no heartbeats are made. Used when running in KRaft mode.";
    public static String NodeIdDoc = "The node ID associated with the roles this process is playing when `process.roles` is non-empty. " +
            "Every node in a KRaft cluster must have a unique `node.id`, this includes broker and controller nodes. " +
            "This is required configuration when running in KRaft mode.";
    public static String MetadataLogDirDoc = "This configuration determines where we put the metadata log for clusters in KRaft mode. " +
            "If it is not set, the metadata log is placed in the first log directory from log.dirs.";
    public static String MetadataSnapshotMaxNewRecordBytesDoc = "This is the maximum number of bytes in the log between the latest snapshot and the high-watermark needed before generating a new snapshot.";
    public static String MetadataMaxIdleIntervalMsDoc = String.format("This configuration controls how often the active " +
            "controller should write no-op records to the metadata partition. If the value is 0, no-op records " +
            "are not appended to the metadata partition. The default value is %s", Defaults.MetadataMaxIdleIntervalMs);
    public static String ControllerListenerNamesDoc = "A comma-separated list of the names of the listeners used by the controller. This is required " +
            "if running in KRaft mode. When communicating with the controller quorum, the broker will always use the first listener in this list.\n " +
            "Note: The ZK-based controller should not set this configuration.";
    public static String SaslMechanismControllerProtocolDoc = "SASL mechanism used for communication with controllers. Default is GSSAPI.";
    public static String MetadataLogSegmentBytesDoc = "The maximum size of a single metadata log file.";
    public static String MetadataLogSegmentMinBytesDoc = "Override the minimum size for a single metadata log file. This should be used for testing only.";

    public static String MetadataLogSegmentMillisDoc = "The maximum time before a new metadata log file is rolled out (in milliseconds).";
    public static String MetadataMaxRetentionBytesDoc = "The maximum combined size of the metadata log and snapshots before deleting old " +
            "snapshots and log files. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";
    public static String MetadataMaxRetentionMillisDoc = "The number of milliseconds to keep a metadata log file or snapshot before " +
            "deleting it. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    /*------------------- Authorizer Configuration -------------------*/
    public static String AuthorizerClassNameDoc = String.format("The fully qualified name of a class that implements <code>%s</code>" +
            " interface, which is used by the broker for authorization.", Authorizer.class.getName());
    public static String EarlyStartListenersDoc = "A comma-separated list of listener names which may be started before the authorizer has finished " +
            "initialization. This is useful when the authorizer is dependent on the cluster itself for bootstrapping, as is the case for " +
            "the StandardAuthorizer (which stores ACLs in the metadata log.) By default, all listeners included in controller.listener.names " +
            "will also be early start listeners. A listener should not appear in this list if it accepts external traffic.";

    /*------------------ Socket Server Configuration -------------------*/
    public static String ListenersDoc = String.format("Listener List - Comma-separated list of URIs we will listen on and the listener names." +
            " If the listener name is not a security protocol, <code>%s</code> must also be set.\n" +
            " Listener names and port numbers must be unique.\n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
            " Leave hostname empty to bind to default interface.\n" +
            " Examples of legal listener lists:\n" +
            " PLAINTEXT://myhost:9092,SSL://:9091\n" +
            " CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093\n", ListenerSecurityProtocolMapProp);
    public static String AdvertisedListenersDoc = String.format("Listeners to publish to ZooKeeper for clients to use, if different than the <code>%s</code> config property." +
                    " In IaaS environments, this may need to be different from the interface to which the broker binds." +
                    " If this is not set, the value for <code>%s</code> will be used." +
                    " Unlike <code>%s</code>, it is not valid to advertise the 0.0.0.0 meta-address.\n" +
                    " Also unlike <code>%s</code>, there can be duplicated ports in this property," +
                    " so that one listener can be configured to advertise another listener's address." +
                    " This can be useful in some cases where external load balancers are used.",
            ListenersProp, ListenersProp, ListenersProp, ListenersProp);
    public static String ListenerSecurityProtocolMapDoc = "Map between listener names and security protocols. This must be defined for " +
            "the same security protocol to be usable in more than one port or IP. For example, internal and " +
            "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners " +
            "with names INTERNAL and EXTERNAL and this property as: `INTERNAL:SSL,EXTERNAL:SSL`. As shown, key and value are " +
            "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. " +
            "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
            "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the " +
            "INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> would be set. " +
            "If the config for the listener name is not set, the config will fallback to the generic config (i.e. <code>ssl.keystore.location</code>). " +
            "Note that in KRaft a default mapping from the listener names defined by <code>controller.listener.names</code> to PLAINTEXT " +
            "is assumed if no explicit mapping is provided and no other security protocol is in use.";
    public static String controlPlaneListenerNameDoc = String.format("Name of listener used for communication between controller and brokers. " +
                    "Broker will use the %s to locate the endpoint in %s list, to listen for connections from the controller. " +
                    "For example, if a broker's config is :\n" +
                    "listeners = INTERNAL://192.1.1.8:9092, EXTERNAL://10.1.1.5:9093, CONTROLLER://192.1.1.8:9094\n" +
                    "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL\n" +
                    "control.plane.listener.name = CONTROLLER\n" +
                    "On startup, the broker will start listening on \"192.1.1.8:9094\" with security protocol \"SSL\".\n" +
                    "On controller side, when it discovers a broker's published endpoints through zookeeper, it will use the %s " +
                    "to find the endpoint, which it will use to establish connection to the broker.\n" +
                    "For example, if the broker's published endpoints on zookeeper are :\n" +
                    "\"endpoints\" : [\"INTERNAL://broker1.example.com:9092\",\"EXTERNAL://broker1.example.com:9093\",\"CONTROLLER://broker1.example.com:9094\"]\n" +
                    " and the controller's config is :\n" +
                    "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL\n" +
                    "control.plane.listener.name = CONTROLLER\n" +
                    "then controller will use \"broker1.example.com:9094\" with security protocol \"SSL\" to connect to the broker.\n" +
                    "If not explicitly configured, the default value will be null and there will be no dedicated endpoints for controller connections.\n" +
                    "If explicitly configured, the value cannot be the same as the value of <code>%s</code>.",
            ControlPlaneListenerNameProp, ListenersProp, ControlPlaneListenerNameProp, InterBrokerListenerNameProp);

    public static String SocketSendBufferBytesDoc = "The SO_SNDBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";
    public static String SocketReceiveBufferBytesDoc = "The SO_RCVBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";
    public static String SocketRequestMaxBytesDoc = "The maximum number of bytes in a socket request";
    public static String SocketListenBacklogSizeDoc = "The maximum number of pending connections on the socket. " +
            "In Linux, you may also need to configure `somaxconn` and `tcp_max_syn_backlog` kernel parameters " +
            "accordingly to make the configuration takes effect.";
    public static String MaxConnectionsPerIpDoc = String.format("The maximum number of connections we allow from each ip address. This can be set to 0 if there are overrides " +
            "configured using %s property. New connections from the ip address are dropped if the limit is reached.", MaxConnectionsPerIpOverridesProp);
    public static String MaxConnectionsPerIpOverridesDoc = "A comma-separated list of per-ip or hostname overrides to the default maximum number of connections. " +
            "An example value is \"hostName:100,127.0.0.1:200\"";
    public static String MaxConnectionsDoc = String.format("The maximum number of connections we allow in the broker at any time. This limit is applied in addition " +
                    "to any per-ip limits configured using %s. Listener-level limits may also be configured by prefixing the " +
                    "config name with the listener prefix, for example, <code>listener.name.internal.%s</code>. Broker-wide limit " +
                    "should be configured based on broker capacity while listener limits should be configured based on application requirements. " +
                    "New connections are blocked if either the listener or broker limit is reached. Connections on the inter-broker listener are " +
                    "permitted even if broker-wide limit is reached. The least recently used connection on another listener will be closed in this case.",
            MaxConnectionsPerIpProp, MaxConnectionsProp);
    public static String MaxConnectionCreationRateDoc = String.format("The maximum connection creation rate we allow in the broker at any time. Listener-level limits " +
                    "may also be configured by prefixing the config name with the listener prefix, for example, <code>listener.name.internal.%s</code>." +
                    "Broker-wide connection rate limit should be configured based on broker capacity while listener limits should be configured based on " +
                    "application requirements. New connections will be throttled if either the listener or the broker limit is reached, with the exception " +
                    "of inter-broker listener. Connections on the inter-broker listener will be throttled only when the listener-level rate limit is reached.",
            MaxConnectionCreationRateProp);
    public static String ConnectionsMaxIdleMsDoc = "Idle connections timeout: the server socket processor threads close the connections that idle more than this";
    public static String FailedAuthenticationDelayMsDoc = String.format("Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure. " +
            "This must be configured to be less than %s to prevent connection timeout.", ConnectionsMaxIdleMsProp);
    /*------------------ Rack Configuration ------------------*/
    public static String RackDoc = "Rack of the broker. This will be used in rack aware replication assignment for fault tolerance. Examples: `RACK1`, `us-east-1d`";
    /*------------------ Log Configuration ------------------*/
    public static String NumPartitionsDoc = "The default number of log partitions per topic";
    public static String LogDirDoc = "The directory in which the log data is kept (supplemental for " + LogDirsProp + " property)";
    public static String LogDirsDoc = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LogDirProp + " is used.";
    public static String LogSegmentBytesDoc = "The maximum size of a single log file";
    public static String LogRollTimeMillisDoc = "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in " + LogRollTimeHoursProp + " is used";
    public static String LogRollTimeHoursDoc = "The maximum time before a new log segment is rolled out (in hours), secondary to " + LogRollTimeMillisProp + " property";

    public static String LogRollTimeJitterMillisDoc = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in " + LogRollTimeJitterHoursProp + " is used";
    public static String LogRollTimeJitterHoursDoc = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to " + LogRollTimeJitterMillisProp + " property";

    public static String LogRetentionTimeMillisDoc = "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in " + LogRetentionTimeMinutesProp + " is used. If set to -1, no time limit is applied.";
    public static String LogRetentionTimeMinsDoc = "The number of minutes to keep a log file before deleting it (in minutes), secondary to " + LogRetentionTimeMillisProp + " property. If not set, the value in " + LogRetentionTimeHoursProp + " is used";
    public static String LogRetentionTimeHoursDoc = "The number of hours to keep a log file before deleting it (in hours), tertiary to " + LogRetentionTimeMillisProp + " property";

    public static String LogRetentionBytesDoc = "The maximum size of the log before deleting it";
    public static String LogCleanupIntervalMsDoc = "The frequency in milliseconds that the log cleaner checks whether any log is eligible for deletion";
    public static String LogCleanupPolicyDoc = "The default cleanup policy for segments beyond the retention window. A comma separated list of valid policies. Valid policies are: \"delete\" and \"compact\"";
    public static String LogCleanerThreadsDoc = "The number of background threads to use for log cleaning";
    public static String LogCleanerIoMaxBytesPerSecondDoc = "The log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average";
    public static String LogCleanerDedupeBufferSizeDoc = "The total memory used for log deduplication across all cleaner threads";
    public static String LogCleanerIoBufferSizeDoc = "The total memory used for log cleaner I/O buffers across all cleaner threads";
    public static String LogCleanerDedupeBufferLoadFactorDoc = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value " +
            "will allow more log to be cleaned at once but will lead to more hash collisions";
    public static String LogCleanerBackoffMsDoc = "The amount of time to sleep when there are no logs to clean";
    public static String LogCleanerMinCleanRatioDoc = "The minimum ratio of dirty log to total log for a log to eligible for cleaning. " +
            "If the " + LogCleanerMaxCompactionLagMsProp + " or the " + LogCleanerMinCompactionLagMsProp +
            " configurations are also specified, then the log compactor considers the log eligible for compaction " +
            "as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) " +
            "records for at least the " + LogCleanerMinCompactionLagMsProp + " duration, or (ii) if the log has had " +
            "dirty (uncompacted) records for at most the " + LogCleanerMaxCompactionLagMsProp + " period.";
    public static String LogCleanerEnableDoc = "Enable the log cleaner process to run on the server. Should be enabled if using any topics with a cleanup.policy=compact including the internal offsets topic. If disabled those topics will not be compacted and continually grow in size.";
    public static String LogCleanerDeleteRetentionMsDoc = "The amount of time to retain delete tombstone markers for log compacted topics. This setting also gives a bound " +
            "on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete " +
            "tombstones may be collected before they complete their scan).";
    public static String LogCleanerMinCompactionLagMsDoc = "The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted.";
    public static String LogCleanerMaxCompactionLagMsDoc = "The maximum time a message will remain ineligible for compaction in the log. Only applicable for logs that are being compacted.";
    public static String LogIndexSizeMaxBytesDoc = "The maximum size in bytes of the offset index";
    public static String LogIndexIntervalBytesDoc = "The interval with which we add an entry to the offset index";
    public static String LogFlushIntervalMessagesDoc = "The number of messages accumulated on a log partition before messages are flushed to disk ";
    public static String LogDeleteDelayMsDoc = "The amount of time to wait before deleting a file from the filesystem";
    public static String LogFlushSchedulerIntervalMsDoc = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk";
    public static String LogFlushIntervalMsDoc = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in " + LogFlushSchedulerIntervalMsProp + " is used";
    public static String LogFlushOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point";
    public static String LogFlushStartOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of log start offset";
    public static String LogPreAllocateEnableDoc = "Should pre allocate file when create new segment? If you are using Kafka on Windows, you probably need to set it to true.";
    public static String LogMessageFormatVersionDoc = "Specify the message format version the broker will use to append messages to the logs. The value should be a valid MetadataVersion. " +
            "Some examples are: 0.8.2, 0.9.0.0, 0.10.0, check MetadataVersion for more details. By setting a particular message format version, the " +
            "user is certifying that all the existing messages on disk are smaller or equal than the specified version. Setting this value incorrectly " +
            "will cause consumers with older versions to break as they will receive messages with a format that they don't understand.";

    public static String LogMessageTimestampTypeDoc = "Define whether the timestamp in the message is message create time or log append time. The value should be either " +
            "`CreateTime` or `LogAppendTime`";

    public static String LogMessageTimestampDifferenceMaxMsDoc = "The maximum difference allowed between the timestamp when a broker receives " +
            "a message and the timestamp specified in the message. If log.message.timestamp.type=CreateTime, a message will be rejected " +
            "if the difference in timestamp exceeds this threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime." +
            "The maximum timestamp difference allowed should be no greater than log.retention.ms to avoid unnecessarily frequent log rolling.";
    public static String NumRecoveryThreadsPerDataDirDoc = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown";
    public static String AutoCreateTopicsEnableDoc = "Enable auto creation of topic on the server";
    public static String MinInSyncReplicasDoc = "When a producer sets acks to \"all\" (or \"-1\"), " +
            "min.insync.replicas specifies the minimum number of replicas that must acknowledge " +
            "a write for the write to be considered successful. If this minimum cannot be met, " +
            "then the producer will raise an exception (either NotEnoughReplicas or " +
            "NotEnoughReplicasAfterAppend).<br>When used together, min.insync.replicas and acks " +
            "allow you to enforce greater durability guarantees. A typical scenario would be to " +
            "create a topic with a replication factor of 3, set min.insync.replicas to 2, and " +
            "produce with acks of \"all\". This will ensure that the producer raises an exception " +
            "if a majority of replicas do not receive a write.";

    public static String CreateTopicPolicyClassNameDoc = "The create topic policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.CreateTopicPolicy</code> interface.";
    public static String AlterConfigPolicyClassNameDoc = "The alter configs policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.AlterConfigPolicy</code> interface.";
    public static String LogMessageDownConversionEnableDoc = TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC;

    /*------------------ Replication configuration ***********/
    public static String ControllerSocketTimeoutMsDoc = "The socket timeout for controller-to-broker channels";
    public static String ControllerMessageQueueSizeDoc = "The buffer size for controller-to-broker-channels";
    public static String DefaultReplicationFactorDoc = "The default replication factors for automatically created topics";
    public static String ReplicaLagTimeMaxMsDoc = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
            " the leader will remove the follower from isr";
    public static String ReplicaSocketTimeoutMsDoc = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms";
    public static String ReplicaSocketReceiveBufferBytesDoc = "The socket receive buffer for network requests";
    public static String ReplicaFetchMaxBytesDoc = "The number of bytes of messages to attempt to fetch for each partition. This is not an absolute maximum, " +
            "if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch will still be returned " +
            "to ensure that progress can be made. The maximum record batch size accepted by the broker is defined via " +
            "<code>message.max.bytes</code> (broker config) or <code>max.message.bytes</code> (topic config).";
    public static String ReplicaFetchWaitMaxMsDoc = "The maximum wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
            "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics";
    public static String ReplicaFetchMinBytesDoc = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to <code>replica.fetch.wait.max.ms</code> (broker config).";
    public static String ReplicaFetchResponseMaxBytesDoc = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch " +
            "will still be returned to ensure that progress can be made. As such, this is not an absolute maximum. The maximum " +
            "record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config).";
    public static String NumReplicaFetchersDoc = "Number of fetcher threads used to replicate records from each source broker. The total number of fetchers " +
            "on each broker is bound by <code>num.replica.fetchers</code> multiplied by the number of brokers in the cluster." +
            "Increasing this value can increase the degree of I/O parallelism in the follower and leader broker at the cost " +
            "of higher CPU and memory utilization.";
    public static String ReplicaFetchBackoffMsDoc = "The amount of time to sleep when fetch partition error occurs.";
    public static String ReplicaHighWatermarkCheckpointIntervalMsDoc = "The frequency with which the high watermark is saved out to disk";
    public static String FetchPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the fetch request purgatory";
    public static String ProducerPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the producer request purgatory";
    public static String DeleteRecordsPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the delete records request purgatory";
    public static String AutoLeaderRebalanceEnableDoc = "Enables auto leader balancing. A background thread checks the distribution of partition leaders at regular intervals, configurable by `leader.imbalance.check.interval.seconds`. If the leader imbalance exceeds `leader.imbalance.per.broker.percentage`, leader rebalance to the preferred leader for partitions is triggered.";
    public static String LeaderImbalancePerBrokerPercentageDoc = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage.";
    public static String LeaderImbalanceCheckIntervalSecondsDoc = "The frequency with which the partition rebalance check is triggered by the controller";
    public static String UncleanLeaderElectionEnableDoc = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss";
    public static String InterBrokerSecurityProtocolDoc = String.format("Security protocol used to communicate between brokers. Valid values are: " +
                    "%s. It is an error to set this and %s properties at the same time.",
            String.join(", ", SecurityProtocol.names()), InterBrokerListenerNameProp);
    public static String InterBrokerProtocolVersionDoc = "Specify which version of the inter-broker protocol will be used.\n" +
            " This is typically bumped after all brokers were upgraded to a new version.\n" +
            " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check MetadataVersion for the full list.";
    public static String InterBrokerListenerNameDoc = String.format("Name of listener used for communication between brokers. If this is unset, the listener name is defined by $InterBrokerSecurityProtocolProp. " +
            "It is an error to set this and %s properties at the same time.", InterBrokerSecurityProtocolProp);
    public static String ReplicaSelectorClassDoc = "The fully qualified class name that implements ReplicaSelector. This is used by the broker to find the preferred read replica. By default, we use an implementation that returns the leader.";
    /*------------------ Controlled shutdown configuration ------------------*/
    public static String ControlledShutdownMaxRetriesDoc = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens";
    public static String ControlledShutdownRetryBackoffMsDoc = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying.";
    public static String ControlledShutdownEnableDoc = "Enable controlled shutdown of the server";
    /*------------------ Group coordinator configuration ------------------*/
    public static String GroupMinSessionTimeoutMsDoc = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources.";
    public static String GroupMaxSessionTimeoutMsDoc = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures.";
    public static String GroupInitialRebalanceDelayMsDoc = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins.";
    public static String GroupMaxSizeDoc = "The maximum number of consumers that a single consumer group can accommodate.";
    /*------------------ Offset management configuration ------------------*/
    public static String OffsetMetadataMaxSizeDoc = "The maximum size for a metadata entry associated with an offset commit";
    public static String OffsetsLoadBufferSizeDoc = "Batch size for reading from the offsets segments when loading offsets into the cache (soft-limit, overridden if records are too large).";
    public static String OffsetsTopicReplicationFactorDoc = "The replication factor for the offsets topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";
    public static String OffsetsTopicPartitionsDoc = "The number of partitions for the offset commit topic (should not change after deployment)";
    public static String OffsetsTopicSegmentBytesDoc = "The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads";
    public static String OffsetsTopicCompressionCodecDoc = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits";
    public static String OffsetsRetentionMinutesDoc = "After a consumer group loses all its consumers (i.e. becomes empty) its offsets will be kept for this retention period before getting discarded. " +
            "For standalone consumers (using manual assignment), offsets will be expired after the time of last commit plus this retention period.";
    public static String OffsetsRetentionCheckIntervalMsDoc = "Frequency at which to check for stale offsets";
    public static String OffsetCommitTimeoutMsDoc = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
            "or this timeout is reached. This is similar to the producer request timeout.";
    public static String OffsetCommitRequiredAcksDoc = "The required acks before the commit can be accepted. In general, the default (-1) should not be overridden";
    /*------------------ Transaction management configuration ------------------*/
    public static String TransactionalIdExpirationMsDoc = "The time in ms that the transaction coordinator will wait without receiving any transaction status updates " +
            "for the current transaction before expiring its transactional id. This setting also influences producer id expiration - producer ids are expired " +
            "once this time has elapsed after the last write with the given producer id. Note that producer ids may expire sooner if the last write from the producer id is deleted due to the topic's retention settings.";
    public static String TransactionsMaxTimeoutMsDoc = "The maximum allowed timeout for transactions. " +
            "If a client’s requested transaction time exceed this, then the broker will return an error in InitProducerIdRequest. This prevents a client from too large of a timeout, which can stall consumers reading from topics included in the transaction.";
    public static String TransactionsTopicMinISRDoc = "Overridden " + MinInSyncReplicasProp + " config for the transaction topic.";
    public static String TransactionsLoadBufferSizeDoc = "Batch size for reading from the transaction log segments when loading producer ids and transactions into the cache (soft-limit, overridden if records are too large).";
    public static String TransactionsTopicReplicationFactorDoc = "The replication factor for the transaction topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";
    public static String TransactionsTopicPartitionsDoc = "The number of partitions for the transaction topic (should not change after deployment).";
    public static String TransactionsTopicSegmentBytesDoc = "The transaction topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads";
    public static String TransactionsAbortTimedOutTransactionsIntervalMsDoc = "The interval at which to rollback transactions that have timed out";
    public static String TransactionsRemoveExpiredTransactionsIntervalMsDoc = "The interval at which to remove transactions that have expired due to <code>transactional.id.expiration.ms</code> passing";

    /*------------------ Fetch Configuration ------------------*/
    public static String MaxIncrementalFetchSessionCacheSlotsDoc = "The maximum number of incremental fetch sessions that we will maintain.";
    public static String FetchMaxBytesDoc = "The maximum number of bytes we will return for a fetch request. Must be at least 1024.";

    /*------------------ Quota Configuration ------------------*/
    public static String NumQuotaSamplesDoc = "The number of samples to retain in memory for client quotas";
    public static String NumReplicationQuotaSamplesDoc = "The number of samples to retain in memory for replication quotas";
    public static String NumAlterLogDirsReplicationQuotaSamplesDoc = "The number of samples to retain in memory for alter log dirs replication quotas";
    public static String NumControllerQuotaSamplesDoc = "The number of samples to retain in memory for controller mutation quotas";
    public static String QuotaWindowSizeSecondsDoc = "The time span of each sample for client quotas";
    public static String ReplicationQuotaWindowSizeSecondsDoc = "The time span of each sample for replication quotas";
    public static String AlterLogDirsReplicationQuotaWindowSizeSecondsDoc = "The time span of each sample for alter log dirs replication quotas";
    public static String ControllerQuotaWindowSizeSecondsDoc = "The time span of each sample for controller mutations quotas";

    public static String ClientQuotaCallbackClassDoc = "The fully qualified name of a class that implements the ClientQuotaCallback interface, " +
            "which is used to determine quota limits applied to client requests. By default, the &lt;user&gt; and &lt;client-id&gt; " +
            "quotas that are stored in ZooKeeper are applied. For any given request, the most specific quota that matches the user principal " +
            "of the session and the client-id of the request is applied.";

    public static String DeleteTopicEnableDoc = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off";
    public static String CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
            "('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
            "'producer' which means retain the original compression codec set by the producer.";

    /*------------------ Kafka Metrics Configuration ***********/
    public static String MetricSampleWindowMsDoc = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC;
    public static String MetricNumSamplesDoc = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC;
    public static String MetricReporterClassesDoc = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC;
    public static String MetricRecordingLevelDoc = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC;


    /*------------------ Kafka Yammer Metrics Reporter Configuration ***********/
    public static String KafkaMetricsReporterClassesDoc = "A list of classes to use as Yammer metrics custom reporters." +
            " The reporters should implement <code>kafka.metrics.KafkaMetricsReporter</code> trait. If a client wants" +
            " to expose JMX operations on a custom reporter, the custom reporter needs to additionally implement an MBean" +
            " trait that extends <code>kafka.metrics.KafkaMetricsReporterMBean</code> trait so that the registered MBean is compliant with" +
            " the standard MBean convention.";

    public static String KafkaMetricsPollingIntervalSecondsDoc = String.format("The metrics polling interval (in seconds) which can be used" +
            " in %s implementations.", KafkaMetricsReporterClassesProp);

    /**
     * ******* Common Security Configuration
     *************/
    public static String PrincipalBuilderClassDoc = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC;
    public static String ConnectionsMaxReauthMsDoc = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC;
    public static String SaslServerMaxReceiveSizeDoc = BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC;
    public static String securityProviderClassDoc = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /*------------------ SSL Configuration ------------------***/
    public static String SslProtocolDoc = SslConfigs.SSL_PROTOCOL_DOC;
    public static String SslProviderDoc = SslConfigs.SSL_PROVIDER_DOC;
    public static String SslCipherSuitesDoc = SslConfigs.SSL_CIPHER_SUITES_DOC;
    public static String SslEnabledProtocolsDoc = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC;
    public static String SslKeystoreTypeDoc = SslConfigs.SSL_KEYSTORE_TYPE_DOC;
    public static String SslKeystoreLocationDoc = SslConfigs.SSL_KEYSTORE_LOCATION_DOC;
    public static String SslKeystorePasswordDoc = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC;
    public static String SslKeyPasswordDoc = SslConfigs.SSL_KEY_PASSWORD_DOC;
    public static String SslKeystoreKeyDoc = SslConfigs.SSL_KEYSTORE_KEY_DOC;
    public static String SslKeystoreCertificateChainDoc = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC;
    public static String SslTruststoreTypeDoc = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC;
    public static String SslTruststorePasswordDoc = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;
    public static String SslTruststoreLocationDoc = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC;
    public static String SslTruststoreCertificatesDoc = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC;
    public static String SslKeyManagerAlgorithmDoc = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC;
    public static String SslTrustManagerAlgorithmDoc = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC;
    public static String SslEndpointIdentificationAlgorithmDoc = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;
    public static String SslSecureRandomImplementationDoc = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC;
    public static String SslClientAuthDoc = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC;
    public static String SslPrincipalMappingRulesDoc = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC;
    public static String SslEngineFactoryClassDoc = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC;

    /*------------------ Sasl Configuration ------------------***/
    public static String SaslMechanismInterBrokerProtocolDoc = "SASL mechanism used for inter-broker communication. Default is GSSAPI.";
    public static String SaslJaasConfigDoc = SaslConfigs.SASL_JAAS_CONFIG_DOC;
    public static String SaslEnabledMechanismsDoc = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC;
    public static String SaslServerCallbackHandlerClassDoc = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC;
    public static String SaslClientCallbackHandlerClassDoc = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC;
    public static String SaslLoginClassDoc = SaslConfigs.SASL_LOGIN_CLASS_DOC;
    public static String SaslLoginCallbackHandlerClassDoc = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC;
    public static String SaslKerberosServiceNameDoc = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC;
    public static String SaslKerberosKinitCmdDoc = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC;
    public static String SaslKerberosTicketRenewWindowFactorDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC;
    public static String SaslKerberosTicketRenewJitterDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC;
    public static String SaslKerberosMinTimeBeforeReloginDoc = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC;
    public static String SaslKerberosPrincipalToLocalRulesDoc = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC;
    public static String SaslLoginRefreshWindowFactorDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC;
    public static String SaslLoginRefreshWindowJitterDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC;
    public static String SaslLoginRefreshMinPeriodSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC;
    public static String SaslLoginRefreshBufferSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC;

    public static String SaslLoginConnectTimeoutMsDoc = SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC;
    public static String SaslLoginReadTimeoutMsDoc = SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC;
    public static String SaslLoginRetryBackoffMaxMsDoc = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC;
    public static String SaslLoginRetryBackoffMsDoc = SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC;
    public static String SaslOAuthBearerScopeClaimNameDoc = SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC;
    public static String SaslOAuthBearerSubClaimNameDoc = SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC;
    public static String SaslOAuthBearerTokenEndpointUrlDoc = SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC;
    public static String SaslOAuthBearerJwksEndpointUrlDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC;
    public static String SaslOAuthBearerJwksEndpointRefreshMsDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC;
    public static String SaslOAuthBearerJwksEndpointRetryBackoffMaxMsDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC;
    public static String SaslOAuthBearerJwksEndpointRetryBackoffMsDoc = SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC;
    public static String SaslOAuthBearerClockSkewSecondsDoc = SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC;
    public static String SaslOAuthBearerExpectedAudienceDoc = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC;
    public static String SaslOAuthBearerExpectedIssuerDoc = SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC;

    /*------------------ Delegation Token Configuration ------------------***/
    public static String DelegationTokenSecretKeyAliasDoc = String.format("DEPRECATED: An alias for %s, which should be used instead of this config.", DelegationTokenSecretKeyProp);
    public static String DelegationTokenSecretKeyDoc = "Secret key to generate and verify delegation tokens. The same key must be configured across all the brokers. " +
            " If the key is not set or set to empty string, brokers will disable the delegation token support.";
    public static String DelegationTokenMaxLifeTimeDoc = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days.";
    public static String DelegationTokenExpiryTimeMsDoc = "The token validity time in miliseconds before the token needs to be renewed. Default value 1 day.";
    public static String DelegationTokenExpiryCheckIntervalDoc = "Scan interval to remove expired delegation tokens.";

    /*------------------ Password encryption configuration for dynamic configs *********/
    public static String PasswordEncoderSecretDoc = "The secret used for encoding dynamically configured passwords for this broker.";
    public static String PasswordEncoderOldSecretDoc = String.format("The old secret that was used for encoding dynamically configured passwords. " +
            "This is required only when the secret is updated. If specified, all dynamically encoded passwords are " +
            "decoded using this old secret and re-encoded using %s when broker starts up.", PasswordEncoderSecretProp);
    public static String PasswordEncoderKeyFactoryAlgorithmDoc = "The SecretKeyFactory algorithm used for encoding dynamically configured passwords. " +
            "Default is PBKDF2WithHmacSHA512 if available and PBKDF2WithHmacSHA1 otherwise.";
    public static String PasswordEncoderCipherAlgorithmDoc = "The Cipher algorithm used for encoding dynamically configured passwords.";
    public static String PasswordEncoderKeyLengthDoc = "The key length used for encoding dynamically configured passwords.";
    public static String PasswordEncoderIterationsDoc = "The iteration count used for encoding dynamically configured passwords.";

    //    @nowarn("cat=deprecation")
    protected static ConfigDef configDef = new ConfigDef()

            /*------------------ Zookeeper Configuration ***********/
            .define(ZkConnectProp, STRING, null, HIGH, ZkConnectDoc)
            .define(ZkSessionTimeoutMsProp, INT, Defaults.ZkSessionTimeoutMs, HIGH, ZkSessionTimeoutMsDoc)
            .define(ZkConnectionTimeoutMsProp, INT, null, HIGH, ZkConnectionTimeoutMsDoc)
            .define(ZkEnableSecureAclsProp, BOOLEAN, Defaults.ZkEnableSecureAcls, HIGH, ZkEnableSecureAclsDoc)
            .define(ZkMaxInFlightRequestsProp, INT, Defaults.ZkMaxInFlightRequests, atLeast(1), HIGH, ZkMaxInFlightRequestsDoc)
            .define(ZkSslClientEnableProp, BOOLEAN, Defaults.ZkSslClientEnable, MEDIUM, ZkSslClientEnableDoc)
            .define(ZkClientCnxnSocketProp, STRING, null, MEDIUM, ZkClientCnxnSocketDoc)
            .define(ZkSslKeyStoreLocationProp, STRING, null, MEDIUM, ZkSslKeyStoreLocationDoc)
            .define(ZkSslKeyStorePasswordProp, PASSWORD, null, MEDIUM, ZkSslKeyStorePasswordDoc)
            .define(ZkSslKeyStoreTypeProp, STRING, null, MEDIUM, ZkSslKeyStoreTypeDoc)
            .define(ZkSslTrustStoreLocationProp, STRING, null, MEDIUM, ZkSslTrustStoreLocationDoc)
            .define(ZkSslTrustStorePasswordProp, PASSWORD, null, MEDIUM, ZkSslTrustStorePasswordDoc)
            .define(ZkSslTrustStoreTypeProp, STRING, null, MEDIUM, ZkSslTrustStoreTypeDoc)
            .define(ZkSslProtocolProp, STRING, Defaults.ZkSslProtocol, LOW, ZkSslProtocolDoc)
            .define(ZkSslEnabledProtocolsProp, LIST, null, LOW, ZkSslEnabledProtocolsDoc)
            .define(ZkSslCipherSuitesProp, LIST, null, LOW, ZkSslCipherSuitesDoc)
            .define(ZkSslEndpointIdentificationAlgorithmProp, STRING, Defaults.ZkSslEndpointIdentificationAlgorithm, LOW, ZkSslEndpointIdentificationAlgorithmDoc)
            .define(ZkSslCrlEnableProp, BOOLEAN, Defaults.ZkSslCrlEnable, LOW, ZkSslCrlEnableDoc)
            .define(ZkSslOcspEnableProp, BOOLEAN, Defaults.ZkSslOcspEnable, LOW, ZkSslOcspEnableDoc)

            /*------------------ General Configuration ***********/
            .define(BrokerIdGenerationEnableProp, BOOLEAN, Defaults.BrokerIdGenerationEnable, MEDIUM, BrokerIdGenerationEnableDoc)
            .define(MaxReservedBrokerIdProp, INT, Defaults.MaxReservedBrokerId, atLeast(0), MEDIUM, MaxReservedBrokerIdDoc)
            .define(BrokerIdProp, INT, Defaults.BrokerId, HIGH, BrokerIdDoc)
            .define(MessageMaxBytesProp, INT, Defaults.MessageMaxBytes, atLeast(0), HIGH, MessageMaxBytesDoc)
            .define(NumNetworkThreadsProp, INT, Defaults.NumNetworkThreads, atLeast(1), HIGH, NumNetworkThreadsDoc)
            .define(NumIoThreadsProp, INT, Defaults.NumIoThreads, atLeast(1), HIGH, NumIoThreadsDoc)
            .define(NumReplicaAlterLogDirsThreadsProp, INT, null, HIGH, NumReplicaAlterLogDirsThreadsDoc)
            .define(BackgroundThreadsProp, INT, Defaults.BackgroundThreads, atLeast(1), HIGH, BackgroundThreadsDoc)
            .define(QueuedMaxRequestsProp, INT, Defaults.QueuedMaxRequests, atLeast(1), HIGH, QueuedMaxRequestsDoc)
            .define(QueuedMaxBytesProp, LONG, Defaults.QueuedMaxRequestBytes, MEDIUM, QueuedMaxRequestBytesDoc)
            .define(RequestTimeoutMsProp, INT, Defaults.RequestTimeoutMs, HIGH, RequestTimeoutMsDoc)
            .define(ConnectionSetupTimeoutMsProp, LONG, Defaults.ConnectionSetupTimeoutMs, MEDIUM, ConnectionSetupTimeoutMsDoc)
            .define(ConnectionSetupTimeoutMaxMsProp, LONG, Defaults.ConnectionSetupTimeoutMaxMs, MEDIUM, ConnectionSetupTimeoutMaxMsDoc)

            /*
             * KRaft mode configs.
             */
            .define(MetadataSnapshotMaxNewRecordBytesProp, LONG, Defaults.MetadataSnapshotMaxNewRecordBytes, atLeast(1), HIGH, MetadataSnapshotMaxNewRecordBytesDoc)

            /*
             * KRaft mode private configs. Note that these configs are defined as internal. We will make them public in the 3.0.0 release.
             */
            .define(ProcessRolesProp, LIST, Collections.emptyList(), ValidList.in("broker", "controller"), HIGH, ProcessRolesDoc)
            .define(NodeIdProp, INT, Defaults.EmptyNodeId, null, HIGH, NodeIdDoc)
            .define(InitialBrokerRegistrationTimeoutMsProp, INT, Defaults.InitialBrokerRegistrationTimeoutMs, null, MEDIUM, InitialBrokerRegistrationTimeoutMsDoc)
            .define(BrokerHeartbeatIntervalMsProp, INT, Defaults.BrokerHeartbeatIntervalMs, null, MEDIUM, BrokerHeartbeatIntervalMsDoc)
            .define(BrokerSessionTimeoutMsProp, INT, Defaults.BrokerSessionTimeoutMs, null, MEDIUM, BrokerSessionTimeoutMsDoc)
            .define(ControllerListenerNamesProp, STRING, null, null, HIGH, ControllerListenerNamesDoc)
            .define(SaslMechanismControllerProtocolProp, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, SaslMechanismControllerProtocolDoc)
            .define(MetadataLogDirProp, STRING, null, null, HIGH, MetadataLogDirDoc)
            .define(MetadataLogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(Records.LOG_OVERHEAD), HIGH, MetadataLogSegmentBytesDoc)
            .defineInternal(MetadataLogSegmentMinBytesProp, INT, 8 * 1024 * 1024, atLeast(Records.LOG_OVERHEAD), HIGH, MetadataLogSegmentMinBytesDoc)
            .define(MetadataLogSegmentMillisProp, LONG, Defaults.LogRollHours * 60 * 60 * 1000L, null, HIGH, MetadataLogSegmentMillisDoc)
            .define(MetadataMaxRetentionBytesProp, LONG, Defaults.LogRetentionBytes, null, HIGH, MetadataMaxRetentionBytesDoc)
            .define(MetadataMaxRetentionMillisProp, LONG, Defaults.LogRetentionHours * 60 * 60 * 1000L, null, HIGH, MetadataMaxRetentionMillisDoc)
            .define(MetadataMaxIdleIntervalMsProp, INT, Defaults.MetadataMaxIdleIntervalMs, atLeast(0), LOW, MetadataMaxIdleIntervalMsDoc)

            /*------------------ Authorizer Configuration ------------------*/
            .define(AuthorizerClassNameProp, STRING, Defaults.AuthorizerClassName, new ConfigDef.NonNullValidator(), LOW, AuthorizerClassNameDoc)
            .define(EarlyStartListenersProp, STRING, null, HIGH, EarlyStartListenersDoc)

            /*------------------ Socket Server Configuration ------------------*/
            .define(ListenersProp, STRING, Defaults.Listeners, HIGH, ListenersDoc)
            .define(AdvertisedListenersProp, STRING, null, HIGH, AdvertisedListenersDoc)
            .define(ListenerSecurityProtocolMapProp, STRING, Defaults.ListenerSecurityProtocolMap, LOW, ListenerSecurityProtocolMapDoc)
            .define(ControlPlaneListenerNameProp, STRING, null, HIGH, controlPlaneListenerNameDoc)
            .define(SocketSendBufferBytesProp, INT, Defaults.SocketSendBufferBytes, HIGH, SocketSendBufferBytesDoc)
            .define(SocketReceiveBufferBytesProp, INT, Defaults.SocketReceiveBufferBytes, HIGH, SocketReceiveBufferBytesDoc)
            .define(SocketRequestMaxBytesProp, INT, Defaults.SocketRequestMaxBytes, atLeast(1), HIGH, SocketRequestMaxBytesDoc)
            .define(SocketListenBacklogSizeProp, INT, Defaults.SocketListenBacklogSize, atLeast(1), MEDIUM, SocketListenBacklogSizeDoc)
            .define(MaxConnectionsPerIpProp, INT, Defaults.MaxConnectionsPerIp, atLeast(0), MEDIUM, MaxConnectionsPerIpDoc)
            .define(MaxConnectionsPerIpOverridesProp, STRING, Defaults.MaxConnectionsPerIpOverrides, MEDIUM, MaxConnectionsPerIpOverridesDoc)
            .define(MaxConnectionsProp, INT, Defaults.MaxConnections, atLeast(0), MEDIUM, MaxConnectionsDoc)
            .define(MaxConnectionCreationRateProp, INT, Defaults.MaxConnectionCreationRate, atLeast(0), MEDIUM, MaxConnectionCreationRateDoc)
            .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, MEDIUM, ConnectionsMaxIdleMsDoc)
            .define(FailedAuthenticationDelayMsProp, INT, Defaults.FailedAuthenticationDelayMs, atLeast(0), LOW, FailedAuthenticationDelayMsDoc)

            /*------------------ Rack Configuration ------------------*/
            .define(RackProp, STRING, null, MEDIUM, RackDoc)

            /*------------------ Log Configuration ------------------*/
            .define(NumPartitionsProp, INT, Defaults.NumPartitions, atLeast(1), MEDIUM, NumPartitionsDoc)
            .define(LogDirProp, STRING, Defaults.LogDir, HIGH, LogDirDoc)
            .define(LogDirsProp, STRING, null, HIGH, LogDirsDoc)
            .define(LogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, LogSegmentBytesDoc)

            .define(LogRollTimeMillisProp, LONG, null, HIGH, LogRollTimeMillisDoc)
            .define(LogRollTimeHoursProp, INT, Defaults.LogRollHours, atLeast(1), HIGH, LogRollTimeHoursDoc)

            .define(LogRollTimeJitterMillisProp, LONG, null, HIGH, LogRollTimeJitterMillisDoc)
            .define(LogRollTimeJitterHoursProp, INT, Defaults.LogRollJitterHours, atLeast(0), HIGH, LogRollTimeJitterHoursDoc)

            .define(LogRetentionTimeMillisProp, LONG, null, HIGH, LogRetentionTimeMillisDoc)
            .define(LogRetentionTimeMinutesProp, INT, null, HIGH, LogRetentionTimeMinsDoc)
            .define(LogRetentionTimeHoursProp, INT, Defaults.LogRetentionHours, HIGH, LogRetentionTimeHoursDoc)

            .define(LogRetentionBytesProp, LONG, Defaults.LogRetentionBytes, HIGH, LogRetentionBytesDoc)
            .define(LogCleanupIntervalMsProp, LONG, Defaults.LogCleanupIntervalMs, atLeast(1), MEDIUM, LogCleanupIntervalMsDoc)
            .define(LogCleanupPolicyProp, LIST, Defaults.LogCleanupPolicy, ValidList.in(Defaults.Compact, Defaults.Delete), MEDIUM, LogCleanupPolicyDoc)
            .define(LogCleanerThreadsProp, INT, Defaults.LogCleanerThreads, atLeast(0), MEDIUM, LogCleanerThreadsDoc)
            .define(LogCleanerIoMaxBytesPerSecondProp, DOUBLE, Defaults.LogCleanerIoMaxBytesPerSecond, MEDIUM, LogCleanerIoMaxBytesPerSecondDoc)
            .define(LogCleanerDedupeBufferSizeProp, LONG, Defaults.LogCleanerDedupeBufferSize, MEDIUM, LogCleanerDedupeBufferSizeDoc)
            .define(LogCleanerIoBufferSizeProp, INT, Defaults.LogCleanerIoBufferSize, atLeast(0), MEDIUM, LogCleanerIoBufferSizeDoc)
            .define(LogCleanerDedupeBufferLoadFactorProp, DOUBLE, Defaults.LogCleanerDedupeBufferLoadFactor, MEDIUM, LogCleanerDedupeBufferLoadFactorDoc)
            .define(LogCleanerBackoffMsProp, LONG, Defaults.LogCleanerBackoffMs, atLeast(0), MEDIUM, LogCleanerBackoffMsDoc)
            .define(LogCleanerMinCleanRatioProp, DOUBLE, Defaults.LogCleanerMinCleanRatio, between(0, 1), MEDIUM, LogCleanerMinCleanRatioDoc)
            .define(LogCleanerEnableProp, BOOLEAN, Defaults.LogCleanerEnable, MEDIUM, LogCleanerEnableDoc)
            .define(LogCleanerDeleteRetentionMsProp, LONG, Defaults.LogCleanerDeleteRetentionMs, atLeast(0), MEDIUM, LogCleanerDeleteRetentionMsDoc)
            .define(LogCleanerMinCompactionLagMsProp, LONG, Defaults.LogCleanerMinCompactionLagMs, atLeast(0), MEDIUM, LogCleanerMinCompactionLagMsDoc)
            .define(LogCleanerMaxCompactionLagMsProp, LONG, Defaults.LogCleanerMaxCompactionLagMs, atLeast(1), MEDIUM, LogCleanerMaxCompactionLagMsDoc)
            .define(LogIndexSizeMaxBytesProp, INT, Defaults.LogIndexSizeMaxBytes, atLeast(4), MEDIUM, LogIndexSizeMaxBytesDoc)
            .define(LogIndexIntervalBytesProp, INT, Defaults.LogIndexIntervalBytes, atLeast(0), MEDIUM, LogIndexIntervalBytesDoc)
            .define(LogFlushIntervalMessagesProp, LONG, Defaults.LogFlushIntervalMessages, atLeast(1), HIGH, LogFlushIntervalMessagesDoc)
            .define(LogDeleteDelayMsProp, LONG, Defaults.LogDeleteDelayMs, atLeast(0), HIGH, LogDeleteDelayMsDoc)
            .define(LogFlushSchedulerIntervalMsProp, LONG, Defaults.LogFlushSchedulerIntervalMs, HIGH, LogFlushSchedulerIntervalMsDoc)
            .define(LogFlushIntervalMsProp, LONG, null, HIGH, LogFlushIntervalMsDoc)
            .define(LogFlushOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushOffsetCheckpointIntervalMsDoc)
            .define(LogFlushStartOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushStartOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushStartOffsetCheckpointIntervalMsDoc)
            .define(LogPreAllocateProp, BOOLEAN, Defaults.LogPreAllocateEnable, MEDIUM, LogPreAllocateEnableDoc)
            .define(NumRecoveryThreadsPerDataDirProp, INT, Defaults.NumRecoveryThreadsPerDataDir, atLeast(1), HIGH, NumRecoveryThreadsPerDataDirDoc)
            .define(AutoCreateTopicsEnableProp, BOOLEAN, Defaults.AutoCreateTopicsEnable, HIGH, AutoCreateTopicsEnableDoc)
            .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), HIGH, MinInSyncReplicasDoc)
            .define(LogMessageFormatVersionProp, STRING, Defaults.LogMessageFormatVersion, new MetadataVersionValidator(), MEDIUM, LogMessageFormatVersionDoc)
            .define(LogMessageTimestampTypeProp, STRING, Defaults.LogMessageTimestampType, in("CreateTime", "LogAppendTime"), MEDIUM, LogMessageTimestampTypeDoc)
            .define(LogMessageTimestampDifferenceMaxMsProp, LONG, Defaults.LogMessageTimestampDifferenceMaxMs, atLeast(0), MEDIUM, LogMessageTimestampDifferenceMaxMsDoc)
            .define(CreateTopicPolicyClassNameProp, CLASS, null, LOW, CreateTopicPolicyClassNameDoc)
            .define(AlterConfigPolicyClassNameProp, CLASS, null, LOW, AlterConfigPolicyClassNameDoc)
            .define(LogMessageDownConversionEnableProp, BOOLEAN, Defaults.MessageDownConversionEnable, LOW, LogMessageDownConversionEnableDoc)

            /*------------------ Replication configuration ------------------*/
            .define(ControllerSocketTimeoutMsProp, INT, Defaults.ControllerSocketTimeoutMs, MEDIUM, ControllerSocketTimeoutMsDoc)
            .define(DefaultReplicationFactorProp, INT, Defaults.DefaultReplicationFactor, MEDIUM, DefaultReplicationFactorDoc)
            .define(ReplicaLagTimeMaxMsProp, LONG, Defaults.ReplicaLagTimeMaxMs, HIGH, ReplicaLagTimeMaxMsDoc)
            .define(ReplicaSocketTimeoutMsProp, INT, Defaults.ReplicaSocketTimeoutMs, HIGH, ReplicaSocketTimeoutMsDoc)
            .define(ReplicaSocketReceiveBufferBytesProp, INT, Defaults.ReplicaSocketReceiveBufferBytes, HIGH, ReplicaSocketReceiveBufferBytesDoc)
            .define(ReplicaFetchMaxBytesProp, INT, Defaults.ReplicaFetchMaxBytes, atLeast(0), MEDIUM, ReplicaFetchMaxBytesDoc)
            .define(ReplicaFetchWaitMaxMsProp, INT, Defaults.ReplicaFetchWaitMaxMs, HIGH, ReplicaFetchWaitMaxMsDoc)
            .define(ReplicaFetchBackoffMsProp, INT, Defaults.ReplicaFetchBackoffMs, atLeast(0), MEDIUM, ReplicaFetchBackoffMsDoc)
            .define(ReplicaFetchMinBytesProp, INT, Defaults.ReplicaFetchMinBytes, HIGH, ReplicaFetchMinBytesDoc)
            .define(ReplicaFetchResponseMaxBytesProp, INT, Defaults.ReplicaFetchResponseMaxBytes, atLeast(0), MEDIUM, ReplicaFetchResponseMaxBytesDoc)
            .define(NumReplicaFetchersProp, INT, Defaults.NumReplicaFetchers, HIGH, NumReplicaFetchersDoc)
            .define(ReplicaHighWatermarkCheckpointIntervalMsProp, LONG, Defaults.ReplicaHighWatermarkCheckpointIntervalMs, HIGH, ReplicaHighWatermarkCheckpointIntervalMsDoc)
            .define(FetchPurgatoryPurgeIntervalRequestsProp, INT, Defaults.FetchPurgatoryPurgeIntervalRequests, MEDIUM, FetchPurgatoryPurgeIntervalRequestsDoc)
            .define(ProducerPurgatoryPurgeIntervalRequestsProp, INT, Defaults.ProducerPurgatoryPurgeIntervalRequests, MEDIUM, ProducerPurgatoryPurgeIntervalRequestsDoc)
            .define(DeleteRecordsPurgatoryPurgeIntervalRequestsProp, INT, Defaults.DeleteRecordsPurgatoryPurgeIntervalRequests, MEDIUM, DeleteRecordsPurgatoryPurgeIntervalRequestsDoc)
            .define(AutoLeaderRebalanceEnableProp, BOOLEAN, Defaults.AutoLeaderRebalanceEnable, HIGH, AutoLeaderRebalanceEnableDoc)
            .define(LeaderImbalancePerBrokerPercentageProp, INT, Defaults.LeaderImbalancePerBrokerPercentage, HIGH, LeaderImbalancePerBrokerPercentageDoc)
            .define(LeaderImbalanceCheckIntervalSecondsProp, LONG, Defaults.LeaderImbalanceCheckIntervalSeconds, atLeast(1), HIGH, LeaderImbalanceCheckIntervalSecondsDoc)
            .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable, HIGH, UncleanLeaderElectionEnableDoc)
            .define(InterBrokerSecurityProtocolProp, STRING, Defaults.InterBrokerSecurityProtocol, in(Utils.enumOptions(SecurityProtocol.class)), MEDIUM, InterBrokerSecurityProtocolDoc)
            .define(InterBrokerProtocolVersionProp, STRING, Defaults.InterBrokerProtocolVersion, new MetadataVersionValidator(), MEDIUM, InterBrokerProtocolVersionDoc)
            .define(InterBrokerListenerNameProp, STRING, null, MEDIUM, InterBrokerListenerNameDoc)
            .define(ReplicaSelectorClassProp, STRING, null, MEDIUM, ReplicaSelectorClassDoc)

            /*------------------ Controlled shutdown configuration ------------------*/
            .define(ControlledShutdownMaxRetriesProp, INT, Defaults.ControlledShutdownMaxRetries, MEDIUM, ControlledShutdownMaxRetriesDoc)
            .define(ControlledShutdownRetryBackoffMsProp, LONG, Defaults.ControlledShutdownRetryBackoffMs, MEDIUM, ControlledShutdownRetryBackoffMsDoc)
            .define(ControlledShutdownEnableProp, BOOLEAN, Defaults.ControlledShutdownEnable, MEDIUM, ControlledShutdownEnableDoc)

            /*------------------ Group coordinator configuration ------------------*/
            .define(GroupMinSessionTimeoutMsProp, INT, Defaults.GroupMinSessionTimeoutMs, MEDIUM, GroupMinSessionTimeoutMsDoc)
            .define(GroupMaxSessionTimeoutMsProp, INT, Defaults.GroupMaxSessionTimeoutMs, MEDIUM, GroupMaxSessionTimeoutMsDoc)
            .define(GroupInitialRebalanceDelayMsProp, INT, Defaults.GroupInitialRebalanceDelayMs, MEDIUM, GroupInitialRebalanceDelayMsDoc)
            .define(GroupMaxSizeProp, INT, Defaults.GroupMaxSize, atLeast(1), MEDIUM, GroupMaxSizeDoc)

            /*------------------ Offset management configuration ------------------*/
            .define(OffsetMetadataMaxSizeProp, INT, Defaults.OffsetMetadataMaxSize, HIGH, OffsetMetadataMaxSizeDoc)
            .define(OffsetsLoadBufferSizeProp, INT, Defaults.OffsetsLoadBufferSize, atLeast(1), HIGH, OffsetsLoadBufferSizeDoc)
            .define(OffsetsTopicReplicationFactorProp, SHORT, Defaults.OffsetsTopicReplicationFactor, atLeast(1), HIGH, OffsetsTopicReplicationFactorDoc)
            .define(OffsetsTopicPartitionsProp, INT, Defaults.OffsetsTopicPartitions, atLeast(1), HIGH, OffsetsTopicPartitionsDoc)
            .define(OffsetsTopicSegmentBytesProp, INT, Defaults.OffsetsTopicSegmentBytes, atLeast(1), HIGH, OffsetsTopicSegmentBytesDoc)
            .define(OffsetsTopicCompressionCodecProp, INT, Defaults.OffsetsTopicCompressionCodec, HIGH, OffsetsTopicCompressionCodecDoc)
            .define(OffsetsRetentionMinutesProp, INT, Defaults.OffsetsRetentionMinutes, atLeast(1), HIGH, OffsetsRetentionMinutesDoc)
            .define(OffsetsRetentionCheckIntervalMsProp, LONG, Defaults.OffsetsRetentionCheckIntervalMs, atLeast(1), HIGH, OffsetsRetentionCheckIntervalMsDoc)
            .define(OffsetCommitTimeoutMsProp, INT, Defaults.OffsetCommitTimeoutMs, atLeast(1), HIGH, OffsetCommitTimeoutMsDoc)
            .define(OffsetCommitRequiredAcksProp, SHORT, Defaults.OffsetCommitRequiredAcks, HIGH, OffsetCommitRequiredAcksDoc)
            .define(DeleteTopicEnableProp, BOOLEAN, Defaults.DeleteTopicEnable, HIGH, DeleteTopicEnableDoc)
            .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(CompressionCodec.BrokerCompressionCodec.brokerCompressionOptions.toArray(new String[0])), HIGH, CompressionTypeDoc)

            /*------------------ Transaction management configuration ------------------*/
            .define(TransactionalIdExpirationMsProp, INT, Defaults.TransactionalIdExpirationMs, atLeast(1), HIGH, TransactionalIdExpirationMsDoc)
            .define(TransactionsMaxTimeoutMsProp, INT, Defaults.TransactionsMaxTimeoutMs, atLeast(1), HIGH, TransactionsMaxTimeoutMsDoc)
            .define(TransactionsTopicMinISRProp, INT, Defaults.TransactionsTopicMinISR, atLeast(1), HIGH, TransactionsTopicMinISRDoc)
            .define(TransactionsLoadBufferSizeProp, INT, Defaults.TransactionsLoadBufferSize, atLeast(1), HIGH, TransactionsLoadBufferSizeDoc)
            .define(TransactionsTopicReplicationFactorProp, SHORT, Defaults.TransactionsTopicReplicationFactor, atLeast(1), HIGH, TransactionsTopicReplicationFactorDoc)
            .define(TransactionsTopicPartitionsProp, INT, Defaults.TransactionsTopicPartitions, atLeast(1), HIGH, TransactionsTopicPartitionsDoc)
            .define(TransactionsTopicSegmentBytesProp, INT, Defaults.TransactionsTopicSegmentBytes, atLeast(1), HIGH, TransactionsTopicSegmentBytesDoc)
            .define(TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, INT, Defaults.TransactionsAbortTimedOutTransactionsCleanupIntervalMS, atLeast(1), LOW, TransactionsAbortTimedOutTransactionsIntervalMsDoc)
            .define(TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp, INT, Defaults.TransactionsRemoveExpiredTransactionsCleanupIntervalMS, atLeast(1), LOW, TransactionsRemoveExpiredTransactionsIntervalMsDoc)

            /*------------------ Fetch Configuration ------------------*/
            .define(MaxIncrementalFetchSessionCacheSlots, INT, Defaults.MaxIncrementalFetchSessionCacheSlots, atLeast(0), MEDIUM, MaxIncrementalFetchSessionCacheSlotsDoc)
            .define(FetchMaxBytes, INT, Defaults.FetchMaxBytes, atLeast(1024), MEDIUM, FetchMaxBytesDoc)

            /*------------------ Kafka Metrics Configuration ------------------*/
            .define(MetricNumSamplesProp, INT, Defaults.MetricNumSamples, atLeast(1), LOW, MetricNumSamplesDoc)
            .define(MetricSampleWindowMsProp, LONG, Defaults.MetricSampleWindowMs, atLeast(1), LOW, MetricSampleWindowMsDoc)
            .define(MetricReporterClassesProp, LIST, Defaults.MetricReporterClasses, LOW, MetricReporterClassesDoc)
            .define(MetricRecordingLevelProp, STRING, Defaults.MetricRecordingLevel, LOW, MetricRecordingLevelDoc)

            /*------------------ Kafka Yammer Metrics Reporter Configuration for docs ------------------*/
            .define(KafkaMetricsReporterClassesProp, LIST, Defaults.KafkaMetricReporterClasses, LOW, KafkaMetricsReporterClassesDoc)
            .define(KafkaMetricsPollingIntervalSecondsProp, INT, Defaults.KafkaMetricsPollingIntervalSeconds, atLeast(1), LOW, KafkaMetricsPollingIntervalSecondsDoc)

            /*------------------ Quota configuration ------------------*/
            .define(NumQuotaSamplesProp, INT, Defaults.NumQuotaSamples, atLeast(1), LOW, NumQuotaSamplesDoc)
            .define(NumReplicationQuotaSamplesProp, INT, Defaults.NumReplicationQuotaSamples, atLeast(1), LOW, NumReplicationQuotaSamplesDoc)
            .define(NumAlterLogDirsReplicationQuotaSamplesProp, INT, Defaults.NumAlterLogDirsReplicationQuotaSamples, atLeast(1), LOW, NumAlterLogDirsReplicationQuotaSamplesDoc)
            .define(NumControllerQuotaSamplesProp, INT, Defaults.NumControllerQuotaSamples, atLeast(1), LOW, NumControllerQuotaSamplesDoc)
            .define(QuotaWindowSizeSecondsProp, INT, Defaults.QuotaWindowSizeSeconds, atLeast(1), LOW, QuotaWindowSizeSecondsDoc)
            .define(ReplicationQuotaWindowSizeSecondsProp, INT, Defaults.ReplicationQuotaWindowSizeSeconds, atLeast(1), LOW, ReplicationQuotaWindowSizeSecondsDoc)
            .define(AlterLogDirsReplicationQuotaWindowSizeSecondsProp, INT, Defaults.AlterLogDirsReplicationQuotaWindowSizeSeconds, atLeast(1), LOW, AlterLogDirsReplicationQuotaWindowSizeSecondsDoc)
            .define(ControllerQuotaWindowSizeSecondsProp, INT, Defaults.ControllerQuotaWindowSizeSeconds, atLeast(1), LOW, ControllerQuotaWindowSizeSecondsDoc)
            .define(ClientQuotaCallbackClassProp, CLASS, null, LOW, ClientQuotaCallbackClassDoc)

            /*------------------ General Security Configuration ------------------*/
            .define(ConnectionsMaxReauthMsProp, LONG, Defaults.ConnectionsMaxReauthMsDefault, MEDIUM, ConnectionsMaxReauthMsDoc)
            .define(SaslServerMaxReceiveSizeProp, INT, Defaults.DefaultServerMaxMaxReceiveSize, MEDIUM, SaslServerMaxReceiveSizeDoc)
            .define(securityProviderClassProp, STRING, null, LOW, securityProviderClassDoc)

            /*------------------ SSL Configuration ------------------*/
            .define(PrincipalBuilderClassProp, CLASS, Defaults.DefaultPrincipalSerde, MEDIUM, PrincipalBuilderClassDoc)
            .define(SslProtocolProp, STRING, Defaults.SslProtocol, MEDIUM, SslProtocolDoc)
            .define(SslProviderProp, STRING, null, MEDIUM, SslProviderDoc)
            .define(SslEnabledProtocolsProp, LIST, Defaults.SslEnabledProtocols, MEDIUM, SslEnabledProtocolsDoc)
            .define(SslKeystoreTypeProp, STRING, Defaults.SslKeystoreType, MEDIUM, SslKeystoreTypeDoc)
            .define(SslKeystoreLocationProp, STRING, null, MEDIUM, SslKeystoreLocationDoc)
            .define(SslKeystorePasswordProp, PASSWORD, null, MEDIUM, SslKeystorePasswordDoc)
            .define(SslKeyPasswordProp, PASSWORD, null, MEDIUM, SslKeyPasswordDoc)
            .define(SslKeystoreKeyProp, PASSWORD, null, MEDIUM, SslKeystoreKeyDoc)
            .define(SslKeystoreCertificateChainProp, PASSWORD, null, MEDIUM, SslKeystoreCertificateChainDoc)
            .define(SslTruststoreTypeProp, STRING, Defaults.SslTruststoreType, MEDIUM, SslTruststoreTypeDoc)
            .define(SslTruststoreLocationProp, STRING, null, MEDIUM, SslTruststoreLocationDoc)
            .define(SslTruststorePasswordProp, PASSWORD, null, MEDIUM, SslTruststorePasswordDoc)
            .define(SslTruststoreCertificatesProp, PASSWORD, null, MEDIUM, SslTruststoreCertificatesDoc)
            .define(SslKeyManagerAlgorithmProp, STRING, Defaults.SslKeyManagerAlgorithm, MEDIUM, SslKeyManagerAlgorithmDoc)
            .define(SslTrustManagerAlgorithmProp, STRING, Defaults.SslTrustManagerAlgorithm, MEDIUM, SslTrustManagerAlgorithmDoc)
            .define(SslEndpointIdentificationAlgorithmProp, STRING, Defaults.SslEndpointIdentificationAlgorithm, LOW, SslEndpointIdentificationAlgorithmDoc)
            .define(SslSecureRandomImplementationProp, STRING, null, LOW, SslSecureRandomImplementationDoc)
            .define(SslClientAuthProp, STRING, Defaults.SslClientAuthentication, in(Defaults.SslClientAuthenticationValidValues.toArray(new String[0])), MEDIUM, SslClientAuthDoc)
            .define(SslCipherSuitesProp, LIST, Collections.emptyList(), MEDIUM, SslCipherSuitesDoc)
            .define(SslPrincipalMappingRulesProp, STRING, Defaults.SslPrincipalMappingRules, LOW, SslPrincipalMappingRulesDoc)
            .define(SslEngineFactoryClassProp, CLASS, null, LOW, SslEngineFactoryClassDoc)

            /*------------------ Sasl Configuration ------------------*/
            .define(SaslMechanismInterBrokerProtocolProp, STRING, Defaults.SaslMechanismInterBrokerProtocol, MEDIUM, SaslMechanismInterBrokerProtocolDoc)
            .define(SaslJaasConfigProp, PASSWORD, null, MEDIUM, SaslJaasConfigDoc)
            .define(SaslEnabledMechanismsProp, LIST, Defaults.SaslEnabledMechanisms, MEDIUM, SaslEnabledMechanismsDoc)
            .define(SaslServerCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslServerCallbackHandlerClassDoc)
            .define(SaslClientCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslClientCallbackHandlerClassDoc)
            .define(SaslLoginClassProp, CLASS, null, MEDIUM, SaslLoginClassDoc)
            .define(SaslLoginCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslLoginCallbackHandlerClassDoc)
            .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, SaslKerberosServiceNameDoc)
            .define(SaslKerberosKinitCmdProp, STRING, Defaults.SaslKerberosKinitCmd, MEDIUM, SaslKerberosKinitCmdDoc)
            .define(SaslKerberosTicketRenewWindowFactorProp, DOUBLE, Defaults.SaslKerberosTicketRenewWindowFactor, MEDIUM, SaslKerberosTicketRenewWindowFactorDoc)
            .define(SaslKerberosTicketRenewJitterProp, DOUBLE, Defaults.SaslKerberosTicketRenewJitter, MEDIUM, SaslKerberosTicketRenewJitterDoc)
            .define(SaslKerberosMinTimeBeforeReloginProp, LONG, Defaults.SaslKerberosMinTimeBeforeRelogin, MEDIUM, SaslKerberosMinTimeBeforeReloginDoc)
            .define(SaslKerberosPrincipalToLocalRulesProp, LIST, Defaults.SaslKerberosPrincipalToLocalRules, MEDIUM, SaslKerberosPrincipalToLocalRulesDoc)
            .define(SaslLoginRefreshWindowFactorProp, DOUBLE, Defaults.SaslLoginRefreshWindowFactor, MEDIUM, SaslLoginRefreshWindowFactorDoc)
            .define(SaslLoginRefreshWindowJitterProp, DOUBLE, Defaults.SaslLoginRefreshWindowJitter, MEDIUM, SaslLoginRefreshWindowJitterDoc)
            .define(SaslLoginRefreshMinPeriodSecondsProp, SHORT, Defaults.SaslLoginRefreshMinPeriodSeconds, MEDIUM, SaslLoginRefreshMinPeriodSecondsDoc)
            .define(SaslLoginRefreshBufferSecondsProp, SHORT, Defaults.SaslLoginRefreshBufferSeconds, MEDIUM, SaslLoginRefreshBufferSecondsDoc)
            .define(SaslLoginConnectTimeoutMsProp, INT, null, LOW, SaslLoginConnectTimeoutMsDoc)
            .define(SaslLoginReadTimeoutMsProp, INT, null, LOW, SaslLoginReadTimeoutMsDoc)
            .define(SaslLoginRetryBackoffMaxMsProp, LONG, Defaults.SaslLoginRetryBackoffMaxMs, LOW, SaslLoginRetryBackoffMaxMsDoc)
            .define(SaslLoginRetryBackoffMsProp, LONG, Defaults.SaslLoginRetryBackoffMs, LOW, SaslLoginRetryBackoffMsDoc)
            .define(SaslOAuthBearerScopeClaimNameProp, STRING, Defaults.SaslOAuthBearerScopeClaimName, LOW, SaslOAuthBearerScopeClaimNameDoc)
            .define(SaslOAuthBearerSubClaimNameProp, STRING, Defaults.SaslOAuthBearerSubClaimName, LOW, SaslOAuthBearerSubClaimNameDoc)
            .define(SaslOAuthBearerTokenEndpointUrlProp, STRING, null, MEDIUM, SaslOAuthBearerTokenEndpointUrlDoc)
            .define(SaslOAuthBearerJwksEndpointUrlProp, STRING, null, MEDIUM, SaslOAuthBearerJwksEndpointUrlDoc)
            .define(SaslOAuthBearerJwksEndpointRefreshMsProp, LONG, Defaults.SaslOAuthBearerJwksEndpointRefreshMs, LOW, SaslOAuthBearerJwksEndpointRefreshMsDoc)
            .define(SaslOAuthBearerJwksEndpointRetryBackoffMsProp, LONG, Defaults.SaslOAuthBearerJwksEndpointRetryBackoffMs, LOW, SaslOAuthBearerJwksEndpointRetryBackoffMsDoc)
            .define(SaslOAuthBearerJwksEndpointRetryBackoffMaxMsProp, LONG, Defaults.SaslOAuthBearerJwksEndpointRetryBackoffMaxMs, LOW, SaslOAuthBearerJwksEndpointRetryBackoffMaxMsDoc)
            .define(SaslOAuthBearerClockSkewSecondsProp, INT, Defaults.SaslOAuthBearerClockSkewSeconds, LOW, SaslOAuthBearerClockSkewSecondsDoc)
            .define(SaslOAuthBearerExpectedAudienceProp, LIST, null, LOW, SaslOAuthBearerExpectedAudienceDoc)
            .define(SaslOAuthBearerExpectedIssuerProp, STRING, null, LOW, SaslOAuthBearerExpectedIssuerDoc)

            /*------------------ Delegation Token Configuration ------------------*/
            .define(DelegationTokenSecretKeyAliasProp, PASSWORD, null, MEDIUM, DelegationTokenSecretKeyAliasDoc)
            .define(DelegationTokenSecretKeyProp, PASSWORD, null, MEDIUM, DelegationTokenSecretKeyDoc)
            .define(DelegationTokenMaxLifeTimeProp, LONG, Defaults.DelegationTokenMaxLifeTimeMsDefault, atLeast(1), MEDIUM, DelegationTokenMaxLifeTimeDoc)
            .define(DelegationTokenExpiryTimeMsProp, LONG, Defaults.DelegationTokenExpiryTimeMsDefault, atLeast(1), MEDIUM, DelegationTokenExpiryTimeMsDoc)
            .define(DelegationTokenExpiryCheckIntervalMsProp, LONG, Defaults.DelegationTokenExpiryCheckIntervalMsDefault, atLeast(1), LOW, DelegationTokenExpiryCheckIntervalDoc)

            /*------------------ Password encryption configuration for dynamic configs ------------------*/
            .define(PasswordEncoderSecretProp, PASSWORD, null, MEDIUM, PasswordEncoderSecretDoc)
            .define(PasswordEncoderOldSecretProp, PASSWORD, null, MEDIUM, PasswordEncoderOldSecretDoc)
            .define(PasswordEncoderKeyFactoryAlgorithmProp, STRING, null, LOW, PasswordEncoderKeyFactoryAlgorithmDoc)
            .define(PasswordEncoderCipherAlgorithmProp, STRING, Defaults.PasswordEncoderCipherAlgorithm, LOW, PasswordEncoderCipherAlgorithmDoc)
            .define(PasswordEncoderKeyLengthProp, INT, Defaults.PasswordEncoderKeyLength, atLeast(8), LOW, PasswordEncoderKeyLengthDoc)
            .define(PasswordEncoderIterationsProp, INT, Defaults.PasswordEncoderIterations, atLeast(1024), LOW, PasswordEncoderIterationsDoc)

            /*------------------ Raft Quorum Configuration ------------------*/
            .define(RaftConfig.QUORUM_VOTERS_CONFIG, LIST, Defaults.QuorumVoters, new RaftConfig.ControllerQuorumVotersValidator(), HIGH, RaftConfig.QUORUM_VOTERS_DOC)
            .define(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, Defaults.QuorumElectionTimeoutMs, null, HIGH, RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_DOC)
            .define(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, Defaults.QuorumFetchTimeoutMs, null, HIGH, RaftConfig.QUORUM_FETCH_TIMEOUT_MS_DOC)
            .define(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, Defaults.QuorumElectionBackoffMs, null, HIGH, RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
            .define(RaftConfig.QUORUM_LINGER_MS_CONFIG, INT, Defaults.QuorumLingerMs, null, MEDIUM, RaftConfig.QUORUM_LINGER_MS_DOC)
            .define(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, Defaults.QuorumRequestTimeoutMs, null, MEDIUM, RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_DOC)
            .define(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, Defaults.QuorumRetryBackoffMs, null, LOW, RaftConfig.QUORUM_RETRY_BACKOFF_MS_DOC);

    ///*------------------ Remote Log Management Configuration *********/
//  RemoteLogManagerConfig.CONFIG_DEF.configKeys().values().forEach(key => configDef.define(key))
//
//          def configNames: Seq[String] = configDef.names.asScala.toBuffer.sorted
    protected static Map<String, ?> defaultValues() {
        return configDef.defaultValues();
    }

    protected static Map<String, ConfigDef.ConfigKey> configKeys() {
        return configDef.configKeys();
    }

    public static KafkaConfig fromProps(Properties props) {
        return fromProps(props, true);
    }


    public static KafkaConfig fromProps(Properties props, Boolean doLog) {
        return new KafkaConfig(props, doLog);
    }

//        def fromProps(defaults: Properties, overrides: Properties): KafkaConfig =
//        fromProps(defaults, overrides, true)
//
//        def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaConfig = {
//        val props = new Properties()
//        props ++= defaults
//        props ++= overrides
//        fromProps(props, doLog)
//        }
//
//        def apply(props: java.util.Map[_, _], doLog: Boolean = true): KafkaConfig = new KafkaConfig(props, doLog)
//
//private def typeOf(name: String): Option[ConfigDef.Type] = Option(configDef.configKeys.get(name)).map(_.`type`)
//
//        def configType(configName: String): Option[ConfigDef.Type] = {
//        val configType = configTypeExact(configName)
//        if (configType.isDefined) {
//        return configType
//        }
//        typeOf(configName) match {
//        case Some(t) => Some(t)
//        case None =>
//        DynamicBrokerConfig.brokerConfigSynonyms(configName, matchListenerOverride = true).flatMap(typeOf).headOption
//        }
//        }
//
//private def configTypeExact(exactName: String): Option[ConfigDef.Type] = {
//        val configType = typeOf(exactName).orNull
//        if (configType != null) {
//        Some(configType)
//        } else {
//        val configKey = DynamicConfig.Broker.brokerConfigDef.configKeys().get(exactName)
//        if (configKey != null) {
//        Some(configKey.`type`)
//        } else {
//        None
//        }
//        }
//        }
//
//        def maybeSensitive(configType: Option[ConfigDef.Type]): Boolean = {
//        // If we can't determine the config entry type, treat it as a sensitive config to be safe
//        configType.isEmpty || configType.contains(ConfigDef.Type.PASSWORD)
//        }
//
//        def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
//        val maybeSensitive = resourceType match {
//        case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
//        case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name))
//        case ConfigResource.Type.BROKER_LOGGER => false
//        case _ => true
//        }
//        if (maybeSensitive) Password.HIDDEN else value
//        }

    /**
     * Copy a configuration map, populating some keys that we want to treat as synonyms.
     */
    public static Map<Object, Object> populateSynonyms(Map<Object, Object> input) {
        Map<Object, Object> output = new HashMap<>(input);
        Object brokerId = output.get(KafkaConfig.BrokerIdProp);
        Object nodeId = output.get(KafkaConfig.NodeIdProp);
        if (brokerId == null && nodeId != null) {
            output.put(KafkaConfig.BrokerIdProp, nodeId);
        } else if (brokerId != null && nodeId == null) {
            output.put(KafkaConfig.NodeIdProp, brokerId);
        }
        return output;
    }

    /*--------非静态成员-----------*/
    private final Map<Object, Object> props;
    private Optional<DynamicBrokerConfig> dynamicConfigOverride;

    // Cache the current config to avoid acquiring read lock to access from dynamicConfig
    private volatile KafkaConfig currentConfig = this;
    private final Set<KafkaRaftServer.ProcessRole> processRoles = parseProcessRoles();
    protected DynamicBrokerConfig dynamicConfig;

    // The following captures any system properties impacting ZooKeeper TLS configuration
    // and defines the default values this instance will use if no explicit config is given.
    // We make it part of each instance rather than the object to facilitate testing.
    private final ZKClientConfig zkClientConfigViaSystemProperties = new ZKClientConfig();

    public KafkaConfig(Map<Object, Object> props) {
        this(true, KafkaConfig.populateSynonyms(props), Optional.empty());
    }

    public KafkaConfig(Map<Object, Object> props, Boolean doLog) {
        this(doLog, KafkaConfig.populateSynonyms(props), Optional.empty());
    }

    public KafkaConfig(Map<Object, Object> props, Boolean doLog, Optional<DynamicBrokerConfig> dynamicConfigOverride) {
        this(doLog, KafkaConfig.populateSynonyms(props), dynamicConfigOverride);
    }

    private KafkaConfig(Boolean doLog, Map<Object, Object> props, Optional<DynamicBrokerConfig> dynamicConfigOverride) {
        super(KafkaConfig.configDef, props, doLog);
        this.props = props;
        this.dynamicConfigOverride = dynamicConfigOverride;
        this.dynamicConfig = dynamicConfigOverride.orElse(new DynamicBrokerConfig(this));

        if (CollectionUtils.isEmpty(processRoles)) {
            this.interBrokerProtocolVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString);
        } else {
            if (originals().containsKey(KafkaConfig.InterBrokerProtocolVersionProp)) {
                // A user-supplied IBP was given
                MetadataVersion configuredVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString);
                if (!configuredVersion.isKRaftSupported()) {
                    String msg = String.format("A non-KRaft version %s given for %s. The minimum version is %s",
                            interBrokerProtocolVersionString, KafkaConfig.InterBrokerProtocolVersionProp, MetadataVersion.MINIMUM_KRAFT_VERSION);
                    throw new ConfigException(msg);
                } else {
                    LOG.warn("{} is deprecated in KRaft mode as of 3.3 and will only " +
                            "be read when first upgrading from a KRaft prior to 3.3. See kafka-storage.sh help for details on setting " +
                            "the metadata version for a new KRaft cluster.", KafkaConfig.InterBrokerProtocolVersionProp);
                }
            }
            // In KRaft mode, we pin this value to the minimum KRaft-supported version. This prevents inadvertent usage of
            // the static IBP config in broker components running in KRaft mode
            this.interBrokerProtocolVersion = MetadataVersion.MINIMUM_KRAFT_VERSION;
        }

        if (LogConfig.shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion)) {
            this.logMessageFormatVersion = MetadataVersion.fromVersionString(Defaults.LogMessageFormatVersion);
        } else {
            this.logMessageFormatVersion = MetadataVersion.fromVersionString(logMessageFormatVersionString);
        }
    }

    protected void updateCurrentConfig(KafkaConfig newConfig) {
        this.currentConfig = newConfig;
    }

    @Override
    public Map<String, Object> originals() {
        if (this == currentConfig) {
            return super.originals();
        } else {
            return currentConfig.originals();
        }
    }

    @Override
    public Map<String, ?> values() {
        if (this == currentConfig) {
            return super.values();
        } else {
            return currentConfig.values();
        }
    }

    @Override
    public Map<String, ?> nonInternalValues() {
        if (this == currentConfig) {
            return super.nonInternalValues();
        } else {
            return currentConfig.values();
        }
    }

    @Override
    public Map<String, String> originalsStrings() {
        if (this == currentConfig) {
            return super.originalsStrings();
        } else {
            return currentConfig.originalsStrings();
        }
    }

    @Override
    public Map<String, Object> originalsWithPrefix(String prefix) {
        if (this == currentConfig) {
            return super.originalsWithPrefix(prefix);
        } else {
            return currentConfig.originalsWithPrefix(prefix);
        }
    }

    @Override
    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        if (this == currentConfig) {
            return super.valuesWithPrefixOverride(prefix);
        } else {
            return currentConfig.valuesWithPrefixOverride(prefix);
        }
    }

    @Override
    public Object get(String key) {
        if (this == currentConfig) {
            return super.get(key);
        } else {
            return currentConfig.get(key);
        }
    }

    //    //  During dynamic update, we use the values from this config, these are only used in DynamicBrokerConfig
    protected Map<String, Object> originalsFromThisConfig() {
        return super.originals();
    }

    protected Map<String, ?> valuesFromThisConfig() {
        return super.values();
    }

    //    def valuesFromThisConfigWithPrefixOverride(prefix: String): util.Map[String, AnyRef] =
//            super.valuesWithPrefixOverride(prefix)
//
//    /*------------------ Zookeeper Configuration ***********/
//    val zkConnect: String = getString(KafkaConfig.ZkConnectProp)
//    val zkSessionTimeoutMs: Int = getInt(KafkaConfig.ZkSessionTimeoutMsProp)
//    val zkConnectionTimeoutMs: Int =
//    Option(getInt(KafkaConfig.ZkConnectionTimeoutMsProp)).map(_.toInt).getOrElse(getInt(KafkaConfig.ZkSessionTimeoutMsProp))
//    val zkEnableSecureAcls: Boolean = getBoolean(KafkaConfig.ZkEnableSecureAclsProp)
//    val zkMaxInFlightRequests: Int = getInt(KafkaConfig.ZkMaxInFlightRequestsProp)
//
//    private val _remoteLogManagerConfig = new RemoteLogManagerConfig(this)
//    def remoteLogManagerConfig = _remoteLogManagerConfig
//
    private Boolean zkBooleanConfigOrSystemPropertyWithDefaultValue(String propKey) {
        // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
        // Need to translate any system property value from true/false (String) to true/false (Boolean)
        boolean actuallyProvided = originals().containsKey(propKey);
        if (actuallyProvided) {
            return getBoolean(propKey);
        } else {
            Optional<String> sysPropValue = KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey);
            // not specified so use the default value
            return sysPropValue.map("true"::equals).orElseGet(() -> getBoolean(propKey));
        }
    }

    //
//    private def zkStringConfigOrSystemPropertyWithDefaultValue(propKey: String): String = {
//        // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
//        val actuallyProvided = originals.containsKey(propKey)
//        if (actuallyProvided) getString(propKey) else {
//            KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey) match {
//                case Some(v) => v
//                case _ => getString(propKey) // not specified so use the default value
//            }
//        }
//    }
//
//    private def zkOptionalStringConfigOrSystemProperty(propKey: String): Option[String] = {
//        Option(getString(propKey)).orElse {
//            KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
//        }
//    }
//    private def zkPasswordConfigOrSystemProperty(propKey: String): Option[Password] = {
//        Option(getPassword(propKey)).orElse {
//            KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map(new Password(_))
//        }
//    }
//    private def zkListConfigOrSystemProperty(propKey: String): Option[util.List[String]] = {
//        Option(getList(propKey)).orElse {
//            KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map { sysProp =>
//                sysProp.split("\\s*,\\s*").toBuffer.asJava
//            }
//        }
//    }
//
    private final Boolean zkSslClientEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslClientEnableProp);
    //    val zkClientCnxnSocketClassName = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkClientCnxnSocketProp)
//    val zkSslKeyStoreLocation = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslKeyStoreLocationProp)
//    val zkSslKeyStorePassword = zkPasswordConfigOrSystemProperty(KafkaConfig.ZkSslKeyStorePasswordProp)
//    val zkSslKeyStoreType = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslKeyStoreTypeProp)
//    val zkSslTrustStoreLocation = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslTrustStoreLocationProp)
//    val zkSslTrustStorePassword = zkPasswordConfigOrSystemProperty(KafkaConfig.ZkSslTrustStorePasswordProp)
//    val zkSslTrustStoreType = zkOptionalStringConfigOrSystemProperty(KafkaConfig.ZkSslTrustStoreTypeProp)
//    val ZkSslProtocol = zkStringConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslProtocolProp)
//    val ZkSslEnabledProtocols = zkListConfigOrSystemProperty(KafkaConfig.ZkSslEnabledProtocolsProp)
//    val ZkSslCipherSuites = zkListConfigOrSystemProperty(KafkaConfig.ZkSslCipherSuitesProp)
//    val ZkSslEndpointIdentificationAlgorithm = {
//            // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
//            // Need to translate any system property value from true/false to HTTPS/<blank>
//            val kafkaProp = KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp
//            val actuallyProvided = originals.containsKey(kafkaProp)
//    if (actuallyProvided)
//    getString(kafkaProp)
//    else {
//        KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, kafkaProp) match {
//            case Some("true") => "HTTPS"
//            case Some(_) => ""
//            case None => getString(kafkaProp) // not specified so use the default value
//        }
//    }
//}
//    val ZkSslCrlEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslCrlEnableProp)
//    val ZkSslOcspEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(KafkaConfig.ZkSslOcspEnableProp)
//    /*------------------ General Configuration ***********/
//    val brokerIdGenerationEnable: Boolean = getBoolean(KafkaConfig.BrokerIdGenerationEnableProp)
    public Integer maxReservedBrokerId = getInt(KafkaConfig.MaxReservedBrokerIdProp);
    public Integer brokerId = getInt(KafkaConfig.BrokerIdProp);

    public Integer nodeId = getInt(KafkaConfig.NodeIdProp);

    //        val initialRegistrationTimeoutMs: Int = getInt(KafkaConfig.InitialBrokerRegistrationTimeoutMsProp)
//        val brokerHeartbeatIntervalMs: Int = getInt(KafkaConfig.BrokerHeartbeatIntervalMsProp)
//        val brokerSessionTimeoutMs: Int = getInt(KafkaConfig.BrokerSessionTimeoutMsProp)
//
//        def requiresZookeeper: Boolean = processRoles.isEmpty
    public Boolean usesSelfManagedQuorum() {
        return CollectionUtils.isNotEmpty(processRoles);
    }

    //
    private Set<KafkaRaftServer.ProcessRole> parseProcessRoles() {
        List<KafkaRaftServer.ProcessRole> roles = new ArrayList<>();
        for (String role : getList(KafkaConfig.ProcessRolesProp)) {
            if ("broker".equals(role)) {
                roles.add(BrokerRole);
            } else if ("controller".equals(role)) {
                roles.add(ControllerRole);
            } else {
                throw new ConfigException(String.format("Unknown process role '%s' (only 'broker' and 'controller' are allowed roles)", role));
            }
        }

        Set<KafkaRaftServer.ProcessRole> distinctRoles = new HashSet<>(roles);

        if (distinctRoles.size() != roles.size()) {
            throw new ConfigException(String.format("Duplicate role names found in `%s`: %s", KafkaConfig.ProcessRolesProp, roles));
        }

        return distinctRoles;
    }

    //
//        def isKRaftCoResidentMode: Boolean = {
//        processRoles == Set(BrokerRole, ControllerRole)
//        }

    public String metadataLogDir() {
        String dir = getString(KafkaConfig.MetadataLogDirProp);
        if (StringUtils.isNoneBlank(dir)) {
            return dir;
        } else {
            return CollectionUtilExt.head(logDirs);
        }
    }

//        def metadataLogSegmentBytes = getInt(KafkaConfig.MetadataLogSegmentBytesProp)
//        def metadataLogSegmentMillis = getLong(KafkaConfig.MetadataLogSegmentMillisProp)
//        def metadataRetentionBytes = getLong(KafkaConfig.MetadataMaxRetentionBytesProp)
//        def metadataRetentionMillis = getLong(KafkaConfig.MetadataMaxRetentionMillisProp)
//
//        def numNetworkThreads = getInt(KafkaConfig.NumNetworkThreadsProp)
//        def backgroundThreads = getInt(KafkaConfig.BackgroundThreadsProp)
//        val queuedMaxRequests = getInt(KafkaConfig.QueuedMaxRequestsProp)
//        val queuedMaxBytes = getLong(KafkaConfig.QueuedMaxBytesProp)
//        def numIoThreads = getInt(KafkaConfig.NumIoThreadsProp)

    public Integer messageMaxBytes() {
        return getInt(KafkaConfig.MessageMaxBytesProp);
    }

//        val requestTimeoutMs = getInt(KafkaConfig.RequestTimeoutMsProp)
//        val connectionSetupTimeoutMs = getLong(KafkaConfig.ConnectionSetupTimeoutMsProp)
//        val connectionSetupTimeoutMaxMs = getLong(KafkaConfig.ConnectionSetupTimeoutMaxMsProp)
//
//        def getNumReplicaAlterLogDirsThreads: Int = {
//        val numThreads: Integer = Option(getInt(KafkaConfig.NumReplicaAlterLogDirsThreadsProp)).getOrElse(logDirs.size)
//        numThreads
//        }
//
//        /************* Metadata Configuration ***********/
//        val metadataSnapshotMaxNewRecordBytes = getLong(KafkaConfig.MetadataSnapshotMaxNewRecordBytesProp)
//        val metadataMaxIdleIntervalNs: Option[Long] = {
//        val value = TimeUnit.NANOSECONDS.convert(getInt(KafkaConfig.MetadataMaxIdleIntervalMsProp).toLong, TimeUnit.MILLISECONDS)
//        if (value > 0) Some(value) else None
//        }
//
//        /************* Authorizer Configuration ***********/
//        def createNewAuthorizer(): Option[Authorizer] = {
//        val className = getString(KafkaConfig.AuthorizerClassNameProp)
//        if (className == null || className.isEmpty)
//        None
//        else {
//        Some(AuthorizerUtils.createAuthorizer(className))
//        }
//        }
//
//        val earlyStartListeners: Set[ListenerName] = {
//        val listenersSet = listeners.map(_.listenerName).toSet
//        val controllerListenersSet = controllerListeners.map(_.listenerName).toSet
//        Option(getString(KafkaConfig.EarlyStartListenersProp)) match {
//        case None => controllerListenersSet
//        case Some(str) =>
//        str.split(",").map(_.trim()).filter(!_.isEmpty).map { str =>
//        val listenerName = new ListenerName(str)
//        if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName))
//        throw new ConfigException(s"${KafkaConfig.EarlyStartListenersProp} contains " +
//        s"listener ${listenerName.value()}, but this is not contained in " +
//        s"${KafkaConfig.ListenersProp} or ${KafkaConfig.ControllerListenerNamesProp}")
//        listenerName
//        }.toSet
//        }
//        }
//
//        /*------------------ Socket Server Configuration ***********/
//        val socketSendBufferBytes = getInt(KafkaConfig.SocketSendBufferBytesProp)
//        val socketReceiveBufferBytes = getInt(KafkaConfig.SocketReceiveBufferBytesProp)
//        val socketRequestMaxBytes = getInt(KafkaConfig.SocketRequestMaxBytesProp)
//        val socketListenBacklogSize = getInt(KafkaConfig.SocketListenBacklogSizeProp)
//        val maxConnectionsPerIp = getInt(KafkaConfig.MaxConnectionsPerIpProp)
//        val maxConnectionsPerIpOverrides: Map[String, Int] =
//        getMap(KafkaConfig.MaxConnectionsPerIpOverridesProp, getString(KafkaConfig.MaxConnectionsPerIpOverridesProp)).map { case (k, v) => (k, v.toInt)}
//        def maxConnections = getInt(KafkaConfig.MaxConnectionsProp)
//        def maxConnectionCreationRate = getInt(KafkaConfig.MaxConnectionCreationRateProp)
//        val connectionsMaxIdleMs = getLong(KafkaConfig.ConnectionsMaxIdleMsProp)
//        val failedAuthenticationDelayMs = getInt(KafkaConfig.FailedAuthenticationDelayMsProp)
//
//        /***************** rack configuration ------------------*/
//        val rack = Option(getString(KafkaConfig.RackProp))
//        val replicaSelectorClassName = Option(getString(KafkaConfig.ReplicaSelectorClassProp))

    /*-------------- Log Configuration ---------------*/
    private final Boolean autoCreateTopicsEnable = getBoolean(KafkaConfig.AutoCreateTopicsEnableProp);
    private final Integer numPartitions = getInt(KafkaConfig.NumPartitionsProp);

    private final List<String> logDirs = CoreUtils.parseCsvList(Optional.ofNullable(getString(KafkaConfig.LogDirsProp)).orElseGet(() -> getString(KafkaConfig.LogDirProp)));

    public Integer logSegmentBytes() {
        return getInt(KafkaConfig.LogSegmentBytesProp);
    }

    public Long logFlushIntervalMessages() {
        return getLong(KafkaConfig.LogFlushIntervalMessagesProp);
    }

    private final Integer logCleanerThreads = getInt(KafkaConfig.LogCleanerThreadsProp);

    public Integer numRecoveryThreadsPerDataDir() {
        return getInt(KafkaConfig.NumRecoveryThreadsPerDataDirProp);
    }

    private final Long logFlushSchedulerIntervalMs = getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp);
    private final Long logFlushOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushOffsetCheckpointIntervalMsProp).longValue();
    private final Long logFlushStartOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushStartOffsetCheckpointIntervalMsProp).longValue();
    private final Long logCleanupIntervalMs = getLong(KafkaConfig.LogCleanupIntervalMsProp);

    public List<String> logCleanupPolicy() {
        return getList(KafkaConfig.LogCleanupPolicyProp);
    }

    private final Integer offsetsRetentionMinutes = getInt(KafkaConfig.OffsetsRetentionMinutesProp);
    private final Long offsetsRetentionCheckIntervalMs = getLong(KafkaConfig.OffsetsRetentionCheckIntervalMsProp);

    public Long logRetentionBytes() {
        return getLong(KafkaConfig.LogRetentionBytesProp);
    }

    private final Long logCleanerDedupeBufferSize = getLong(KafkaConfig.LogCleanerDedupeBufferSizeProp);
    private final Double logCleanerDedupeBufferLoadFactor = getDouble(KafkaConfig.LogCleanerDedupeBufferLoadFactorProp);
    private final Integer logCleanerIoBufferSize = getInt(KafkaConfig.LogCleanerIoBufferSizeProp);
    private final Double logCleanerIoMaxBytesPerSecond = getDouble(KafkaConfig.LogCleanerIoMaxBytesPerSecondProp);

    public Long logCleanerDeleteRetentionMs() {
        return getLong(KafkaConfig.LogCleanerDeleteRetentionMsProp);
    }

    public Long logCleanerMinCompactionLagMs() {
        return getLong(KafkaConfig.LogCleanerMinCompactionLagMsProp);
    }

    public Long logCleanerMaxCompactionLagMs() {
        return getLong(KafkaConfig.LogCleanerMaxCompactionLagMsProp);
    }

    private final Long logCleanerBackoffMs = getLong(KafkaConfig.LogCleanerBackoffMsProp);

    public Double logCleanerMinCleanRatio() {
        return getDouble(KafkaConfig.LogCleanerMinCleanRatioProp);
    }

    private final Boolean logCleanerEnable = getBoolean(KafkaConfig.LogCleanerEnableProp);

    public Integer logIndexSizeMaxBytes() {
        return getInt(KafkaConfig.LogIndexSizeMaxBytesProp);
    }

    public Integer logIndexIntervalBytes() {
        return getInt(KafkaConfig.LogIndexIntervalBytesProp);
    }

    public Long logDeleteDelayMs() {
        return getLong(KafkaConfig.LogDeleteDelayMsProp);
    }

    public Long logRollTimeMillis() {
        return Optional.ofNullable(getLong(KafkaConfig.LogRollTimeMillisProp))
                .orElseGet(() -> 60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeHoursProp));
    }

    public Long logRollTimeJitterMillis() {
        return Optional.ofNullable(getLong(KafkaConfig.LogRollTimeJitterMillisProp))
                .orElseGet(() -> 60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeJitterHoursProp));
    }

    public Long logFlushIntervalMs() {
        return Optional.ofNullable(getLong(KafkaConfig.LogFlushIntervalMsProp))
                .orElseGet(() -> getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp));
    }

    public Integer minInSyncReplicas() {
        return getInt(KafkaConfig.MinInSyncReplicasProp);
    }

    public Boolean logPreAllocateEnable() {
        return getBoolean(KafkaConfig.LogPreAllocateProp);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
    // is passed, `0.10.0-IV0` may be picked)
//    @nowarn("cat=deprecation")
    private final String logMessageFormatVersionString = getString(KafkaConfig.LogMessageFormatVersionProp);

    /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
    @Deprecated
    private final MetadataVersion logMessageFormatVersion;

    public TimestampType logMessageTimestampType() {
        return TimestampType.forName(getString(KafkaConfig.LogMessageTimestampTypeProp));
    }

    public Long logMessageTimestampDifferenceMaxMs() {
        return getLong(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp);
    }

    public Boolean logMessageDownConversionEnable() {
        return getBoolean(KafkaConfig.LogMessageDownConversionEnableProp);
    }

//
//    /*------------------ Replication configuration ***********/
//    val controllerSocketTimeoutMs: Int = getInt(KafkaConfig.ControllerSocketTimeoutMsProp)
//    val defaultReplicationFactor: Int = getInt(KafkaConfig.DefaultReplicationFactorProp)
//    val replicaLagTimeMaxMs = getLong(KafkaConfig.ReplicaLagTimeMaxMsProp)
//    val replicaSocketTimeoutMs = getInt(KafkaConfig.ReplicaSocketTimeoutMsProp)
//    val replicaSocketReceiveBufferBytes = getInt(KafkaConfig.ReplicaSocketReceiveBufferBytesProp)
//    val replicaFetchMaxBytes = getInt(KafkaConfig.ReplicaFetchMaxBytesProp)
//    val replicaFetchWaitMaxMs = getInt(KafkaConfig.ReplicaFetchWaitMaxMsProp)
//    val replicaFetchMinBytes = getInt(KafkaConfig.ReplicaFetchMinBytesProp)
//    val replicaFetchResponseMaxBytes = getInt(KafkaConfig.ReplicaFetchResponseMaxBytesProp)
//    val replicaFetchBackoffMs = getInt(KafkaConfig.ReplicaFetchBackoffMsProp)
//    def numReplicaFetchers = getInt(KafkaConfig.NumReplicaFetchersProp)
//    val replicaHighWatermarkCheckpointIntervalMs = getLong(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp)
//    val fetchPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp)
//    val producerPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp)
//    val deleteRecordsPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.DeleteRecordsPurgatoryPurgeIntervalRequestsProp)
//    val autoLeaderRebalanceEnable = getBoolean(KafkaConfig.AutoLeaderRebalanceEnableProp)
//    val leaderImbalancePerBrokerPercentage = getInt(KafkaConfig.LeaderImbalancePerBrokerPercentageProp)
//    val leaderImbalanceCheckIntervalSeconds: Long = getLong(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp)

    public Boolean uncleanLeaderElectionEnable() {
        return getBoolean(KafkaConfig.UncleanLeaderElectionEnableProp);
    }

    // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
    // is passed, `0.10.0-IV0` may be picked)
    private final String interBrokerProtocolVersionString = getString(KafkaConfig.InterBrokerProtocolVersionProp);
    private final MetadataVersion interBrokerProtocolVersion;

//    /*------------------ Controlled shutdown configuration ***********/
//    val controlledShutdownMaxRetries = getInt(KafkaConfig.ControlledShutdownMaxRetriesProp)
//    val controlledShutdownRetryBackoffMs = getLong(KafkaConfig.ControlledShutdownRetryBackoffMsProp)
//    val controlledShutdownEnable = getBoolean(KafkaConfig.ControlledShutdownEnableProp)
//
//    /*------------------ Feature configuration ***********/
//    def isFeatureVersioningSupported = interBrokerProtocolVersion.isFeatureVersioningSupported()
//
//    /*------------------ Group coordinator configuration ***********/
//    val groupMinSessionTimeoutMs = getInt(KafkaConfig.GroupMinSessionTimeoutMsProp)
//    val groupMaxSessionTimeoutMs = getInt(KafkaConfig.GroupMaxSessionTimeoutMsProp)
//    val groupInitialRebalanceDelay = getInt(KafkaConfig.GroupInitialRebalanceDelayMsProp)
//    val groupMaxSize = getInt(KafkaConfig.GroupMaxSizeProp)
//
//    /*------------------ Offset management configuration ***********/
//    val offsetMetadataMaxSize = getInt(KafkaConfig.OffsetMetadataMaxSizeProp)
//    val offsetsLoadBufferSize = getInt(KafkaConfig.OffsetsLoadBufferSizeProp)
//    val offsetsTopicReplicationFactor = getShort(KafkaConfig.OffsetsTopicReplicationFactorProp)
//    val offsetsTopicPartitions = getInt(KafkaConfig.OffsetsTopicPartitionsProp)
//    val offsetCommitTimeoutMs = getInt(KafkaConfig.OffsetCommitTimeoutMsProp)
//    val offsetCommitRequiredAcks = getShort(KafkaConfig.OffsetCommitRequiredAcksProp)
//    val offsetsTopicSegmentBytes = getInt(KafkaConfig.OffsetsTopicSegmentBytesProp)
//    val offsetsTopicCompressionCodec = Option(getInt(KafkaConfig.OffsetsTopicCompressionCodecProp)).map(value => CompressionCodec.getCompressionCodec(value)).orNull

    /*------------------* Transaction management configuration -----------------*/
    private final Integer transactionalIdExpirationMs = getInt(KafkaConfig.TransactionalIdExpirationMsProp);
    private final Integer transactionMaxTimeoutMs = getInt(KafkaConfig.TransactionsMaxTimeoutMsProp);
    private final Integer transactionTopicMinISR = getInt(KafkaConfig.TransactionsTopicMinISRProp);
    private final Integer transactionsLoadBufferSize = getInt(KafkaConfig.TransactionsLoadBufferSizeProp);
    private final Short transactionTopicReplicationFactor = getShort(KafkaConfig.TransactionsTopicReplicationFactorProp);
    private final Integer transactionTopicPartitions = getInt(KafkaConfig.TransactionsTopicPartitionsProp);
    private final Integer transactionTopicSegmentBytes = getInt(KafkaConfig.TransactionsTopicSegmentBytesProp);
    private final Integer transactionAbortTimedOutTransactionCleanupIntervalMs = getInt(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp);
    private final Integer transactionRemoveExpiredTransactionalIdCleanupIntervalMs = getInt(KafkaConfig.TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp);

    //    /*------------------ Metric Configuration ------------------*/
    public Integer metricNumSamples = getInt(KafkaConfig.MetricNumSamplesProp);
    public Long metricSampleWindowMs = getLong(KafkaConfig.MetricSampleWindowMsProp);
    public String metricRecordingLevel = getString(KafkaConfig.MetricRecordingLevelProp);
    //
//    /*------------------ SSL/SASL Configuration ------------------*/
//    // Security configs may be overridden for listeners, so it is not safe to use the base values
//    // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
//    // retrieved using KafkaConfig#valuesWithPrefixOverride
//    private def saslEnabledMechanisms(listenerName: ListenerName): Set[String] = {
//        val value = valuesWithPrefixOverride(listenerName.configPrefix).get(KafkaConfig.SaslEnabledMechanismsProp)
//        if (value != null)
//            value.asInstanceOf[util.List[String]].asScala.toSet
//        else
//            Set.empty[String]
//    }
//
//    def interBrokerListenerName = getInterBrokerListenerNameAndSecurityProtocol._1
//    def interBrokerSecurityProtocol = getInterBrokerListenerNameAndSecurityProtocol._2
//    def controlPlaneListenerName = getControlPlaneListenerNameAndSecurityProtocol.map { case (listenerName, _) => listenerName }
//    def controlPlaneSecurityProtocol = getControlPlaneListenerNameAndSecurityProtocol.map { case (_, securityProtocol) => securityProtocol }
//    def saslMechanismInterBrokerProtocol = getString(KafkaConfig.SaslMechanismInterBrokerProtocolProp)
//    val saslInterBrokerHandshakeRequestEnable = interBrokerProtocolVersion.isSaslInterBrokerHandshakeRequestEnabled()
//
//    /*------------------ DelegationToken Configuration ------------------*/
//    val delegationTokenSecretKey = Option(getPassword(KafkaConfig.DelegationTokenSecretKeyProp))
//            .getOrElse(getPassword(KafkaConfig.DelegationTokenSecretKeyAliasProp))
//    val tokenAuthEnabled = (delegationTokenSecretKey != null && !delegationTokenSecretKey.value.isEmpty)
//    val delegationTokenMaxLifeMs = getLong(KafkaConfig.DelegationTokenMaxLifeTimeProp)
//    val delegationTokenExpiryTimeMs = getLong(KafkaConfig.DelegationTokenExpiryTimeMsProp)
//    val delegationTokenExpiryCheckIntervalMs = getLong(KafkaConfig.DelegationTokenExpiryCheckIntervalMsProp)
//
//    /*------------------ Password encryption configuration for dynamic configs *********/
    public Optional<Password> passwordEncoderSecret = Optional.ofNullable(getPassword(KafkaConfig.PasswordEncoderSecretProp));
//    def passwordEncoderOldSecret = Option(getPassword(KafkaConfig.PasswordEncoderOldSecretProp))
//    def passwordEncoderCipherAlgorithm = getString(KafkaConfig.PasswordEncoderCipherAlgorithmProp)
//    def passwordEncoderKeyFactoryAlgorithm = Option(getString(KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp))
//    def passwordEncoderKeyLength = getInt(KafkaConfig.PasswordEncoderKeyLengthProp)
//    def passwordEncoderIterations = getInt(KafkaConfig.PasswordEncoderIterationsProp)
//
//    /*------------------ Quota Configuration ------------------*/
//    val numQuotaSamples = getInt(KafkaConfig.NumQuotaSamplesProp)
//    val quotaWindowSizeSeconds = getInt(KafkaConfig.QuotaWindowSizeSecondsProp)
//    val numReplicationQuotaSamples = getInt(KafkaConfig.NumReplicationQuotaSamplesProp)
//    val replicationQuotaWindowSizeSeconds = getInt(KafkaConfig.ReplicationQuotaWindowSizeSecondsProp)
//    val numAlterLogDirsReplicationQuotaSamples = getInt(KafkaConfig.NumAlterLogDirsReplicationQuotaSamplesProp)
//    val alterLogDirsReplicationQuotaWindowSizeSeconds = getInt(KafkaConfig.AlterLogDirsReplicationQuotaWindowSizeSecondsProp)
//    val numControllerQuotaSamples = getInt(KafkaConfig.NumControllerQuotaSamplesProp)
//    val controllerQuotaWindowSizeSeconds = getInt(KafkaConfig.ControllerQuotaWindowSizeSecondsProp)
//
//    /*------------------ Fetch Configuration ------------------*/
//    val maxIncrementalFetchSessionCacheSlots = getInt(KafkaConfig.MaxIncrementalFetchSessionCacheSlots)
//    val fetchMaxBytes = getInt(KafkaConfig.FetchMaxBytes)
//
//    val deleteTopicEnable = getBoolean(KafkaConfig.DeleteTopicEnableProp)

    public String compressionType() {
        return getString(KafkaConfig.CompressionTypeProp);
    }


    /*------------------ Raft Quorum Configuration *********/
    private final List<String> quorumVoters = getList(RaftConfig.QUORUM_VOTERS_CONFIG);
    private final int quorumElectionTimeoutMs = getInt(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG);
    private final int quorumFetchTimeoutMs = getInt(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG);
    private final int quorumElectionBackoffMs = getInt(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG);
    private final int quorumLingerMs = getInt(RaftConfig.QUORUM_LINGER_MS_CONFIG);
    private final int quorumRequestTimeoutMs = getInt(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG);
    private final int quorumRetryBackoffMs = getInt(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG);

//    public void addReconfigurable(Reconfigurable reconfigurable) {
//        dynamicConfig.addReconfigurable(reconfigurable);
//    }
//
//    public void removeReconfigurable(Reconfigurable reconfigurable) {
//        dynamicConfig.removeReconfigurable(reconfigurable);
//    }

    public Long logRetentionTimeMillis() {
        long millisInMinute = 60L * 1000L;
        long millisInHour = 60L * millisInMinute;

        long millis = 0L;

        Optional<Long> optional = Optional.ofNullable(getLong(KafkaConfig.LogRetentionTimeMillisProp));
        if (!optional.isPresent()) {
            optional = Optional.ofNullable(getInt(KafkaConfig.LogRetentionTimeMinutesProp).longValue());
        }
        if (optional.isPresent()) {
            Long mins = optional.get();
            millis = millisInMinute * mins;
        } else {
            millis = getInt(KafkaConfig.LogRetentionTimeHoursProp) * millisInHour;
        }

        if (millis < 0) {
            return -1L;
        }
        return millis;
    }

//    private def getMap(propName: String, propValue: String): Map[String, String] = {
//        try {
//            CoreUtils.parseCsvMap(propValue)
//        } catch {
//            case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(propName, e.getMessage))
//        }
//    }
//
//    def listeners: Seq[EndPoint] =
//            CoreUtils.listenerListToEndPoints(getString(KafkaConfig.ListenersProp), effectiveListenerSecurityProtocolMap)
//
//    def controllerListenerNames: Seq[String] = {
//        val value = Option(getString(KafkaConfig.ControllerListenerNamesProp)).getOrElse("")
//        if (value.isEmpty) {
//            Seq.empty
//        } else {
//            value.split(",")
//        }
//    }
//
//    def controllerListeners: Seq[EndPoint] =
//            listeners.filter(l => controllerListenerNames.contains(l.listenerName.value()))
//
//    def saslMechanismControllerProtocol: String = getString(KafkaConfig.SaslMechanismControllerProtocolProp)
//
//    def controlPlaneListener: Option[EndPoint] = {
//        controlPlaneListenerName.map { listenerName =>
//            listeners.filter(endpoint => endpoint.listenerName.value() == listenerName.value()).head
//        }
//    }
//
//    def dataPlaneListeners: Seq[EndPoint] = {
//        listeners.filterNot { listener =>
//            val name = listener.listenerName.value()
//            name.equals(getString(KafkaConfig.ControlPlaneListenerNameProp)) ||
//                    controllerListenerNames.contains(name)
//        }
//    }
//
//    // Use advertised listeners if defined, fallback to listeners otherwise
//    def effectiveAdvertisedListeners: Seq[EndPoint] = {
//        val advertisedListenersProp = getString(KafkaConfig.AdvertisedListenersProp)
//        if (advertisedListenersProp != null)
//            CoreUtils.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, requireDistinctPorts=false)
//        else
//            listeners.filterNot(l => controllerListenerNames.contains(l.listenerName.value()))
//    }
//
//    private def getInterBrokerListenerNameAndSecurityProtocol: (ListenerName, SecurityProtocol) = {
//        Option(getString(KafkaConfig.InterBrokerListenerNameProp)) match {
//            case Some(_) if originals.containsKey(KafkaConfig.InterBrokerSecurityProtocolProp) =>
//                throw new ConfigException(s"Only one of ${KafkaConfig.InterBrokerListenerNameProp} and " +
//                        s"${KafkaConfig.InterBrokerSecurityProtocolProp} should be set.")
//            case Some(name) =>
//                val listenerName = ListenerName.normalised(name)
//                val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
//                throw new ConfigException(s"Listener with name ${listenerName.value} defined in " +
//                        s"${KafkaConfig.InterBrokerListenerNameProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}."))
//                (listenerName, securityProtocol)
//            case None =>
//                val securityProtocol = getSecurityProtocol(getString(KafkaConfig.InterBrokerSecurityProtocolProp),
//                        KafkaConfig.InterBrokerSecurityProtocolProp)
//                (ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
//        }
//    }
//
//    private def getControlPlaneListenerNameAndSecurityProtocol: Option[(ListenerName, SecurityProtocol)] = {
//        Option(getString(KafkaConfig.ControlPlaneListenerNameProp)) match {
//            case Some(name) =>
//                val listenerName = ListenerName.normalised(name)
//                val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
//                throw new ConfigException(s"Listener with ${listenerName.value} defined in " +
//                        s"${KafkaConfig.ControlPlaneListenerNameProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}."))
//                Some(listenerName, securityProtocol)
//
//            case None => None
//        }
//    }
//
//    private def getSecurityProtocol(protocolName: String, configName: String): SecurityProtocol = {
//        try SecurityProtocol.forName(protocolName)
//    catch {
//            case _: IllegalArgumentException =>
//                throw new ConfigException(s"Invalid security protocol `$protocolName` defined in $configName")
//        }
//    }
//
//    def effectiveListenerSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = {
//        val mapValue = getMap(KafkaConfig.ListenerSecurityProtocolMapProp, getString(KafkaConfig.ListenerSecurityProtocolMapProp))
//                .map { case (listenerName, protocolName) =>
//            ListenerName.normalised(listenerName) -> getSecurityProtocol(protocolName, KafkaConfig.ListenerSecurityProtocolMapProp)
//        }
//        if (usesSelfManagedQuorum && !originals.containsKey(ListenerSecurityProtocolMapProp)) {
//            // Nothing was specified explicitly for listener.security.protocol.map, so we are using the default value,
//            // and we are using KRaft.
//            // Add PLAINTEXT mappings for controller listeners as long as there is no SSL or SASL_{PLAINTEXT,SSL} in use
//            def isSslOrSasl(name: String): Boolean = name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name)
//            // check controller listener names (they won't appear in listeners when process.roles=broker)
//            // as well as listeners for occurrences of SSL or SASL_*
//            if (controllerListenerNames.exists(isSslOrSasl) ||
//                    parseCsvList(getString(KafkaConfig.ListenersProp)).exists(listenerValue => isSslOrSasl(EndPoint.parseListenerName(listenerValue)))) {
//                mapValue // don't add default mappings since we found something that is SSL or SASL_*
//            } else {
//                // add the PLAINTEXT mappings for all controller listener names that are not explicitly PLAINTEXT
//                mapValue ++ controllerListenerNames.filter(!SecurityProtocol.PLAINTEXT.name.equals(_)).map(
//                        new ListenerName(_) -> SecurityProtocol.PLAINTEXT)
//            }
//        } else {
//            mapValue
//        }
//    }


//    // Topic IDs are used with all self-managed quorum clusters and ZK cluster with IBP greater than or equal to 2.8
//    def usesTopicId: Boolean =
//    usesSelfManagedQuorum || interBrokerProtocolVersion.isTopicIdsSupported()
//
//    validateValues()
//
//    @nowarn("cat=deprecation")
//    private def validateValues(): Unit = {
//        if (nodeId != brokerId) {
//            throw new ConfigException(s"You must set `${KafkaConfig.NodeIdProp}` to the same value as `${KafkaConfig.BrokerIdProp}`.")
//        }
//        if (requiresZookeeper) {
//            if (zkConnect == null) {
//                throw new ConfigException(s"Missing required configuration `${KafkaConfig.ZkConnectProp}` which has no default value.")
//            }
//            if (brokerIdGenerationEnable) {
//                require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id")
//            } else {
//                require(brokerId >= 0, "broker.id must be greater than or equal to 0")
//            }
//        } else {
//            // KRaft-based metadata quorum
//            if (nodeId < 0) {
//                throw new ConfigException(s"Missing configuration `${KafkaConfig.NodeIdProp}` which is required " +
//                        s"when `process.roles` is defined (i.e. when running in KRaft mode).")
//            }
//        }
//        require(logRollTimeMillis >= 1, "log.roll.ms must be greater than or equal to 1")
//        require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be greater than or equal to 0")
//        require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1")
//        require(logDirs.nonEmpty, "At least one log directory must be defined via log.dirs or log.dir.")
//        require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
//        require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
//                " to prevent unnecessary socket timeouts")
//        require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
//                " to prevent frequent changes in ISR")
//        require(offsetCommitRequiredAcks >= -1 && offsetCommitRequiredAcks <= offsetsTopicReplicationFactor,
//                "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor")
//        require(BrokerCompressionCodec.isValid(compressionType), "compression.type : " + compressionType + " is not valid." +
//                " Valid options are " + BrokerCompressionCodec.brokerCompressionOptions.mkString(","))
//        val advertisedListenerNames = effectiveAdvertisedListeners.map(_.listenerName).toSet
//
//        // validate KRaft-related configs
//        val voterAddressSpecsByNodeId = RaftConfig.parseVoterConnections(quorumVoters)
//        def validateNonEmptyQuorumVotersForKRaft(): Unit = {
//        if (voterAddressSpecsByNodeId.isEmpty) {
//            throw new ConfigException(s"If using ${KafkaConfig.ProcessRolesProp}, ${KafkaConfig.QuorumVotersProp} must contain a parseable set of voters.")
//        }
//    }
//        def validateControlPlaneListenerEmptyForKRaft(): Unit = {
//                require(controlPlaneListenerName.isEmpty,
//                        s"${KafkaConfig.ControlPlaneListenerNameProp} is not supported in KRaft mode.")
//        }
//        def validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(): Unit = {
//                require(!advertisedListenerNames.exists(aln => controllerListenerNames.contains(aln.value())),
//                s"The advertised.listeners config must not contain KRaft controller listeners from ${KafkaConfig.ControllerListenerNamesProp} when ${KafkaConfig.ProcessRolesProp} contains the broker role because Kafka clients that send requests via advertised listeners do not send requests to KRaft controllers -- they only send requests to KRaft brokers.")
//    }
//        def validateControllerQuorumVotersMustContainNodeIdForKRaftController(): Unit = {
//                require(voterAddressSpecsByNodeId.containsKey(nodeId),
//                        s"If ${KafkaConfig.ProcessRolesProp} contains the 'controller' role, the node id $nodeId must be included in the set of voters ${KafkaConfig.QuorumVotersProp}=${voterAddressSpecsByNodeId.asScala.keySet.toSet}")
//        }
//        def validateControllerListenerExistsForKRaftController(): Unit = {
//                require(controllerListeners.nonEmpty,
//                        s"${KafkaConfig.ControllerListenerNamesProp} must contain at least one value appearing in the '${KafkaConfig.ListenersProp}' configuration when running the KRaft controller role")
//        }
//        def validateControllerListenerNamesMustAppearInListenersForKRaftController(): Unit = {
//                val listenerNameValues = listeners.map(_.listenerName.value).toSet
//                require(controllerListenerNames.forall(cln => listenerNameValues.contains(cln)),
//                s"${KafkaConfig.ControllerListenerNamesProp} must only contain values appearing in the '${KafkaConfig.ListenersProp}' configuration when running the KRaft controller role")
//    }
//        def validateAdvertisedListenersNonEmptyForBroker(): Unit = {
//                require(advertisedListenerNames.nonEmpty,
//                        "There must be at least one advertised listener." + (
//        if (processRoles.contains(BrokerRole)) s" Perhaps all listeners appear in ${ControllerListenerNamesProp}?" else ""))
//    }
//        if (processRoles == Set(BrokerRole)) {
//            // KRaft broker-only
//            validateNonEmptyQuorumVotersForKRaft()
//            validateControlPlaneListenerEmptyForKRaft()
//            validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
//            // nodeId must not appear in controller.quorum.voters
//            require(!voterAddressSpecsByNodeId.containsKey(nodeId),
//                    s"If ${KafkaConfig.ProcessRolesProp} contains just the 'broker' role, the node id $nodeId must not be included in the set of voters ${KafkaConfig.QuorumVotersProp}=${voterAddressSpecsByNodeId.asScala.keySet.toSet}")
//            // controller.listener.names must be non-empty...
//            require(controllerListenerNames.nonEmpty,
//                    s"${KafkaConfig.ControllerListenerNamesProp} must contain at least one value when running KRaft with just the broker role")
//            // controller.listener.names are forbidden in listeners...
//            require(controllerListeners.isEmpty,
//                    s"${KafkaConfig.ControllerListenerNamesProp} must not contain a value appearing in the '${KafkaConfig.ListenersProp}' configuration when running KRaft with just the broker role")
//            // controller.listener.names must all appear in listener.security.protocol.map
//            controllerListenerNames.foreach { name =>
//                val listenerName = ListenerName.normalised(name)
//                if (!effectiveListenerSecurityProtocolMap.contains(listenerName)) {
//                    throw new ConfigException(s"Controller listener with name ${listenerName.value} defined in " +
//                            s"${KafkaConfig.ControllerListenerNamesProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}  (an explicit security mapping for each controller listener is required if ${KafkaConfig.ListenerSecurityProtocolMapProp} is non-empty, or if there are security protocols other than PLAINTEXT in use)")
//                }
//            }
//            // warn that only the first controller listener is used if there is more than one
//            if (controllerListenerNames.size > 1) {
//                warn(s"${KafkaConfig.ControllerListenerNamesProp} has multiple entries; only the first will be used since ${KafkaConfig.ProcessRolesProp}=broker: ${controllerListenerNames.asJava}")
//            }
//            validateAdvertisedListenersNonEmptyForBroker()
//        } else if (processRoles == Set(ControllerRole)) {
//            // KRaft controller-only
//            validateNonEmptyQuorumVotersForKRaft()
//            validateControlPlaneListenerEmptyForKRaft()
//            // advertised listeners must be empty when not also running the broker role
//            val sourceOfAdvertisedListeners: String =
//            if (getString(KafkaConfig.AdvertisedListenersProp) != null)
//                s"${KafkaConfig.AdvertisedListenersProp}"
//        else
//            s"${KafkaConfig.ListenersProp}"
//            require(effectiveAdvertisedListeners.isEmpty,
//                    s"The $sourceOfAdvertisedListeners config must only contain KRaft controller listeners from ${KafkaConfig.ControllerListenerNamesProp} when ${KafkaConfig.ProcessRolesProp}=controller")
//            validateControllerQuorumVotersMustContainNodeIdForKRaftController()
//            validateControllerListenerExistsForKRaftController()
//            validateControllerListenerNamesMustAppearInListenersForKRaftController()
//        } else if (isKRaftCoResidentMode) {
//            // KRaft colocated broker and controller
//            validateNonEmptyQuorumVotersForKRaft()
//            validateControlPlaneListenerEmptyForKRaft()
//            validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
//            validateControllerQuorumVotersMustContainNodeIdForKRaftController()
//            validateControllerListenerExistsForKRaftController()
//            validateControllerListenerNamesMustAppearInListenersForKRaftController()
//            validateAdvertisedListenersNonEmptyForBroker()
//        } else {
//            // ZK-based
//            // controller listener names must be empty when not in KRaft mode
//            require(controllerListenerNames.isEmpty, s"${KafkaConfig.ControllerListenerNamesProp} must be empty when not running in KRaft mode: ${controllerListenerNames.asJava}")
//            validateAdvertisedListenersNonEmptyForBroker()
//        }
//
//        val listenerNames = listeners.map(_.listenerName).toSet
//        if (processRoles.isEmpty || processRoles.contains(BrokerRole)) {
//            // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
//            validateAdvertisedListenersNonEmptyForBroker()
//            require(advertisedListenerNames.contains(interBrokerListenerName),
//                    s"${KafkaConfig.InterBrokerListenerNameProp} must be a listener name defined in ${KafkaConfig.AdvertisedListenersProp}. " +
//                            s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
//            require(advertisedListenerNames.subsetOf(listenerNames),
//                    s"${KafkaConfig.AdvertisedListenersProp} listener names must be equal to or a subset of the ones defined in ${KafkaConfig.ListenersProp}. " +
//                            s"Found ${advertisedListenerNames.map(_.value).mkString(",")}. The valid options based on the current configuration " +
//                            s"are ${listenerNames.map(_.value).mkString(",")}"
//            )
//        }
//
//        require(!effectiveAdvertisedListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
//                s"${KafkaConfig.AdvertisedListenersProp} cannot use the nonroutable meta-address 0.0.0.0. "+
//                s"Use a routable IP address.")
//
//        // validate control.plane.listener.name config
//        if (controlPlaneListenerName.isDefined) {
//            require(advertisedListenerNames.contains(controlPlaneListenerName.get),
//                    s"${KafkaConfig.ControlPlaneListenerNameProp} must be a listener name defined in ${KafkaConfig.AdvertisedListenersProp}. " +
//                            s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
//            // controlPlaneListenerName should be different from interBrokerListenerName
//            require(!controlPlaneListenerName.get.value().equals(interBrokerListenerName.value()),
//                    s"${KafkaConfig.ControlPlaneListenerNameProp}, when defined, should have a different value from the inter broker listener name. " +
//                            s"Currently they both have the value ${controlPlaneListenerName.get}")
//        }
//
//        val messageFormatVersion = new MessageFormatVersion(logMessageFormatVersionString, interBrokerProtocolVersionString)
//        if (messageFormatVersion.shouldWarn)
//            warn(messageFormatVersion.brokerWarningMessage)
//
//        val recordVersion = logMessageFormatVersion.highestSupportedRecordVersion
//        require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= recordVersion.value,
//                s"log.message.format.version $logMessageFormatVersionString can only be used when inter.broker.protocol.version " +
//                        s"is set to version ${MetadataVersion.minSupportedFor(recordVersion).shortVersion} or higher")
//
//        if (offsetsTopicCompressionCodec == ZStdCompressionCodec)
//            require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= IBP_2_1_IV0.highestSupportedRecordVersion().value,
//                    "offsets.topic.compression.codec zstd can only be used when inter.broker.protocol.version " +
//                            s"is set to version ${IBP_2_1_IV0.shortVersion} or higher")
//
//        val interBrokerUsesSasl = interBrokerSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol == SecurityProtocol.SASL_SSL
//        require(!interBrokerUsesSasl || saslInterBrokerHandshakeRequestEnable || saslMechanismInterBrokerProtocol == SaslConfigs.GSSAPI_MECHANISM,
//                s"Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to $interBrokerProtocolVersionString")
//        require(!interBrokerUsesSasl || saslEnabledMechanisms(interBrokerListenerName).contains(saslMechanismInterBrokerProtocol),
//                s"${KafkaConfig.SaslMechanismInterBrokerProtocolProp} must be included in ${KafkaConfig.SaslEnabledMechanismsProp} when SASL is used for inter-broker communication")
//        require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
//                s"${KafkaConfig.QueuedMaxBytesProp} must be larger or equal to ${KafkaConfig.SocketRequestMaxBytesProp}")
//
//        if (maxConnectionsPerIp == 0)
//            require(!maxConnectionsPerIpOverrides.isEmpty, s"${KafkaConfig.MaxConnectionsPerIpProp} can be set to zero only if" +
//                    s" ${KafkaConfig.MaxConnectionsPerIpOverridesProp} property is set.")
//
//        val invalidAddresses = maxConnectionsPerIpOverrides.keys.filterNot(address => Utils.validHostPattern(address))
//        if (!invalidAddresses.isEmpty)
//            throw new IllegalArgumentException(s"${KafkaConfig.MaxConnectionsPerIpOverridesProp} contains invalid addresses : ${invalidAddresses.mkString(",")}")
//
//        if (connectionsMaxIdleMs >= 0)
//            require(failedAuthenticationDelayMs < connectionsMaxIdleMs,
//                    s"${KafkaConfig.FailedAuthenticationDelayMsProp}=$failedAuthenticationDelayMs should always be less than" +
//                            s" ${KafkaConfig.ConnectionsMaxIdleMsProp}=$connectionsMaxIdleMs to prevent failed" +
//                            s" authentication responses from timing out")
//
//        val principalBuilderClass = getClass(KafkaConfig.PrincipalBuilderClassProp)
//        require(principalBuilderClass != null, s"${KafkaConfig.PrincipalBuilderClassProp} must be non-null")
//        require(classOf[KafkaPrincipalSerde].isAssignableFrom(principalBuilderClass),
//                s"${KafkaConfig.PrincipalBuilderClassProp} must implement KafkaPrincipalSerde")
//    }


}