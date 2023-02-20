package cn.pockethub.permanentqueue;

import cn.pockethub.permanentqueue.kafka.log.CleanerConfig;
import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogManager;
import cn.pockethub.permanentqueue.kafka.server.BrokerTopicStats;
import cn.pockethub.permanentqueue.kafka.server.ConfigRepository;
import cn.pockethub.permanentqueue.kafka.server.LogDirFailureChannel;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.utils.Scheduler;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class PermanentQueueManagerBuilder {

    private File logDir = null;
    private List<File> initialOfflineDirs = Collections.emptyList();
    private ConfigRepository configRepository = null;
    private LogConfig initialDefaultConfig = null;
    private CleanerConfig cleanerConfig = null;
    private int recoveryThreadsPerDataDir = 1;
    private long flushCheckMs = 1000L;
    private long flushRecoveryOffsetCheckpointMs = 10000L;
    private long flushStartOffsetCheckpointMs = 10000L;
    private long retentionCheckMs = 1000L;
    private int maxTransactionTimeoutMs = 15 * 60 * 1000;
    private int maxPidExpirationMs = 60000;
    private MetadataVersion interBrokerProtocolVersion = MetadataVersion.latest();
    private Scheduler scheduler = null;
    private BrokerTopicStats brokerTopicStats = null;
    private LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(1);
    private Time time = Time.SYSTEM;
    private boolean keepPartitionMetadataFile = true;

    public PermanentQueueManagerBuilder setLogDir(File logDir) {
        this.logDir = logDir;
        return this;
    }

    public PermanentQueueManagerBuilder setInitialOfflineDirs(List<File> initialOfflineDirs) {
        this.initialOfflineDirs = initialOfflineDirs;
        return this;
    }

    public PermanentQueueManagerBuilder setConfigRepository(ConfigRepository configRepository) {
        this.configRepository = configRepository;
        return this;
    }

    public PermanentQueueManagerBuilder setInitialDefaultConfig(LogConfig initialDefaultConfig) {
        this.initialDefaultConfig = initialDefaultConfig;
        return this;
    }

    public PermanentQueueManagerBuilder setCleanerConfig(CleanerConfig cleanerConfig) {
        this.cleanerConfig = cleanerConfig;
        return this;
    }

    public PermanentQueueManagerBuilder setRecoveryThreadsPerDataDir(int recoveryThreadsPerDataDir) {
        this.recoveryThreadsPerDataDir = recoveryThreadsPerDataDir;
        return this;
    }

    public PermanentQueueManagerBuilder setFlushCheckMs(long flushCheckMs) {
        this.flushCheckMs = flushCheckMs;
        return this;
    }

    public PermanentQueueManagerBuilder setFlushRecoveryOffsetCheckpointMs(long flushRecoveryOffsetCheckpointMs) {
        this.flushRecoveryOffsetCheckpointMs = flushRecoveryOffsetCheckpointMs;
        return this;
    }

    public PermanentQueueManagerBuilder setFlushStartOffsetCheckpointMs(long flushStartOffsetCheckpointMs) {
        this.flushStartOffsetCheckpointMs = flushStartOffsetCheckpointMs;
        return this;
    }

    public PermanentQueueManagerBuilder setRetentionCheckMs(long retentionCheckMs) {
        this.retentionCheckMs = retentionCheckMs;
        return this;
    }

    public PermanentQueueManagerBuilder setMaxTransactionTimeoutMs(int maxTransactionTimeoutMs) {
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        return this;
    }

    public PermanentQueueManagerBuilder setMaxPidExpirationMs(int maxPidExpirationMs) {
        this.maxPidExpirationMs = maxPidExpirationMs;
        return this;
    }

    public PermanentQueueManagerBuilder setInterBrokerProtocolVersion(MetadataVersion interBrokerProtocolVersion) {
        this.interBrokerProtocolVersion = interBrokerProtocolVersion;
        return this;
    }

    public PermanentQueueManagerBuilder setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public PermanentQueueManagerBuilder setBrokerTopicStats(BrokerTopicStats brokerTopicStats) {
        this.brokerTopicStats = brokerTopicStats;
        return this;
    }

    public PermanentQueueManagerBuilder setTime(Time time) {
        this.time = time;
        return this;
    }

    public PermanentQueueManagerBuilder setKeepPartitionMetadataFile(boolean keepPartitionMetadataFile) {
        this.keepPartitionMetadataFile = keepPartitionMetadataFile;
        return this;
    }

    public PermanentQueueManager build() throws Throwable {
        if (logDir == null) {
            throw new RuntimeException("you must set logDirs");
        }
        if (configRepository == null) {
            throw new RuntimeException("you must set configRepository");
        }
        if (initialDefaultConfig == null) {
            throw new RuntimeException("you must set initialDefaultConfig");
        }
        if (cleanerConfig == null) {
            throw new RuntimeException("you must set cleanerConfig");
        }
        if (scheduler == null) {
            throw new RuntimeException("you must set scheduler");
        }
        if (brokerTopicStats == null) {
            throw new RuntimeException("you must set brokerTopicStats");
        }

        LogManager logManager = new LogManager(Arrays.asList(logDir),
                initialOfflineDirs,
                configRepository,
                initialDefaultConfig,
                cleanerConfig,
                recoveryThreadsPerDataDir,
                flushCheckMs,
                flushRecoveryOffsetCheckpointMs,
                flushStartOffsetCheckpointMs,
                retentionCheckMs,
                maxTransactionTimeoutMs,
                maxPidExpirationMs,
                interBrokerProtocolVersion,
                scheduler,
                brokerTopicStats,
                logDirFailureChannel,
                time,
                keepPartitionMetadataFile);

        return new PermanentQueueManager(logManager,logDir);
    }
}
