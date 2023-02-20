package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.common.*;
import cn.pockethub.permanentqueue.kafka.common.function.BiFunctionWithIOException;
import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import cn.pockethub.permanentqueue.kafka.raft.OffsetAndEpoch;
import cn.pockethub.permanentqueue.kafka.server.*;
import cn.pockethub.permanentqueue.kafka.server.checkpoints.LeaderEpochCheckpointFile;
import cn.pockethub.permanentqueue.kafka.server.common.MetadataVersion;
import cn.pockethub.permanentqueue.kafka.server.epoch.EpochEntry;
import cn.pockethub.permanentqueue.kafka.server.epoch.LeaderEpochFileCache;
import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import cn.pockethub.permanentqueue.kafka.utils.Scheduler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A log which presents a unified view of local and tiered log segments.
 * <p>
 * The log consists of tiered and local segments with the tiered portion of the log being optional. There could be an
 * overlap between the tiered and local segments. The active segment is always guaranteed to be local. If tiered segments
 * are present, they always appear at the beginning of the log, followed by an optional region of overlap, followed by the local
 * segments including the active segment.
 * <p>
 * NOTE: this class handles state and behavior specific to tiered segments as well as any behavior combining both tiered
 * and local segments. The state and behavior specific to local segments are handled by the encapsulated LocalLog instance.
 */
//@threadsafe
@Getter
public class UnifiedLog extends KafkaMetricsGroup {
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedLog.class);

    /*构造方法*/
    private volatile Long logStartOffset;
    private LocalLog localLog;
    private BrokerTopicStats brokerTopicStats;
    private Integer producerIdExpirationCheckIntervalMs;
    private volatile Optional<LeaderEpochFileCache> leaderEpochCache;
    private ProducerStateManager producerStateManager;
    private volatile Optional<Uuid> _topicId;
    private Boolean keepPartitionMetadataFile;
    /*构造方法*/

    /*成员*/
    private String logIdent;

    /* A lock that guards all modifications to the log */
    private final Object lock = new Object();

    /* The earliest offset which is part of an incomplete transaction. This is used to compute the
     * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
     * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
     * will point to the log start offset, which may actually be either part of a completed transaction or not
     * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
     * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
     * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
     * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
     * that this could result in disagreement between replicas depending on when they began replicating the log.
     * In the worst case, the LSO could be seen by a consumer to go backwards.
     */
    private volatile Optional<LogOffsetMetadata> firstUnstableOffsetMetadata = Optional.empty();

    /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
     * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
     * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
     * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
     */
    private volatile LogOffsetMetadata highWatermarkMetadata;

    public volatile Optional<PartitionMetadataFile> partitionMetadataFile = Optional.empty();

    private ImmutableMap<String, String> tags;

    private ScheduledFuture producerExpireCheck;

    /*static*/
    public static final String LogFileSuffix = LocalLog.LogFileSuffix;
    public static final String IndexFileSuffix = LocalLog.IndexFileSuffix;
    public static final String TimeIndexFileSuffix = LocalLog.TimeIndexFileSuffix;
    public static final String ProducerSnapshotFileSuffix = ".snapshot";
    public static final String TxnIndexFileSuffix = LocalLog.TxnIndexFileSuffix;
    public static final String DeletedFileSuffix = LocalLog.DeletedFileSuffix;
    public static final String CleanedFileSuffix = LocalLog.CleanedFileSuffix;
    public static final String SwapFileSuffix = LocalLog.SwapFileSuffix;
    public static final String DeleteDirSuffix = LocalLog.DeleteDirSuffix;
    public static final String FutureDirSuffix = LocalLog.FutureDirSuffix;
    protected static final Pattern DeleteDirPattern = LocalLog.DeleteDirPattern;
    protected static final Pattern FutureDirPattern = LocalLog.FutureDirPattern;
    public static final Long UnknownOffset = LocalLog.UnknownOffset;
    /*static*/

    /**
     * @param logStartOffset                      The earliest offset allowed to be exposed to kafka client.
     *                                            The logStartOffset can be updated by :
     *                                            - user's DeleteRecordsRequest
     *                                            - broker's log retention
     *                                            - broker's log truncation
     *                                            - broker's log recovery
     *                                            The logStartOffset is used to decide the following:
     *                                            - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
     *                                            It may trigger log rolling if the active segment is deleted.
     *                                            - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
     *                                            we make sure that logStartOffset <= log's highWatermark
     *                                            Other activities such as log cleaning are not affected by logStartOffset.
     * @param localLog                            The LocalLog instance containing non-empty log segments recovered from disk
     * @param brokerTopicStats                    Container for Broker Topic Yammer Metrics
     * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
     * @param leaderEpochCache                    The LeaderEpochFileCache instance (if any) containing state associated
     *                                            with the provided logStartOffset and nextOffsetMetadata
     * @param producerStateManager                The ProducerStateManager instance containing state associated with the provided segments
     * @param _topicId                            optional Uuid to specify the topic ID for the topic if it exists. Should only be specified when
     *                                            first creating the log through Partition.makeLeader or Partition.makeFollower. When reloading a log,
     *                                            this field will be populated by reading the topic ID value from partition.metadata if it exists.
     * @param keepPartitionMetadataFile           boolean flag to indicate whether the partition.metadata file should be kept in the
     *                                            log directory. A partition.metadata file is only created when the raft controller is used
     *                                            or the ZK controller and this broker's inter-broker protocol version is at least 2.8.
     *                                            This file will persist the topic ID on the broker. If inter-broker protocol for a ZK controller
     *                                            is downgraded below 2.8, a topic ID may be lost and a new ID generated upon re-upgrade.
     *                                            If the inter-broker protocol version on a ZK cluster is below 2.8, partition.metadata
     *                                            will be deleted to avoid ID conflicts upon re-upgrade.
     */
    public UnifiedLog(Long logStartOffset,
                      LocalLog localLog,
                      BrokerTopicStats brokerTopicStats,
                      Integer producerIdExpirationCheckIntervalMs,
                      Optional<LeaderEpochFileCache> leaderEpochCache,
                      ProducerStateManager producerStateManager,
                      Optional<Uuid> _topicId,
                      Boolean keepPartitionMetadataFile) throws OffsetOutOfRangeException {
        this.logStartOffset = logStartOffset;
        this.localLog = localLog;
        this.brokerTopicStats = brokerTopicStats;
        this.producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs;
        this.leaderEpochCache = leaderEpochCache;
        this.producerStateManager = producerStateManager;
        this._topicId = _topicId;
        this.keepPartitionMetadataFile = keepPartitionMetadataFile;

        this.logIdent = String.format("[UnifiedLog partition=%s, dir=%s] ", topicPartition(), parentDir());

        this.highWatermarkMetadata = new LogOffsetMetadata(logStartOffset);

        initializePartitionMetadata();
        updateLogStartOffset(logStartOffset);
        maybeIncrementFirstUnstableOffset();
        initializeTopicId();

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (isFuture()) {
            builder.put("is-future", "true");
        }
        builder.put("topic", topicPartition().topic());
        builder.put("partition", Integer.toString(topicPartition().partition()));
        this.tags = builder.build();

        newGauge(LogMetricNames.NumLogSegments, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return numberOfSegments();
            }
        }, tags);
        newGauge(LogMetricNames.LogStartOffset, new Gauge<Long>() {
            @Override
            public Long value() {
                return logStartOffset;
            }
        }, tags);
        newGauge(LogMetricNames.LogEndOffset, new Gauge<Long>() {
            @Override
            public Long value() {
                return logEndOffset();
            }
        }, tags);
        newGauge(LogMetricNames.Size, new Gauge<Long>() {
            @Override
            public Long value() {
                return size();
            }
        }, tags);

        producerExpireCheck = scheduler().schedule("PeriodicProducerExpirationCheck", () -> {
            synchronized (lock) {
                producerStateManager.removeExpiredProducers(time().milliseconds());
            }
        }, producerIdExpirationCheckIntervalMs, producerIdExpirationCheckIntervalMs, TimeUnit.MILLISECONDS);
    }

    public static UnifiedLog apply(File dir,
                                   LogConfig config,
                                   Long logStartOffset,
                                   Long recoveryPoint,
                                   Scheduler scheduler,
                                   BrokerTopicStats brokerTopicStats,
                                   Time time,
                                   Integer maxTransactionTimeoutMs,
                                   Integer maxProducerIdExpirationMs,
                                   Integer producerIdExpirationCheckIntervalMs,
                                   LogDirFailureChannel logDirFailureChannel,
                                   Boolean lastShutdownClean,
                                   Optional<Uuid> topicId,
                                   Boolean keepPartitionMetadataFile,
                                   ConcurrentMap<String, Integer> numRemainingSegments) throws IOException {
        // create the log directory if it doesn't exist
        Files.createDirectories(dir.toPath());
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
        LogSegments segments = new LogSegments(topicPartition);
        Optional<LeaderEpochFileCache> leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
                dir,
                topicPartition,
                logDirFailureChannel,
                config.recordVersion(),
                String.format("[UnifiedLog partition=%s, dir=%s] ", topicPartition, dir.getParent()));
        ProducerStateManager producerStateManager = new ProducerStateManager(topicPartition, dir,
                maxTransactionTimeoutMs, maxProducerIdExpirationMs, time);
        LogLoader.LoadedLogOffsets offsets = new LogLoader(
                dir,
                topicPartition,
                config,
                scheduler,
                time,
                logDirFailureChannel,
                lastShutdownClean,
                segments,
                logStartOffset,
                recoveryPoint,
                leaderEpochCache,
                producerStateManager,
                numRemainingSegments
        ).load();
        LocalLog localLog = new LocalLog(dir, config, segments, offsets.getRecoveryPoint(),
                offsets.getNextOffsetMetadata(), scheduler, time, topicPartition, logDirFailureChannel);
        return new UnifiedLog(offsets.getLogStartOffset(),
                localLog,
                brokerTopicStats,
                producerIdExpirationCheckIntervalMs,
                leaderEpochCache,
                producerStateManager,
                topicId,
                keepPartitionMetadataFile);
    }

//    @Override
//    public String logIdent() {
//        return logIdent;
//    }

    /**
     * Initialize topic ID information for the log by maintaining the partition metadata file and setting the in-memory _topicId.
     * Delete partition metadata file if the version does not support topic IDs.
     * Set _topicId based on a few scenarios:
     * - Recover topic ID if present and topic IDs are supported. Ensure we do not try to assign a provided topicId that is inconsistent
     * with the ID on file.
     * - If we were provided a topic ID when creating the log, partition metadata files are supported, and one does not yet exist
     * set _topicId and write to the partition metadata file.
     * - Otherwise set _topicId to None
     */
    public void initializeTopicId() {
        if (!partitionMetadataFile.isPresent()) {
            throw new KafkaException("The partitionMetadataFile should have been initialized");
        }
        PartitionMetadataFile partMetadataFile = partitionMetadataFile.get();

        if (partMetadataFile.exists()) {
            if (keepPartitionMetadataFile) {
                Uuid fileTopicId = partMetadataFile.read().getTopicId();
                if (_topicId.isPresent() && !_topicId.get().equals(fileTopicId)) {
                    String msg = String.format("Tried to assign topic ID %s to log for topic partition %s, but log already contained topic ID %s",
                            topicId(), topicPartition(), fileTopicId);
                    throw new InconsistentTopicIdException(msg);
                }
                _topicId = Optional.of(fileTopicId);

            } else {
                try {
                    partMetadataFile.delete();
                } catch (IOException e) {
                    LOG.error("Error while trying to delete partition metadata file {}", partMetadataFile, e);
                }
            }
        } else if (keepPartitionMetadataFile) {
            _topicId.ifPresent(partMetadataFile::record);
            scheduler().schedule("flush-metadata-file", this::maybeFlushMetadataFile, 0, -1, TimeUnit.MILLISECONDS);
        } else {
            // We want to keep the file and the in-memory topic ID in sync.
            _topicId = Optional.empty();
        }
    }

    public Optional<Uuid> topicId() {
        return _topicId;
    }

    public File dir() {
        return localLog.dir();
    }

    public String parentDir() {
        return localLog.parentDir();
    }

    public File parentDirFile() {
        return localLog.parentDirFile();
    }

    public String name() {
        return localLog.name();
    }

    public Long recoveryPoint() {
        return localLog.recoveryPoint;
    }

    public TopicPartition topicPartition() {
        return localLog.topicPartition;
    }

    public Time time() {
        return localLog.time;
    }

    public Scheduler scheduler() {
        return localLog.scheduler;
    }

    public LogConfig config() {
        return localLog.config;
    }

    public LogDirFailureChannel logDirFailureChannel() {
        return localLog.logDirFailureChannel;
    }

    public LogConfig updateConfig(LogConfig newConfig) throws IOException {
        LogConfig oldConfig = localLog.config;
        localLog.updateConfig(newConfig);
        RecordVersion oldRecordVersion = oldConfig.recordVersion();
        RecordVersion newRecordVersion = newConfig.recordVersion();
        if (newRecordVersion != oldRecordVersion) {
            initializeLeaderEpochCache();
        }
        return oldConfig;
    }

    public Long highWatermark() {
        return highWatermarkMetadata.getMessageOffset();
    }

    /**
     * Update the high watermark to a new offset. The new high watermark will be lower
     * bounded by the log start offset and upper bounded by the log end offset.
     * <p>
     * This is intended to be called when initializing the high watermark or when updating
     * it on a follower after receiving a Fetch response from the leader.
     *
     * @param hw the suggested new value for the high watermark
     * @return the updated high watermark offset
     */
    public Long updateHighWatermark(Long hw) throws OffsetOutOfRangeException {
        return updateHighWatermark(new LogOffsetMetadata(hw));
    }

    /**
     * Update high watermark with offset metadata. The new high watermark will be lower
     * bounded by the log start offset and upper bounded by the log end offset.
     *
     * @param highWatermarkMetadata the suggested high watermark with offset metadata
     * @return the updated high watermark offset
     */
    public Long updateHighWatermark(LogOffsetMetadata highWatermarkMetadata) throws OffsetOutOfRangeException {
        LogOffsetMetadata endOffsetMetadata = localLog.logEndOffsetMetadata();
        LogOffsetMetadata newHighWatermarkMetadata = null;
        if (highWatermarkMetadata.getMessageOffset() < logStartOffset) {
            newHighWatermarkMetadata = new LogOffsetMetadata(logStartOffset);
        } else if (highWatermarkMetadata.getMessageOffset() >= endOffsetMetadata.getMessageOffset()) {
            newHighWatermarkMetadata = endOffsetMetadata;
        } else {
            newHighWatermarkMetadata = highWatermarkMetadata;
        }

        updateHighWatermarkMetadata(newHighWatermarkMetadata);
        return newHighWatermarkMetadata.getMessageOffset();
    }

    /**
     * Update the high watermark to a new value if and only if it is larger than the old value. It is
     * an error to update to a value which is larger than the log end offset.
     * <p>
     * This method is intended to be used by the leader to update the high watermark after follower
     * fetch offsets have been updated.
     *
     * @return the old high watermark, if updated by the new value
     */
    public Optional<LogOffsetMetadata> maybeIncrementHighWatermark(LogOffsetMetadata newHighWatermark) throws OffsetOutOfRangeException {
        if (newHighWatermark.getMessageOffset() > logEndOffset()) {
            String msg = String.format("High watermark %s update exceeds current log end offset %s",
                    newHighWatermark, localLog.logEndOffsetMetadata());
            throw new IllegalArgumentException(msg);
        }

        synchronized (lock) {
            LogOffsetMetadata oldHighWatermark = fetchHighWatermarkMetadata();

            // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
            // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
            if (oldHighWatermark.getMessageOffset() < newHighWatermark.getMessageOffset() ||
                    (oldHighWatermark.getMessageOffset() == newHighWatermark.getMessageOffset() && oldHighWatermark.onOlderSegment(newHighWatermark))) {
                updateHighWatermarkMetadata(newHighWatermark);
                return Optional.of(oldHighWatermark);
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Get the offset and metadata for the current high watermark. If offset metadata is not
     * known, this will do a lookup in the index and cache the result.
     */
    private LogOffsetMetadata fetchHighWatermarkMetadata() throws OffsetOutOfRangeException {
        localLog.checkIfMemoryMappedBufferClosed();

        LogOffsetMetadata offsetMetadata = highWatermarkMetadata;
        if (offsetMetadata.messageOffsetOnly()) {
            synchronized (lock) {
                LogOffsetMetadata fullOffset = convertToOffsetMetadataOrThrow(highWatermark());
                updateHighWatermarkMetadata(fullOffset);
                return fullOffset;
            }
        } else {
            return offsetMetadata;
        }
    }

    private void updateHighWatermarkMetadata(LogOffsetMetadata newHighWatermark) throws OffsetOutOfRangeException {
        if (newHighWatermark.getMessageOffset() < 0) {
            throw new IllegalArgumentException("High watermark offset should be non-negative");
        }

        synchronized (lock) {
            if (newHighWatermark.getMessageOffset() < highWatermarkMetadata.getMessageOffset()) {
                LOG.warn("Non-monotonic update of high watermark from {} to {}", highWatermarkMetadata, newHighWatermark);
            }

            highWatermarkMetadata = newHighWatermark;
            producerStateManager.onHighWatermarkUpdated(newHighWatermark.getMessageOffset());
            maybeIncrementFirstUnstableOffset();
        }
        LOG.trace("Setting high watermark {}", newHighWatermark);
    }

    /**
     * Get the first unstable offset. Unlike the last stable offset, which is always defined,
     * the first unstable offset only exists if there are transactions in progress.
     *
     * @return the first unstable offset, if it exists
     */
    private Optional<Long> firstUnstableOffset() {
        return firstUnstableOffsetMetadata.map(LogOffsetMetadata::getMessageOffset);
    }

    private LogOffsetMetadata fetchLastStableOffsetMetadata() throws OffsetOutOfRangeException {
        localLog.checkIfMemoryMappedBufferClosed();

        // cache the current high watermark to avoid a concurrent update invalidating the range check
        LogOffsetMetadata highWatermarkMetadata = fetchHighWatermarkMetadata();

        if (firstUnstableOffsetMetadata.isPresent()
                && firstUnstableOffsetMetadata.get().getMessageOffset() < highWatermarkMetadata.getMessageOffset()) {
            LogOffsetMetadata offsetMetadata = firstUnstableOffsetMetadata.get();
            if (offsetMetadata.messageOffsetOnly()) {
                synchronized (lock) {
                    LogOffsetMetadata fullOffset = convertToOffsetMetadataOrThrow(offsetMetadata.getMessageOffset());
                    if (firstUnstableOffsetMetadata.isPresent() && firstUnstableOffsetMetadata.get().equals(offsetMetadata)) {
                        firstUnstableOffsetMetadata = Optional.of(fullOffset);
                    }
                    return fullOffset;
                }
            } else {
                return offsetMetadata;
            }
        } else {
            return highWatermarkMetadata;
        }
    }

    /**
     * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
     * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
     * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
     * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
     * beyond the high watermark.
     */
    public Long lastStableOffset() {
        if (firstUnstableOffsetMetadata.isPresent()
                && firstUnstableOffsetMetadata.get().getMessageOffset() < highWatermark()) {
            return firstUnstableOffsetMetadata.get().getMessageOffset();
        } else {
            return highWatermark();
        }
    }

    public Long lastStableOffsetLag() {
        return highWatermark() - lastStableOffset();
    }

    /**
     * Fully materialize and return an offset snapshot including segment position info. This method will update
     * the LogOffsetMetadata for the high watermark and last stable offset if they are message-only. Throws an
     * offset out of range error if the segment info cannot be loaded.
     */
    public LogOffsetSnapshot fetchOffsetSnapshot() throws OffsetOutOfRangeException {
        LogOffsetMetadata lastStable = fetchLastStableOffsetMetadata();
        LogOffsetMetadata highWatermark = fetchHighWatermarkMetadata();

        return new LogOffsetSnapshot(
                logStartOffset,
                localLog.logEndOffsetMetadata(),
                highWatermark,
                lastStable
        );
    }

    // For compatibility, metrics are defined to be under `Log` class
    @Override
    public MetricName metricName(String name, Map<String, String> tags) {
        Package pkg = this.getClass().getPackage();
        String pkgStr = (pkg == null) ? "" : pkg.getName();
        return explicitMetricName(pkgStr, "Log", name, tags);
    }

    private RecordVersion recordVersion() {
        return config().recordVersion();
    }

    private void initializePartitionMetadata() {
        synchronized (lock) {
            File partitionMetadata = PartitionMetadataFile.newFile(dir());
            partitionMetadataFile = Optional.of(new PartitionMetadataFile(partitionMetadata, logDirFailureChannel()));
        }
    }

    private void maybeFlushMetadataFile() {
        partitionMetadataFile.ifPresent(metadataFile -> metadataFile.maybeFlush());
    }

    /**
     * Only used for ZK clusters when we update and start using topic IDs on existing topics
     */
    public void assignTopicId(Uuid topicId) {
        if (_topicId.isPresent()
                && !_topicId.get().equals(topicId)) {
            Uuid currentId = _topicId.get();
            String msg = String.format("Tried to assign topic ID %s to log for topic partition %s, but log already contained topic ID %s",
                    topicId, topicPartition(), currentId);
            throw new InconsistentTopicIdException(msg);
        } else {
            if (keepPartitionMetadataFile) {
                _topicId = Optional.of(topicId);
                if (partitionMetadataFile.isPresent()) {
                    PartitionMetadataFile partMetadataFile = this.partitionMetadataFile.get();
                    if (!partMetadataFile.exists()) {
                        partMetadataFile.record(topicId);
                        scheduler().schedule("flush-metadata-file", this::maybeFlushMetadataFile, 0, -1, TimeUnit.MILLISECONDS);
                    }
                } else {
                    LOG.warn("The topic id {} will not be persisted to the partition metadata file since the partition is deleted", topicId);
                }
            }
        }
    }

    private void initializeLeaderEpochCache() throws IOException {
        synchronized (lock) {
            leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(dir(), topicPartition(), logDirFailureChannel(), recordVersion(), logIdent);
        }
    }

    private void updateHighWatermarkWithLogEndOffset() throws OffsetOutOfRangeException {
        // Update the high watermark in case it has gotten ahead of the log end offset following a truncation
        // or if a new segment has been rolled and the offset metadata needs to be updated.
        if (highWatermark() >= localLog.logEndOffset()) {
            updateHighWatermarkMetadata(localLog.logEndOffsetMetadata());
        }
    }

    private void updateLogStartOffset(Long offset) throws OffsetOutOfRangeException {
        logStartOffset = offset;

        if (highWatermark() < offset) {
            updateHighWatermark(offset);
        }

        if (localLog.recoveryPoint < offset) {
            localLog.updateRecoveryPoint(offset);
        }
    }

    // Rebuild producer state until lastOffset. This method may be called from the recovery code path, and thus must be
    // free of all side-effects, i.e. it must not update any log-specific state.
    private void rebuildProducerState(Long lastOffset, ProducerStateManager producerStateManager) throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            UnifiedLog.rebuildProducerState(producerStateManager, localLog.segments, logStartOffset, lastOffset, recordVersion(), time(),
                    false, logIdent);
        }
    }

    //    @threadsafe
    public Boolean hasLateTransaction(Long currentTimeMs) {
        return producerStateManager.hasLateTransaction(currentTimeMs);
    }

    public Collection<DescribeProducersResponseData.ProducerState> activeProducers() {
        synchronized (lock) {
            return producerStateManager.activeProducers().entrySet().stream()
                    .map(entry -> {
                        Long producerId = entry.getKey();
                        ProducerStateEntry state = entry.getValue();
                        return new DescribeProducersResponseData.ProducerState()
                                .setProducerId(producerId)
                                .setProducerEpoch(state.getProducerEpoch())
                                .setLastSequence(state.lastSeq())
                                .setLastTimestamp(state.getLastTimestamp())
                                .setCoordinatorEpoch(state.getCoordinatorEpoch())
                                .setCurrentTxnStartOffset(state.getCurrentTxnFirstOffset().orElse(-1L));
                    })
                    .collect(Collectors.toList());
        }
    }

    private Map<Long, Integer> activeProducersWithLastSequence() {
        synchronized (lock) {
            return producerStateManager.activeProducers().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().lastSeq()));
        }
    }

    protected Map<Long, LastRecord> lastRecordsOfActiveProducers() {
        synchronized (lock) {
            return producerStateManager.activeProducers().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                                        ProducerStateEntry producerIdEntry = entry.getValue();
                                        Optional<Long> lastDataOffset = producerIdEntry.lastDataOffset() >= 0
                                                ? Optional.ofNullable(producerIdEntry.lastDataOffset())
                                                : Optional.empty();
                                        return new LastRecord(lastDataOffset, producerIdEntry.getProducerEpoch());
                                    }
                            )
                    );
        }
    }

    /**
     * The number of segments in the log.
     * Take care! this is an O(n) operation.
     */
    public Integer numberOfSegments() {
        return localLog.segments.numberOfSegments();
    }

    /**
     * Close this log.
     * The memory mapped buffer for index files of this log will be left open until the log is deleted.
     */
    public void close() {
        LOG.debug("Closing log");
        synchronized (lock) {
            maybeFlushMetadataFile();
            localLog.checkIfMemoryMappedBufferClosed();
            producerExpireCheck.cancel(true);
            String msg = String.format("Error while renaming dir for %s in dir %s", topicPartition(), dir().getParent());
            maybeHandleIOException(msg, (SupplierWithIOException<Void>) () -> {
                // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
                // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
                // (the clean shutdown file is written after the logs are all closed).
                producerStateManager.takeSnapshot();
                return null;
            });
            localLog.close();
        }
    }

    /**
     * Rename the directory of the local log. If the log's directory is being renamed for async deletion due to a
     * StopReplica request, then the shouldReinitialize parameter should be set to false, otherwise it should be set to true.
     *
     * @param name               The new name that this log's directory is being renamed to
     * @param shouldReinitialize Whether the log's metadata should be reinitialized after renaming
     */
    public void renameDir(String name, Boolean shouldReinitialize) {
        synchronized (lock) {
            maybeHandleIOException(String.format("Error while renaming dir for %s in log dir %s", topicPartition(), dir().getParent()), new SupplierWithIOException<Void>() {
                @Override
                public Void get() throws IOException {
                    // Flush partitionMetadata file before initializing again
                    maybeFlushMetadataFile();
                    if (localLog.renameDir(name)) {
                        producerStateManager.updateParentDir(dir());
                        if (shouldReinitialize) {
                            // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
                            // the checkpoint file in renamed log directory
                            initializeLeaderEpochCache();
                            initializePartitionMetadata();
                        } else {
                            leaderEpochCache = Optional.empty();
                            partitionMetadataFile = Optional.empty();
                        }
                    }
                    return null;
                }
            });
        }
    }

    /**
     * Close file handlers used by this log but don't write to disk. This is called if the log directory is offline
     */
    public void closeHandlers() {
        LOG.debug("Closing handlers");
        synchronized (lock) {
            localLog.closeHandlers();
        }
    }

    public LogAppendInfo appendAsLeader(MemoryRecords records,
                                        Integer leaderEpoch) {
        return appendAsLeader(records, leaderEpoch, AppendOrigin.Client, MetadataVersion.latest(), RequestLocal.NoCaching);
    }

    public LogAppendInfo appendAsLeader(MemoryRecords records,
                                        Integer leaderEpoch,
                                        AppendOrigin origin) {
        return appendAsLeader(records, leaderEpoch, origin, MetadataVersion.latest(), RequestLocal.NoCaching);
    }

    /**
     * Append this message set to the active segment of the local log, assigning offsets and Partition Leader Epochs
     *
     * @param records                    The records to append
     * @param origin                     Declares the origin of the append which affects required validations
     * @param interBrokerProtocolVersion Inter-broker message protocol version
     * @param requestLocal               request local instance
     * @return Information about the appended messages including the first and last offset.
     * @throws KafkaStorageException If the append fails due to an I/O error.
     */
    public LogAppendInfo appendAsLeader(MemoryRecords records,
                                        Integer leaderEpoch,
                                        AppendOrigin origin,
                                        MetadataVersion interBrokerProtocolVersion,
                                        RequestLocal requestLocal) {
        Boolean validateAndAssignOffsets = origin != AppendOrigin.RaftLeader;
        return append(records, origin, interBrokerProtocolVersion, validateAndAssignOffsets, leaderEpoch, Optional.of(requestLocal), false);
    }

    /**
     * Append this message set to the active segment of the local log without assigning offsets or Partition Leader Epochs
     *
     * @param records The records to append
     * @return Information about the appended messages including the first and last offset.
     * @throws KafkaStorageException If the append fails due to an I/O error.
     */
    public LogAppendInfo appendAsFollower(MemoryRecords records) throws Throwable {
        return append(records,
                AppendOrigin.Replication,
                MetadataVersion.latest(),
                false,
                -1,
                Optional.empty(),
                // disable to check the validation of record size since the record is already accepted by leader.
                true);
    }

    /**
     * Append this message set to the active segment of the local log, rolling over to a fresh segment if necessary.
     * <p>
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param records                    The log records to append
     * @param origin                     Declares the origin of the append which affects required validations
     * @param interBrokerProtocolVersion Inter-broker message protocol version
     * @param validateAndAssignOffsets   Should the log assign offsets to this message set or blindly apply what it is given
     * @param leaderEpoch                The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
     * @param requestLocal               The request local instance if assignOffsets is true
     * @param ignoreRecordSize           true to skip validation of record size.
     * @return Information about the appended messages including the first and last offset.
     * @throws KafkaStorageException           If the append fails due to an I/O error.
     * @throws OffsetsOutOfOrderException      If out of order offsets found in 'records'
     * @throws UnexpectedAppendOffsetException If the first or last offset in append is less than next offset
     */
    private LogAppendInfo append(MemoryRecords records,
                                 AppendOrigin origin,
                                 MetadataVersion interBrokerProtocolVersion,
                                 Boolean validateAndAssignOffsets,
                                 Integer leaderEpoch,
                                 Optional<RequestLocal> requestLocal,
                                 Boolean ignoreRecordSize) {
        // We want to ensure the partition metadata file is written to the log dir before any log data is written to disk.
        // This will ensure that any log data can be recovered with the correct topic ID in the case of failure.
        maybeFlushMetadataFile();

        LogAppendInfo appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize, leaderEpoch);

        // return if we have no valid messages or if this is a duplicate of the last appended entry
        if (appendInfo.getShallowCount() == 0) {
            return appendInfo;
        } else {

            // trim any invalid bytes or partial messages before appending it to the on-disk log
            final MemoryRecords[] validRecordsArr = new MemoryRecords[]{trimInvalidBytes(records, appendInfo)};

            // they are valid, insert them in the log
            synchronized (lock) {
                String msg = String.format("Error while appending records to %s in dir %s", topicPartition(), dir().getParent());
                return maybeHandleIOException(msg, new SupplierWithIOException<LogAppendInfo>() {
                    @Override
                    public LogAppendInfo get() throws IOException {
                        localLog.checkIfMemoryMappedBufferClosed();
                        if (validateAndAssignOffsets) {
                            // assign offsets to the message set
                            LongRef offset = new LongRef(localLog.logEndOffset());
                            appendInfo.setFirstOffset(Optional.of(new LogOffsetMetadata(offset.getValue())));
                            long now = time().milliseconds();

                            ValidationAndOffsetAssignResult validateAndOffsetAssignResult = null;
                            try {
                                if (!requestLocal.isPresent()) {
                                    throw new IllegalArgumentException("requestLocal should be defined if assignOffsets is true");
                                }
                                validateAndOffsetAssignResult = LogValidator.validateMessagesAndAssignOffsets(validRecordsArr[0],
                                        topicPartition(),
                                        offset,
                                        time(),
                                        now,
                                        appendInfo.getSourceCodec(),
                                        appendInfo.getTargetCodec(),
                                        config().getCompact(),
                                        config().recordVersion().value,
                                        config().getMessageTimestampType(),
                                        config().getMessageTimestampDifferenceMaxMs(),
                                        leaderEpoch,
                                        origin,
                                        interBrokerProtocolVersion,
                                        brokerTopicStats,
                                        requestLocal.get());
                            } catch (IOException e) {
                                throw new KafkaException("Error validating messages while appending to log " + name(), e);
                            }

                            validRecordsArr[0] = validateAndOffsetAssignResult.getValidatedRecords();
                            appendInfo.setMaxTimestamp(validateAndOffsetAssignResult.getMaxTimestamp());
                            appendInfo.setOffsetOfMaxTimestamp(validateAndOffsetAssignResult.getShallowOffsetOfMaxTimestamp());
                            appendInfo.setLastOffset(offset.getValue() - 1);
                            appendInfo.setRecordConversionStats(validateAndOffsetAssignResult.getRecordConversionStats());
                            if (config().getMessageTimestampType() == TimestampType.LOG_APPEND_TIME) {
                                appendInfo.setLogAppendTime(now);
                            }

                            // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
                            // format conversion)
                            if (!ignoreRecordSize && validateAndOffsetAssignResult.getMessageSizeMaybeChanged()) {
                                validRecordsArr[0].batches().forEach(batch -> {
                                    if (batch.sizeInBytes() > config().getMaxMessageSize()) {
                                        // we record the original message set size instead of the trimmed size
                                        // to be consistent with pre-compression bytesRejectedRate recording
                                        brokerTopicStats.topicStats(topicPartition().topic()).bytesRejectedRate().mark(records.sizeInBytes());
                                        brokerTopicStats.allTopicsStats.bytesRejectedRate().mark(records.sizeInBytes());
                                        String msg = String.format("Message batch size is %s bytes in append to partition %s which exceeds the maximum configured size of %s.",
                                                batch.sizeInBytes(), topicPartition(), config().getMaxMessageSize());
                                        throw new RecordTooLargeException(msg);
                                    }
                                });
                            }
                        } else {
                            // we are taking the offsets we are given
                            if (!appendInfo.getOffsetsMonotonic()) {
                                StringBuilder builder = new StringBuilder("[");
                                Iterator<Record> iterator = records.records().iterator();
                                while (iterator.hasNext()) {
                                    Record next = iterator.next();
                                    builder.append(next.offset());
                                    if (iterator.hasNext()) {
                                        builder.append(",");
                                    }
                                }
                                builder.append("]");
                                throw new OffsetsOutOfOrderException("Out of order offsets found in append to $topicPartition: " + builder.toString());
                            }

                            if (appendInfo.firstOrLastOffsetOfFirstBatch() < localLog.logEndOffset()) {
                                // we may still be able to recover if the log is empty
                                // one example: fetching from log start offset on the leader which is not batch aligned,
                                // which may happen as a result of AdminClient#deleteRecords()
                                Optional<LogOffsetMetadata> firstOffsetOptional = appendInfo.getFirstOffset();
                                long firstOffset;
                                if (firstOffsetOptional.isPresent()) {
                                    LogOffsetMetadata offsetMetadata = firstOffsetOptional.get();
                                    firstOffset = offsetMetadata.getMessageOffset();
                                } else {
                                    firstOffset = records.batches().iterator().next().baseOffset();
                                }

                                String firstOrLast = appendInfo.getFirstOffset().isPresent() ? "First offset" : "Last offset of the first batch";
                                String msg = String.format("Unexpected offset in append to %s. %s %s is less than the next offset %s. " +
                                                "First 10 offsets in append: %s, last offset in append: %s. Log start offset = %s",
                                        topicPartition(), firstOrLast, appendInfo.firstOrLastOffsetOfFirstBatch(), localLog.logEndOffset(),
                                        StreamSupport.stream(records.records().spliterator(), false).limit(10).map(Record::offset).collect(Collectors.toList()),
                                        appendInfo.getLastOffset(), logStartOffset);
                                throw new UnexpectedAppendOffsetException(msg, firstOffset, appendInfo.getLastOffset());
                            }
                        }

                        // update the epoch cache with the epoch stamped onto the message by the leader
                        for (MutableRecordBatch batch : validRecordsArr[0].batches()) {
                            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                                maybeAssignEpochStartOffset(batch.partitionLeaderEpoch(), batch.baseOffset());
                            } else {
                                // In partial upgrade scenarios, we may get a temporary regression to the message format. In
                                // order to ensure the safety of leader election, we clear the epoch cache so that we revert
                                // to truncation by high watermark after the next leader election.
                                if (leaderEpochCache.isPresent()) {
                                    LeaderEpochFileCache cache = leaderEpochCache.get();
                                    LOG.warn("Clearing leader epoch cache after unexpected append with message format v{}", batch.magic());
                                    cache.clearAndFlush();
                                }
                            }
                        }

                        // check messages set size may be exceed config.segmentSize
                        if (validRecordsArr[0].sizeInBytes() > config().getSegmentSize()) {
                            String msg = String.format("Message batch size is %s bytes in append to partition %s, which exceeds the maximum configured segment size of %s.",
                                    validRecordsArr[0].sizeInBytes(), topicPartition(), config().getSegmentSize());
                            throw new RecordBatchTooLargeException(msg);
                        }

                        // maybe roll the log if this segment is full
                        LogSegment segment = maybeRoll(validRecordsArr[0].sizeInBytes(), appendInfo);

                        LogOffsetMetadata logOffsetMetadata = new LogOffsetMetadata(
                                appendInfo.firstOrLastOffsetOfFirstBatch(),
                                segment.getBaseOffset(),
                                segment.size());

                        // now that we have valid records, offsets assigned, and timestamps updated, we need to
                        // validate the idempotent/transactional state of the producers and collect some metadata
                        Triple<Map<Long, ProducerAppendInfo>, List<CompletedTxn>, Optional<BatchMetadata>> triple = analyzeAndValidateProducerState(
                                logOffsetMetadata, validRecordsArr[0], origin);
                        Map<Long, ProducerAppendInfo> updatedProducers = triple.getLeft();
                        List<CompletedTxn> completedTxns = triple.getMiddle();
                        Optional<BatchMetadata> maybeDuplicate = triple.getRight();

                        if (maybeDuplicate.isPresent()) {
                            BatchMetadata duplicate = maybeDuplicate.get();
                            appendInfo.setFirstOffset(Optional.of(new LogOffsetMetadata(duplicate.firstOffset())));
                            appendInfo.setLastOffset(duplicate.getLastOffset());
                            appendInfo.setLogAppendTime(duplicate.getTimestamp());
                            appendInfo.setLogStartOffset(logStartOffset);
                        } else {
                            // Before appending update the first offset metadata to include segment information
                            appendInfo.setFirstOffset(appendInfo.getFirstOffset()
                                    .flatMap(
                                            offsetMetadata -> Optional.of(new LogOffsetMetadata(offsetMetadata.getMessageOffset(), segment.getBaseOffset(), segment.size()))
                                    )
                            );

                            // Append the records, and increment the local log end offset immediately after the append because a
                            // write to the transaction index below may fail and we want to ensure that the offsets
                            // of future appends still grow monotonically. The resulting transaction index inconsistency
                            // will be cleaned up after the log directory is recovered. Note that the end offset of the
                            // ProducerStateManager will not be updated and the last stable offset will not advance
                            // if the append to the transaction index fails.
                            localLog.append(appendInfo.getLastOffset(), appendInfo.getMaxTimestamp(), appendInfo.getOffsetOfMaxTimestamp(), validRecordsArr[0]);
                            updateHighWatermarkWithLogEndOffset();

                            // update the producer state
                            updatedProducers.values().forEach(producerAppendInfo -> producerStateManager.update(producerAppendInfo));

                            // update the transaction index with the true last stable offset. The last offset visible
                            // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
                            for (CompletedTxn completedTxn : completedTxns) {
                                Long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                                segment.updateTxnIndex(completedTxn, lastStableOffset);
                                producerStateManager.completeTxn(completedTxn);
                            }

                            // always update the last producer id map offset so that the snapshot reflects the current offset
                            // even if there isn't any idempotent data being written
                            producerStateManager.updateMapEndOffset(appendInfo.getLastOffset() + 1);

                            // update the first unstable offset (which is used to compute LSO)
                            maybeIncrementFirstUnstableOffset();

                            LOG.trace("Appended message set with last offset: {}, first offset: {}, next offset: {}, and messages: {}",
                                    appendInfo.getLastOffset(), appendInfo.getFirstOffset(), localLog.logEndOffset(), validRecordsArr[0]);

                            if (localLog.unflushedMessages() >= config().getFlushInterval()) {
                                flush(false);
                            }

                        }

                        return appendInfo;
                    }
                });
            }
        }
    }

    public void maybeAssignEpochStartOffset(Integer leaderEpoch, Long startOffset) {
        if (leaderEpochCache.isPresent()) {
            LeaderEpochFileCache cache = leaderEpochCache.get();
            cache.assign(leaderEpoch, startOffset);
        }
    }

    public Optional<Integer> latestEpoch() throws Throwable {
        if (leaderEpochCache.isPresent()) {
            return leaderEpochCache.get().latestEpoch();
        } else {
            return Optional.empty();
        }
    }

    public Optional<OffsetAndEpoch> endOffsetForEpoch(Integer leaderEpoch) throws Throwable {
        if (leaderEpochCache.isPresent()) {
            LeaderEpochFileCache cache = leaderEpochCache.get();
            Pair<Integer, Long> pair = cache.endOffsetFor(leaderEpoch, logEndOffset());
            Integer foundEpoch = pair.getKey();
            Long foundOffset = pair.getValue();
            if (foundOffset == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET) {
                return Optional.empty();
            } else {
                return Optional.of(new OffsetAndEpoch(foundOffset, foundEpoch));
            }
        } else {
            return Optional.empty();
        }
    }

    private void maybeIncrementFirstUnstableOffset() throws OffsetOutOfRangeException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();

            Optional<LogOffsetMetadata> updatedFirstUnstableOffset = producerStateManager.firstUnstableOffset();
            if (updatedFirstUnstableOffset.isPresent()) {
                LogOffsetMetadata logOffsetMetadata = updatedFirstUnstableOffset.get();
                if (logOffsetMetadata.messageOffsetOnly() || logOffsetMetadata.getMessageOffset() < logStartOffset) {
                    long offset = Math.max(logOffsetMetadata.getMessageOffset(), logStartOffset);
                    updatedFirstUnstableOffset = Optional.of(convertToOffsetMetadataOrThrow(offset));
                }
            }

            if (updatedFirstUnstableOffset != this.firstUnstableOffsetMetadata) {
                LOG.debug("First unstable offset updated to {}", updatedFirstUnstableOffset);
                this.firstUnstableOffsetMetadata = updatedFirstUnstableOffset;
            }
        }
    }

    /**
     * Increment the log start offset if the provided offset is larger.
     * <p>
     * If the log start offset changed, then this method also update a few key offset such that
     * `logStartOffset <= logStableOffset <= highWatermark`. The leader epoch cache is also updated
     * such that all of offsets referenced in that component point to valid offset in this log.
     *
     * @return true if the log start offset was updated; otherwise false
     * @throws OffsetOutOfRangeException if the log start offset is greater than the high watermark
     */
    public Boolean maybeIncrementLogStartOffset(Long newLogStartOffset, LogStartOffsetIncrementReason reason) throws OffsetOutOfRangeException {
        // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
        // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
        // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
        String msg = String.format("Exception while increasing log start offset for %s to %s in dir %s",
                topicPartition(), newLogStartOffset, dir().getParent());
        return maybeHandleIOException(msg, new SupplierWithIOException<Boolean>() {
            @Override
            public Boolean get() {
                synchronized (lock) {
                    if (newLogStartOffset > highWatermark()) {
                        String msg = String.format("Cannot increment the log start offset to %s of partition %s since it is larger than the high watermark %s",
                                newLogStartOffset, topicPartition(), highWatermark());
                        throw new OffsetOutOfRangeException(msg);
                    }

                    localLog.checkIfMemoryMappedBufferClosed();
                    if (newLogStartOffset > logStartOffset) {
                        updateLogStartOffset(newLogStartOffset);
                        LOG.info("Incremented log start offset to {} due to {}", newLogStartOffset, reason);
                        if (leaderEpochCache.isPresent()) {
                            LeaderEpochFileCache cache = leaderEpochCache.get();
                            cache.truncateFromStart(logStartOffset);
                        }
                        producerStateManager.onLogStartOffsetIncremented(newLogStartOffset);
                        maybeIncrementFirstUnstableOffset();
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        });
    }

    private Triple<Map<Long, ProducerAppendInfo>, List<CompletedTxn>, Optional<BatchMetadata>> analyzeAndValidateProducerState(LogOffsetMetadata appendOffsetMetadata,
                                                                                                                               MemoryRecords records,
                                                                                                                               AppendOrigin origin) {
        Map<Long, ProducerAppendInfo> updatedProducers = new HashMap<>();
        List<CompletedTxn> completedTxns = new ArrayList<>();
        int relativePositionInSegment = appendOffsetMetadata.getRelativePositionInSegment();

        for (MutableRecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
                // If we find a duplicate, we return the metadata of the appended batch to the client.
                if (origin == AppendOrigin.Client) {
                    Optional<ProducerStateEntry> maybeLastEntry = producerStateManager.lastEntry(batch.producerId());

                    Optional<BatchMetadata> batchMetadata = maybeLastEntry.flatMap(entry -> entry.findDuplicateBatch(batch));
                    if (batchMetadata.isPresent()) {
                        BatchMetadata duplicate = batchMetadata.get();
                        return Triple.of(updatedProducers, completedTxns, Optional.of(duplicate));
                    }
                }

                // We cache offset metadata for the start of each transaction. This allows us to
                // compute the last stable offset without relying on additional index lookups.
                Optional<LogOffsetMetadata> firstOffsetMetadata = batch.isTransactional() ?
                        Optional.of(new LogOffsetMetadata(batch.baseOffset(), appendOffsetMetadata.getSegmentBaseOffset(), relativePositionInSegment))
                        : Optional.empty();

                Optional<CompletedTxn> maybeCompletedTxn = updateProducers(producerStateManager, batch, updatedProducers, firstOffsetMetadata, origin);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }

            relativePositionInSegment += batch.sizeInBytes();
        }

        return Triple.of(updatedProducers, completedTxns, Optional.empty());
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid (if ignoreRecordSize is false)
     * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other.
     * </ol>
     * <p>
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private LogAppendInfo analyzeAndValidateRecords(MemoryRecords records,
                                                    AppendOrigin origin,
                                                    Boolean ignoreRecordSize,
                                                    Integer leaderEpoch) {
        int shallowMessageCount = 0;
        int validBytesCount = 0;
        Optional<LogOffsetMetadata> firstOffset = Optional.empty();
        long lastOffset = -1L;
        int lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        CompressionCodec sourceCodec = CompressionCodec.NoCompressionCodec;
        boolean monotonic = true;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        boolean readFirstMessage = false;
        long lastOffsetOfFirstBatch = -1L;

        for (MutableRecordBatch batch : records.batches()) {
            if (origin == AppendOrigin.RaftLeader && batch.partitionLeaderEpoch() != leaderEpoch) {
                throw new InvalidRecordException("Append from Raft leader did not set the batch epoch correctly");
            }
            // we only validate V2 and higher to avoid potential compatibility issues with older clients
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && origin == AppendOrigin.Client && batch.baseOffset() != 0) {
                String msg = String.format("The baseOffset of the record batch in the append to %s should " +
                        "be 0, but it is %s", topicPartition(), batch.baseOffset());
                throw new InvalidRecordException(msg);
            }

            // update the first offset if on the first message. For magic versions older than 2, we use the last offset
            // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
            // For magic version 2, we can get the first offset directly from the batch header.
            // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
            // case, validation will be more lenient.
            // Also indicate whether we have the accurate first offset or not
            if (!readFirstMessage) {
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    firstOffset = Optional.of(new LogOffsetMetadata(batch.baseOffset()));
                }
                lastOffsetOfFirstBatch = batch.lastOffset();
                readFirstMessage = true;
            }

            // check that offsets are monotonically increasing
            if (lastOffset >= batch.lastOffset()) {
                monotonic = false;
            }

            // update the last offset seen
            lastOffset = batch.lastOffset();
            lastLeaderEpoch = batch.partitionLeaderEpoch();

            // Check if the message sizes are valid.
            int batchSize = batch.sizeInBytes();
            if (!ignoreRecordSize && batchSize > config().getMaxMessageSize()) {
                brokerTopicStats.topicStats(topicPartition().topic()).bytesRejectedRate().mark(records.sizeInBytes());
                brokerTopicStats.allTopicsStats.bytesRejectedRate().mark(records.sizeInBytes());
                String msg = String.format("The record batch size in the append to %s is $batchSize bytes " +
                        "which exceeds the maximum configured value of %s.", topicPartition(), config().getMaxMessageSize());
                throw new RecordTooLargeException(msg);
            }

            // check the validity of the message by checking CRC
            if (!batch.isValid()) {
                brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec().mark();
                String msg = String.format("Record is corrupt (stored crc = %s) in topic partition %s.", batch.checksum(), topicPartition());
                throw new CorruptRecordException(msg);
            }

            if (batch.maxTimestamp() > maxTimestamp) {
                maxTimestamp = batch.maxTimestamp();
                offsetOfMaxTimestamp = lastOffset;
            }

            shallowMessageCount += 1;
            validBytesCount += batchSize;

            CompressionCodec messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType().id);
            if (messageCodec != CompressionCodec.NoCompressionCodec) {
                sourceCodec = messageCodec;
            }
        }

        // Apply broker-side compression if any
        CompressionCodec targetCodec = CompressionCodec.BrokerCompressionCodec.getTargetCompressionCodec(config().getCompressionType(), sourceCodec);
        Optional<Integer> lastLeaderEpochOpt = lastLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.of(lastLeaderEpoch) : Optional.empty();
        return new LogAppendInfo(firstOffset,
                lastOffset,
                lastLeaderEpochOpt,
                maxTimestamp,
                offsetOfMaxTimestamp,
                RecordBatch.NO_TIMESTAMP,
                logStartOffset,
                RecordConversionStats.EMPTY,
                sourceCodec,
                targetCodec,
                shallowMessageCount,
                validBytesCount,
                monotonic,
                lastOffsetOfFirstBatch,
                new ArrayList<>(),
                null,
                new LeaderHwChange.None());
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param records The records to trim
     * @param info    The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in or it may not.
     */
    private MemoryRecords trimInvalidBytes(MemoryRecords records, LogAppendInfo info) {
        Integer validBytes = info.getValidBytes();
        if (validBytes < 0) {
            String msg = String.format("Cannot append record batch with illegal length %s to " +
                    "log for %s. A possible cause is a corrupted produce request.", validBytes, topicPartition());
            throw new CorruptRecordException(msg);
        }
        if (validBytes == records.sizeInBytes()) {
            return records;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytes);
            return MemoryRecords.readableRecords(validByteBuffer);
        }
    }

    private void checkLogStartOffset(Long offset) throws OffsetOutOfRangeException {
        if (offset < logStartOffset) {
            String msg = String.format("Received request for offset %s for partition %s, " +
                    "but we only have log segments starting from offset: %s.", offset, topicPartition(), logStartOffset);
            throw new OffsetOutOfRangeException(msg);
        }
    }

//    public FetchDataInfo read(Long startOffset,
//                              Long maxOffset) throws OffsetOutOfRangeException {
//        checkLogStartOffset(startOffset);
//        LogOffsetMetadata maxOffsetMetadata = new LogOffsetMetadata(maxOffset);
//        return localLog.read(startOffset, KafkaConfig.Defaults.FetchMaxBytes, false, maxOffsetMetadata, false);
//    }

    /**
     * Read messages from the log.
     *
     * @param startOffset   The offset to begin reading at
     * @param maxLength     The maximum number of bytes to read
     * @param isolation     The fetch isolation, which controls the maximum offset we are allowed to read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
     * @return The fetch data information including fetch starting offset metadata and messages read.
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
     */
    public FetchDataInfo read(Long startOffset,
                              Integer maxLength,
                              FetchIsolation isolation,
                              Boolean minOneMessage) throws OffsetOutOfRangeException {
        checkLogStartOffset(startOffset);
        LogOffsetMetadata maxOffsetMetadata = null;
        switch (isolation) {
            case FetchLogEnd:
                maxOffsetMetadata = localLog.logEndOffsetMetadata();
                break;
            case FetchHighWatermark:
                maxOffsetMetadata = fetchHighWatermarkMetadata();
                break;
            case FetchTxnCommitted:
                maxOffsetMetadata = fetchLastStableOffsetMetadata();
                break;
        }
        return localLog.read(startOffset, maxLength, minOneMessage, maxOffsetMetadata, isolation == FetchIsolation.FetchTxnCommitted);
    }

    protected List<AbortedTxn> collectAbortedTransactions(Long startOffset, Long upperBoundOffset) throws IOException {
        return localLog.collectAbortedTransactions(logStartOffset, startOffset, upperBoundOffset);
    }

    /**
     * Get an offset based on the given timestamp
     * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
     * given timestamp.
     * <p>
     * If no such message is found, the log end offset is returned.
     * <p>
     * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
     * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
     *
     * @param targetTimestamp The given timestamp for offset fetching.
     * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
     * None if no such message is found.
     */
//    @nowarn("cat=deprecation")
    public Optional<FileRecords.TimestampAndOffset> fetchOffsetByTimestamp(Long targetTimestamp) {
        String msg = String.format("Error while fetching offset by timestamp for %s in dir %s", topicPartition(), dir().getParent());
        return maybeHandleIOException(msg, new SupplierWithIOException<Optional<FileRecords.TimestampAndOffset>>() {
            @Override
            public Optional<FileRecords.TimestampAndOffset> get() throws IOException {
                LOG.debug("Searching offset for timestamp {}", targetTimestamp);

                if (config().getMessageFormatVersion().isLessThan(MetadataVersion.IBP_0_10_0_IV0) &&
                        targetTimestamp != ListOffsetsRequest.EARLIEST_TIMESTAMP &&
                        targetTimestamp != ListOffsetsRequest.LATEST_TIMESTAMP) {
                    String msg = String.format("Cannot search offsets based on timestamp because message format version " +
                            "for partition %s is %s which is earlier than the minimum " +
                            "required version %s", topicPartition(), config().getMessageFormatVersion(), MetadataVersion.IBP_0_10_0_IV0);
                    throw new UnsupportedForMessageFormatException(msg);
                }

                // For the earliest and latest, we do not need to return the timestamp.
                if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                    // The first cached epoch usually corresponds to the log start offset, but we have to verify this since
                    // it may not be true following a message format version bump as the epoch will not be available for
                    // log entries written in the older format.
                    Optional<EpochEntry> earliestEpochEntry = Optional.empty();
                    if (leaderEpochCache.isPresent()) {
                        earliestEpochEntry = leaderEpochCache.get().latestEntry();
                    }
                    Optional<Integer> epochOpt = Optional.empty();
                    if (earliestEpochEntry.isPresent() && earliestEpochEntry.get().getStartOffset() <= logStartOffset) {
                        EpochEntry entry = earliestEpochEntry.get();
                        epochOpt = Optional.of(entry.getEpoch());
                    }

                    return Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt));
                } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
                    Optional<Integer> latestEpochOpt = Optional.empty();
                    if (leaderEpochCache.isPresent()) {
                        latestEpochOpt = leaderEpochCache.get().latestEpoch();
                    }
//                    Optional<Integer> epochOptional = Optional.ofNullable(latestEpochOpt.orElse(null));
                    return Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset(), latestEpochOpt));
                } else if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
                    // constant time access while being safe to use with concurrent collections unlike `toArray`.
                    Collection<LogSegment> segmentsCopy = logSegments();
                    LogSegment latestTimestampSegment = null;
                    for (LogSegment segment : segmentsCopy) {
                        if (Objects.isNull(latestTimestampSegment)) {
                            latestTimestampSegment = segment;
                        } else {
                            if (segment.maxTimestampSoFar() > latestTimestampSegment.maxTimestampSoFar()) {
                                latestTimestampSegment = segment;
                            }
                        }
                    }

                    Optional<Integer> latestEpochOpt = Optional.empty();
                    if (leaderEpochCache.isPresent()) {
                        latestEpochOpt = leaderEpochCache.get().latestEpoch();
                    }
                    Optional<Integer> epochOptional = Optional.ofNullable(latestEpochOpt.orElse(null));
                    TimestampOffset latestTimestampAndOffset = latestTimestampSegment.getMaxTimestampAndOffsetSoFar();
                    return Optional.of(new FileRecords.TimestampAndOffset(latestTimestampAndOffset.getTimestamp(),
                            latestTimestampAndOffset.getOffset(),
                            epochOptional));
                } else {
                    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
                    // constant time access while being safe to use with concurrent collections unlike `toArray`.
                    Collection<LogSegment> segmentsCopy = logSegments();
                    // We need to search the first segment whose largest timestamp is >= the target timestamp if there is one.
                    Optional<LogSegment> targetSeg = Optional.empty();
                    for (LogSegment segment : segmentsCopy) {
                        if (segment.largestTimestamp() >= targetTimestamp) {
                            targetSeg = Optional.of(segment);
                            break;
                        }
                    }
                    if (targetSeg.isPresent()) {
                        return targetSeg.get().findOffsetByTimestamp(targetTimestamp, logStartOffset);
                    } else {
                        return Optional.empty();
                    }
                }
            }
        });
    }

    public List<Long> legacyFetchOffsetsBefore(Long timestamp, Integer maxNumOffsets) {
        // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
        // constant time access while being safe to use with concurrent collections unlike `toArray`.
        final List<LogSegment> allSegments = new ArrayList<>(logSegments());
        boolean lastSegmentHasSize = CollectionUtilExt.last(allSegments).size() > 0;

        Pair<Long, Long>[] offsetTimeArray = lastSegmentHasSize ? new Pair[allSegments.size() + 1] : new Pair[allSegments.size()];

        for (int i = 0; i < allSegments.size(); i++) {
            offsetTimeArray[i] = Pair.of(Math.max(allSegments.get(i).getBaseOffset(), logStartOffset), allSegments.get(i).getLastModified());
        }
        if (lastSegmentHasSize) {
            offsetTimeArray[allSegments.size()] = Pair.of(logEndOffset(), time().milliseconds());
        }

        int startIndex = -1;
        if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            startIndex = offsetTimeArray.length - 1;
        } else if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            startIndex = 0;
        } else {
            boolean isFound = false;
            LOG.debug("Offset time array = {}", Arrays.toString(offsetTimeArray));
            startIndex = offsetTimeArray.length - 1;
            while (startIndex >= 0 && !isFound) {
                if (offsetTimeArray[startIndex].getRight() <= timestamp) {
                    isFound = true;
                } else {
                    startIndex -= 1;
                }
            }
        }

        int retSize = Math.min(maxNumOffsets, startIndex + 1);
        Long[] ret = new Long[retSize];
        for (int j = 0; j < retSize; j++) {
            ret[j] = offsetTimeArray[startIndex].getLeft();
            startIndex -= 1;
        }
        // ensure that the returned seq is in descending order of offsets
        Set<Long> sortSet = new TreeSet<>(Comparator.reverseOrder());
        sortSet.addAll(Arrays.asList(ret));
        return new ArrayList<>(sortSet);
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * If the message offset is out of range, throw an OffsetOutOfRangeException
     */
    private LogOffsetMetadata convertToOffsetMetadataOrThrow(Long offset) throws OffsetOutOfRangeException {
        checkLogStartOffset(offset);
        return localLog.convertToOffsetMetadataOrThrow(offset);
    }

    /**
     * Delete any local log segments starting with the oldest segment and moving forward until until
     * the user-supplied predicate is false or the segment containing the current high watermark is reached.
     * We do not delete segments with offsets at or beyond the high watermark to ensure that the log start
     * offset can never exceed it. If the high watermark has not yet been initialized, no segments are eligible
     * for deletion.
     *
     * @param predicate A function that takes in a candidate log segment and the next higher segment
     *                  (if there is one) and returns true iff it is deletable
     * @param reason    The reason for the segment deletion
     * @return The number of segments deleted
     */
    private Integer deleteOldSegments(final BiFunctionWithIOException<LogSegment, Optional<LogSegment>, Boolean> predicate,
                                      SegmentDeletionReason reason) throws IOException {
        synchronized (lock) {
            Collection<LogSegment> deletable = localLog.deletableSegments(new BiFunctionWithIOException<LogSegment, Optional<LogSegment>, Boolean>() {
                @Override
                public Boolean apply(LogSegment segment, Optional<LogSegment> nextSegmentOpt) throws IOException {
                    return (highWatermark() >= nextSegmentOpt.map(LogSegment::getBaseOffset).orElseGet(() -> localLog.logEndOffset()))
                            && predicate.apply(segment, nextSegmentOpt);
                }
            });
            return CollectionUtils.isNotEmpty(deletable) ? deleteSegments(new ArrayList<>(deletable), reason) : 0;
        }
    }

    private Integer deleteSegments(List<LogSegment> deletable, SegmentDeletionReason reason) throws OffsetOutOfRangeException, IOException {
        String msg = String.format("Error while deleting segments for %s in dir %s", topicPartition(), dir().getParent());
        return maybeHandleIOException(msg, new SupplierWithIOException<Integer>() {
            @Override
            public Integer get() throws IOException {
                Integer numToDelete = deletable.size();
                if (numToDelete > 0) {
                    // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
                    Collection<LogSegment> segmentsToDelete = deletable;
                    if (Objects.equals(localLog.segments.numberOfSegments(), numToDelete)) {
                        LogSegment newSegment = roll(Optional.empty());
                        if (CollectionUtilExt.last(deletable).getBaseOffset() == newSegment.getBaseOffset()) {
                            LOG.warn("Empty active segment at {} was deleted and recreated due to {}", CollectionUtilExt.last(deletable).getBaseOffset(), reason);
                            segmentsToDelete = CollectionUtilExt.dropRight(deletable, 1);
                        }
                    }
                    localLog.checkIfMemoryMappedBufferClosed();
                    // remove the segments for lookups
                    localLog.removeAndDeleteSegments(segmentsToDelete, true, reason);
                    deleteProducerSnapshots(deletable, true);
                    maybeIncrementLogStartOffset(localLog.segments.firstSegmentBaseOffset().get(), LogStartOffsetIncrementReason.segmentDeletion);
                }
                return numToDelete;
            }
        });
    }

    /**
     * If topic deletion is enabled, delete any local log segments that have either expired due to time based retention
     * or because the log size is > retentionSize.
     * <p>
     * Whether or not deletion is enabled, delete any local log segments that are before the log start offset
     */
    public Integer deleteOldSegments() throws Throwable {
        if (config().getDelete()) {
            return deleteLogStartOffsetBreachedSegments() +
                    deleteRetentionSizeBreachedSegments() +
                    deleteRetentionMsBreachedSegments();
        } else {
            return deleteLogStartOffsetBreachedSegments();
        }
    }

    private Integer deleteRetentionMsBreachedSegments() throws Throwable {
        if (config().getRetentionMs() < 0) {
            return 0;
        }
        long startMs = time().milliseconds();

        return deleteOldSegments(new BiFunctionWithIOException<LogSegment, Optional<LogSegment>, Boolean>() {
            @Override
            public Boolean apply(LogSegment segment, Optional<LogSegment> nextSegmentOpt) throws IOException {
                return startMs - segment.largestTimestamp() > config().getRetentionMs();
            }
        }, new SegmentDeletionReason.RetentionMsBreach(this));
    }

    private Integer deleteRetentionSizeBreachedSegments() throws Throwable {
        if (config().getRetentionSize() < 0 || size() < config().getRetentionSize()) {
            return 0;
        }
        final Long[] diff = new Long[1];
        diff[0] = size() - config().getRetentionSize();
        return deleteOldSegments(new BiFunctionWithIOException<LogSegment, Optional<LogSegment>, Boolean>() {
            @Override
            public Boolean apply(LogSegment segment, Optional<LogSegment> nextSegmentOpt) {
                if (diff[0] - segment.size() >= 0) {
                    diff[0] -= segment.size();
                    return true;
                } else {
                    return false;
                }
            }
        }, new SegmentDeletionReason.RetentionSizeBreach(this));
    }

    private Integer deleteLogStartOffsetBreachedSegments() throws IOException {
        return deleteOldSegments(new BiFunctionWithIOException<LogSegment, Optional<LogSegment>, Boolean>() {
            @Override
            public Boolean apply(LogSegment segment, Optional<LogSegment> nextSegmentOpt) {
                return nextSegmentOpt.isPresent() && nextSegmentOpt.get().getBaseOffset() <= logStartOffset;
            }
        }, new SegmentDeletionReason.StartOffsetBreach(this));
    }

    public Boolean isFuture() {
        return localLog.isFuture();
    }

    /**
     * The size of the log in bytes
     */
    public Long size() {
        return localLog.segments.sizeInBytes();
    }

    /**
     * The offset of the next message that will be appended to the log
     */
    public Long logEndOffset() {
        return localLog.logEndOffset();
    }

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    public LogOffsetMetadata logEndOffsetMetadata() {
        return localLog.logEndOffsetMetadata();
    }

    /**
     * Roll the log over to a new empty log segment if necessary.
     * The segment will be rolled if one of the following conditions met:
     * 1. The logSegment is full
     * 2. The maxTime has elapsed since the timestamp of first message in the segment (or since the
     * create time if the first message does not have a timestamp)
     * 3. The index is full
     *
     * @param messagesSize The messages set size in bytes.
     * @param appendInfo   log append information
     * @return The currently active segment after (perhaps) rolling to a new segment
     */
    private LogSegment maybeRoll(Integer messagesSize, LogAppendInfo appendInfo) throws IOException {
        synchronized (lock) {
            LogSegment segment = localLog.segments.activeSegment();
            Long now = time().milliseconds();

            Long maxTimestampInMessages = appendInfo.getMaxTimestamp();
            Long maxOffsetInMessages = appendInfo.getLastOffset();

            if (segment.shouldRoll(RollParams.apply(config(), appendInfo, messagesSize, now))) {
                LOG.debug("Rolling new log segment (log_size = {}/{}}, offset_index_size = {}/{}, " +
                                "time_index_size = {}/{}, inactive_time_ms = {}/{}).",
                        segment.size(), config().getSegmentSize(), segment.offsetIndex().entries(), segment.offsetIndex().maxEntries(),
                        segment.timeIndex().entries(), segment.timeIndex().maxEntries(),
                        segment.timeWaitedForRoll(now, maxTimestampInMessages), config().getSegmentMs() - segment.getRollJitterMs());

                /*
                maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
                Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
                first offset in the set. Determining the true first offset in the set requires decompression, which the follower
                is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
                This was problematic in the case that two consecutive messages differed in offset by
                Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
                base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
                highly compacted topic from scratch.
                Note that this is only required for pre-V2 message formats because these do not store the first message offset
                in the header.
                 */
                Long rollOffset = appendInfo
                        .getFirstOffset()
                        .map(LogOffsetMetadata::getMessageOffset)
                        .orElseGet(() -> maxOffsetInMessages - Integer.MAX_VALUE);

                return roll(Optional.of(rollOffset));
            } else {
                return segment;
            }
        }
    }

    public LogSegment roll() throws OffsetOutOfRangeException, IOException {
        return roll(Optional.empty());
    }

    /**
     * Roll the local log over to a new active segment starting with the expectedNextOffset (when provided),
     * or localLog.logEndOffset otherwise. This will trim the index to the exact size of the number of entries
     * it currently contains.
     *
     * @return The newly rolled segment
     */
    public LogSegment roll(Optional<Long> expectedNextOffset) throws OffsetOutOfRangeException, IOException {
        synchronized (lock) {
            LogSegment newSegment = localLog.roll(expectedNextOffset);
            // Take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
            // offset align with the new segment offset since this ensures we can recover the segment by beginning
            // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
            // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
            // we manually override the state offset here prior to taking the snapshot.
            producerStateManager.updateMapEndOffset(newSegment.getBaseOffset());
            producerStateManager.takeSnapshot();
            updateHighWatermarkWithLogEndOffset();
            // Schedule an asynchronous flush of the old segment
            scheduler().schedule("flush-log", () -> flushUptoOffsetExclusive(newSegment.getBaseOffset()), 0, -1, TimeUnit.MILLISECONDS);
            return newSegment;
        }
    }

    /**
     * Flush all local log segments
     *
     * @param forceFlushActiveSegment should be true during a clean shutdown, and false otherwise. The reason is that
     *                                we have to pass logEndOffset + 1 to the `localLog.flush(offset: Long): Unit` function to flush empty
     *                                active segments, which is important to make sure we persist the active segment file during shutdown, particularly
     *                                when it's empty.
     */
    public void flush(Boolean forceFlushActiveSegment) {
        flush(logEndOffset(), forceFlushActiveSegment);
    }

    /**
     * Flush local log segments for all offsets up to offset-1
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    public void flushUptoOffsetExclusive(Long offset) {
        flush(offset, false);
    }

    /**
     * Flush local log segments for all offsets up to offset-1 if includingOffset=false; up to offset
     * if includingOffset=true. The recovery point is set to offset.
     *
     * @param offset          The offset to flush up to; the new recovery point
     * @param includingOffset Whether the flush includes the provided offset.
     */
    private void flush(Long offset, Boolean includingOffset) {
        Long flushOffset = includingOffset ? offset + 1 : offset;
        Long newRecoveryPoint = offset;
        String includingOffsetStr = includingOffset ? "inclusive" : "exclusive";
        String msg = String.format("Error while flushing log for %s in dir %s with offset %s (%s) and recovery point %s",
                topicPartition(), dir().getParent(), offset, includingOffsetStr, newRecoveryPoint);
        maybeHandleIOException(msg, new SupplierWithIOException<Void>() {
            @Override
            public Void get() throws IOException {
                if (flushOffset > localLog.recoveryPoint) {
                    LOG.debug("Flushing log up to offset {} ({})with recovery point {}, last flushed: {},  current time: {}, unflushed: {}",
                            offset, includingOffsetStr, newRecoveryPoint, lastFlushTime(), time().milliseconds(), localLog.unflushedMessages());
                    localLog.flush(flushOffset);
                    synchronized (lock) {
                        localLog.markFlushed(newRecoveryPoint);
                    }
                }
                return null;
            }
        });
    }

    /**
     * Completely delete the local log directory and all contents from the file system with no delay
     */
    protected void delete() {
        String msg = String.format("Error while deleting log for %s in dir %s", topicPartition(), dir().getParent());
        maybeHandleIOException(msg, () -> {
            synchronized (lock) {
                localLog.checkIfMemoryMappedBufferClosed();
                producerExpireCheck.cancel(true);
                if (leaderEpochCache.isPresent()) {
                    leaderEpochCache.get().clear();
                }
                Collection<LogSegment> deletedSegments = localLog.deleteAllSegments();
                deleteProducerSnapshots(deletedSegments, false);
                localLog.deleteEmptyDir();
            }
            return null;
        });
    }

    // visible for testing
    protected void takeProducerSnapshot() throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            producerStateManager.takeSnapshot();
        }
    }

    // visible for testing
    private Optional<Long> latestProducerSnapshotOffset() {
        synchronized (lock) {
            return producerStateManager.latestSnapshotOffset();
        }
    }

    // visible for testing
    private Optional<Long> oldestProducerSnapshotOffset() {
        synchronized (lock) {
            return producerStateManager.oldestSnapshotOffset();
        }
    }

    // visible for testing
    private Long latestProducerStateEndOffset() {
        synchronized (lock) {
            return producerStateManager.mapEndOffset();
        }
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     * @return True iff targetOffset < logEndOffset
     */
    protected Boolean truncateTo(Long targetOffset) {
        String msg = String.format("Error while truncating log to offset %s for %s in dir %s", targetOffset, topicPartition(), dir().getParent());
        return maybeHandleIOException(msg, () -> {
            if (targetOffset < 0) {
                throw new IllegalArgumentException(String.format("Cannot truncate partition %s to a negative offset (%d).", topicPartition(), targetOffset));
            }
            if (targetOffset >= localLog.logEndOffset()) {
                LOG.info("Truncating to {} has no effect as the largest offset in the log is {}", targetOffset, localLog.logEndOffset() - 1);

                // Always truncate epoch cache since we may have a conflicting epoch entry at the
                // end of the log from the leader. This could happen if this broker was a leader
                // and inserted the first start offset entry, but then failed to append any entries
                // before another leader was elected.
                synchronized (lock) {
                    if (leaderEpochCache.isPresent()) {
                        leaderEpochCache.get().truncateFromEnd(logEndOffset());
                    }
                }

                return false;
            } else {
                LOG.info("Truncating to offset {}", topicPartition());
                synchronized (lock) {
                    localLog.checkIfMemoryMappedBufferClosed();
                    if (localLog.segments.firstSegmentBaseOffset().get() > targetOffset) {
                        truncateFullyAndStartAt(targetOffset);
                    } else {
                        Collection<LogSegment> deletedSegments = localLog.truncateTo(targetOffset);
                        deleteProducerSnapshots(deletedSegments, true);
                        if (leaderEpochCache.isPresent()) {
                            leaderEpochCache.get().truncateFromEnd(targetOffset);
                        }
                        logStartOffset = Math.min(targetOffset, logStartOffset);
                        rebuildProducerState(targetOffset, producerStateManager);
                        if (highWatermark() >= localLog.logEndOffset()) {
                            updateHighWatermark(localLog.logEndOffsetMetadata());
                        }
                    }
                    return true;
                }
            }
        });
    }

    /**
     * Delete all data in the log and start at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    public void truncateFullyAndStartAt(Long newOffset) {
        String msg = String.format("Error while truncating the entire log for %s in dir %s", topicPartition(), dir().getParent());
        maybeHandleIOException(msg, () -> {
            LOG.debug("Truncate and start at offset {}", newOffset);
            synchronized (lock) {
                localLog.truncateFullyAndStartAt(newOffset);
                if (leaderEpochCache.isPresent()) {
                    leaderEpochCache.get().clearAndFlush();
                }
                producerStateManager.truncateFullyAndStartAt(newOffset);
                logStartOffset = newOffset;
                rebuildProducerState(newOffset, producerStateManager);
                updateHighWatermark(localLog.logEndOffsetMetadata());
            }
            return null;
        });
    }

    /**
     * The time this log is last known to have been fully flushed to disk
     */
    public Long lastFlushTime() {
        return localLog.lastFlushTime();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return localLog.segments.activeSegment();
    }

    /**
     * All the log segments in this log ordered from oldest to newest
     */
    public Collection<LogSegment> logSegments() {
        return localLog.segments.values();
    }

    /**
     * Get all segments beginning with the segment that includes "from" and ending with the segment
     * that includes up to "to-1" or the end of the log (if to > logEndOffset).
     */
    public Iterable<LogSegment> logSegments(Long from, Long to) {
        synchronized (lock) {
            return localLog.segments.values(from, to);
        }
    }

    public Iterable<LogSegment> nonActiveLogSegmentsFrom(Long from) {
        synchronized (lock) {
            return localLog.segments.nonActiveLogSegmentsFrom(from);
        }
    }

    @Override
    public String toString() {
        StringBuilder logString = new StringBuilder();
        logString.append("Log(dir=").append(dir());
        if (topicId().isPresent()) {
            logString.append(", topicId=").append(topicId().get());
        }
        logString.append(", topic=").append(topicPartition().topic());
        logString.append(", partition=").append(topicPartition().partition());
        logString.append(", highWatermark=").append(highWatermark());
        logString.append(", lastStableOffset=").append(lastStableOffset());
        logString.append(", logStartOffset=").append(logStartOffset);
        logString.append(", logEndOffset=").append(logEndOffset());
        logString.append(")");
        return logString.toString();
    }

    protected void replaceSegments(List<LogSegment> newSegments, List<LogSegment> oldSegments) throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            Iterable<LogSegment> deletedSegments = UnifiedLog.replaceSegments(localLog.segments, newSegments, oldSegments, dir(), topicPartition(),
                    config(), scheduler(), logDirFailureChannel(), logIdent, false);
            deleteProducerSnapshots(deletedSegments, true);
        }
    }

    /**
     * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
     * this call, and also protects against calling this function on the same segment in parallel.
     * <p>
     * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
     * to ensure no other logcleaner threads and retention thread can work on the same segment.
     */
    protected List<Long> getFirstBatchTimestampForSegments(Iterable<LogSegment> segments) {
        return LogSegments.getFirstBatchTimestampForSegments(segments);
    }

    /**
     * remove deleted log metrics
     */
    protected void removeLogMetrics() {
        removeMetric(LogMetricNames.NumLogSegments, tags);
        removeMetric(LogMetricNames.LogStartOffset, tags);
        removeMetric(LogMetricNames.LogEndOffset, tags);
        removeMetric(LogMetricNames.Size, tags);
    }

    /**
     * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
     *
     * @param segment The segment to add
     */
//    @threadsafe
    private LogSegment addSegment(LogSegment segment) {
        return localLog.segments.add(segment);
    }

    private <T> T maybeHandleIOException(String msg, SupplierWithIOException<T> fun) {
        return LocalLog.maybeHandleIOException(logDirFailureChannel(), parentDir(), msg, fun);
    }

    protected List<LogSegment> splitOverflowedSegment(LogSegment segment) throws IOException {
        synchronized (lock) {
            SplitSegmentResult result = UnifiedLog.splitOverflowedSegment(segment, localLog.segments, dir(),
                    topicPartition(), config(), scheduler(), logDirFailureChannel(), logIdent);
            deleteProducerSnapshots(result.getDeletedSegments(), true);
            return ImmutableList.copyOf(result.getNewSegments());
        }
    }

    private void deleteProducerSnapshots(Iterable<LogSegment> segments, Boolean asyncDelete) throws IOException {
        UnifiedLog.deleteProducerSnapshots(segments, producerStateManager, asyncDelete,
                scheduler(), config(), logDirFailureChannel(), parentDir(), topicPartition());
    }


    public static File logFile(File dir, Long offset, String suffix) {
        return LocalLog.logFile(dir, offset, suffix);
    }

    public static String logDeleteDirName(TopicPartition topicPartition) {
        return LocalLog.logDeleteDirName(topicPartition);
    }

    public static String logFutureDirName(TopicPartition topicPartition) {
        return LocalLog.logFutureDirName(topicPartition);
    }

    public static String logDirName(TopicPartition topicPartition) {
        return LocalLog.logDirName(topicPartition);
    }

    public static File offsetIndexFile(File dir, Long offset, String suffix) {
        return LocalLog.offsetIndexFile(dir, offset, suffix);
    }

    public static File timeIndexFile(File dir, Long offset, String suffix) {
        return LocalLog.timeIndexFile(dir, offset, suffix);
    }

    public static void deleteFileIfExists(File file) throws IOException {
        deleteFileIfExists(file, "");
    }

    public static void deleteFileIfExists(File file, String suffix) throws IOException {
        Files.deleteIfExists(new File(file.getPath() + suffix).toPath());
    }

    /**
     * Construct a producer id snapshot file using the given offset.
     *
     * @param dir    The directory in which the log will reside
     * @param offset The last offset (exclusive) included in the snapshot
     */
    public static File producerSnapshotFile(File dir, Long offset) {
        return new File(dir, LocalLog.filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix);
    }

    public static File transactionIndexFile(File dir, Long offset, String suffix) {
        return LocalLog.transactionIndexFile(dir, offset, suffix);
    }

    public static Long offsetFromFileName(String filename) {
        return LocalLog.offsetFromFileName(filename);
    }

    public static Long offsetFromFile(File file) {
        return LocalLog.offsetFromFile(file);
    }

    public static Long sizeInBytes(Collection<LogSegment> segments) {
        return LogSegments.sizeInBytes(segments);
    }

    public static TopicPartition parseTopicPartitionName(File dir) {
        return LocalLog.parseTopicPartitionName(dir);
    }

    protected static Boolean isIndexFile(File file) {
        return LocalLog.isIndexFile(file);
    }

    protected static Boolean isLogFile(File file) {
        return LocalLog.isLogFile(file);
    }

    private static void loadProducersFromRecords(ProducerStateManager producerStateManager, Records records) {
        Map<Long, ProducerAppendInfo> loadedProducers = new HashMap<>();
        List<CompletedTxn> completedTxns = new ArrayList<>();
        records.batches().forEach(batch -> {
            if (batch.hasProducerId()) {
                Optional<CompletedTxn> maybeCompletedTxn = updateProducers(
                        producerStateManager,
                        batch,
                        loadedProducers,
                        Optional.empty(),
                        AppendOrigin.Replication);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }
        });
        loadedProducers.values().forEach(producerStateManager::update);
        completedTxns.forEach(producerStateManager::completeTxn);
    }

    private static Optional<CompletedTxn> updateProducers(ProducerStateManager producerStateManager,
                                                          RecordBatch batch,
                                                          Map<Long, ProducerAppendInfo> producers,
                                                          Optional<LogOffsetMetadata> firstOffsetMetadata,
                                                          AppendOrigin origin) {
        long producerId = batch.producerId();
        ProducerAppendInfo appendInfo = producers.computeIfAbsent(producerId, k -> producerStateManager.prepareUpdate(producerId, origin));
        return appendInfo.append(batch, firstOffsetMetadata);
    }

    /**
     * If the recordVersion is >= RecordVersion.V2, then create and return a LeaderEpochFileCache.
     * Otherwise, the message format is considered incompatible and the existing LeaderEpoch file
     * is deleted.
     *
     * @param dir                  The directory in which the log will reside
     * @param topicPartition       The topic partition
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param recordVersion        The record version
     * @param logPrefix            The logging prefix
     * @return The new LeaderEpochFileCache instance (if created), none otherwise
     */
    public static Optional<LeaderEpochFileCache> maybeCreateLeaderEpochCache(File dir,
                                                                             TopicPartition topicPartition,
                                                                             LogDirFailureChannel logDirFailureChannel,
                                                                             RecordVersion recordVersion,
                                                                             String logPrefix) throws IOException {
        File leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir);

        if (recordVersion.precedes(RecordVersion.V2)) {
            Optional<LeaderEpochFileCache> currentCache = leaderEpochFile.exists() ? Optional.of(newLeaderEpochFileCache(leaderEpochFile, logDirFailureChannel, topicPartition)) : Optional.empty();

            if (currentCache.isPresent() && currentCache.get().nonEmpty()) {
                LOG.warn("{}Deleting non-empty leader epoch cache due to incompatible message format {}", logPrefix, recordVersion);
            }

            Files.deleteIfExists(leaderEpochFile.toPath());
            return Optional.empty();
        } else {
            return Optional.of(newLeaderEpochFileCache(leaderEpochFile, logDirFailureChannel, topicPartition));
        }
    }

    private static LeaderEpochFileCache newLeaderEpochFileCache(File leaderEpochFile,
                                                                LogDirFailureChannel logDirFailureChannel,
                                                                TopicPartition topicPartition) throws IOException {
        LeaderEpochCheckpointFile checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel);
        return new LeaderEpochFileCache(topicPartition, checkpointFile);
    }

    private static Iterable<LogSegment> replaceSegments(LogSegments existingSegments,
                                                        List<LogSegment> newSegments,
                                                        List<LogSegment> oldSegments,
                                                        File dir,
                                                        TopicPartition topicPartition,
                                                        LogConfig config,
                                                        Scheduler scheduler,
                                                        LogDirFailureChannel logDirFailureChannel,
                                                        String logPrefix,
                                                        Boolean isRecoveredSwapFile) throws IOException {
        return LocalLog.replaceSegments(existingSegments,
                newSegments,
                oldSegments,
                dir,
                topicPartition,
                config,
                scheduler,
                logDirFailureChannel,
                logPrefix,
                isRecoveredSwapFile);
    }

    protected static void deleteSegmentFiles(ImmutableList<LogSegment> segmentsToDelete,
                                             Boolean asyncDelete,
                                             File dir,
                                             TopicPartition topicPartition,
                                             LogConfig config,
                                             Scheduler scheduler,
                                             LogDirFailureChannel logDirFailureChannel,
                                             String logPrefix) throws IOException {
        LocalLog.deleteSegmentFiles(segmentsToDelete, asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix);
    }

    /**
     * Rebuilds producer state until the provided lastOffset. This function may be called from the
     * recovery code path, and thus must be free of all side-effects, i.e. it must not update any
     * log-specific state.
     *
     * @param producerStateManager    The ProducerStateManager instance to be rebuilt.
     * @param segments                The segments of the log whose producer state is being rebuilt
     * @param logStartOffset          The log start offset
     * @param lastOffset              The last offset upto which the producer state needs to be rebuilt
     * @param recordVersion           The record version
     * @param time                    The time instance used for checking the clock
     * @param reloadFromCleanShutdown True if the producer state is being built after a clean shutdown,
     *                                false otherwise.
     * @param logPrefix               The logging prefix
     */
    protected static void rebuildProducerState(ProducerStateManager producerStateManager,
                                               LogSegments segments,
                                               Long logStartOffset,
                                               Long lastOffset,
                                               RecordVersion recordVersion,
                                               Time time,
                                               Boolean reloadFromCleanShutdown,
                                               String logPrefix) throws IOException {
        Set<Optional<Long>> offsetsToSnapshot = null;
        if (segments.nonEmpty()) {
            long lastSegmentBaseOffset = segments.lastSegment().get().getBaseOffset();
            Optional<Long> nextLatestSegmentBaseOffset = segments.lowerSegment(lastSegmentBaseOffset).map(LogSegment::getBaseOffset);
            offsetsToSnapshot = new HashSet<>(Arrays.asList(nextLatestSegmentBaseOffset, Optional.of(lastSegmentBaseOffset), Optional.of(lastOffset)));
        } else {
            offsetsToSnapshot = new HashSet<>(Arrays.asList(Optional.of(lastOffset)));
        }
        LOG.info("{}Loading producer state till offset {} with message format version {}", logPrefix, lastOffset, recordVersion.value);

        // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
        // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
        // but we have to be careful not to assume too much in the presence of broker failures. The two most common
        // upgrade cases in which we expect to find no snapshots are the following:
        //
        // 1. The broker has been upgraded, but the topic is still on the old message format.
        // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
        //
        // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
        // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
        // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
        // from the first segment.
        if (recordVersion.value < RecordBatch.MAGIC_VALUE_V2 ||
                (!producerStateManager.latestSnapshotOffset().isPresent() && reloadFromCleanShutdown)) {
            // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
            // last two segments and the last offset. This should avoid the full scan in the case that the log needs
            // truncation.
            for (Optional<Long> optional : offsetsToSnapshot) {
                if (optional.isPresent()) {
                    Long offset = optional.get();
                    producerStateManager.updateMapEndOffset(offset);
                    producerStateManager.takeSnapshot();
                }
            }
        } else {
            LOG.info("{}Reloading from producer snapshot and rebuilding producer state from offset {}", logPrefix, lastOffset);
            boolean isEmptyBeforeTruncation = producerStateManager.isEmpty() && producerStateManager.mapEndOffset() >= lastOffset;
            long producerStateLoadStart = time.milliseconds();
            producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds());
            long segmentRecoveryStart = time.milliseconds();

            // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
            // offset (which would be the case on first startup) and there were active producers prior to truncation
            // (which could be the case if truncating after initial loading). If there weren't, then truncating
            // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
            // and we can skip the loading. This is an optimization for users which are not yet using
            // idempotent/transactional features yet.
            if (lastOffset > producerStateManager.mapEndOffset() && !isEmptyBeforeTruncation) {
                Optional<LogSegment> segmentOfLastOffset = segments.floorSegment(lastOffset);

                for (LogSegment segment : segments.values(producerStateManager.mapEndOffset(), lastOffset)) {
                    long startOffset = Utils.max(segment.getBaseOffset(), producerStateManager.mapEndOffset(), logStartOffset);
                    producerStateManager.updateMapEndOffset(startOffset);

                    if (offsetsToSnapshot.contains(Optional.of(segment.getBaseOffset()))) {
                        producerStateManager.takeSnapshot();
                    }


                    Integer maxPosition = segmentOfLastOffset.isPresent() && segmentOfLastOffset.get() == segment ?
                            Optional.ofNullable(segment.translateOffset(lastOffset, 0))
                                    .map(logOffsetPosition -> logOffsetPosition.position)
                                    .orElse(segment.size())
                            : segment.size();

                    FetchDataInfo fetchDataInfo = segment.read(startOffset, Integer.MAX_VALUE, new Long(maxPosition), false);
                    if (fetchDataInfo != null) {
                        loadProducersFromRecords(producerStateManager, fetchDataInfo.getRecords());
                    }
                }
            }
            producerStateManager.updateMapEndOffset(lastOffset);
            producerStateManager.takeSnapshot();
            LOG.info("{}Producer state recovery took {}ms for snapshot load and {}ms for segment recovery from offset {}",
                    logPrefix, segmentRecoveryStart - producerStateLoadStart, time.milliseconds() - segmentRecoveryStart, lastOffset);
        }
    }

    protected static SplitSegmentResult splitOverflowedSegment(LogSegment segment,
                                                               LogSegments existingSegments,
                                                               File dir,
                                                               TopicPartition topicPartition,
                                                               LogConfig config,
                                                               Scheduler scheduler,
                                                               LogDirFailureChannel logDirFailureChannel,
                                                               String logPrefix) throws IOException {
        return LocalLog.splitOverflowedSegment(segment, existingSegments, dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix);
    }

    protected static void deleteProducerSnapshots(Iterable<LogSegment> segments,
                                                  ProducerStateManager producerStateManager,
                                                  Boolean asyncDelete,
                                                  Scheduler scheduler,
                                                  LogConfig config,
                                                  LogDirFailureChannel logDirFailureChannel,
                                                  String parentDir,
                                                  TopicPartition topicPartition) throws IOException {
        List<SnapshotFile> snapshotsToDelete = new ArrayList<>();
        for (LogSegment segment : segments) {
            Optional<SnapshotFile> snapshotFile = producerStateManager.removeAndMarkSnapshotForDeletion(segment.getBaseOffset());
            snapshotFile.ifPresent(snapshotsToDelete::add);
        }
        String msg = String.format("Error while deleting producer state snapshots for %s in dir %s", topicPartition, parentDir);
        if (asyncDelete) {
            scheduler.schedule("delete-producer-snapshot",
                    () -> deleteProducerSnapshots(logDirFailureChannel, parentDir, snapshotsToDelete, topicPartition),
                    config.getFileDeleteDelayMs(),
                    -1,
                    TimeUnit.MILLISECONDS);
        } else {
            deleteProducerSnapshots(logDirFailureChannel, parentDir, snapshotsToDelete, topicPartition);
        }
    }

    private static void deleteProducerSnapshots(LogDirFailureChannel logDirFailureChannel,
                                                String parentDir,
                                                List<SnapshotFile> snapshotsToDelete,
                                                TopicPartition topicPartition) {
        String msg = String.format("Error while deleting producer state snapshots for %s in dir %s", topicPartition, parentDir);
        LocalLog.maybeHandleIOException(logDirFailureChannel,
                parentDir,
                msg, (SupplierWithIOException<Void>) () -> {
                    for (SnapshotFile snapshot : snapshotsToDelete) {
                        snapshot.deleteIfExists();
                    }
                    return null;
                }
        );
    }

    protected static LogSegment createNewCleanedSegment(File dir, LogConfig logConfig, Long baseOffset) throws IOException {
        return LocalLog.createNewCleanedSegment(dir, logConfig, baseOffset);
    }


}

interface LogStartOffsetIncrementReason {
    ClientRecordDeletion clientRecordDeletion = new ClientRecordDeletion();
    LeaderOffsetIncremented leaderOffsetIncremented = new LeaderOffsetIncremented();
    SegmentDeletion segmentDeletion = new SegmentDeletion();
    SnapshotGenerated snapshotGenerated = new SnapshotGenerated();

    class ClientRecordDeletion implements LogStartOffsetIncrementReason {
        @Override
        public String toString() {
            return "client delete records request";
        }
    }

    class LeaderOffsetIncremented implements LogStartOffsetIncrementReason {
        @Override
        public String toString() {
            return "leader offset increment";
        }
    }

    class SegmentDeletion implements LogStartOffsetIncrementReason {
        @Override
        public String toString() {
            return "segment deletion";
        }
    }

    class SnapshotGenerated implements LogStartOffsetIncrementReason {
        @Override
        public String toString() {
            return "snapshot generated";
        }
    }
}


