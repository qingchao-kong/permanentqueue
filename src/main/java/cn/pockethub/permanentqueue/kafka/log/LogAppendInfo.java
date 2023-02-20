package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.message.CompressionCodec;
import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.record.RecordConversionStats;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.List;
import java.util.Optional;

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 */
@Getter
public class LogAppendInfo {
    @Setter
    private Optional<LogOffsetMetadata> firstOffset;
    @Setter
    private Long lastOffset;
    private Optional<Integer> lastLeaderEpoch;

    @Setter
    private Long maxTimestamp;

    @Setter
    private Long offsetOfMaxTimestamp;
    @Setter
    private Long logAppendTime;
    @Setter
    private Long logStartOffset;
    @Setter
    private RecordConversionStats recordConversionStats;
    private CompressionCodec sourceCodec;
    private CompressionCodec targetCodec;
    private Integer shallowCount;
    private Integer validBytes;
    private Boolean offsetsMonotonic;
    private Long lastOffsetOfFirstBatch;
    private List<ProduceResponse.RecordError> recordErrors;
    private String errorMessage;
    private LeaderHwChange leaderHwChange;


    /**
     * @param firstOffset      The first offset in the message set
     * @param lastOffset       The last offset in the message set
     * @param shallowCount     The number of shallow messages
     * @param validBytes       The number of valid bytes
     * @param codec            The codec used in the message set
     * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
     */
    public LogAppendInfo(final Optional<LogOffsetMetadata> firstOffset,
                         final Long lastOffset,
                         final Optional<Integer> lastLeaderEpoch,
                         final Long maxTimestamp,
                         final Long offsetOfMaxTimestamp,
                         final Long logAppendTime,
                         final Long logStartOffset,
                         final RecordConversionStats recordConversionStats,
                         final CompressionCodec sourceCodec,
                         final CompressionCodec targetCodec,
                         final Integer shallowCount,
                         final Integer validBytes,
                         final Boolean offsetsMonotonic,
                         final Long lastOffsetOfFirstBatch,
                         final List<ProduceResponse.RecordError> recordErrors,
                         final String errorMessage,
                         final LeaderHwChange leaderHwChange) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.lastLeaderEpoch = lastLeaderEpoch;
        this.maxTimestamp = maxTimestamp;
        this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
        this.logAppendTime = logAppendTime;
        this.logStartOffset = logStartOffset;
        this.recordConversionStats = recordConversionStats;
        this.sourceCodec = sourceCodec;
        this.targetCodec = targetCodec;
        this.shallowCount = shallowCount;
        this.validBytes = validBytes;
        this.offsetsMonotonic = offsetsMonotonic;
        this.lastOffsetOfFirstBatch = lastOffsetOfFirstBatch;
        this.recordErrors = recordErrors;
        this.errorMessage = errorMessage;
        this.leaderHwChange = leaderHwChange;
    }

    /**
     * Get the first offset if it exists, else get the last offset of the first batch
     * For magic versions 2 and newer, this method will return first offset. For magic versions
     * older than 2, we use the last offset of the first batch as an approximation of the first
     * offset to avoid decompressing the data.
     */
    public Long firstOrLastOffsetOfFirstBatch() {
        return firstOffset.map(LogOffsetMetadata::getMessageOffset).orElseGet(() -> lastOffsetOfFirstBatch);
    }

    /**
     * Get the (maximum) number of messages described by LogAppendInfo
     *
     * @return Maximum possible number of messages described by LogAppendInfo
     */
    public Long numMessages() {
        if (firstOffset.isPresent()
                && firstOffset.get().getMessageOffset() >= 0 && lastOffset >= 0) {
            return lastOffset - firstOffset.get().getMessageOffset() + 1;
        } else {
            return 0L;
        }
    }
}
