package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.log.UnifiedLog;
import lombok.Getter;
import org.apache.kafka.common.KafkaException;

import java.util.Comparator;

/*
 * A log offset structure, including:
 *  1. the message offset
 *  2. the base message offset of the located segment
 *  3. the physical position on the located segment
 */
@Getter
public class LogOffsetMetadata {
    public static final LogOffsetMetadata UnknownOffsetMetadata = new LogOffsetMetadata(-1, 0, 0);
    public static final Integer UnknownFilePosition = -1;

    private final long messageOffset;
    private final long segmentBaseOffset;
    private final int relativePositionInSegment;

    public LogOffsetMetadata(final long messageOffset) {
        this(messageOffset, UnifiedLog.UnknownOffset, LogOffsetMetadata.UnknownFilePosition);
    }

    public LogOffsetMetadata(final long messageOffset,
                             final long segmentBaseOffset,
                             final int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    // check if this offset is already on an older segment compared with the given offset
    public Boolean onOlderSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly()) {
            String msg = String.format("%s cannot compare its segment info with %s since it only has message offset info", this, that);
            throw new KafkaException(msg);
        }

        return this.segmentBaseOffset < that.segmentBaseOffset;
    }

    // check if this offset is on the same segment with the given offset
    public Boolean onSameSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly()) {
            String msg = String.format("%s cannot compare its segment info with %s since it only has message offset info", this, that);
            throw new KafkaException(msg);
        }

        return this.segmentBaseOffset == that.segmentBaseOffset;
    }

    // compute the number of messages between this offset to the given offset
    public Long offsetDiff(LogOffsetMetadata that) {
        return this.messageOffset - that.messageOffset;
    }

    // compute the number of bytes between this offset to the given offset
    // if they are on the same segment and this offset precedes the given offset
    public Integer positionDiff(LogOffsetMetadata that) {
        if (!onSameSegment(that)) {
            String msg = String.format("%s cannot compare its segment position with %s since they are not on the same segment", this, that);
            throw new KafkaException(msg);
        }
        if (messageOffsetOnly()) {
            String msg = String.format("%s cannot compare its segment position with %s since it only has message offset info", this, that);
            throw new KafkaException(msg);
        }

        return this.relativePositionInSegment - that.relativePositionInSegment;
    }

    // decide if the offset metadata only contains message offset info
    public Boolean messageOffsetOnly() {
        return segmentBaseOffset == UnifiedLog.UnknownOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition;
    }

    @Override
    public String toString() {
        return String.format("(offset=%s segment=[%s:%s])", messageOffset, segmentBaseOffset, relativePositionInSegment);
    }

    class OffsetOrdering implements Comparator<LogOffsetMetadata> {
        @Override
        public int compare(LogOffsetMetadata x, LogOffsetMetadata y) {
            return ((Long) x.offsetDiff(y)).intValue();
        }
    }

}
