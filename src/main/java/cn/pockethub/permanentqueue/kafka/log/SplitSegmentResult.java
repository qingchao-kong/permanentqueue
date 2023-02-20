package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * Holds the result of splitting a segment into one or more segments, see LocalLog.splitOverflowedSegment().
 */
@Getter
public class SplitSegmentResult {

    private Iterable<LogSegment> deletedSegments;
    private Iterable<LogSegment> newSegments;

    /**
     * @param deletedSegments segments deleted when splitting a segment
     * @param newSegments     new segments created when splitting a segment
     */
    public SplitSegmentResult(Iterable<LogSegment> deletedSegments,
                              Iterable<LogSegment> newSegments) {
        this.deletedSegments = deletedSegments;
        this.newSegments = newSegments;
    }
}
