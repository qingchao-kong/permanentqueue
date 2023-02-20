package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import lombok.Getter;

/**
 * Container class which represents a snapshot of the significant offsets for a partition. This allows fetching
 * of these offsets atomically without the possibility of a leader change affecting their consistency relative
 * to each other. See [[UnifiedLog.fetchOffsetSnapshot()]].
 */
@Getter
public class LogOffsetSnapshot {

    private Long logStartOffset;
    private LogOffsetMetadata logEndOffset;
    private LogOffsetMetadata highWatermark;
    private LogOffsetMetadata lastStableOffset;

    public LogOffsetSnapshot(Long logStartOffset,
                             LogOffsetMetadata logEndOffset,
                             LogOffsetMetadata highWatermark,
                             LogOffsetMetadata lastStableOffset) {
        this.logStartOffset=logStartOffset;
        this.logEndOffset=logEndOffset;
        this.highWatermark=highWatermark;
        this.lastStableOffset=lastStableOffset;
    }
}
