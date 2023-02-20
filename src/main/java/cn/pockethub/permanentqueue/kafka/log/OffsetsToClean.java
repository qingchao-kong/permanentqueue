package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * Helper class for the range of cleanable dirty offsets of a log and whether to update the checkpoint associated with
 * the log
 */
@Getter
public class OffsetsToClean {

    private Long firstDirtyOffset;
    private Long firstUncleanableDirtyOffset;
    private Boolean forceUpdateCheckpoint;

    /**
     * @param firstDirtyOffset            the lower (inclusive) offset to begin cleaning from
     * @param firstUncleanableDirtyOffset the upper(exclusive) offset to clean to
     * @param forceUpdateCheckpoint       whether to update the checkpoint associated with this log. if true, checkpoint should be
     *                                    reset to firstDirtyOffset
     */
    public OffsetsToClean(Long firstDirtyOffset,
                          Long firstUncleanableDirtyOffset,
                          Boolean forceUpdateCheckpoint) {
        this.firstDirtyOffset = firstDirtyOffset;
        this.firstUncleanableDirtyOffset = firstUncleanableDirtyOffset;
        this.forceUpdateCheckpoint = forceUpdateCheckpoint;
    }
}
