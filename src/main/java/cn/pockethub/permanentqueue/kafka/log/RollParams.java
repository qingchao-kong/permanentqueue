package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * A class used to hold params required to decide to rotate a log segment or not.
 */
@Getter
public class RollParams {

    private Long maxSegmentMs;
    private Integer maxSegmentBytes;
    private Long maxTimestampInMessages;
    private Long maxOffsetInMessages;
    private Integer messagesSize;
    private Long now;

    public RollParams(Long maxSegmentMs,
                      Integer maxSegmentBytes,
                      Long maxTimestampInMessages,
                      Long maxOffsetInMessages,
                      Integer messagesSize,
                      Long now) {
        this.maxSegmentMs = maxSegmentMs;
        this.maxSegmentBytes = maxSegmentBytes;
        this.maxTimestampInMessages = maxTimestampInMessages;
        this.maxOffsetInMessages = maxOffsetInMessages;
        this.messagesSize = messagesSize;
        this.now = now;
    }

    public static RollParams apply(LogConfig config,
                                   LogAppendInfo appendInfo,
                                   Integer messagesSize,
                                   Long now) {
        return new RollParams(config.maxSegmentMs(),
                config.getSegmentSize(),
                appendInfo.getMaxTimestamp(),
                appendInfo.getLastOffset(),
                messagesSize,
                now);
    }
}
