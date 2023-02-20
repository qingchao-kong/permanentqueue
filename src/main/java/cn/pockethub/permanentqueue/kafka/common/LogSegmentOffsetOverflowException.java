package cn.pockethub.permanentqueue.kafka.common;

import cn.pockethub.permanentqueue.kafka.log.LogSegment;
import lombok.Getter;
import org.apache.kafka.common.KafkaException;

/**
 * Indicates that the log segment contains one or more messages that overflow the offset (and / or time) index. This is
 * not a typical scenario, and could only happen when brokers have log segments that were created before the patch for
 * KAFKA-5413. With KAFKA-6264, we have the ability to split such log segments into multiple log segments such that we
 * do not have any segments with offset overflow.
 */
@Getter
public class LogSegmentOffsetOverflowException extends KafkaException {

    private LogSegment segment;
    private Long offset;

    public LogSegmentOffsetOverflowException(LogSegment segment, Long offset) {
        super(String.format("Detected offset overflow at offset %s in segment %s", offset, segment));
        this.segment = segment;
        this.offset = offset;
    }
}
