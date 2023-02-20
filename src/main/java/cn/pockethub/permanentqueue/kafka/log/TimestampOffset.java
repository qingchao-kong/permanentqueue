package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import org.apache.kafka.common.requests.ListOffsetsResponse;

/**
 * The mapping between a timestamp to a message offset. The entry means that any message whose timestamp is greater
 * than that timestamp must be at or after that offset.
 */
@Getter
public class TimestampOffset implements IndexEntry {
    public static TimestampOffset Unknown = new TimestampOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, ListOffsetsResponse.UNKNOWN_OFFSET);

    private Long timestamp;
    private Long offset;

    /**
     * @param timestamp The max timestamp before the given offset.
     * @param offset    The message offset.
     */
    public TimestampOffset(Long timestamp, Long offset) {
        this.timestamp = timestamp;
        this.offset = offset;
    }

    @Override
    public Long indexKey() {
        return timestamp;
    }

    @Override
    public Long indexValue() {
        return offset;
    }
}
