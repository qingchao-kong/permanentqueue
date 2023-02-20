package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * The mapping between a logical log offset and the physical position
 * in some log file of the beginning of the message set entry with the
 * given offset.
 */
@Getter
public class OffsetPosition implements IndexEntry{

    private Long offset;
    private Integer position;

    public OffsetPosition(Long offset,Integer position){
        this.offset=offset;
        this.position=position;
    }

    @Override
    public Long indexKey() {
        return offset;
    }

    @Override
    public Long indexValue() {
        return position.longValue();
    }
}
