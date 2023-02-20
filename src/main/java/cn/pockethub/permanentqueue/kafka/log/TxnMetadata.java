package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.server.LogOffsetMetadata;
import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
public class TxnMetadata {

    private Long producerId;
    private LogOffsetMetadata firstOffset;
    @Setter
    private Optional<Long> lastOffset;

    public TxnMetadata(Long producerId,
                       Long firstoffset) {
        this(producerId, new LogOffsetMetadata(firstoffset), Optional.empty());
    }

    public TxnMetadata(Long producerId,
                       LogOffsetMetadata firstOffset) {
        this(producerId,firstOffset,Optional.empty());
    }

    public TxnMetadata(Long producerId,
                       LogOffsetMetadata firstOffset,
                       Optional<Long> lastOffset) {
        this.producerId = producerId;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }

    @Override
    public String toString(){
        return String.format("TxnMetadata(" +
                        "producerId=%s, " +
                        "firstOffset=%s, " +
                        "lastOffset=%s)",
                producerId, firstOffset, lastOffset);
    }
}
