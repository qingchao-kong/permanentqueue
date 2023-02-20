package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

import java.util.Optional;

/**
 * The last written record for a given producer. The last data offset may be undefined
 * if the only log entry for a producer is a transaction marker.
 */
@Getter
public class LastRecord {

    private Optional<Long> lastDataOffset;
    private Short producerEpoch;

    public LastRecord(Optional<Long> lastDataOffset,Short producerEpoch){
        this.lastDataOffset=lastDataOffset;
        this.producerEpoch=producerEpoch;
    }
}
