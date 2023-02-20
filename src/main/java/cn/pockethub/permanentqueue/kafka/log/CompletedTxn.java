package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 *
 * @param producerId  The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset  The last offset (inclusive) of the transaction. This is always the offset of the
 *                    COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted   Whether or not the transaction was aborted
 */
@Getter
public class CompletedTxn {

    private Long producerId;
    private Long firstOffset;
    private Long lastOffset;
    private Boolean isAborted;

    public CompletedTxn(Long producerId,
                        Long firstOffset,
                        Long lastOffset,
                        Boolean isAborted) {
        this.producerId = producerId;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.isAborted = isAborted;
    }

    @Override
    public String toString(){
        return String.format("CompletedTxn(" +
                        "producerId=%s, " +
                        "firstOffset=%s, " +
                        "lastOffset=%s, " +
                        "isAborted=%s)",
                producerId, firstOffset, lastOffset, isAborted);
    }
}
