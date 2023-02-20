package cn.pockethub.permanentqueue.kafka.log;

import cn.pockethub.permanentqueue.kafka.utils.CollectionUtilExt;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

// the batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
// batch with the highest sequence is at the tail of the queue. We will retain at most ProducerStateEntry.NumBatchesToRetain
// elements in the queue. When the queue is at capacity, we remove the first element to make space for the incoming batch.
@Getter
public class ProducerStateEntry {

    private static final Integer NumBatchesToRetain = 5;

    private Long producerId;
    private Queue<BatchMetadata> batchMetadata;
    @Setter
    private Short producerEpoch;
    @Setter
    private Integer coordinatorEpoch;
    @Setter
    private Long lastTimestamp;
    @Setter
    private Optional<Long> currentTxnFirstOffset;

    public ProducerStateEntry(Long producerId,
                              Queue<BatchMetadata> batchMetadata,
                              Short producerEpoch,
                              Integer coordinatorEpoch,
                              Long lastTimestamp,
                              Optional<Long> currentTxnFirstOffset){
        this.producerId=producerId;
        this.batchMetadata=batchMetadata;
        this.producerEpoch=producerEpoch;
        this.coordinatorEpoch=coordinatorEpoch;
        this.lastTimestamp=lastTimestamp;
        this.currentTxnFirstOffset=currentTxnFirstOffset;
    }

    public Integer firstSeq(){
        if (isEmpty()) {
            return RecordBatch.NO_SEQUENCE;
        } else {
            return CollectionUtilExt.head(batchMetadata).firstSeq();
        }
    }

    public Long firstDataOffset(){
        if (isEmpty()) {
            return -1L;
        } else {
            return CollectionUtilExt.head(batchMetadata).firstOffset();
        }
    }

    public Integer lastSeq(){
        if (isEmpty()) {
            return RecordBatch.NO_SEQUENCE;
        } else {
            return CollectionUtilExt.last(batchMetadata).getLastSeq();
        }
    }

    public Long lastDataOffset(){
        if (isEmpty()) {
            return -1L;
        } else {
            return CollectionUtilExt.last(batchMetadata).getLastOffset();
        }
    }

    public Integer lastOffsetDelta(){
        if (isEmpty()) {
            return 0;
        } else {
            return CollectionUtilExt.last(batchMetadata).getOffsetDelta();
        }
    }

    public Boolean isEmpty(){
        return batchMetadata.isEmpty();
    }

    public void addBatch(Short producerEpoch,Integer lastSeq,Long lastOffset,Integer offsetDelta,Long timestamp) {
        maybeUpdateProducerEpoch(producerEpoch);
        addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    public Boolean maybeUpdateProducerEpoch(Short producerEpoch) {
        if (!this.producerEpoch.equals(producerEpoch)) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }

    private void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == ProducerStateEntry.NumBatchesToRetain) {
            batchMetadata.poll();
        }
        batchMetadata.add(batch);
    }

    public void update(ProducerStateEntry nextEntry) {
        maybeUpdateProducerEpoch(nextEntry.producerEpoch);
        while (CollectionUtils.isNotEmpty(nextEntry.batchMetadata)) {
            addBatchMetadata(nextEntry.batchMetadata.poll());
        }
        this.coordinatorEpoch = nextEntry.coordinatorEpoch;
        this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset;
        this.lastTimestamp = nextEntry.lastTimestamp;
    }

    public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        if (batch.producerEpoch() != producerEpoch) {
            return Optional.empty();
        }
        else {
            return batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
        }
    }

    // Return the batch metadata of the cached batch having the exact sequence range, if any.
    public Optional<BatchMetadata> batchWithSequenceRange(Integer firstSeq,Integer lastSeq) {
        List<BatchMetadata> duplicate = batchMetadata.stream()
                .filter(metadata -> firstSeq.equals(metadata.firstSeq()) && lastSeq.equals(metadata.getLastSeq()))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(duplicate)) {
            return Optional.of(duplicate.get(0));
        }else {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return String.format("ProducerStateEntry(producerId=%s, producerEpoch=%s, currentTxnFirstOffset=%s, " +
                        "coordinatorEpoch=%s, lastTimestamp=%s, batchMetadata=%s",
                producerId, producerEpoch, currentTxnFirstOffset, coordinatorEpoch, lastTimestamp, batchMetadata);
    }

    public static ProducerStateEntry empty(Long producerId) {
        return new ProducerStateEntry(producerId,
                new ArrayDeque<>(),
                RecordBatch.NO_PRODUCER_EPOCH,
                -1,
                RecordBatch.NO_TIMESTAMP,
                Optional.empty());
    }
}
