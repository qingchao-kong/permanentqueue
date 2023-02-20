package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.io.IOException;
import java.util.*;

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It maintains a set
 * of the ongoing aborted and committed transactions as the cleaner is working its way through the log. This
 * class is responsible for deciding when transaction markers can be removed and is therefore also responsible
 * for updating the cleaned transaction index accordingly.
 */
@Getter
public class CleanedTransactionMetadata {
    private Set<Long> ongoingCommittedTxns = new HashSet<>();
    private Map<Long, AbortedTransactionMetadata> ongoingAbortedTxns = new HashMap<>();
    // Minheap of aborted transactions sorted by the transaction first offset
    private PriorityQueue<AbortedTxn> abortedTransactions = new PriorityQueue<>(Comparator.comparing(AbortedTxn::firstOffset).reversed());

    // Output cleaned index to write retained aborted transactions
    @Setter
    private Optional<TransactionIndex> cleanedIndex = Optional.empty();

    public void addAbortedTransactions(List<AbortedTxn> abortedTransactions) {
        this.abortedTransactions.addAll(abortedTransactions);
    }

    /**
     * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
     * Return true if the control batch can be discarded.
     */
    public Boolean onControlBatchRead(RecordBatch controlBatch) throws IOException {
        consumeAbortedTxnsUpTo(controlBatch.lastOffset());

        Iterator<Record> controlRecordIterator = controlBatch.iterator();
        if (controlRecordIterator.hasNext()) {
            Record controlRecord = controlRecordIterator.next();
            ControlRecordType controlType = ControlRecordType.parse(controlRecord.key());
            long producerId = controlBatch.producerId();
            switch (controlType) {
                case ABORT:
                    AbortedTransactionMetadata abortedTxnMetadata = ongoingAbortedTxns.remove(producerId);
                    if (Objects.nonNull(abortedTxnMetadata) && abortedTxnMetadata.getLastObservedBatchOffset().isPresent()) {
                        if (cleanedIndex.isPresent()) {
                            cleanedIndex.get().append(abortedTxnMetadata.getAbortedTxn());
                        }
                        return false;
                    } else {
                        return true;
                    }
                case COMMIT:
                    // This marker is eligible for deletion if we didn't traverse any batches from the transaction
                    return !ongoingCommittedTxns.remove(producerId);
                default:
                    return false;
            }
        } else {
            // An empty control batch was already cleaned, so it's safe to discard
            return true;
        }
    }

    private void consumeAbortedTxnsUpTo(Long offset) {
        AbortedTxn peek = abortedTransactions.peek();
        while (Objects.nonNull(peek) && peek.firstOffset() <= offset) {
            AbortedTxn abortedTxn = abortedTransactions.poll();
            ongoingAbortedTxns.computeIfAbsent(abortedTxn.producerId(), k -> new AbortedTransactionMetadata(abortedTxn));
            peek = abortedTransactions.peek();
        }
    }

    /**
     * Update the transactional state for the incoming non-control batch. If the batch is part of
     * an aborted transaction, return true to indicate that it is safe to discard.
     */
    public Boolean onBatchRead(RecordBatch batch) {
        consumeAbortedTxnsUpTo(batch.lastOffset());
        if (batch.isTransactional()) {
            AbortedTransactionMetadata abortedTransactionMetadata = ongoingAbortedTxns.get(batch.producerId());
            if (Objects.nonNull(abortedTransactionMetadata)) {
                abortedTransactionMetadata.setLastObservedBatchOffset(Optional.of(batch.lastOffset()));
                return true;
            } else {
                ongoingCommittedTxns.add(batch.producerId());
                return false;
            }
        } else {
            return false;
        }
    }

}
