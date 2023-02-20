package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
public class AbortedTransactionMetadata {
    private final AbortedTxn abortedTxn;

    public AbortedTransactionMetadata(AbortedTxn abortedTxn) {
        this.abortedTxn = abortedTxn;
    }

    @Setter
    private Optional<Long> lastObservedBatchOffset = Optional.empty();

    @Override
    public String toString() {
        return String.format("(txn: %s, lastOffset: %s)", abortedTxn, lastObservedBatchOffset);
    }
}
