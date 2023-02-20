package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

import java.util.List;

@Getter
public class TxnIndexSearchResult {

    private List<AbortedTxn> abortedTransactions;
    private Boolean isComplete;

    public TxnIndexSearchResult(List<AbortedTxn> abortedTransactions,Boolean isComplete){
        this.abortedTransactions=abortedTransactions;
        this.isComplete=isComplete;
    }
}
