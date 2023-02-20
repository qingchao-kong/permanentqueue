package cn.pockethub.permanentqueue.kafka.server;

import lombok.Getter;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.util.List;
import java.util.Optional;

@Getter
public class FetchDataInfo {
    private final LogOffsetMetadata fetchOffsetMetadata;
    private final Records records;
    private final Boolean firstEntryIncomplete;
    private final Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions;

    public FetchDataInfo(final LogOffsetMetadata fetchOffsetMetadata,
                         final Records records) {
        this(fetchOffsetMetadata, records, false, Optional.empty());
    }

    public FetchDataInfo(final LogOffsetMetadata fetchOffsetMetadata,
                         final Records records,
                         Boolean firstEntryIncomplete,
                         Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions) {
        this.fetchOffsetMetadata = fetchOffsetMetadata;
        this.records = records;
        this.firstEntryIncomplete = firstEntryIncomplete;
        this.abortedTransactions = abortedTransactions;
    }

    public static FetchDataInfo empty(Long fetchOffset) {
        return new FetchDataInfo(
                new LogOffsetMetadata(fetchOffset),
                MemoryRecords.EMPTY,
                false,
                Optional.empty()
        );
    }
}
