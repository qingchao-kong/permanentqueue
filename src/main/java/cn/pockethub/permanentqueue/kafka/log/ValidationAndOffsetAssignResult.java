package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordConversionStats;

@Getter
public class ValidationAndOffsetAssignResult {

    private MemoryRecords validatedRecords;
    private Long maxTimestamp;
    private Long shallowOffsetOfMaxTimestamp;
    private Boolean messageSizeMaybeChanged;
    private RecordConversionStats recordConversionStats;

    public ValidationAndOffsetAssignResult(MemoryRecords validatedRecords,
                                           Long maxTimestamp,
                                           Long shallowOffsetOfMaxTimestamp,
                                           Boolean messageSizeMaybeChanged,
                                           RecordConversionStats recordConversionStats) {
        this.validatedRecords=validatedRecords;
        this.maxTimestamp=maxTimestamp;
        this.shallowOffsetOfMaxTimestamp=shallowOffsetOfMaxTimestamp;
        this.messageSizeMaybeChanged=messageSizeMaybeChanged;
        this.recordConversionStats=recordConversionStats;
    }
}
