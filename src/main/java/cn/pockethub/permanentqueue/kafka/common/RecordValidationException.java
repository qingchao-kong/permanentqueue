package cn.pockethub.permanentqueue.kafka.common;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.List;

public class RecordValidationException extends RuntimeException {

    private List<ProduceResponse.RecordError> recordErrors;

    public RecordValidationException(ApiException invalidException,
                                     List<ProduceResponse.RecordError> recordErrors) {
        super(invalidException);
        this.recordErrors = recordErrors;
    }
}
