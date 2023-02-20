package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import org.apache.kafka.common.KafkaException;

@Getter
public class LogCleaningException extends KafkaException {
    private UnifiedLog log;

    public LogCleaningException(UnifiedLog log,
                             String message,
                             Throwable cause){
        super(message,cause);
        this.log=log;
    }
}
