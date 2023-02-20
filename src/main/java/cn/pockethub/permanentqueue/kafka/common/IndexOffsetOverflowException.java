package cn.pockethub.permanentqueue.kafka.common;

import org.apache.kafka.common.KafkaException;

public class IndexOffsetOverflowException extends KafkaException {

    public IndexOffsetOverflowException(String message,Throwable cause){
        super(message, cause);
    }

    public IndexOffsetOverflowException(String message){
        this(message,null);
    }
}
