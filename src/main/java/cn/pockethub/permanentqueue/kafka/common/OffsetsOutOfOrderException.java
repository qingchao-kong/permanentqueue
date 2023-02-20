package cn.pockethub.permanentqueue.kafka.common;

public class OffsetsOutOfOrderException extends RuntimeException{
    public OffsetsOutOfOrderException(String message){
        super(message);
    }
}
