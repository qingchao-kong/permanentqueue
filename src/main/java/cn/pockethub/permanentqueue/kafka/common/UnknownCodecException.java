package cn.pockethub.permanentqueue.kafka.common;

public class UnknownCodecException extends RuntimeException{
    public UnknownCodecException(){
        this(null);
    }

    public UnknownCodecException(String message){
        super(message);
    }
}
