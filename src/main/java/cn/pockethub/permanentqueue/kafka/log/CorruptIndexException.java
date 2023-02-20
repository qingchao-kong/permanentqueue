package cn.pockethub.permanentqueue.kafka.log;

public class CorruptIndexException extends RuntimeException{
    public CorruptIndexException(String message){
        super(message);
    }
}
