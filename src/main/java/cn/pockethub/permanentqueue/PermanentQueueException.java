package cn.pockethub.permanentqueue;

public class PermanentQueueException extends Exception{

    public PermanentQueueException(String message) {
        super(message);
    }

    public PermanentQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
