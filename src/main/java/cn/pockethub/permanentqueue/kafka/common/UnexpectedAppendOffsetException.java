package cn.pockethub.permanentqueue.kafka.common;

public class UnexpectedAppendOffsetException extends RuntimeException {

    private String message;
    private Long firstOffset;
    private Long lastOffset;

    public UnexpectedAppendOffsetException(String message,
                                           Long firstOffset,
                                           Long lastOffset) {
        super(message);

        this.message = message;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
    }
}
