package cn.pockethub.permanentqueue.kafka.common;

public class InconsistentBrokerMetadataException extends RuntimeException {
    public InconsistentBrokerMetadataException(String message) {
        super(message, null);
    }

    public InconsistentBrokerMetadataException(Throwable cause) {
        super(null, cause);
    }

    public InconsistentBrokerMetadataException() {
        super(null, null);
    }
}
