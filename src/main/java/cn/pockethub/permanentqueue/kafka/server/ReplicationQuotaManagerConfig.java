package cn.pockethub.permanentqueue.kafka.server;

public class ReplicationQuotaManagerConfig {

    public static final long QuotaBytesPerSecondDefault = Long.MAX_VALUE;
    // Always have 10 whole windows + 1 current window
    public static final int DefaultNumQuotaSamples = 11;
    public static final int DefaultQuotaWindowSizeSeconds = 1;
    // Purge sensors after 1 hour of inactivity
    public static final int InactiveSensorExpirationTimeSeconds = 3600;
}
