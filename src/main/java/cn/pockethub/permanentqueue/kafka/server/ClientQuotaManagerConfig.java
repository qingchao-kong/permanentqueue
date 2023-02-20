package cn.pockethub.permanentqueue.kafka.server;

public class ClientQuotaManagerConfig {

    // Always have 10 whole windows + 1 current window
    public static final int DefaultNumQuotaSamples = 11;
    public static final int DefaultQuotaWindowSizeSeconds = 1;
}
