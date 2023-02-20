package cn.pockethub.permanentqueue.kafka.coordinator.transaction;

import java.util.concurrent.TimeUnit;

public class TransactionStateManager {
    // default transaction management config values
    public static final int DefaultTransactionsMaxTimeoutMs = ((Long) TimeUnit.MINUTES.toMillis(15)).intValue();
    public static final int DefaultTransactionalIdExpirationMs = ((Long) TimeUnit.DAYS.toMillis(7)).intValue();
    public static final int DefaultAbortTimedOutTransactionsIntervalMs = ((Long) TimeUnit.SECONDS.toMillis(10)).intValue();
    public static final int DefaultRemoveExpiredTransactionalIdsIntervalMs = ((Long) TimeUnit.HOURS.toMillis(1)).intValue();

    public static final String MetricsGroup = "transaction-coordinator-metrics";
    public static final String LoadTimeSensor = "TransactionsPartitionLoadTime";
}
