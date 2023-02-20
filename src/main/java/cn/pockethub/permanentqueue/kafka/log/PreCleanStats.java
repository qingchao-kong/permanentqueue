package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

/**
 * A simple struct for collecting pre-clean stats
 */
@Getter
public class PreCleanStats {
    private long maxCompactionDelayMs = 0L;
    private int delayedPartitions = 0;
    private int cleanablePartitions = 0;

    public void updateMaxCompactionDelay(Long delayMs) {
        maxCompactionDelayMs = Math.max(maxCompactionDelayMs, delayMs);
        if (delayMs > 0) {
            delayedPartitions += 1;
        }
    }

    public void recordCleanablePartitions(Integer numOfCleanables) {
        cleanablePartitions = numOfCleanables;
    }
}
