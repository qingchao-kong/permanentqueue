package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;

/**
 * Helper class for a log, its topic/partition, the first cleanable position, the first uncleanable dirty position,
 * and whether it needs compaction immediately.
 */
@Getter
public class LogToClean implements Comparable<LogToClean> {
    private final TopicPartition topicPartition;
    private final UnifiedLog log;
    private final long firstDirtyOffset;
    private final Long uncleanableOffset;
    private final Boolean needCompactionNow;

    private long cleanBytes;
    private long firstUncleanableOffset;
    private long cleanableBytes;
    private long totalBytes;
    private double cleanableRatio;

    public LogToClean(final TopicPartition topicPartition,
                      final UnifiedLog log,
                      final long firstDirtyOffset,
                      Long uncleanableOffset,
                      Boolean needCompactionNow) {
        this.topicPartition = topicPartition;
        this.log = log;
        this.firstDirtyOffset = firstDirtyOffset;
        this.uncleanableOffset = uncleanableOffset;
        this.needCompactionNow = needCompactionNow;

        long sum = 0;
        for (LogSegment seg : log.logSegments(-1L, firstDirtyOffset)) {
            sum += seg.size();
        }
        cleanBytes = sum;

        Pair<Long, Long> pair = LogCleanerManager.calculateCleanableBytes(log, firstDirtyOffset, uncleanableOffset);
        this.firstUncleanableOffset = pair.getKey();
        this.cleanableBytes = pair.getValue();

        this.totalBytes = cleanBytes + cleanableBytes;

        this.cleanableRatio = cleanableBytes / (double) totalBytes;
    }

    @Override
    public int compareTo(LogToClean that) {
        return (int) Math.signum(this.cleanableRatio - that.cleanableRatio);
    }
}
