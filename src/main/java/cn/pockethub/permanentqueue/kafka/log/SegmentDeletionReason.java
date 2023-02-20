package cn.pockethub.permanentqueue.kafka.log;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public interface SegmentDeletionReason {

    void logReason(List<LogSegment> toDelete)throws IOException;

    default String segmentsToString(List<LogSegment> toDelete) {
        return toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(","));
    }

    class LogTruncation implements SegmentDeletionReason {
        private LocalLog log;

        public LogTruncation(LocalLog log) {
            this.log = log;
        }

        @Override
        public void logReason(List<LogSegment> toDelete) {
            log.logger.info("Deleting segments as part of log truncation: {}", segmentsToString(toDelete));
        }
    }

    class LogRoll implements SegmentDeletionReason {
        private LocalLog log;

        public LogRoll(LocalLog log) {
            this.log = log;
        }

        @Override
        public void logReason(List<LogSegment> toDelete) {
            log.logger.info("Deleting segments as part of log roll: {}", segmentsToString(toDelete));
        }
    }

    class LogDeletion implements SegmentDeletionReason {
        private LocalLog log;

        public LogDeletion(LocalLog log) {
            this.log = log;
        }

        @Override
        public void logReason(List<LogSegment> toDelete) {
            log.logger.info("Deleting segments as the log has been deleted: {}", segmentsToString(toDelete));
        }
    }

    class RetentionMsBreach implements SegmentDeletionReason {

        private UnifiedLog log;

        public RetentionMsBreach(UnifiedLog log) {
            this.log = log;
        }

        @Override
        public void logReason(List<LogSegment> toDelete) throws IOException{
            long retentionMs = log.config().getRetentionMs();
            for (LogSegment segment : toDelete) {
                if (segment.largestRecordTimestamp().isPresent()) {
                    log.logger.info("Deleting segment {} due to retention time {}ms breach based on the largest " +
                            "record timestamp in the segment", segment, retentionMs);
                } else {
                    log.logger.info("Deleting segment {} due to retention time {}ms breach based on the " +
                            "last modified time of the segment", segment, retentionMs);
                }
            }
        }
    }

    class RetentionSizeBreach implements SegmentDeletionReason {

        private UnifiedLog log;

        public RetentionSizeBreach(UnifiedLog log) {
            this.log = log;
        }

        @Override
        public void logReason(List<LogSegment> toDelete) {
            long size = log.size();
            for (LogSegment segment : toDelete) {
                size -= segment.size();
                log.logger.info("Deleting segment {} due to retention size {} breach. Log size " +
                        "after deletion will be {}.", segment, log.config().getRetentionSize(), size);
            }
        }
    }

    class StartOffsetBreach implements SegmentDeletionReason {

        private UnifiedLog log;

        public StartOffsetBreach(UnifiedLog log) {
            this.log = log;
        }

        @Override
        public void logReason(List<LogSegment> toDelete) {
            log.logger.info("Deleting segments due to log start offset {} breach: {}",
                    log.getLogStartOffset(),toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(", ")));
        }
    }
}
