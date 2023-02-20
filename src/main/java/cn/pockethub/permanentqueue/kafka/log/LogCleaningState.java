package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;

public interface LogCleaningState {

    class LogCleaningInProgress implements LogCleaningState {

    }

    class LogCleaningAborted implements LogCleaningState {

    }

    @Getter
    class LogCleaningPaused implements LogCleaningState {
        private Integer pausedCount;

        public LogCleaningPaused(Integer pausedCount) {
            this.pausedCount = pausedCount;
        }
    }
}

