package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.utils.Time;

/**
 * A simple struct for collecting stats about log cleaning
 */
@Getter
public class CleanerStats {
    private Time time;

    private Long startTime;
    private Long mapCompleteTime = -1L;
    private Long endTime = -1L;
    private Long bytesRead = 0L;
    private Long bytesWritten = 0L;
    private Long mapBytesRead = 0L;
    private Long mapMessagesRead = 0L;
    private Long messagesRead = 0L;
    private Long invalidMessagesRead = 0L;
    private Long messagesWritten = 0L;
    @Setter
    private Double bufferUtilization = 0.0d;

    public CleanerStats() {
        this(Time.SYSTEM);
    }

    public CleanerStats(Time time) {
        this.time = time;
        this.startTime = time.milliseconds();
    }

    public void readMessages(Integer messagesRead, Integer bytesRead) {
        this.messagesRead += messagesRead;
        this.bytesRead += bytesRead;
    }

    public void invalidMessage() {
        invalidMessagesRead += 1;
    }

    public void recopyMessages(Integer messagesWritten, Integer bytesWritten) {
        this.messagesWritten += messagesWritten;
        this.bytesWritten += bytesWritten;
    }

    public void indexMessagesRead(Integer size) {
        mapMessagesRead += size;
    }

    public void indexBytesRead(Integer size) {
        mapBytesRead += size;
    }

    public void indexDone() {
        mapCompleteTime = time.milliseconds();
    }

    public void allDone() {
        endTime = time.milliseconds();
    }

    public Double elapsedSecs() {
        return (endTime - startTime) / 1000.0;
    }

    public Double elapsedIndexSecs() {
        return (mapCompleteTime - startTime) / 1000.0;
    }
}
