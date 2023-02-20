package cn.pockethub.permanentqueue.kafka.log;

import java.util.Arrays;
import java.util.List;

public class LogMetricNames {
    public static final String NumLogSegments = "NumLogSegments";
    public static final String LogStartOffset = "LogStartOffset";
    public static final String LogEndOffset = "LogEndOffset";
    public static final String Size = "Size";

    public static List<String> allMetricNames() {
        return Arrays.asList(NumLogSegments, LogStartOffset, LogEndOffset, Size);
    }
}
