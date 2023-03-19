package cn.pockethub.permanentqueue;

import lombok.Getter;

@Getter
public class QueueConfig {
    private int flushConsumerOffsetInterval = 1000 * 5;
    private long consumerOffsetUpdateVersionStep = 500;
    private boolean useServerSideResetOffset = false;
}
