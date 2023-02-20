package cn.pockethub.permanentqueue.kafka.utils;

import lombok.Getter;

/**
 * A class used for unit testing things which depend on the Time interface.
 * There a couple of difference between this class and `org.apache.kafka.common.utils.MockTime`:
 * <p>
 * 1. This has an associated scheduler instance for managing background tasks in a deterministic way.
 * 2. This doesn't support the `auto-tick` functionality as it interacts badly with the current implementation of `MockScheduler`.
 */
@Getter
public class MockTime extends cn.pockethub.permanentqueue.kafka.common.utils.MockTime {

    public MockTime() {
        this(System.currentTimeMillis(), System.nanoTime());
    }

    public MockTime(Long currentTimeMs, Long currentHiResTimeNs) {
        super(0, currentTimeMs, currentHiResTimeNs);
    }


    private final MockScheduler scheduler = new MockScheduler(this);

    @Override
    public void sleep(long ms) {
        super.sleep(ms);
        scheduler.tick();
    }

}
