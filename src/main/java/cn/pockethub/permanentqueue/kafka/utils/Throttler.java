package cn.pockethub.permanentqueue.kafka.utils;

import cn.pockethub.permanentqueue.kafka.metrics.KafkaMetricsGroup;
import com.yammer.metrics.core.Meter;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 */
//@threadsafe
public class Throttler extends KafkaMetricsGroup {
    private static final Logger LOG = LoggerFactory.getLogger(Throttler.class);

    private double desiredRatePerSec;
    private final long checkIntervalMs;
    private final boolean throttleDown;
    private final String metricName;
    private final String units;
    private final Time time;

    private final Object lock = new Object();
    private final Meter meter;
    private final Long checkIntervalNs;
    private long periodStartNs;
    private double observedSoFar = 0.0;

    public Throttler(final double desiredRatePerSec) {
        this(desiredRatePerSec, 100L, true, "throttler", "entries", new SystemTime());
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs) {
        this(desiredRatePerSec, checkIntervalMs, true, "throttler", "entries", new SystemTime());
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown) {
        this(desiredRatePerSec, checkIntervalMs, throttleDown, "throttler", "entries", new SystemTime());
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown,
                     final Time time) {
        this(desiredRatePerSec, checkIntervalMs, throttleDown, "throttler", "entries", time);
    }

    /**
     * @param desiredRatePerSec: The rate we want to hit in units/sec
     * @param checkIntervalMs:   The interval at which to check our rate
     * @param throttleDown:      Does throttling increase or decrease our rate?
     * @param time               The time implementation to use
     */
    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown,
                     final String metricName,
                     final String units,
                     final Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalMs = checkIntervalMs;
        this.throttleDown = throttleDown;
        this.metricName = metricName;
        this.units = units;
        this.time = time;

        this.meter = newMeter(metricName, units, TimeUnit.SECONDS, new HashMap<>());
        this.checkIntervalNs = TimeUnit.MILLISECONDS.toNanos(checkIntervalMs);
        this.periodStartNs = time.nanoseconds();
    }

    public void maybeThrottle(Double observed) {
        long msPerSec = TimeUnit.SECONDS.toMillis(1);
        long nsPerSec = TimeUnit.SECONDS.toNanos(1);
        double currentDesiredRatePerSec = desiredRatePerSec;

        meter.mark(observed.longValue());
        synchronized (lock) {
            observedSoFar += observed;
            long now = time.nanoseconds();
            long elapsedNs = now - periodStartNs;
            // if we have completed an interval AND we have observed something, maybe
            // we should take a little nap
            if (elapsedNs > checkIntervalNs && observedSoFar > 0) {
                double rateInSecs = (observedSoFar * nsPerSec) / elapsedNs;
                boolean needAdjustment = !(throttleDown ^ (rateInSecs > currentDesiredRatePerSec));
                if (needAdjustment) {
                    // solve for the amount of time to sleep to make us hit the desired rate
                    double desiredRateMs = currentDesiredRatePerSec / (double) msPerSec;
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNs);
                    long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
                    if (sleepTime > 0) {
                        LOG.trace("Natural rate is {} per second but desired rate is {}, sleeping for {} ms to compensate.",
                                rateInSecs, currentDesiredRatePerSec, sleepTime);
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = time.nanoseconds();
                observedSoFar = 0;
            }
        }
    }

    public void updateDesiredRatePerSec(Double updatedDesiredRatePerSec) {
        desiredRatePerSec = updatedDesiredRatePerSec;
    }

    public static void main(final String[] args) throws InterruptedException {
        Random rand = new Random();
        Time time = new SystemTime();
        Throttler throttler = new Throttler(100000, 100, true, time);
        long interval = 30000;
        long start = time.milliseconds();
        long total = 0;
        while (true) {
            int value = rand.nextInt(1000);
            time.sleep(1);
            throttler.maybeThrottle((double) value);
            total += value;
            long now = time.milliseconds();
            if (now - start >= interval) {
                System.out.println(total / (interval / 1000.0));
                start = now;
                total = 0;
            }
        }
    }
}
