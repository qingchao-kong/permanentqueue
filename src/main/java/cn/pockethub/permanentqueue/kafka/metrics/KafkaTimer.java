package cn.pockethub.permanentqueue.kafka.metrics;

import cn.pockethub.permanentqueue.kafka.common.function.SupplierWithIOException;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.io.IOException;

/**
 * A wrapper around metrics timer object that provides a convenient mechanism
 * to time code blocks. This pattern was borrowed from the metrics-scala_2.9.1
 * package.
 */
public class KafkaTimer {

    private Timer metric;

    /**
     *
     * @param metric The underlying timer object.
     */
    public KafkaTimer(Timer metric){
        this.metric=metric;
    }

    public <A> A time(SupplierWithIOException<A> f) throws IOException {
        TimerContext ctx = metric.time();
        try {
            return f.get();
        } finally {
            ctx.stop();
        }
    }
}