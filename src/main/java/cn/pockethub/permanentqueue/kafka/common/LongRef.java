package cn.pockethub.permanentqueue.kafka.common;

import lombok.Getter;

/**
 * A mutable cell that holds a value of type `Long`. One should generally prefer using value-based programming (i.e.
 * passing and returning `Long` values), but this class can be useful in some scenarios.
 *
 * Unlike `AtomicLong`, this class is not thread-safe and there are no atomicity guarantees.
 */
@Getter
public class LongRef {

    private Long value;

    public LongRef(Long value){
        this.value=value;
    }

    public Long addAndGet(Long delta) {
        value += delta;
        return value;
    }

    public Long getAndAdd(Long delta) {
        Long result = value;
        value += delta;
        return result;
    }

    public Long getAndIncrement() {
        Long v = value;
        value += 1;
        return v;
    }

    public Long incrementAndGet() {
        value += 1;
        return value;
    }

    public Long getAndDecrement() {
        Long v = value;
        value -= 1;
        return v;
    }

    public Long decrementAndGet() {
        value -= 1;
        return value;
    }
}
