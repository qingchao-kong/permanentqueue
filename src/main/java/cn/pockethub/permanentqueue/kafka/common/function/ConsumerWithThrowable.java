package cn.pockethub.permanentqueue.kafka.common.function;

@FunctionalInterface
public interface ConsumerWithThrowable<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     */
    void accept(T t)throws Throwable;
}