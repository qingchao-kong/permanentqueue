package cn.pockethub.permanentqueue.kafka.common.function;

public interface BiFunctionWithThrowable<T, U, R> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @return the function result
     */
    R apply(T t, U u)throws Throwable;

}