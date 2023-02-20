package cn.pockethub.permanentqueue.kafka.common.function;

import java.io.IOException;

public interface BiFunctionWithIOException<T, U, R> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @return the function result
     */
    R apply(T t, U u)throws IOException;

}