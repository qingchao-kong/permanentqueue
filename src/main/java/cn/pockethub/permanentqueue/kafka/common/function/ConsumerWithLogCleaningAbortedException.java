package cn.pockethub.permanentqueue.kafka.common.function;


import cn.pockethub.permanentqueue.kafka.common.LogCleaningAbortedException;

@FunctionalInterface
public interface ConsumerWithLogCleaningAbortedException<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     */
    void accept(T t)throws LogCleaningAbortedException;
}