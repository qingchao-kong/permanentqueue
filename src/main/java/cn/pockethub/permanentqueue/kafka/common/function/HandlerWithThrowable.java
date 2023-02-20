package cn.pockethub.permanentqueue.kafka.common.function;

@FunctionalInterface
public interface HandlerWithThrowable {

    /**
     * handle
     */
    void handle()throws Throwable;
}
