package cn.pockethub.permanentqueue.kafka.common.function;

import java.io.IOException;

@FunctionalInterface
public interface HandlerWithIOException {

    /**
     * handle
     */
    void handle()throws IOException;
}
