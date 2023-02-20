package cn.pockethub.permanentqueue.kafka.common.function;

import java.io.IOException;

public interface Iterator <E>{

    boolean hasNext()throws IOException;

    E next();
}
