package cn.pockethub.permanentqueue.kafka.log;

import java.nio.ByteBuffer;
import java.security.DigestException;

public interface OffsetMap {

    Integer slots();

    void put(ByteBuffer key, Long offset)throws DigestException;

    Long get(ByteBuffer key)throws DigestException;

    void updateLatestOffset(Long offset);

    void clear();

    Integer size();

    default Double utilization() {
        return size().doubleValue() / slots();
    }

    Long latestOffset();
}
