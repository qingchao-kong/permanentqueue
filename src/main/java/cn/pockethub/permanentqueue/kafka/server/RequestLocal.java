package cn.pockethub.permanentqueue.kafka.server;

import lombok.Getter;
import org.apache.kafka.common.utils.BufferSupplier;

/**
 * Container for stateful instances where the lifecycle is scoped to one request.
 *
 * When each request is handled by one thread, efficient data structures with no locking or atomic operations
 * can be used (see RequestLocal.withThreadConfinedCaching).
 */
@Getter
public class RequestLocal {

    public static final RequestLocal NoCaching =new  RequestLocal(BufferSupplier.NO_CACHING);

    private BufferSupplier bufferSupplier;

    public RequestLocal(BufferSupplier bufferSupplier){
        this.bufferSupplier=bufferSupplier;
    }

    /** The returned instance should be confined to a single thread. */
    public static RequestLocal withThreadConfinedCaching(){
        return new RequestLocal(BufferSupplier.create());
    }
}
