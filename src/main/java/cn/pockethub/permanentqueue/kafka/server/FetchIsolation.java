package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.api.Request;
import org.apache.kafka.common.IsolationLevel;

/**
 * 根据isolation，确定拉取消息偏移量的上限
 */
public enum FetchIsolation {

    /**
     * 上限为日志尾偏移量
     */
    FetchLogEnd,

    /**
     * 上限为最高水位(一般小于日志尾偏移量)，该水位前的数据已经复制到了给定数量多从副本
     */
    FetchHighWatermark,

    /**
     * 上限为最后一个事物提及的偏移量
     */
    FetchTxnCommitted,
    ;

//    public FetchIsolation apply(FetchRequest request) {
//        return apply(request.replicaId(), request.isolationLevel());
//    }

    public FetchIsolation apply(Integer replicaId, IsolationLevel isolationLevel) {
        if (!Request.isConsumer(replicaId)) {
            return FetchLogEnd;
        } else if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return FetchTxnCommitted;
        } else {
            return FetchHighWatermark;
        }
    }

}
