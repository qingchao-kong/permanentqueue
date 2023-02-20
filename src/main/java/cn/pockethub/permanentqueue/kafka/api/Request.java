package cn.pockethub.permanentqueue.kafka.api;

public class Request {
    public static final int OrdinaryConsumerId = -1;
    public static final int DebuggingConsumerId = -2;
    public static final int FutureLocalReplicaId = -3;

    // Broker ids are non-negative int.
    public static Boolean isValidBrokerId(Integer brokerId){
        return brokerId >= 0;
    }

    public static Boolean isConsumer(Integer replicaId){
       return replicaId < 0 && !replicaId.equals(FutureLocalReplicaId);
    }

    public static String describeReplicaId(Integer replicaId){
        switch (replicaId){
            case OrdinaryConsumerId:
                return "consumer";
            case DebuggingConsumerId:
                return "debug consumer";
            case FutureLocalReplicaId:
                return "future local replica";
            default:
                if (isValidBrokerId(replicaId)) {
                    return String.format("replica [%s]", replicaId);
                }else {
                    return String.format("invalid replica [%s]", replicaId);
                }
        }
    }
}
