package cn.pockethub.permanentqueue.kafka.log;

/**
 * The source of an append to the log. This is used when determining required validations.
 */
public enum AppendOrigin {

    /**
     * The log append came through replication from the leader. This typically implies minimal validation.
     * Particularly, we do not decompress record batches in order to validate records individually.
     */
    Replication,

    /**
     * The log append came from either the group coordinator or the transaction coordinator. We validate
     * producer epochs for normal log entries (specifically offset commits from the group coordinator) and
     * we validate coordinate end transaction markers from the transaction coordinator.
     */
    Coordinator,

    /**
     * The log append came from the client, which implies full validation.
     */
    Client,

    /**
     * The log append come from the raft leader, which implies the offsets has been assigned
     */
    RaftLeader,
    ;
}
