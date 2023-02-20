package cn.pockethub.permanentqueue.kafka.metadata;


import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;

/**
 * The broker state.
 *
 * The numeric values used here are part of Kafka's public API.  They appear in metrics,
 * and are also sent over the wire in some cases.
 *
 * The expected state transitions are:
 *
 * NOT_RUNNING
 *     ↓
 * STARTING
 *     ↓
 * RECOVERY
 *     ↓
 * RUNNING
 *     ↓
 * PENDING_CONTROLLED_SHUTDOWN
 *     ↓
 * SHUTTING_DOWN
 */
@InterfaceStability.Evolving
public enum BrokerState {
    /**
     * The state the broker is in when it first starts up.
     */
    NOT_RUNNING((byte) 0),

    /**
     * The state the broker is in when it is catching up with cluster metadata.
     */
    STARTING((byte) 1),

    /**
     * The broker has caught up with cluster metadata, but has not yet
     * been unfenced by the controller.
     */
    RECOVERY((byte) 2),

    /**
     * The state the broker is in when it has registered at least once, and is
     * accepting client requests.
     */
    RUNNING((byte) 3),

    /**
     * The state the broker is in when it is attempting to perform a controlled
     * shutdown.
     */
    PENDING_CONTROLLED_SHUTDOWN((byte) 6),

    /**
     * The state the broker is in when it is shutting down.
     */
    SHUTTING_DOWN((byte) 7),

    /**
     * The broker is in an unknown state.
     */
    UNKNOWN((byte) 127);

    private final static Map<Byte, BrokerState> VALUES_TO_ENUMS = new HashMap<>();

    static {
        for (BrokerState state : BrokerState.values()) {
            VALUES_TO_ENUMS.put(state.value(), state);
        }
    }

    private final byte value;

    BrokerState(byte value) {
        this.value = value;
    }

    public static BrokerState fromValue(byte value) {
        BrokerState state = VALUES_TO_ENUMS.get(value);
        if (state == null) {
            return UNKNOWN;
        }
        return state;
    }

    public byte value() {
        return value;
    }
}
