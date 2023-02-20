package cn.pockethub.permanentqueue.kafka.cluster;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Part of the broker definition - matching host/port pair to a protocol
 */
public class EndPoint {

//    private static final Pattern uriParseExp =Pattern.compile( "^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)");

    public static Map<ListenerName, SecurityProtocol> DefaultSecurityProtocolMap=new HashMap<>();
    static {
        for (SecurityProtocol sp :SecurityProtocol.values()) {
            DefaultSecurityProtocolMap.put(ListenerName.forSecurityProtocol(sp),sp);
        }
    }

    private String host;
    private Integer port;
    private ListenerName listenerName;
    private SecurityProtocol securityProtocol;

    public EndPoint(String host,Integer port,ListenerName listenerName,SecurityProtocol securityProtocol){
        this.host=host;
        this.port=port;
        this.listenerName=listenerName;
        this.securityProtocol=securityProtocol;
    }

    public String connectionString() {
        String hostport =host == null?":" + port: Utils.formatAddress(host, port);
        return listenerName.value() + "://" + hostport;
    }

    public Endpoint toJava(){
        return new Endpoint(listenerName.value(), securityProtocol, host, port);
    }

//    /**
//     * Create EndPoint object from `connectionString` and optional `securityProtocolMap`. If the latter is not provided,
//     * we fallback to the default behaviour where listener names are the same as security protocols.
//     *
//     * @param connectionString the format is listener_name://host:port or listener_name://[ipv6 host]:port
//     *                         for example: PLAINTEXT://myhost:9092, CLIENT://myhost:9092 or REPLICATION://[::1]:9092
//     *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
//     *                         Negative ports are also accepted, since they are used in some unit tests
//     */
//    public static EndPoint createEndPoint(String connectionString, Optional<Map<ListenerName, SecurityProtocol>> securityProtocolMap) {
//        Map<ListenerName, SecurityProtocol> protocolMap = securityProtocolMap.orElse(DefaultSecurityProtocolMap);
//
//        connectionString match {
//            case uriParseExp(listenerNameString, "", port) =>
//                ListenerName listenerName = ListenerName.normalised(listenerNameString);
//                return new EndPoint(null, port.toInt, listenerName, securityProtocol(protocolMap, listenerName));
//            case uriParseExp(listenerNameString, host, port) =>
//                ListenerName listenerName = ListenerName.normalised(listenerNameString);
//                return new EndPoint(host, port.toInt, listenerName, securityProtocol(protocolMap, listenerName));
//            case _ =>throw new KafkaException(String.format("Unable to parse %s to a broker endpoint", connectionString));
//        }
//    }

    private static SecurityProtocol securityProtocol(Map<ListenerName, SecurityProtocol> protocolMap,ListenerName listenerName){
        SecurityProtocol securityProtocol = protocolMap.get(listenerName);
        if (Objects.isNull(securityProtocol)) {
            throw new IllegalArgumentException(String.format("No security protocol defined for listener %s",listenerName.value() ));
        }
        return securityProtocol;
    }

//    public static String parseListenerName(String connectionString) {
//        connectionString match {
//            case uriParseExp(listenerNameString, _, _) =>listenerNameString.toUpperCase(Locale.ROOT);
//            case _ =>throw new KafkaException(s"Unable to parse a listener name from $connectionString");
//        }
//    }

    public static EndPoint fromJava(Endpoint endpoint) {
        return new EndPoint(endpoint.host(),
                endpoint.port(),
                new ListenerName(endpoint.listenerName().get()),
                endpoint.securityProtocol());
    }
}
