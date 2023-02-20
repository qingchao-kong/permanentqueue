package cn.pockethub.permanentqueue.kafka.server;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ConfigType {
    public static final String Topic = "topics";
    public static final String Client = "clients";
    public static final String User = "users";
    public static final String Broker = "brokers";
    public static final String Ip = "ips";
    public static final Set<String> all = new HashSet<>(Arrays.asList(Topic, Client, User, Broker, Ip));
}
