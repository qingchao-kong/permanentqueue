package cn.pockethub.permanentqueue.kafka.server;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TopicConfigHandler {

    public static class ThrottledReplicaListValidator implements ConfigDef.Validator {
        public void ensureValidString(String name, String value) {
            ensureValid(name, Arrays.stream(value.split(",")).map(String::trim).collect(Collectors.toList()));
        }

        @Override
        public void ensureValid(String name, Object value) {
            Consumer<List<Object>> check = proposed -> {
                boolean allEleMath = true;
                boolean equalWildcard = false;
                if (CollectionUtils.isNotEmpty(proposed)) {
                    equalWildcard = proposed.stream()
                            .map(p -> p.toString().trim())
                            .collect(Collectors.joining()).trim().equals("*");
                    for (Object o : proposed) {
                        if (!o.toString().trim().matches("([0-9]+:[0-9]+)?")) {
                            allEleMath = false;
                        }
                    }
                }


                if (!(allEleMath || equalWildcard)) {
                    String msg = String.format("%s must be the literal '*' or a list of replicas in the following format: [partitionId]:[brokerId],[partitionId]:[brokerId],...",
                            name);
                    throw new ConfigException(name, value, msg);
                }
            };

            if (value instanceof List) {
                List javaList = (List) value;
                check.accept(javaList);
            } else {
                String msg = String.format("$name must be a List but was %s", value.getClass().getName());
                throw new ConfigException(name, value, msg);
            }
        }

        @Override
        public String toString() {
            return "[partitionId]:[brokerId],[partitionId]:[brokerId],...";
        }
    }
}
