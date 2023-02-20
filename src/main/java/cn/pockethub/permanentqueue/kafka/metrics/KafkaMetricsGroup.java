package cn.pockethub.permanentqueue.kafka.metrics;

import cn.pockethub.permanentqueue.kafka.server.metrics.KafkaYammerMetrics;
import cn.pockethub.permanentqueue.kafka.utils.Logging;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.*;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.utils.Sanitizer;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class KafkaMetricsGroup extends Logging {

    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this
     * metrics group.
     *
     * @param name Descriptive name of the metric.
     * @param tags Additional attributes which mBean will have.
     * @return Sanitized metric name object.
     */
    public MetricName metricName(String name, Map<String, String> tags) {
        Class<? extends KafkaMetricsGroup> klass = this.getClass();
        String pkg = (klass.getPackage() == null) ? "" : klass.getPackage().getName();
        String simpleName = klass.getSimpleName().replaceAll("\\$$", "");

        return explicitMetricName(pkg, simpleName, name, tags);
    }

    public static MetricName metricNameStatic(String name, Map<String, String> tags, Class<? extends KafkaMetricsGroup> klass) {
        String pkg = (klass.getPackage() == null) ? "" : klass.getPackage().getName();
        String simpleName = klass.getSimpleName().replaceAll("\\$$", "");

        return explicitMetricNameStatic(pkg, simpleName, name, tags);
    }

    public MetricName explicitMetricName(String group, String typeName, String name, Map<String, String> tags) {
        StringBuilder nameBuilder = new StringBuilder();

        nameBuilder.append(group);

        nameBuilder.append(":type=");

        nameBuilder.append(typeName);

        if (StringUtils.isNoneBlank(name)) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        String scope = toScope(tags).orElse(null);
        Optional<String> tagsName = toMBeanName(tags);
        tagsName.ifPresent(k -> nameBuilder.append(",").append(k));

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public static MetricName explicitMetricNameStatic(String group, String typeName, String name, Map<String, String> tags) {
        StringBuilder nameBuilder = new StringBuilder();

        nameBuilder.append(group);

        nameBuilder.append(":type=");

        nameBuilder.append(typeName);

        if (StringUtils.isNoneBlank(name)) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        String scope = toScope(tags).orElse(null);
        Optional<String> tagsName = toMBeanName(tags);
        tagsName.ifPresent(k -> nameBuilder.append(",").append(k));

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public <T> Gauge<T> newGauge(String name, Gauge<T> metric, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric);
    }

    public Meter newMeter(String name, String eventType, TimeUnit timeUnit, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit);
    }

    public Meter newMeter(MetricName metricName, String eventType, TimeUnit timeUnit) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName, eventType, timeUnit);
    }

    public Histogram newHistogram(String name, Boolean biased, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased);
    }

    public Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit);
    }

    public static Timer newTimerStatic(String name, TimeUnit durationUnit, TimeUnit rateUnit, Map<String, String> tags, Class<? extends KafkaMetricsGroup> klass) {
        return KafkaYammerMetrics.defaultRegistry().newTimer(metricNameStatic(name, tags, klass), durationUnit, rateUnit);
    }

    public void removeMetric(String name, Map<String, String> tags) {
        KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags));
    }

    private static Optional<String> toMBeanName(Map<String, String> tags) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (!Objects.equals(entry.getValue(), "")) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        ImmutableMap<String, String> filteredTags = builder.build();
        if (MapUtils.isNotEmpty(filteredTags)) {
            String tagsString = filteredTags.entrySet().stream()
                    .map(entry -> String.format("%s=%s", entry.getKey(), Sanitizer.jmxSanitize(entry.getValue()))).collect(Collectors.joining(","));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<String> toScope(Map<String, String> tags) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (!Objects.equals(entry.getValue(), "")) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        ImmutableMap<String, String> filteredTags = builder.build();
        if (MapUtils.isNotEmpty(filteredTags)) {
            String tagsString = filteredTags.entrySet().stream()
                    .sorted((o1, o2) -> o2.getKey().compareTo(o1.getKey()))
                    .map(entry -> String.format("%s.%s", entry.getKey(), entry.getValue().replaceAll("\\.", "_")))
                    .collect(Collectors.joining("."));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }
}
