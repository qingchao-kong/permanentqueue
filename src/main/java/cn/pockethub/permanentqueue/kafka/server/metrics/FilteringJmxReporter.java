package cn.pockethub.permanentqueue.kafka.server.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import java.util.function.Predicate;

public class FilteringJmxReporter extends JmxReporter {

    private volatile Predicate<MetricName> metricPredicate;

    public FilteringJmxReporter(MetricsRegistry registry, Predicate<MetricName> metricPredicate) {
        super(registry);
        this.metricPredicate = metricPredicate;
    }

    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        if (metricPredicate.test(name)) {
            super.onMetricAdded(name, metric);
        }
    }

    public void updatePredicate(Predicate<MetricName> predicate) {
        this.metricPredicate = predicate;
        // re-register metrics on update
        getMetricsRegistry()
                .allMetrics()
                .forEach((name, metric) -> {
                            if (metricPredicate.test(name)) {
                                super.onMetricAdded(name, metric);
                            } else {
                                super.onMetricRemoved(name);
                            }
                        }
                );
    }
}