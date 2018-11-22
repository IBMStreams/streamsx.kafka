/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.metrics.OperatorMetrics;
//import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streamsx.kafka.KafkaMetricException;

/**
 * This class exposes Kafka metrics as Operator Custom metrics.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public abstract class AbstractCustomMetricReporter implements MetricsReporter {

    private static final Logger trace = Logger.getLogger (AbstractCustomMetricReporter.class);
    private OperatorContext operatorCtx = PERuntime.getCurrentContext();

    public AbstractCustomMetricReporter() {
    }

    /**
     * Gets the filter for this metric reporter.
     * The filters decides, which Kafka metrics get exposed as operator metrics, and which are suppressed.
     * 
     * @return The MetricFilter of this reporter
     */
    public abstract MetricFilter getFilter();

    
    @Override
    public void configure (Map<String, ?> configs) {
    }

    /**
     * This is called when the reporter is first registered to initially register all existing metrics
     * @param metrics All currently existing metrics
     */
    @Override
    public void init (List<KafkaMetric> metrics) {
        for (KafkaMetric m: metrics) {
            if (getFilter().apply (m)) {
                tryCreateCustomMetric (m);
            }
            else {
                trace.debug ("Kafka metric NOT exposed as operator metric: " + m.metricName());
            }
        }
    }

    /**
     * This is called whenever a metric is updated or added
     * @param metric
     */
    @Override
    public void metricChange (KafkaMetric metric) {
        synchronized (this) {
            if (getFilter().apply (metric)) {
                tryCreateCustomMetric (metric);
            }
            else {
                trace.debug ("Kafka metric NOT exposed as custom metric: " + metric.metricName());
            }
        }
    }

    public abstract String createCustomMetricName (MetricName metricName) throws KafkaMetricException;

    /**
     * creates a custom metric if it is not yet present.
     * The custom metric name is created using {@link #createCustomMetricName(MetricName)}.
     * @param metricName The name of the Kafka metric
     */
    private void tryCreateCustomMetric (final Metric metric) {
        final MetricName metricName = metric.metricName();
        try {
            final String customMetricName = createCustomMetricName (metricName);
            OperatorMetrics operatorMetrics = operatorCtx.getMetrics();
            if (operatorMetrics.getCustomMetrics().containsKey (customMetricName)) {
                trace.info ("Custom metric already created: " + customMetricName);
                operatorMetrics.getCustomMetric(customMetricName).setValue(getFilter().getConverter(metric).convert(metric.metricValue()));
                return;
            }
            operatorMetrics.createCustomMetric (customMetricName, metricName.description(), com.ibm.streams.operator.metrics.Metric.Kind.GAUGE);
            operatorMetrics.getCustomMetric(customMetricName).setValue(getFilter().getConverter(metric).convert(metric.metricValue()));
            trace.info ("custom metric created: " + customMetricName + " (" + metricName.description() + ")");
        } catch (KafkaMetricException e) {
            trace.warn ("Cannot create custom metric from Kafka producer metric '" + metricName + "': " + e.getLocalizedMessage());
        }
    }


    /**
     * This is called whenever a metric is removed
     * @param metric
     */
    @Override
    public void metricRemoval (KafkaMetric metric) {
        trace.debug ("metricRemoval(): " + metric.metricName());
        // No way to remove custom metrics from the operator context
    }

    /**
     * Called when the metrics repository is closed.
     */
    @Override
    public void close() {
        trace.debug ("close()");
    }
}
