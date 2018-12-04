/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.ibm.streamsx.kafka.KafkaMetricException;

/**
 * @author The IBM Kafka toolkit maintainers 
 */
public interface MetricsProvider {
    /**
     * Gets the metrics.
     * @return the metrics.
     */
    public Map <MetricName,? extends Metric> getMetrics();
    /**
     * Creates a metric name for the custom metric of an operator from a Kafka metric name.
     * 
     * @param metricName
     * @return the name of the operator metric
     * @throws KafkaMetricException A name cannot be built.
     */
    public String createCustomMetricName (MetricName metricName) throws KafkaMetricException;
}
