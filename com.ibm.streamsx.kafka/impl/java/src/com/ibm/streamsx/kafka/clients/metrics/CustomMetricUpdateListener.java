/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

import org.apache.kafka.common.MetricName;

/**
 * This listener is called back, when the custom metric, 
 * for which the listener has been registered, has been updated.
 * The listener is also called when the metric value has been updated and not changed.
 * 
 * @see MetricsFetcher#registerUpdateListener(String, CustomMetricUpdateListener)
 *
 * @author The IBM Kafka toolkit team
 */
public interface CustomMetricUpdateListener {
    /**
     * Called when the metric has been updated.
     * 
     * @param customMetricName  the name of the custom metric in the operator context
     * @param kafkaMetricName   the metric name object in the Kafka client
     * @param value             the metric value that has been set in the operator metric
     */
    public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value);
}
