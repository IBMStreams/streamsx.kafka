/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

/**
 * This listener is called back, after all custom metrics have been updated.
 * 
 * @author The IBM Kafka toolkit team
 * 
 * @see MetricsFetcher#registerUpdateListener(MetricsUpdatedListener)
 */
public interface MetricsUpdatedListener {
    /**
     * called back after all registered instances of {@link CustomMetricUpdateListener} have been called.
     */
    public void afterCustomMetricsUpdated();
    /**
     * called back before all registered instances of {@link CustomMetricUpdateListener} have been called.
     */
    public void beforeCustomMetricsUpdated();
}
