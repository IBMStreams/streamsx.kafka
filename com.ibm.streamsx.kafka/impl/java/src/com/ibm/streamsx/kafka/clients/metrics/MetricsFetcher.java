/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.kafka.KafkaMetricException;

/**
 * This class represents a metrics fetcher that periodically fetches Kafka metrics and updates the operator metrics.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class MetricsFetcher implements Runnable {
    private static final Logger trace = Logger.getLogger (MetricsFetcher.class);

    private final MetricFilter metricsFilter;
    private final long fetchInterval;
    private MetricsProvider provider;
    private final OperatorContext operatorContext;
    private final Thread runnerThread;
    private Object lock = new Object();
    private Map <MetricName, String> nameMap = new HashMap<>();
    private Map <String, CustomMetricUpdateListener> updatedListeners = new HashMap<>();
    private MetricsUpdatedListener updateFinishedListener = null;

    /**
     * @param operatorContext The operator context
     * @param provider        The instance that provides the metrics
     * @param metricsFilter   The filter to filter only the desired metrics
     * @param fetchInterval   The fetch and update interval in milliseconds
     */
    public MetricsFetcher (OperatorContext operatorContext, MetricsProvider provider, MetricFilter metricsFilter, long fetchInterval) {
        this.operatorContext = operatorContext;
        this.provider = provider;
        this.metricsFilter = metricsFilter;
        this.fetchInterval = fetchInterval;
        this.runnerThread = operatorContext.getThreadFactory().newThread (this);
        this.runnerThread.start();
    }


    /**
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            trace.info("starting metrics fetcher");
            while (!Thread.interrupted()) {
                fetchMetrics();
                Thread.sleep (fetchInterval);
            }
        } catch (InterruptedException e) {
            // nothing. Terminate thread
        }
        trace.info("metrics fetcher ended");
    }

    /**
     * Registers a callback for an operator custom metric, which is called back when the metric value has been updated.
     * When multiple listeners are registered the programmer must not do any assumptions about the sequence in
     * which the listeners are invoked.
     * 
     * @param metricName the operator custom metric name, for which the listener is registered
     * @param callback   the callback
     * @return the previously registered callback if there is one or null.
     * @see #registerUpdateListener(MetricsUpdatedListener)
     */
    public CustomMetricUpdateListener registerUpdateListener (final String metricName, CustomMetricUpdateListener callback) {
        return this.updatedListeners.put (metricName, callback);
    }

    /**
     * Registers a callback that is invoked after all operator custom metrics have 
     * been updated and its update listeners have been invoked.
     * 
     * @param callback   the callback
     * @return the previously registered callback if there is one or null.
     * @see #registerUpdateListener(String, CustomMetricUpdateListener)
     */
    public MetricsUpdatedListener registerUpdateListener (MetricsUpdatedListener callback) {
        MetricsUpdatedListener previous = this.updateFinishedListener;
        this.updateFinishedListener = callback;
        return previous;
    }

    /**
     * Fetches the metrics and updates the custom metrics.
     */
    private void fetchMetrics () {
        final Map<MetricName, ? extends Metric> metrics;
        synchronized (lock) {
            metrics = provider.getMetrics();
            if (trace.isTraceEnabled()) {
                trace.trace ("# Kafka producer metrics fetched: " + metrics.size());
            }
        }
        if (this.updateFinishedListener != null) {
            this.updateFinishedListener.beforeCustomMetricsUpdated();
        }
        for (Metric m: metrics.values()) {
            if (!metricsFilter.apply (m)) {
                continue;
            }
            try {
                final MetricName mName = m.metricName();
                String customMetricName = nameMap.get (mName);
                if (customMetricName == null) {
                    customMetricName = provider.createCustomMetricName (m.metricName());
                    nameMap.put (mName, customMetricName);
                }
                com.ibm.streams.operator.metrics.Metric customMetric = operatorContext.getMetrics().getCustomMetric (customMetricName);
                final long val = metricsFilter.getConverter (m).convert (m.metricValue());
                customMetric.setValue (val);
                if (trace.isTraceEnabled()) {
                    trace.trace ("custom metric updated: " + customMetricName + ", value = " + val);
                }
                CustomMetricUpdateListener callback = updatedListeners.get (customMetricName);
                if (callback != null) {
                    callback.customMetricUpdated (customMetricName, mName, val);
                }
            } catch (Exception e) {
                trace.warn ("Failed to update metric: " + e.getLocalizedMessage());
            }
        }
        if (this.updateFinishedListener != null) {
            this.updateFinishedListener.afterCustomMetricsUpdated();
        }
    }

    public void setProvider (MetricsProvider provider) {
        synchronized (lock) {
            this.provider = provider;
        }
    }

    /**
     * gets the current value of the metric given by the metric name
     * @param metricName the name of the metric for which the value is to be fetched
     * @return the metric value converted for a custom metric by a. The converter is either a specific converter or the default converter.
     * @throws KafkaMetricException the metric value cannot be fetched - No such metric, metric does not match the filter 
     */
    public long getCurrentValue (MetricName metricName) throws KafkaMetricException {
        final Map<MetricName, ? extends Metric> metrics;
        synchronized (lock) {
            metrics = provider.getMetrics();
            final Metric m = metrics.get (metricName);
            if (m == null) throw new KafkaMetricException ("No such metric: " + metricName);
            final long val = metricsFilter.getConverter (m).convert (m.metricValue());
            return val;
        }
    }
}
