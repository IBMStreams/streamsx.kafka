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
                Thread.sleep (fetchInterval);
                fetchMetrics();
            }
        } catch (InterruptedException e) {
            // nothing. Terminate thread
        }
        trace.info("metrics fetcher ended");
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
            } catch (Exception e) {
                trace.warn ("Failed to update metric: " + e.getLocalizedMessage());
            }
        }
    }

    public void setProvider (MetricsProvider provider) {
        synchronized (lock) {
            this.provider = provider;
        }
    }
}