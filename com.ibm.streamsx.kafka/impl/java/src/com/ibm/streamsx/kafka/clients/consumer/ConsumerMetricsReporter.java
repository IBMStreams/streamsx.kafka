/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Map;

import org.apache.kafka.common.MetricName;

import com.ibm.streamsx.kafka.KafkaMetricException;
import com.ibm.streamsx.kafka.clients.metrics.AbstractCustomMetricReporter;
import com.ibm.streamsx.kafka.clients.metrics.MetricFilter;
import com.ibm.streamsx.kafka.clients.metrics.RoundingConverter;

/**
 * This class represents a metrics reporter for consumer metrics.
 * It can be configured as consumer property 'metric.reporters'.
 *
 * @author The IBM Kafka toolkit Maintainers
 */
public class ConsumerMetricsReporter extends AbstractCustomMetricReporter {

    private final static String CONSUMER_METRICS_GROUP = "consumer-metrics";
    //    private final static String CONSUMER_NODE_METRICS_GROUP = "consumer-node-metrics";
    private final static String CONSUMER_FETCH_MANAGER_METRICS_GROUP = "consumer-fetch-manager-metrics";
    private final static String CONSUMER_COORDINATOR_METRICS_GROUP = "consumer-coordinator-metrics";
    private static MetricFilter filter = new DynamicMetricNameFilter();

    public static MetricFilter getMetricsFilter() {
        return filter;
    }
    /*
Consumer metric groups:

group=consumer-metrics,
group=consumer-coordinator-metrics,
group=consumer-fetch-manager-metrics,
group=consumer-node-metrics,
group=app-info,
group=kafka-metrics-count,

     */
    static {
        filter
        .add (CONSUMER_METRICS_GROUP, "connection-count", new RoundingConverter("connection-count"))
        .add (CONSUMER_METRICS_GROUP, "incoming-byte-rate", new RoundingConverter("incoming-byte-rate"))
        //        .add (CONSUMER_COORDINATOR_METRICS_GROUP, "join-time-avg", new RoundingConverter("join-time-avg"))
        //        .add (CONSUMER_COORDINATOR_METRICS_GROUP, "join-time-max", new RoundingConverter("join-time-max"))
        .add (CONSUMER_COORDINATOR_METRICS_GROUP, "commit-latency-avg", new RoundingConverter("commit-latency-avg"))
        .add (CONSUMER_COORDINATOR_METRICS_GROUP, "commit-rate", new RoundingConverter("commit-rate"))
        //        .add (CONSUMER_COORDINATOR_METRICS_GROUP, "assigned-partitions", new RoundingConverter("assigned-partitions"))
        .add (CONSUMER_FETCH_MANAGER_METRICS_GROUP, "fetch-size-avg", new RoundingConverter("fetch-size-avg"))
        .add (CONSUMER_FETCH_MANAGER_METRICS_GROUP, "records-lag-max", new RoundingConverter("records-lag-max"))
        // placeholder that we have this metric group in the filter; There is one metric per topic partition <topic-partition>.records-lag
        .add (CONSUMER_FETCH_MANAGER_METRICS_GROUP, "*.records-lag", new RoundingConverter("*.records-lag"))
        ;
    }

    /**
     * Constructor must be public and argument-less as this class is instantiated by Kafka.
     */
    public ConsumerMetricsReporter() {
        super();
    }

    @Override
    public MetricFilter getFilter() {
        return filter;
    }

    /**
     * Creates a name for a operator custom metric.
     * The name is topic + ":" + metricName.name() for metrics that have a topic tag 
     * or only the kafka metric name, where '.' is reeplaced by ':'.
     * @param metricName
     * @return The name of a the corresponding operator custom metric
     * @throws KafkaMetricException
     */
    @Override
    public String createCustomMetricName (MetricName metricName) throws KafkaMetricException {
        return createOperatorMetricName (metricName);
    }

    /**
     * Creates a name for a operator custom metric.
     * The name is topic + ":" + metricName.name() for metrics that have a topic tag 
     * or only the kafka metric name, where '.' is replaced by ':'.
     * @param metricName
     * @return The name of a the corresponding operator custom metric
     * @throws KafkaMetricException
     */
    public static String createOperatorMetricName (MetricName metricName) throws KafkaMetricException {
        final String group = metricName.group();
        String customMetricName = metricName.name();
        if (group.equals (CONSUMER_FETCH_MANAGER_METRICS_GROUP)) {
            final Map <String, String> tags = metricName.tags();
            if (tags != null) {
                final String topic = tags.get ("topic");
                if (topic != null) {
                    return topic + ":" + customMetricName;
                }
            }
        }
        // replace the last dot in the dynamic metrics <topic-partition>.xxx-yyy
        int lastDotIdx = customMetricName.lastIndexOf('.');
        if (lastDotIdx == -1) return customMetricName;
        return customMetricName.substring(0,  lastDotIdx) + ":" + customMetricName.substring (lastDotIdx +1);
    }
}
