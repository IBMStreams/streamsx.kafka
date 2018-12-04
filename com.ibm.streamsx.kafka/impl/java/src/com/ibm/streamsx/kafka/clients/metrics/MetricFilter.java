/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.ibm.streamsx.kafka.KafkaMetricException;

/**
 * This class represents a filter for Kafka metrics.
 * Metrics are filtered by full match of their group and name.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class MetricFilter {

    // group metric group maps to map of metric name->converter
    protected Map<String, Map<String, MetricConverter>> filters = new HashMap<>();
    private MetricConverter defaultConverter = new RoundingConverter ("default");
    
    /**
     * Returns true when the Metric passes the filter, false otherwise
     * @param m the metric to which the filter is applied
     * @return true when the Metric passes the filter, false otherwise
     */
    public boolean apply (Metric m) {
        final MetricName mName = m.metricName();
        Map<String, MetricConverter> names = filters.get (mName.group());
        if (names == null)
            return false;
        return names.containsKey (mName.name());
    }

    /**
     * Adds a Metric given by the group and the metric name.
     * @param group  the group to which the metric belongs to
     * @param name   the name of the metric
     * @param valueConverter  the converter to be used for the metric value
     * @return this
     */
    public MetricFilter add (String group, String name, MetricConverter valueConverter) {
        Map<String, MetricConverter> names = filters.get (group);
        if (names == null) {
            names = new HashMap<>();
            filters.put (group, names);
        }
        names.put (name, valueConverter);
        return this;
    }

    /**
     * Returns the metric converter for a metric.
     * @param m the Kafka metric
     * @return the converter for the given metric
     * @throws KafkaMetricException The Metric does not apply the the filter.
     */
    public MetricConverter getConverter (Metric m) throws KafkaMetricException {
        final MetricName metricName = m.metricName();
        if (!apply(m)) {
            throw new KafkaMetricException ("Metric " + metricName.group() + ":" + metricName.name() + " does not apply to the filter");
        }
        MetricConverter c = filters.get (metricName.group()).get(metricName.name());
        return c == null? this.defaultConverter: c;
    }
}
