/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

import org.apache.log4j.Logger;

/**
 * This converter multiplies the Kafka metric value with a factor and rounds it to a long value.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class MultiplyConverter implements MetricConverter {

    private static final Logger trace = Logger.getLogger (MultiplyConverter.class);
    private String metricName;
    private double factor;
    
    /**
     * @param metricName  the metric name - for error tracing only
     * @param factor      the multiplier
     */
    public MultiplyConverter (String metricName, double factor) {
        this.metricName = metricName;
        this.factor = factor;
    }

    /**
     * @param factor the multiplier
     */
    public MultiplyConverter (double factor) {
        this ("", factor);
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.metrics.MetricConverter#convert(java.lang.Object)
     */
    @Override
    public long convert (Object metricValue) {
        if (metricValue == null) return 0l;
        if (metricValue instanceof java.lang.Double) {
            long l = Math.round (factor * ((Double)metricValue).doubleValue());
            // Math.round() rounds to Long.MIN_VALUE for negative infinity, we want 0 in this case: 
            return l == Long.MIN_VALUE? 0l: l;
        }
        trace.error ("convert (" + metricName + "): class: " + metricValue.getClass().getCanonicalName() + ", no conversion for value = " + metricValue);
        return 0l;
    }
}
