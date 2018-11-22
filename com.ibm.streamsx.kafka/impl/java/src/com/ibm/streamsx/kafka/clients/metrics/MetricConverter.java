/**
 * 
 */
package com.ibm.streamsx.kafka.clients.metrics;

/**
 * The MetricConverter converts the value of a Kafka metric into the value range of operator custom metrics.
 * Kafka metric values are typically doubles, often in the range 0..1, whereas operator metrics 
 * are positive integers in the long range.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public interface MetricConverter {

    public long convert (Object metricValue);
}
