/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * Exception thrown when a Kafka Metric cannot be converted into an operator custom metric.
 * @author The IBM Kafka toolkit maintainers
 */
public class KafkaMetricException extends KafkaOperatorException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new KafkaMetricException
     */
    public KafkaMetricException() {
        super();
    }

    /**
     * Constructs a new KafkaMetricException
     * @param message
     */
    public KafkaMetricException (String message) {
        super(message);
    }

    /**
     * Constructs a new KafkaMetricException
     * @param rootCause
     */
    public KafkaMetricException (Throwable rootCause) {
        super (rootCause);
    }

    /**
     * Constructs a new KafkaMetricException
     * @param message
     * @param rootCause
     */
    public KafkaMetricException (String message, Throwable rootCause) {
        super (message, rootCause);
    }
}
