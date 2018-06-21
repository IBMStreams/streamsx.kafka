/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * Indicates Kafka client initialization failures.
 * This exception should be treated as non-recoverable.
 */
public class KafkaClientInitializationException extends KafkaOperatorException {

    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public KafkaClientInitializationException() {
        super();
    }

    /**
     * @param message
     */
    public KafkaClientInitializationException (String message) {
        super(message);
    }

    /**
     * @param rootCause
     */
    public KafkaClientInitializationException (Throwable rootCause) {
        super(rootCause);
    }

    /**
     * @param message
     * @param rootCause
     */
    public KafkaClientInitializationException(String message, Throwable rootCause) {
        super(message, rootCause);
    }
}
