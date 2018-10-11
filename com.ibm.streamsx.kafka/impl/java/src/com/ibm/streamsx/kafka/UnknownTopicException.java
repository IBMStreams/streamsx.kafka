/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * Exception indicating that a topic is missing.
 * @author IBM Kafka toolkit team
 */
public class UnknownTopicException extends KafkaClientInitializationException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new UnknownTopicException
     */
    public UnknownTopicException() {
        super();
    }

    /**
     * @param message
     */
    public UnknownTopicException (String message) {
        super(message);
    }

    /**
     * @param rootCause
     */
    public UnknownTopicException (Throwable rootCause) {
        super(rootCause);
    }

    /**
     * @param message
     * @param rootCause
     */
    public UnknownTopicException (String message, Throwable rootCause) {
        super(message, rootCause);
    }
}
