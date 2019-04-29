/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * This exception indicates commit failures. It is a RuntimeException (unchecked).
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class ConsumerCommitFailedException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     */
    public ConsumerCommitFailedException() {
        super();
    }

    /**
     * @param message
     */
    public ConsumerCommitFailedException (String message) {
        super (message);
    }

    /**
     * @param cause
     */
    public ConsumerCommitFailedException (Throwable cause) {
        super (cause);
    }

    /**
     * @param message
     * @param cause
     */
    public ConsumerCommitFailedException (String message, Throwable cause) {
        super (message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public ConsumerCommitFailedException (String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super (message, cause, enableSuppression, writableStackTrace);
    }
}
