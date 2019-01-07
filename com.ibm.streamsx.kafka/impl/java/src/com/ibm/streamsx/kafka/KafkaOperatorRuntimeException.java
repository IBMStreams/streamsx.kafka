/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * This Exception is a RuntimeException, which is an unchecked exception.
 * @author The IBM Kafka toolkit team
 */
public class KafkaOperatorRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public KafkaOperatorRuntimeException() {
        super ();
    }

    /**
     * @param message
     */
    public KafkaOperatorRuntimeException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public KafkaOperatorRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public KafkaOperatorRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public KafkaOperatorRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
