/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * @author IBM Kafka toolkit team
 */
public class KafkaOperatorException extends Exception {

    private static final long serialVersionUID = 1449871652960826013L;

    /**
     * Constructs a new KafkaOperatorException
     */
    public KafkaOperatorException() {
    }

    /**
     * @param message
     */
    public KafkaOperatorException (String message) {
        super(message);
    }

    /**
     * @param rootCause
     */
    public KafkaOperatorException (Throwable rootCause) {
        super(rootCause);
    }

    /**
     * @param message
     * @param rootCause
     */
    public KafkaOperatorException (String message, Throwable rootCause) {
        super(message, rootCause);
    }
}
