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
    
    /**
     * gets the root cause of the exception
     * 
     * @return the root cause or `null` if there is none.
     */
    public Throwable getRootCause() {
        Throwable rootCause = null;
        Throwable cause = getCause();
        while (cause != null) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }
}
