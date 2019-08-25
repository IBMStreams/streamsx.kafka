package com.ibm.streamsx.kafka;

/**
 * RuntimeException that is thrown when reset of the operator within a Consistent Region failed.
 * @author IBM Kafka toolkit team
 */
public class KafkaOperatorResetFailedException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new KafkaOperatorResetFailedException
     */
    public KafkaOperatorResetFailedException() {
        super();
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param message - the detail message.
     */
    public KafkaOperatorResetFailedException (String message) {
        super (message);
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public KafkaOperatorResetFailedException (Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param message - the detail message.
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public KafkaOperatorResetFailedException (String message, Throwable cause) {
        super (message, cause);
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param message - the detail message.
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     * @param enableSuppression - whether or not suppression is enabled or disabled
     * @param writableStackTrace - whether or not the stack trace should be writable
     */
    public KafkaOperatorResetFailedException (String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super (message, cause, enableSuppression, writableStackTrace);
    }
}
