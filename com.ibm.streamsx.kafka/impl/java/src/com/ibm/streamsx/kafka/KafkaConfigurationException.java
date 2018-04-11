/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * Exception that indicates missing Kafka configuration that makes an operator unable to proceed.
 * @author IBM Kafka toolkit team
 */
public class KafkaConfigurationException extends KafkaOperatorException {

    private static final long serialVersionUID = -4396047142878340026L;

    /**
     * 
     */
    public KafkaConfigurationException() {
        super();
    }

    /**
     * @param message
     */
    public KafkaConfigurationException(String message) {
        super(message);
    }

    /**
     * @param rootCause
     */
    public KafkaConfigurationException(Throwable rootCause) {
        super(rootCause);
    }

    /**
     * @param message
     * @param rootCause
     */
    public KafkaConfigurationException(String message, Throwable rootCause) {
        super(message, rootCause);
    }
}
