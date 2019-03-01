/**
 * 
 */
package com.ibm.streamsx.kafka;

/**
 * @author IBM Kafka toolkit maintainers
 */
public class TopicPartitionUpdateParseException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    private String json = null;

    /**
     * @return the json
     */
    public String getJson() {
        return json;
    }

    /**
     * @param json the json to set
     */
    public void setJson (String json) {
        this.json = json;
    }

    /**
     * 
     */
    public TopicPartitionUpdateParseException() {
        super();
    }

    /**
     * @param message
     */
    public TopicPartitionUpdateParseException (String message) {
        super (message);
    }

    /**
     * @param cause
     */
    public TopicPartitionUpdateParseException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public TopicPartitionUpdateParseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public TopicPartitionUpdateParseException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
