/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer;

/**
 * This class treats all Exceptiona as recoverable.
 * @author The IBM Kafka toolkit maintainers
 * @sinve toolkit version 2.2
 */
public class RecoverRetriable implements ErrorCategorizer {

    /**
     * This class treats all Exceptions, which are instance of org.apache.kafka.common.errors.RetriableException as retriable.
     * All other exceptions are treated as final failures
     * @see com.ibm.streamsx.kafka.clients.producer.ErrorCategorizer#isRecoverable(java.lang.Exception)
     */
    @Override
    public ErrCategory isRecoverable (Exception e) {
        if (e instanceof org.apache.kafka.common.errors.RetriableException)
            return ErrCategory.RECOVERABLE;
        else
            return ErrCategory.FINAL_FAIL;
    }
}
