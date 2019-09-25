/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer;

/**
 * This class treats all Exceptiona as recoverable.
 * @author The IBM Kafka toolkit maintainers
 * @sinve toolkit version 2.2
 */
public class RecoverAllErrors implements ErrorCategorizer {

    /**
     * Returns always RECOVERABLE
     * @see com.ibm.streamsx.kafka.clients.producer.ErrorCategorizer#isRecoverable(java.lang.Exception)
     */
    @Override
    public ErrCategory isRecoverable(Exception e) {
        return ErrCategory.RECOVERABLE;
    }
}
