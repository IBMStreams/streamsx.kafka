/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer.queuing;

import org.apache.kafka.common.TopicPartition;

/**
 * @author The IBM Kafka toolkit team
 */
public interface RecordProduceExceptionHandler {
    /**
     * Called when an exception occurred asynchronous producing a record.
     * 
     * @param seqNo  the sequence number of the {@link RecordProduceAttempt}
     * @param tp     the failed topic partition; note that the partition may be undefined (-1)
     * @param e      the exception
     * @param nProducerGenerations the number of producer generations for the record.
     *                             This number should be the same as the number of send attempts.
     */
    void onRecordProduceException (long seqNo, TopicPartition tp, Exception e, int nProducerGenerations);
}
