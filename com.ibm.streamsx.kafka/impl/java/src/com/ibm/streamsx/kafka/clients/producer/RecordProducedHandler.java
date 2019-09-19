package com.ibm.streamsx.kafka.clients.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author The IBM Kafka toolkit team
 */
public interface RecordProducedHandler {
    /**
     * called when the producer record has been produced successfully
     * @param seqNo     the sequence number of the {@link RecordProduceAttempt}
     * @param record    the produced record
     * @param metadata  the meta data of the produced record
     */
    void onRecordProduced (long seqNo, ProducerRecord<?, ?> record, RecordMetadata metadata);
}
