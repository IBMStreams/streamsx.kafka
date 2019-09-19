/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer;

import java.text.MessageFormat;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class represents a produce attempt for a producer record. It is associated with one ProducerRecord,
 * and with a single callback for the KafkaProducer.
 * 
 * @author The IBM Kafka toolkit team
 */
public class RecordProduceAttempt {
    private static final Logger trace = Logger.getLogger (RecordProduceAttempt.class);
//    private static final Level DEBUG_LEVEL = SystemProperties.getDebugLevelOverride();
    private static final Level DEBUG_LEVEL = Level.TRACE;

    /**
     * This class is the Callback implementation that is invoked by the KafkaProducer.
     */
    private class KafkaProducerCallback implements Callback {
        private final int producerGeneration;

        private KafkaProducerCallback (int producerGeneration) {
            trace.log (DEBUG_LEVEL, "creating Kafka Callback for record " + producerRecordSeqNumber + " with producer generation " + producerGeneration);
            this.producerGeneration = producerGeneration;
        }

        @Override
        public void onCompletion (RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                if (trace.isEnabledFor(DEBUG_LEVEL))
                    trace.log (DEBUG_LEVEL, "record " + producerRecordSeqNumber + " successfully produced for topic '" + metadata.topic() + "'. Invoking produced handler...");
                producedHandler.onRecordProduced (producerRecordSeqNumber, producerRecord, metadata);
                return;
            }
            // when we are here, producing the record failed with an exception
            TopicPartition tp = new TopicPartition (topic, metadata == null? -1: metadata.partition());
            if (trace.isEnabledFor(DEBUG_LEVEL))
                trace.log (DEBUG_LEVEL, "record " + producerRecordSeqNumber + " failed for " + tp + ": " + exception.getClass());
            if (this.producerGeneration == RecordProduceAttempt.this.producerGeneration.get()) {
                trace.log (DEBUG_LEVEL, "Invoking exception handler...");
                final int nProducerGenerations = RecordProduceAttempt.this.producerGeneration.get() - initialProducerGeneration +1;
                exceptionHandler.onRecordProduceException (producerRecordSeqNumber, tp, exception, nProducerGenerations);
            }
            else {
                trace.log (DEBUG_LEVEL, MessageFormat.format ("skipping exception handler. producer generation of callback = {0,number,#}. "
                        + "producer generation of pending record = {1,number,#}.", 
                        this.producerGeneration, RecordProduceAttempt.this.producerGeneration.get()));
            }
        }
    }

    private static AtomicLong nextProducerRecordSeqNumber = new AtomicLong();
    private KafkaProducerCallback callback;
    private final int initialProducerGeneration;
    private AtomicInteger producerGeneration;
    private final ProducerRecord<?, ?> producerRecord;
    private Future<RecordMetadata> future;
    private final long producerRecordSeqNumber;
    private final String topic;
    private RecordProducedHandler producedHandler;
    private RecordProduceExceptionHandler exceptionHandler;

    /**
     * Creates a new Attempt to produce a record.
     */
    public RecordProduceAttempt (ProducerRecord<?, ?> producerRecord, int producerGeneration) {
        this.initialProducerGeneration = producerGeneration;
        this.producerGeneration = new AtomicInteger (initialProducerGeneration);
        this.producerRecord = producerRecord;
        this.topic = producerRecord.topic();
        this.producerRecordSeqNumber = nextProducerRecordSeqNumber.incrementAndGet();
        this.callback = new KafkaProducerCallback (producerGeneration);
    }


    /**
     * Returns the Callback implementation for the KafkaProducer.
     * @return the callback
     */
    public KafkaProducerCallback getCallback() {
        return callback;
    }


    /**
     * @return the producerRecordSeqNumber
     */
    public long getProducerRecordSeqNumber() {
        return producerRecordSeqNumber;
    }


    /**
     * Returns the Future of the asynchronous send task.
     * @return the future. Note, that the Future is null when the records are not yet sent.
     */
    public Future<RecordMetadata> getFuture() {
        return future;
    }


    /**
     * @param future the future to set
     */
    public void setFuture (Future<RecordMetadata> future) {
        this.future = future;
    }


    /**
     * @param producedHandler the producedHandler to set
     */
    public void setProducedHandler (RecordProducedHandler producedHandler) {
        this.producedHandler = producedHandler;
    }


    /**
     * @param exceptionHandler the exceptionHandler to set
     */
    public void setExceptionHandler (RecordProduceExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }


    /**
     * get the producer record
     * @return the producer record
     */
    public ProducerRecord<?, ?> getRecord() {
        return producerRecord;
    }


    /**
     * increments the producer generation as well as creates
     * a new callback and tries to cancel the task over its Future object.
     */
    public void incrementProducerGenerationCancelTask() {
        this.callback = new KafkaProducerCallback (producerGeneration.incrementAndGet());
        final Future<RecordMetadata> future = getFuture();
        if (future != null && !future.isDone()) future.cancel (true);
    }

    /**
     * Returns the destination topic for the producer record.
     * @return the destination topic of the record 
     */
    public String getTopic() {
        return topic;
    }
}
