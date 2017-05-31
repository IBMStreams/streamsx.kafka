package com.ibm.streamsx.kafka.clients.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public abstract class KafkaProducerClient extends AbstractKafkaClient {

    private static final Logger logger = Logger.getLogger(KafkaProducerClient.class);
    private static final int CLOSE_TIMEOUT = 5;
    private static final TimeUnit CLOSE_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
    private static final String GENERATED_PRODUCERID_PREFIX = "producer-";

    protected KafkaProducer<?, ?> producer;
    protected ProducerCallback callback;
    protected Exception sendException;
    protected KafkaOperatorProperties kafkaProperties;
    protected List<Future<RecordMetadata>> futuresList;

    public <K, V> KafkaProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws Exception {
        this.kafkaProperties = kafkaProperties;
        if (!this.kafkaProperties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializer(keyClass));
        }

        if (!kafkaProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializer(valueClass));
        }

        // Create a random client ID for the producer if one is not specified.
        // This is important, otherwise running multiple producers from the same
        // application will result in a KafkaException when registering the
        // client
        if (!kafkaProperties.containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
            this.kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, getRandomId(GENERATED_PRODUCERID_PREFIX));
        }

        this.futuresList = Collections.synchronizedList(new ArrayList<Future<RecordMetadata>>());
        producer = new KafkaProducer<>(this.kafkaProperties);

        callback = new ProducerCallback(this);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<RecordMetadata> send(ProducerRecord record) throws Exception {
        if (sendException != null) {
            logger.error(Messages.getString("PREVIOUS_BATCH_FAILED_TO_SEND", sendException.getLocalizedMessage()), //$NON-NLS-1$
                    sendException);
            throw sendException;
        }

        logger.trace("Sending: " + record); //$NON-NLS-1$
        Future<RecordMetadata> future = producer.send(record, callback);
        futuresList.add(future);

        return future;
    }

    public synchronized void flush() throws Exception {
        logger.trace("Flusing..."); //$NON-NLS-1$
        producer.flush();

        // wait until all messages have
        // been received successfully,
        // otherwise throw an exception
        for (Future<RecordMetadata> future : futuresList)
            future.get();

        futuresList.clear();
    }

    public void close() {
        logger.trace("Closing..."); //$NON-NLS-1$
        producer.close(CLOSE_TIMEOUT, CLOSE_TIMEOUT_TIMEUNIT);
    }

    public void setSendException(Exception sendException) {
        this.sendException = sendException;
    }

    @SuppressWarnings("rawtypes")
    public abstract boolean processTuple(ProducerRecord producerRecord) throws Exception;

    public abstract void drain() throws Exception;

    public abstract void checkpoint(Checkpoint checkpoint) throws Exception;

    public abstract void reset(Checkpoint checkpoint) throws Exception;

    public abstract void resetToInitialState() throws Exception;
}