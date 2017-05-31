package com.ibm.streamsx.kafka.clients.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public class AtLeastOnceKafkaProducerClient extends KafkaProducerClient {

    private static final Logger logger = Logger.getLogger(AtLeastOnceKafkaProducerClient.class);

    public <K, V> AtLeastOnceKafkaProducerClient(OperatorContext operatorContext, Class<?> keyType,
            Class<?> messageType, KafkaOperatorProperties props) throws Exception {
        super(operatorContext, keyType, messageType, props);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean processTuple(ProducerRecord producerRecord) throws Exception {
        logger.trace("Sending: " + producerRecord);
        producer.send(producerRecord);
        return true;
    }

    @Override
    public void drain() throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- DRAIN");
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- CHECKPOINT id=" + checkpoint.getSequenceId());
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- RESET id=" + checkpoint.getSequenceId());
    }

    @Override
    public void resetToInitialState() throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- RESET_TO_INIT");
    }
}
