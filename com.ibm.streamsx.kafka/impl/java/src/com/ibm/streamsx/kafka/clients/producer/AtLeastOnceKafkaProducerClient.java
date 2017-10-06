package com.ibm.streamsx.kafka.clients.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public class AtLeastOnceKafkaProducerClient extends KafkaProducerClient {

    private static final Logger logger = Logger.getLogger(AtLeastOnceKafkaProducerClient.class);

    protected List<Future<RecordMetadata>> futuresList;
    
    public <K, V> AtLeastOnceKafkaProducerClient(OperatorContext operatorContext, Class<?> keyType,
            Class<?> messageType, KafkaOperatorProperties props) throws Exception {
        super(operatorContext, keyType, messageType, props);
        logger.debug("AtLeastOnceKafkaProducerClient starting...");
        
        this.futuresList = Collections.synchronizedList(new ArrayList<Future<RecordMetadata>>());
    }

    @SuppressWarnings("rawtypes")
	@Override
    public Future<RecordMetadata> send(ProducerRecord record) throws Exception {
    	Future<RecordMetadata> future = super.send(record);
        futuresList.add(future);
        
        return future;
    }
    
    @Override
    public synchronized void flush() throws Exception {
    	super.flush();
    	
        // wait until all messages have
        // been received successfully,
        // otherwise throw an exception
        for (Future<RecordMetadata> future : futuresList)
            future.get();

        futuresList.clear();
    }
    
    @Override
    public void drain() throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- DRAIN"); //$NON-NLS-1$
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- CHECKPOINT id=" + checkpoint.getSequenceId()); //$NON-NLS-1$
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- RESET id=" + checkpoint.getSequenceId()); //$NON-NLS-1$
    }

    @Override
    public void resetToInitialState() throws Exception {
        logger.debug("AtLeastOnceKafkaProducer -- RESET_TO_INIT"); //$NON-NLS-1$
    }
}
