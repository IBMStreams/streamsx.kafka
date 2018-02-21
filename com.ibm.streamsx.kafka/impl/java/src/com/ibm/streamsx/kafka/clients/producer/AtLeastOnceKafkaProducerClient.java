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

    private List<Future<RecordMetadata>> futuresList;
    
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
    
    /**
     * Makes all buffered records immediately available to send and blocks until completion of the associated requests.
     * 
     * @throws InterruptedException. If flush is interrupted, an InterruptedException is thrown.
     */
    @Override
    public synchronized void flush() {
        super.flush();
        // post-condition is, that all futures are in done state.
        // No need to wait by calling future.get() on all futures in futuresList
        futuresList.clear();
    }
    
    @Override
    public void drain() throws Exception {
        if (logger.isDebugEnabled()) logger.debug("AtLeastOnceKafkaProducerClient -- DRAIN"); //$NON-NLS-1$
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        if (logger.isDebugEnabled()) logger.debug("AtLeastOnceKafkaProducerClient -- CHECKPOINT id=" + checkpoint.getSequenceId()); //$NON-NLS-1$
    }
    
    /**
     * Tries to cancel all send requests that are not yet done.
     */
    @Override
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- trying to cancel requests");
        int nCancelled = 0;
        for (Future<RecordMetadata> future : futuresList) {
            if (future.cancel (mayInterruptIfRunning)) ++nCancelled;
        }
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- number of cancelled send requests: " + nCancelled); //$NON-NLS-1$
        futuresList.clear();
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("AtLeastOnceKafkaProducerClient -- RESET id=" + checkpoint.getSequenceId()); //$NON-NLS-1$
        }
        setSendException (null);
    }
}
