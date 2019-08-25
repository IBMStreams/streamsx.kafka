package com.ibm.streamsx.kafka.clients.producer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaOperatorRuntimeException;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Instantiated in consistent region, when consistentRegionPolicy is not transactional.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class AtLeastOnceKafkaProducerClient extends KafkaProducerClient {

    private static final Logger logger = Logger.getLogger(AtLeastOnceKafkaProducerClient.class);

    private List<Future<RecordMetadata>> futuresList;
    private ConsistentRegionContext crContext;
    private AtomicBoolean resetInitiatedOnce = new AtomicBoolean (false);
    
    public <K, V> AtLeastOnceKafkaProducerClient(OperatorContext operatorContext, Class<?> keyType,
            Class<?> messageType, boolean guaranteeOrdering, KafkaOperatorProperties props) throws Exception {
        super(operatorContext, keyType, messageType, guaranteeOrdering, props);
        this.futuresList = Collections.synchronizedList(new ArrayList<Future<RecordMetadata>>());
        this.crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#processRecord(org.apache.kafka.clients.producer.ProducerRecord, com.ibm.streams.operator.Tuple)
     */
    @Override
    public void processRecord (ProducerRecord<?, ?> producerRecord, Tuple associatedTuple) throws Exception {
        Future<RecordMetadata> future = send (producerRecord);
        futuresList.add (future);
    }

    /**
     * Makes all buffered records immediately available to send and blocks until completion of the associated requests.
     * 
     * @throws InterruptedException. If flush is interrupted, an InterruptedException is thrown.
     * @throws KafkaOperatorRuntimeException. flushing the producer operator failed (exception received in callback)
     */
    @Override
    public synchronized void flush() {
        super.flush();
        // post-condition is, that all futures are in done state.
        // No need to wait by calling future.get() on all futures in futuresList
        futuresList.clear();
        if (sendException != null) {
            logger.error (Messages.getString ("PREVIOUS_BATCH_FAILED_TO_SEND", sendException.getMessage()), sendException);
            sendException.printStackTrace();
            throw new KafkaOperatorRuntimeException ("flushing the producer failed.", sendException);
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#handleSendException(java.lang.Exception)
     */
    @Override
    public void handleSendException (Exception e) {
        // reset only once; 
        if (!resetInitiatedOnce.getAndSet (true)) {
            logger.error ("Exception received producing messages: " + e);
            try {
                sendException = e;
                logger.info ("triggering reset of the consistent region");
                crContext.reset();
            }
            catch (IOException ioe) {
                producer.close (Duration.ofMillis (0L));
                // stop the PE, the runtime may re-launch it
                System.exit (1);
            }
        }
        else {
            logger.info ("Exception received producing messages (CR reset triggered): " + e);
        }
    }


    @Override
    public void drain() throws Exception {
        if (logger.isEnabledFor (DEBUG_LEVEL)) logger.log (DEBUG_LEVEL, getThisClassName() + " -- DRAIN"); //$NON-NLS-1$
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        if (logger.isEnabledFor (DEBUG_LEVEL)) logger.log (DEBUG_LEVEL, getThisClassName() + " -- CHECKPOINT id=" + checkpoint.getSequenceId()); //$NON-NLS-1$
    }
    
    /**
     * Tries to cancel all send requests that are not yet done.
     */
    @Override
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        if (logger.isEnabledFor (DEBUG_LEVEL)) logger.log (DEBUG_LEVEL, getThisClassName() + " -- trying to cancel requests");
        int nCancelled = 0;
        for (Future<RecordMetadata> future : futuresList) {
            if (!future.isDone() && future.cancel (mayInterruptIfRunning)) ++nCancelled;
        }
        if (logger.isEnabledFor (DEBUG_LEVEL)) logger.log (DEBUG_LEVEL, getThisClassName() + " -- number of cancelled send requests: " + nCancelled); //$NON-NLS-1$
        futuresList.clear();
    }

    @Override
    public void reset (Checkpoint checkpoint) throws Exception {
        if (logger.isEnabledFor (DEBUG_LEVEL)) {
            logger.log (DEBUG_LEVEL, getThisClassName() + " -- RESET id=" + (checkpoint == null? -1L: checkpoint.getSequenceId())); //$NON-NLS-1$
        }
    }
}
