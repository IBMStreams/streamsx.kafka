package com.ibm.streamsx.kafka.clients.producer;

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

public class KafkaProducerClient extends AbstractKafkaClient {

    private static final Logger logger = Logger.getLogger(KafkaProducerClient.class);
    private static final int CLOSE_TIMEOUT = 5;
    private static final TimeUnit CLOSE_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
    
    protected KafkaProducer<?, ?> producer;
    protected ProducerCallback callback;
    protected Exception sendException;
    protected KafkaOperatorProperties kafkaProperties;
    protected Class<?> keyClass;
    protected Class<?> valueClass;
	protected OperatorContext operatorContext;
    
    public <K, V> KafkaProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws Exception {
        super (operatorContext, kafkaProperties, false);
        this.kafkaProperties = kafkaProperties;
        this.operatorContext = operatorContext;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        
        configureProperties();
        createProducer();
    }

    protected void createProducer() {
        producer = new KafkaProducer<>(this.kafkaProperties);
        callback = new ProducerCallback(this);
    }
    
    protected void configureProperties() throws Exception {
        if (!this.kafkaProperties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
        	if(keyClass != null) {
        		this.kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializer(keyClass));	
        	} else {
        		// Kafka requires a key serializer to be specified, even if no
        		// key is going to be used. Setting the StringSerializer class.  
        		this.kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializer(String.class));
        	}
            
        }

        if (!kafkaProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializer(valueClass));
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<RecordMetadata> send(ProducerRecord record) throws Exception {
        if (sendException != null) {
            logger.error(Messages.getString("PREVIOUS_BATCH_FAILED_TO_SEND", sendException.getLocalizedMessage()), //$NON-NLS-1$
                    sendException);
            throw sendException;
        }

        //logger.trace("Sending: " + record); //$NON-NLS-1$
       return producer.send(record, callback);
    }
    
    /**
     * Makes all buffered records immediately available to send and blocks until completion of the associated requests.
     * The post-conditioin is, that all Futures are in done state.
     * 
     * @throws InterruptedException. If flush is interrupted, an InterruptedException is thrown.
     */
    public synchronized void flush() {
        logger.trace("Flusing..."); //$NON-NLS-1$
        producer.flush();
    }

    public void close() {
        logger.trace("Closing..."); //$NON-NLS-1$
        producer.close(CLOSE_TIMEOUT, CLOSE_TIMEOUT_TIMEUNIT);
    }

    public void setSendException(Exception sendException) {
        this.sendException = sendException;
    }

    @SuppressWarnings("rawtypes")
    public boolean processTuple(ProducerRecord producerRecord) throws Exception {
    	send(producerRecord);
    	return true;
    }
    
    /**
     * Tries to cancel all send requests that are not yet done. 
     * The base class has an empty implementation as it does not maintain the futures of send request.
     * @param mayInterruptIfRunning - true if the thread executing this task send request should be interrupted;
     *                              otherwise, in-progress tasks are allowed to complete
     */
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }

    public void drain() throws Exception {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }

    public void checkpoint(Checkpoint checkpoint) throws Exception {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }

    public void reset(Checkpoint checkpoint) throws Exception {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }
}
