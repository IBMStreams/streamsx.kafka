package com.ibm.streamsx.kafka.clients.producer;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public class ExactlyOnceKafkaProducerClient extends KafkaProducerClient {

    private static final Logger logger = Logger.getLogger(ExactlyOnceKafkaProducerClient.class);
    private static final String CONSUMER_ID_PREFIX = "__internal_consumer_";
    private static final String GROUP_ID_PREFIX = "__internal_group_";
	private static final String EXACTLY_ONCE_STATE_TOPIC = "__streams_control_topic";
	private static final String TRANSACTION_ID = "tid";
	private static final String COMMITTED_SEQUENCE_ID = "seqId";
	
    private ControlVariableAccessor<String> transactionalIdCV;
    private String transactionalId;
    private long lastSuccessfulSequenceId = 0;
    private HashMap<TopicPartition, Long> controlTopicInitialOffsets;
	private RecordMetadata lastCommittedControlRecordMetadata;
    
    public <K, V> ExactlyOnceKafkaProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws Exception {
        super(operatorContext, keyClass, valueClass, kafkaProperties);
        logger.debug("ExaxtlyOnceKafkaProducerClient starting...");
        
        // If this variable has not been set before, then set it to the current end offset.
        // Otherwise, this variable will be overridden with the value is retrieved
        controlTopicInitialOffsets = getControlTopicEndOffsets();
        ControlPlaneContext cpContext = operatorContext.getOptionalContext(ControlPlaneContext.class);
        ControlVariableAccessor<String> startOffsetsCV = cpContext.createStringControlVariable("control_topic_start_offsets", false, serializeObject(controlTopicInitialOffsets));        
        controlTopicInitialOffsets = SerializationUtils
                .deserialize(Base64.getDecoder().decode(startOffsetsCV.sync().getValue()));
        logger.debug("controlTopicInitialOffsets=" + controlTopicInitialOffsets);
        
        initTransactions();
        beginTransaction(); // begin a new transaction before the operator starts processing tuples
    }
    
	@Override
    protected void configureProperties() throws Exception {
    	super.configureProperties();
    	
        // Need to generate a transaction ID that is unique but persists 
        // across operator instances. In order to guarantee this, we will
        // store the transaction ID in the JCP
        ControlPlaneContext crContext = operatorContext.getOptionalContext(ControlPlaneContext.class);
        transactionalIdCV = crContext.createStringControlVariable("transactional_id", false, getRandomId("tid-"));
        transactionalId = transactionalIdCV.sync().getValue();
        logger.debug("Transactional ID=" + transactionalId);
        
        // The "enable.idempotence" property is required in order to guarantee idempotence
        this.kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        
        // The "transaction.id" property is mandatory in order to support transactions.
        this.kafkaProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }
        
    public String getTransactionalId() {
		return transactionalId;
	}
    
    private void initTransactions() {
        // Initialize the transactions. Previously uncommitted 
        // transactions will be aborted. 
        logger.debug("Initializating transactions...");
        producer.initTransactions();
        logger.debug("Transaction initialization finished!");
    }
    
    private void beginTransaction() {
        logger.debug("Starting new transaction");
        producer.beginTransaction();
    }
    
	private void abortTransaction() {
        logger.debug("Aborting transaction");
        producer.abortTransaction();
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
	private void commitTransaction(long sequenceId) throws Exception {    	
    	ProducerRecord controlRecord = new ProducerRecord(EXACTLY_ONCE_STATE_TOPIC, null);
    	Headers headers = controlRecord.headers();
    	headers.add(TRANSACTION_ID, getTransactionalId().getBytes());
    	headers.add(COMMITTED_SEQUENCE_ID, String.valueOf(sequenceId).getBytes());
    	Future<RecordMetadata> controlRecordFuture = producer.send(controlRecord);
    	logger.debug("Sent control record: " + controlRecord);
    	
    	logger.debug("Committing transaction...");
        producer.commitTransaction();
        
        // controlRecordFuture information should be available now
        lastCommittedControlRecordMetadata = controlRecordFuture.get();
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean processTuple(ProducerRecord producerRecord) throws Exception {
    	logger.trace("Sending: " + producerRecord);
        producer.send(producerRecord);
		
        return true;
    }

    
    private HashMap<TopicPartition, Long> getControlTopicEndOffsets() throws Exception {
    	KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(getConsumerProperties());
    	HashMap<TopicPartition, Long> controlTopicEndOffsets = getControlTopicEndOffsets(consumer);
    	consumer.close(1, TimeUnit.SECONDS);
    	
    	return controlTopicEndOffsets;
	}

    private HashMap<TopicPartition, Long> getControlTopicEndOffsets(KafkaConsumer<?, ?> consumer) throws Exception {
    	List<PartitionInfo> partitionInfoList = consumer.partitionsFor(EXACTLY_ONCE_STATE_TOPIC);
    	List<TopicPartition> partitions = new ArrayList<TopicPartition>();
    	partitionInfoList.forEach(pi -> partitions.add(new TopicPartition(pi.topic(), pi.partition())));
    	Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
    	
    	return new HashMap<>(endOffsets);
    }

    @SuppressWarnings("rawtypes")
	private long getCommittedSequenceId() throws Exception {
    	KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(getConsumerProperties());
    	HashMap<TopicPartition, Long> endOffsets = getControlTopicEndOffsets(consumer);

    	// move the consumer to initial offset to begin consuming from
    	consumer.assign(controlTopicInitialOffsets.keySet());
    	controlTopicInitialOffsets.forEach((tp, offset) -> {
    		consumer.seek(tp, offset);
    	});
    	
    	long committedSeqId = 0;
    	boolean consumerAtEnd = false;
    	while(!consumerAtEnd) {
    		ConsumerRecords<?, ?> records = consumer.poll(1000);
    		logger.debug("ConsumerRecords: " + records);
    		Iterator<?> it = records.iterator();
    		while(it.hasNext()) {
    			ConsumerRecord record = (ConsumerRecord)it.next();
    			Headers headers = record.headers();
    			logger.debug("Headers: " + headers);
    			String tid = new String(headers.lastHeader(TRANSACTION_ID).value(), StandardCharsets.UTF_8);
    			logger.debug("Checking tid=" + tid + " (currentTid=" + getTransactionalId() + ")");
    			if(tid.equals(getTransactionalId())) {
    				committedSeqId = Long.valueOf(new String(headers.lastHeader(COMMITTED_SEQUENCE_ID).value(), StandardCharsets.UTF_8));
    			}
    		}
    		
    		consumerAtEnd = isConsumerAtEnd(consumer, endOffsets);
    		logger.debug("consumerAtEnd=" + consumerAtEnd);
    	}    	
    	
    	consumer.close(1l, TimeUnit.SECONDS);
    	
    	return committedSeqId;
    }
    
    private boolean isConsumerAtEnd(KafkaConsumer<?, ?> consumer, Map<TopicPartition, Long> endOffsets) {
    	for(Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
    		long currentOffset = consumer.position(entry.getKey());
    		long endOffset = entry.getValue();
    		if(currentOffset < endOffset-1) {
    			return false;
    		}
    	}
    	
    	return true;
	}

	private KafkaOperatorProperties getConsumerProperties() throws Exception {
		// TODO: Can we simply copy the producer configs into the consumer configs? What happens?
		KafkaOperatorProperties consumerProps = new KafkaOperatorProperties();
		consumerProps.putAll(this.kafkaProperties);
		consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, getRandomId(CONSUMER_ID_PREFIX));
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, getRandomId(GROUP_ID_PREFIX));
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, inferDeserializerFromSerializer(this.kafkaProperties.getValueSerializer()));
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, inferDeserializerFromSerializer(this.kafkaProperties.getKeySerializer()));
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		logger.debug("Consumer properties: " + consumerProps);

		return consumerProps;
	}    
    
    protected String serializeObject(Serializable obj) {
        return new String(Base64.getEncoder().encode(SerializationUtils.serialize(obj)));
    }
	
	@Override
    public void drain() throws Exception {
        logger.debug("ExactlyOnceKafkaProducer -- DRAIN");
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.debug("ExactlyOnceKafkaProducer -- CHECKPOINT id=" + checkpoint.getSequenceId());
        long currentSequenceId = checkpoint.getSequenceId();
        
        logger.debug("currentSequenceId=" + currentSequenceId + ", lastSuccessSequenceId=" + lastSuccessfulSequenceId);
        
        // check if this is a new transaction
        boolean doCommit = true;
    	if(currentSequenceId - lastSuccessfulSequenceId > 1) {
    		long committedSequenceId = getCommittedSequenceId();
    		logger.debug("committedSequenceId=" + committedSequenceId);
    		
    		if(lastSuccessfulSequenceId < committedSequenceId) {
    			logger.debug("Aborting transaction due to lastSuccessfulSequenceId < committedSequenceId");
    			// If the last successful sequence ID is less than
    			// the committed sequence ID, this transaction has
    			// been processed before and is a duplicate.
    			// Discard this transaction.
    			abortTransaction();
    			doCommit = false;
    			lastSuccessfulSequenceId = committedSequenceId;
    		}
    	}

    	logger.debug("doCommit=" + doCommit);    	
    	if(doCommit) {
			commitTransaction(checkpoint.getSequenceId());
			lastSuccessfulSequenceId = checkpoint.getSequenceId();   
			
			TopicPartition tp = new TopicPartition(lastCommittedControlRecordMetadata.topic(), lastCommittedControlRecordMetadata.partition());
			controlTopicInitialOffsets.put(tp, lastCommittedControlRecordMetadata.offset());
    	}
    	
    	// save the last successful seq ID
    	logger.debug("Checkpointing lastSuccessfulSequenceId: " + lastSuccessfulSequenceId);
    	checkpoint.getOutputStream().writeLong(lastSuccessfulSequenceId);

    	// save the control topic offsets
    	logger.debug("Checkpointing control topic offsets: " + controlTopicInitialOffsets);
    	checkpoint.getOutputStream().writeObject(controlTopicInitialOffsets);
    	
        // start a new transaction
        beginTransaction();
    }

	@Override
	@SuppressWarnings("unchecked")
    public void reset(Checkpoint checkpoint) throws Exception {
        logger.debug("ExactlyOnceKafkaProducer -- RESET id=" + checkpoint.getSequenceId());
        
        lastSuccessfulSequenceId = checkpoint.getInputStream().readLong();
        logger.debug("Reset lastSuccessfulSequenceId: " + lastSuccessfulSequenceId);        

        controlTopicInitialOffsets = (HashMap<TopicPartition, Long>)checkpoint.getInputStream().readObject();
        logger.debug("Reset controlTopicInitialOffsets: " + lastSuccessfulSequenceId);
        
        // abort the current transaction and start a new one
        abortTransaction();
        beginTransaction();
    }

    @Override
    public void resetToInitialState() throws Exception {
        logger.debug("ExactlyOnceKafkaProducer -- RESET_TO_INIT");
        
        // abort the current transaction and start a new
        abortTransaction();
        beginTransaction();
    }
}