package com.ibm.streamsx.kafka.clients.producer;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

public class TransactionalKafkaProducerClient extends KafkaProducerClient {

    private static final Logger logger = Logger.getLogger(TransactionalKafkaProducerClient.class);
    private static final String CONSUMER_ID_PREFIX = "__internal_consumer_";
    private static final String GROUP_ID_PREFIX = "__internal_group_";
    private static final String EXACTLY_ONCE_STATE_TOPIC = "__streams_control_topic";
    private static final String TRANSACTION_ID = "tid";
    private static final String COMMITTED_SEQUENCE_ID = "seqId";

    private List<Future<RecordMetadata>> futuresList;
    private ControlVariableAccessor<String> transactionalIdCV;
    private String transactionalId;
    private final boolean lazyTransactionBegin;
    private long lastSuccessfulSequenceId = 0;
    private HashMap<TopicPartition, Long> controlTopicInitialOffsets;
    private RecordMetadata lastCommittedControlRecordMetadata;
    private AtomicBoolean transactionInProgress = new AtomicBoolean (false);

    public <K, V> TransactionalKafkaProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, boolean lazyTransactionBegin) throws Exception {
        super(operatorContext, keyClass, valueClass, kafkaProperties);
        logger.debug("ExaxtlyOnceKafkaProducerClient starting...");
        this.lazyTransactionBegin = lazyTransactionBegin;
        // If this variable has not been set before, then set it to the current end offset.
        // Otherwise, this variable will be overridden with the value is retrieved
        controlTopicInitialOffsets = getControlTopicEndOffsets();
        ControlPlaneContext cpContext = operatorContext.getOptionalContext(ControlPlaneContext.class);
        ControlVariableAccessor<String> startOffsetsCV = cpContext.createStringControlVariable("control_topic_start_offsets", false, serializeObject(controlTopicInitialOffsets));        
        controlTopicInitialOffsets = SerializationUtils
                .deserialize(Base64.getDecoder().decode(startOffsetsCV.sync().getValue()));
        logger.debug("controlTopicInitialOffsets=" + controlTopicInitialOffsets);
        this.futuresList = Collections.synchronizedList(new ArrayList<Future<RecordMetadata>>());
        initTransactions();
        if (!lazyTransactionBegin) {
            // begin a new transaction before the operator starts processing tuples
            checkAndBeginTransaction();
        }
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
        logger.debug("Transaction initialization finished.");
    }

    private void beginTransaction() {
        if (logger.isDebugEnabled()) logger.debug("Starting new transaction");
        producer.beginTransaction();
    }

    private void abortTransaction() {
        if (logger.isDebugEnabled()) logger.debug("Aborting transaction");
        producer.abortTransaction();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void commitTransaction(long sequenceId) throws Exception {
        ProducerRecord controlRecord = new ProducerRecord(EXACTLY_ONCE_STATE_TOPIC, null);
        Headers headers = controlRecord.headers();
        headers.add(TRANSACTION_ID, getTransactionalId().getBytes());
        headers.add(COMMITTED_SEQUENCE_ID, String.valueOf(sequenceId).getBytes());
        Future<RecordMetadata> controlRecordFuture = producer.send(controlRecord);
        if (logger.isDebugEnabled()) logger.debug("Sent control record: " + controlRecord);

        if (logger.isDebugEnabled()) logger.debug("Committing transaction...");
        producer.commitTransaction();

        // controlRecordFuture information should be available now
        lastCommittedControlRecordMetadata = controlRecordFuture.get();
    }

    /**
     * Begins a transaction if no transaction is already in progress.
     * Uses member variable `transactionInProgress`.
     */
    private void checkAndBeginTransaction() {
        if (transactionInProgress.get()) return;
        synchronized (this) {
            if (!transactionInProgress.get()) {
                beginTransaction();
                transactionInProgress.set (true);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Future<RecordMetadata> send (ProducerRecord record) throws Exception {
        Future<RecordMetadata> future = super.send(record);
        futuresList.add(future);
        return future;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public boolean processTuple(ProducerRecord producerRecord) throws Exception {
        logger.trace("Sending: " + producerRecord);
        // send always within a transaction
        checkAndBeginTransaction();
        this.send(producerRecord);
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
            if (logger.isDebugEnabled()) logger.debug("ConsumerRecords: " + records);
            Iterator<?> it = records.iterator();
            while(it.hasNext()) {
                ConsumerRecord record = (ConsumerRecord)it.next();
                Headers headers = record.headers();
                if (logger.isDebugEnabled()) logger.debug("Headers: " + headers);
                String tid = new String(headers.lastHeader(TRANSACTION_ID).value(), StandardCharsets.UTF_8);
                if (logger.isDebugEnabled()) logger.debug("Checking tid=" + tid + " (currentTid=" + getTransactionalId() + ")");
                if(tid.equals(getTransactionalId())) {
                    committedSeqId = Long.valueOf(new String(headers.lastHeader(COMMITTED_SEQUENCE_ID).value(), StandardCharsets.UTF_8));
                }
            }

            consumerAtEnd = isConsumerAtEnd(consumer, endOffsets);
            if (logger.isDebugEnabled()) logger.debug("consumerAtEnd=" + consumerAtEnd);
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
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- DRAIN");
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        long currentSequenceId = checkpoint.getSequenceId();
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- CHECKPOINT id=" + currentSequenceId);
        // when we checkpoint, we must have a transaction. open a transaction if not yet done ...
        checkAndBeginTransaction();

        if (logger.isDebugEnabled()) logger.debug("currentSequenceId=" + currentSequenceId + ", lastSuccessSequenceId=" + lastSuccessfulSequenceId);
        boolean doCommit = true;
        // check if this is a new transaction
        if(currentSequenceId - lastSuccessfulSequenceId > 1) {
            long committedSequenceId = getCommittedSequenceId();
            if (logger.isDebugEnabled()) logger.debug("committedSequenceId=" + committedSequenceId);

            if(lastSuccessfulSequenceId < committedSequenceId) {
                if (logger.isDebugEnabled()) logger.debug("Aborting transaction due to lastSuccessfulSequenceId < committedSequenceId");
                // If the last successful sequence ID is less than
                // the committed sequence ID, this transaction has
                // been processed before and is a duplicate.
                // Discard this transaction.
                abortTransaction();
                doCommit = false;
                lastSuccessfulSequenceId = committedSequenceId;
            }
        }

        if (logger.isDebugEnabled()) logger.debug("doCommit = " + doCommit);
        if(doCommit) {
            commitTransaction(checkpoint.getSequenceId());
            lastSuccessfulSequenceId = checkpoint.getSequenceId();   

            TopicPartition tp = new TopicPartition(lastCommittedControlRecordMetadata.topic(), lastCommittedControlRecordMetadata.partition());
            controlTopicInitialOffsets.put(tp, lastCommittedControlRecordMetadata.offset());
        }
        transactionInProgress.set (false);
        // save the last successful seq ID
        if (logger.isDebugEnabled()) logger.debug("Checkpointing lastSuccessfulSequenceId: " + lastSuccessfulSequenceId);
        checkpoint.getOutputStream().writeLong(lastSuccessfulSequenceId);

        // save the control topic offsets
        if (logger.isDebugEnabled()) logger.debug("Checkpointing control topic offsets: " + controlTopicInitialOffsets);
        checkpoint.getOutputStream().writeObject(controlTopicInitialOffsets);

        if (!lazyTransactionBegin) {
            // start a new transaction
            checkAndBeginTransaction();
        }
    }

    /**
     * Tries to cancel all send requests that are not yet done.
     */
    @Override
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- trying to cancel requests");
        int nCancelled = 0;
        for (Future<RecordMetadata> future : futuresList) {
            if (!future.isDone() && future.cancel (mayInterruptIfRunning)) ++nCancelled;
        }
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- number of cancelled send requests: " + nCancelled); //$NON-NLS-1$
        futuresList.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void reset(Checkpoint checkpoint) throws Exception {
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- RESET id=" + checkpoint.getSequenceId());
        lastSuccessfulSequenceId = checkpoint.getInputStream().readLong();
        if (logger.isDebugEnabled()) logger.debug("Reset lastSuccessfulSequenceId: " + lastSuccessfulSequenceId);        

        controlTopicInitialOffsets = (HashMap<TopicPartition, Long>)checkpoint.getInputStream().readObject();
        if (logger.isDebugEnabled()) logger.debug("Reset controlTopicInitialOffsets: " + lastSuccessfulSequenceId);

        // check 'transactionInProgress' for true and set atomically to false
        if (transactionInProgress.compareAndSet (true, false)) {
            // abort the current transaction
            abortTransaction();
        }
        if (!lazyTransactionBegin) {
            // start a new transaction; `transactionInProgress` is false. 
            checkAndBeginTransaction();
        }
        setSendException(null);
    }
}
