package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class represents a consumer client that can be used in a consistent region.
 * It always assigns to topic partitions. Group coordination is disabled.
 */
public class CrKafkaStaticAssignConsumerClient extends AbstractKafkaConsumerClient {

    private static final Logger logger = Logger.getLogger(CrKafkaStaticAssignConsumerClient.class);

    private long triggerCount; 
    private long nSubmittedRecords = 0l;
    private ConsistentRegionContext crContext;
    private OffsetManager offsetManager;
    private ControlVariableAccessor<String> offsetManagerCV;
    //    private Collection<Integer> partitions;


    private <K, V> CrKafkaStaticAssignConsumerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {

        super (operatorContext, keyClass, valueClass, kafkaProperties);

        this.crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        if (crContext == null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' must be used in a consistent region. This consumer client implementation (" 
                    + this.getClass() + ") requires a consistent region context.");
        }
        // always disable auto commit - we commit on drain
        if (kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            if (kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true")) {
                logger.warn("consumer config '" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "' has been turned to 'false'. In a consistent region, offsets are always committed when the region drains.");
            }
        }
        else {
            logger.info("consumer config '" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "' has been set to 'false' for CR.");
        }
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        offsetManager = new OffsetManager();
        //    this.partitions = partitions == null ? Collections.emptyList() : partitions;
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        super.startConsumer();
        // now we have a consumer object.
        offsetManager.setOffsetConsumer(getConsumer());
    }

    /**
     * sets the trigger count parameter.
     * This parameter specifies the number of tuples after which the consistent region 
     * is made consistent when the operator is its trigger operator.
     * @param triggerCount
     */
    public void setTriggerCount (long triggerCount) {
        this.triggerCount = triggerCount;
    }



    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#validate()
     */
    @Override
    protected void validate() throws Exception {
        // empty
    }

    /**
     * Retrieves the Offset Manager from the JCP control variable.
     * @return  an OffsetManager object
     * @throws Exception
     */
    private OffsetManager getDeserializedOffsetManagerCV() throws Exception {
        return SerializationUtils.deserialize(Base64.getDecoder().decode (offsetManagerCV.sync().getValue()));
    }

    /**
     * Creates an operator-scoped JCP control variable and stores the Offset manager in serialized format.
     * @throws Exception
     */
    private void createJcpCvFromOffsetManagerl() throws Exception {
        logger.info("createJcpCvFromOffsetManagerl(). offsetManager = " + offsetManager); 
        ControlPlaneContext controlPlaneContext = getOperatorContext().getOptionalContext(ControlPlaneContext.class);
        offsetManagerCV = controlPlaneContext.createStringControlVariable(OffsetManager.class.getName(),
                false, serializeObject(offsetManager));
        OffsetManager mgr = getDeserializedOffsetManagerCV();
        logger.debug("Retrieved value for offsetManagerCV = " + mgr); 
    }


    /**
     * Assigns with topic partitions and seeks to the given start position.
     * @param topics  the topics
     * @param partitions partitions. The partitions. can be null or empty. Then the metadata of the topics is read to get all partitions of each topic.
     * @param startPosition Must be StartPosition.Default, StartPosition.Beginning, or StartPosition.End.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.Collection, java.util.Collection, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics(Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
        logger.debug("subscribeToTopics: topics=" + topics + ", partitions=" + partitions + ", startPosition=" + startPosition);
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;

        if(topics != null && !topics.isEmpty()) {
            Set<TopicPartition> partsToAssign;
            if(partitions == null || partitions.isEmpty()) {
                // read meta data of the given topics to fetch all topic partitions
                partsToAssign = getAllTopicPartitionsForTopic (topics);
            } else {
                partsToAssign = new HashSet<TopicPartition>();
                topics.forEach(topic -> {
                    partitions.forEach(partition -> partsToAssign.add(new TopicPartition(topic, partition)));
                });
            }
            assign(partsToAssign);
            if(startPosition != StartPosition.Default) {
                seekToPosition(partsToAssign, startPosition);
            }
            // update the offset manager
            offsetManager.addTopics (partsToAssign);
            // save the consumer offsets after moving it's position
            offsetManager.savePositionFromCluster();
            createJcpCvFromOffsetManagerl();
        }
    }

    /**
     * Assigns to topic partitions and seeks to the nearest offset given by a timestamp.
     *
     * @param topics         the topics
     * @param partitions     partition numbers. Every given topic must have the given partition numbers.
     * @param timestamp      the timestamp where to start reading in milliseconds since Epoch.
     * @throws Exception 
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.Collection, java.util.Collection, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Collection<String> topics, Collection<Integer> partitions, long timestamp) throws Exception {
        logger.debug("subscribeToTopicsWithTimestamp: topic = " + topics + ", partitions = " + partitions + ", timestamp = " + timestamp);
        Map<TopicPartition, Long /* timestamp */> topicPartitionTimestampMap = new HashMap<TopicPartition, Long>();
        if(partitions == null || partitions.isEmpty()) {
            Set<TopicPartition> topicPartitions = getAllTopicPartitionsForTopic(topics);
            topicPartitions.forEach(tp -> topicPartitionTimestampMap.put(tp, timestamp));
        } else {
            topics.forEach(topic -> {
                partitions.forEach(partition -> topicPartitionTimestampMap.put(new TopicPartition(topic, partition), timestamp));
            });
        }
        logger.debug("subscribeToTopicsWithTimestamp: topicPartitionTimestampMap = " + topicPartitionTimestampMap);
        Set<TopicPartition> partsToAssign = topicPartitionTimestampMap.keySet();
        assign (partsToAssign);
        seekToTimestamp (topicPartitionTimestampMap);
        // update the offset manager
        offsetManager.addTopics (partsToAssign);
        // save the consumer offsets after moving it's position
        offsetManager.savePositionFromCluster();
        createJcpCvFromOffsetManagerl();
    }

    /**
     * Assigns to topic partitions and seeks to the given offsets.
     * A single topic can be specified. The collections for partitions and offsets must have equal size.
     * 
     * @param topic the topic
     * @param partitions the partitions of the topic
     * @param startOffsets the offsets to seek
     *  
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithOffsets(java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public void subscribeToTopicsWithOffsets(String topic, List<Integer> partitions, List<Long> startOffsets) throws Exception {
        if(partitions.size() != startOffsets.size())
            throw new IllegalArgumentException("The number of partitions and the number of offsets must be equal");

        Map<TopicPartition, Long> topicPartitionOffsetMap = new HashMap<TopicPartition, Long>();
        int i = 0;
        for (int partitionNo: partitions) {
            topicPartitionOffsetMap.put(new TopicPartition(topic, partitionNo), startOffsets.get(i++));
        }
        // assign and seek:
        assignToPartitionsWithOffsets (topicPartitionOffsetMap);
        // update the offset manager
        offsetManager.addTopics (topicPartitionOffsetMap.keySet());
        // save the consumer offsets after moving it's position
        offsetManager.savePositionFromCluster();
        createJcpCvFromOffsetManagerl();
    }

    /**
     * Saves the offset +1 to the offset manager and, if the operator is trigger operator of a consistent region, 
     * makes the region consistent when the tuple counter reached the trigger count member variable.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#postSubmit(org.apache.kafka.clients.consumer.ConsumerRecord)
     */
    @Override
    public void postSubmit (ConsumerRecord<?, ?> submittedRecord) {
        // save offset for *next* record for {topic, partition} 
        try {
            synchronized (offsetManager) {
                offsetManager.savePositionWhenRegistered (submittedRecord.topic(), submittedRecord.partition(), submittedRecord.offset() +1l);
            }
        } catch (Exception e) {
            // the topic/partition can have been removed when a partition has been removed via updateAssignment (control port method)
            logger.warn (e.getLocalizedMessage());
        }
        if (crContext.isTriggerOperator() && ++nSubmittedRecords >= triggerCount) {
            logger.debug("Making region consistent..."); //$NON-NLS-1$
            // makeConsistent blocks until all operators in the CR have drained and checkpointed
            boolean isSuccess = crContext.makeConsistent();
            nSubmittedRecords = 0l;
            logger.debug("Completed call to makeConsistent: isSuccess=" + isSuccess); //$NON-NLS-1$
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#updateAssignment(com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate)
     */
    @Override
    protected void updateAssignment(TopicPartitionUpdate update) {
        // trace with info. to see this method call is important, and it happens not frequently.
        logger.info ("updateAssignment(): update = " + update);
        try {
            // create a map of current topic partitions and their offsets
            Map<TopicPartition, Long /* offset */> currentTopicPartitionOffsets = new HashMap<TopicPartition, Long>();

            Set<TopicPartition> topicPartitions = getConsumer().assignment();
            topicPartitions.forEach(tp -> currentTopicPartitionOffsets.put(tp, getConsumer().position(tp)));

            switch (update.getAction()) {
            case ADD:
                update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
                    // offset can be -2, -1, or a valid offset o >= 0
                    // -2 means 'seek to beginning', -1 means 'seek to end'
                    currentTopicPartitionOffsets.put(tp, offset);
                });
                assignToPartitionsWithOffsets (currentTopicPartitionOffsets);
                synchronized (offsetManager) {  // avoid concurrent access with tuple submission thread
                    // update offset manager: add topics or updates their partition lists
                    offsetManager.updateTopics (currentTopicPartitionOffsets.keySet());
                    // save the consumer offsets after moving it's position
                    offsetManager.savePositionFromCluster();
                    createJcpCvFromOffsetManagerl();
                }
                break;
            case REMOVE:
                update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
                    currentTopicPartitionOffsets.remove(tp);
                });
                // remove messages of removed topic partitions from the message queue; we would not be able to commit them later
                getMessageQueue().removeIf (record -> belongsToPartition (record, update.getTopicPartitionOffsetMap().keySet()));
                // remove removed partitions from offset manager.
                synchronized (offsetManager) {  // avoid concurrent access with tuple submission thread
                    update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
                        offsetManager.remove (tp.topic(), tp.partition());
                    });
                    assignToPartitionsWithOffsets (currentTopicPartitionOffsets);
                    // save the consumer offsets after moving it's position
                    offsetManager.savePositionFromCluster();
                    createJcpCvFromOffsetManagerl();
                }
                break;
            default:
                throw new Exception ("updateAssignment: unimplemented action: " + update.getAction());
            }
        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        } 
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long)
     */
    @Override
    protected int pollAndEnqueue (long pollTimeout) throws InterruptedException, SerializationException {
        if (logger.isTraceEnabled()) logger.trace("Polling for records..."); //$NON-NLS-1$
        ConsumerRecords<?, ?> records = getConsumer().poll (pollTimeout);
        int numRecords = records == null? 0: records.count();
        if (logger.isTraceEnabled() && numRecords == 0) logger.trace("# polled records: " + (records == null? "0 (records == null)": "0"));
        if (numRecords > 0) {
            if (logger.isDebugEnabled()) logger.debug("# polled records: " + numRecords);
            records.forEach(cr -> {
                if (logger.isTraceEnabled()) {
                    logger.trace (cr.topic() + "-" + cr.partition() + " key=" + cr.key() + " - offset=" + cr.offset()); //$NON-NLS-1$
                }
                getMessageQueue().add(cr);
            });
        }
        return numRecords;
    }


    /**
     * Called when the consistent region is drained.
     * This implementation commits offsets on drain.
     */
    @Override
    public void onDrain() throws Exception {
        logger.debug("onDrain() - entering");
        final boolean commitSync = true;
        final boolean commitPartitionWise = false;   // commit all partitions in one server request
        CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);

        synchronized (offsetManager) {
            for (TopicPartition tp: offsetManager.getMappedTopicPartitions()) {
                offsets.put (tp, offsetManager.getOffset(tp.topic(), tp.partition()));
            }
        }
        try {
            // Here we must terminates the poll loop.
            if (offsets.isEmpty()) {
                sendStopPollingEvent();
            }
            else {
                // commit event stops polling before committing
                sendCommitEvent (offsets);
            }
            // drain is followed by checkpoint. 
            // Don't poll for new messages in the meantime. - Don't send a 'start polling event'
        } catch (InterruptedException e) {
            logger.info("Interrupted waiting for committing offsets");
            // NOT to start polling for Kafka messages again, is ok after interruption
        }
        logger.debug("onDrain() - exiting");
    }


    /**
     * Resets the client to an initial state when no checkpoint is available.
     */
    @Override
    protected void resetToInitialState() {
        logger.debug("resetToInitialState() - entering");
        try {
            offsetManager = getDeserializedOffsetManagerCV();
            offsetManager.setOffsetConsumer (getConsumer());
            logger.debug("offsetManager=" + offsetManager); //$NON-NLS-1$

            // refresh from the cluster as we may
            // have written to the topics
            refreshFromCluster();

            // remove records from queue
            getMessageQueue().clear();

        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        logger.debug("resetToInitialState() - exiting");
    }

    /**
     * 1. get all partitions for each topic in the offset manager from the server.
     * 2. assign and seek to the partitions/offsets from the offset manager.
     * 
     * The offset manager must contain fetch positions for all previous assignments.
     * This is typically achieved via {@link OffsetManager#savePositionFromCluster()}
     */
    private void refreshFromCluster() {
        logger.debug("Refreshing from cluster..."); //$NON-NLS-1$
        List<String> topics = offsetManager.getTopics();
        Map<TopicPartition, Long> startOffsetMap = new HashMap<TopicPartition, Long>();
        for (String topic : topics) {
            List<PartitionInfo> parts = getConsumer().partitionsFor(topic);
            Set<Integer> registeredPartitionNumbers = offsetManager.getRegisteredPartitionNumbers (topic);
            parts.forEach(pi -> {
                if (registeredPartitionNumbers.contains(pi.partition())) {
                    TopicPartition tp = new TopicPartition(pi.topic(), pi.partition());
                    long startOffset = offsetManager.getOffset(pi.topic(), pi.partition());
                    if(startOffset > -1l) {
                        // start offset should never be -1 because the offset manager has been refreshed from the broker
                        startOffsetMap.put(tp, startOffset);
                    }
                    else {
                        logger.warn("refreshFromCluster(): invalid offset retrieved from offset manager. "
                                + "topic = " + pi.topic() + "; partition = " + pi.partition() + "; offset = " + startOffset);
                    }
                }
            });
        }
        logger.debug("startOffsets=" + startOffsetMap); //$NON-NLS-1$

        // assign the consumer to the partitions and seek to the
        // last saved offset
        getConsumer().assign(startOffsetMap.keySet());
        for (Entry<TopicPartition, Long> entry : startOffsetMap.entrySet()) {
            logger.debug("Consumer seeking: TopicPartition=" + entry.getKey() + ", new_offset=" + entry.getValue()); //$NON-NLS-1$ //$NON-NLS-2$
            getConsumer().seek(entry.getKey(), entry.getValue());
        }
    }

    /** 
     * Resets the client to a previous state.
     * @param checkpoint the checkpoint that contains the previous state.
     */
    @Override
    protected void reset (Checkpoint checkpoint) {
        logger.debug("reset() - entering. seq = " + checkpoint.getSequenceId());
        try {
            offsetManager = (OffsetManager) checkpoint.getInputStream().readObject();
            offsetManager.setOffsetConsumer (getConsumer());
            refreshFromCluster();
            getMessageQueue().clear();
        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        logger.debug("reset() - exiting");
    }


    /**
     * Creates a checkpoint of the current state when used in consistent region.
     * Only the offset manager is included into the checkpoint.
     * @param the reference of the checkpoint object
     */
    @Override
    protected void checkpoint (Checkpoint checkpoint) {
        logger.debug("checkpoint() - entering. seq = " + checkpoint.getSequenceId());
        try {
            // offsetManager.savePositionFromCluster();
            checkpoint.getOutputStream().writeObject(offsetManager);
            if (logger.isDebugEnabled()) {
                logger.debug("offsetManager=" + offsetManager); //$NON-NLS-1$
            }
        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        logger.debug("checkpoint() - exiting");
    }



    /**
     * The builder for the consumer client following the builder pattern.
     */
    public static class Builder {

        private OperatorContext operatorContext;
        private Class<?> keyClass;
        private Class<?> valueClass;
        private KafkaOperatorProperties kafkaProperties;
        private long pollTimeout;
        private long triggerCount;

        public final Builder setOperatorContext(OperatorContext c) {
            this.operatorContext = c;
            return this;
        }

        public final Builder setKafkaProperties(KafkaOperatorProperties p) {
            this.kafkaProperties = p;
            return this;
        }

        public final Builder setKeyClass(Class<?> c) {
            this.keyClass = c;
            return this;
        }

        public final Builder setValueClass (Class<?> c) {
            this.valueClass = c;
            return this;
        }

        public final Builder setPollTimeout (long t) {
            this.pollTimeout = t;
            return this;
        }

        public final Builder setTriggerCount (long c) {
            this.triggerCount = c;
            return this;
        }

        public ConsumerClient build() throws Exception {
            CrKafkaStaticAssignConsumerClient client = new CrKafkaStaticAssignConsumerClient (operatorContext, keyClass, valueClass, kafkaProperties);
            client.setPollTimeout (pollTimeout);
            client.setTriggerCount (triggerCount);
            return client;
        }
    }
}
