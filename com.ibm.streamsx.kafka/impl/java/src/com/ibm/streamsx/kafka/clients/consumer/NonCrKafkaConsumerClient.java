/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Kafka consumer client to be used when not in a consistent region.
 */
public class NonCrKafkaConsumerClient extends AbstractNonCrKafkaConsumerClient implements ConsumerRebalanceListener {

    private static final Logger logger = Logger.getLogger(NonCrKafkaConsumerClient.class);

    private boolean autoCommitEnabled;
    private long commitCount = 500l; 
    private long nSubmittedRecords = 0l;
    private OffsetManager offsetManager = null;

    /**
     * Constructs a new NonCrKafkaConsumerClient.
     * 
     * @param operatorContext the operator context
     * @param keyClass the key class for Kafka messages
     * @param valueClass the value class for Kafka messages
     * @param commitCount the tuple count after which offsets are committed. This parameter is ignored when auto-commit is explicitly enabled.
     * @param kafkaProperties Kafka properties
     * 
     * @throws KafkaConfigurationException
     */
    private <K, V> NonCrKafkaConsumerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, int nTopics) throws KafkaConfigurationException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);

        ConsistentRegionContext crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        if (crContext != null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' is used in a consistent region. This consumer client implementation (" 
                    + this.getClass() + ") does not support CR.");
        }
         // if not explicitly configured, disable auto commit
        // TODO: remove this code; auto commit is always disabled by consumer base class
        if (kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            autoCommitEnabled = kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true");
        }
        else {
            kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            autoCommitEnabled = false;
        }
        if (!autoCommitEnabled) {
            offsetManager = new OffsetManager ();
        }
        // if no partition assignment strategy is specified, set the round-robin when nTopics > 1
        if (!kafkaProperties.containsKey (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG) && nTopics > 1) {
            String assignmentStrategy = RoundRobinAssignor.class.getCanonicalName();
            kafkaProperties.put (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
            logger.info (MessageFormat.format ("Multiple topics specified. Using the ''{0}'' partition assignment strategy for group management", assignmentStrategy));
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        super.startConsumer();
        // offsetManager is null when auto-commit is enabled
        if (offsetManager != null) {
            offsetManager.setOffsetConsumer(getConsumer());
        }
    }

    /**
     * @param commitCount the commitCount to set
     */
    public void setCommitCount(long commitCount) {
        this.commitCount = commitCount;
    }

    /**
     * see {@link AbstractKafkaConsumerClient#validate()}
     */
    @Override
    protected void validate() throws Exception {
        if (autoCommitEnabled) {
            logger.warn (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + " is 'true'. 'commitCount' parameter - id set - is ignored.");
        }
        else {
            if (commitCount <= 0) {
                throw new KafkaConfigurationException (Messages.getString ("INVALID_PARAMETER_VALUE_GT", new Integer(0)));
            }
        }
    }

    /**
     * Subscribes to topics or assigns with topic partitions.
     * Subscription happens when a) partitions is null or empty AND startPosition is StartPosition.Default.
     * In all other cases the consumer gets assigned. When partitions are assigned, the consumer is seeked
     * to the given start position (begin or end of the topic partitions).
     * @param topics  the topics
     * @param partitions partitions. The partitions. can be null or empty. Then the metadata of the topics is read to get all partitions of each topic.
     * @param startPosition Must be StartPosition.Default, StartPosition.Beginning, or StartPosition.End.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.Collection, java.util.Collection, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
        logger.debug("subscribeToTopics: topics=" + topics + ", partitions=" + partitions + ", startPosition=" + startPosition);
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;

        if(topics != null && !topics.isEmpty()) {
            if (!isGroupIdGenerated() && (partitions == null || partitions.isEmpty()) && startPosition == StartPosition.Default) {
                subscribe (topics, this);
            }
            else {
                Set<TopicPartition> partsToAssign;
                if (partitions == null || partitions.isEmpty()) {
                    // no partition information provided
                    partsToAssign = getAllTopicPartitionsForTopic(topics);
                }
                else {
                    partsToAssign = new HashSet<TopicPartition>();
                    topics.forEach(topic -> {
                        partitions.forEach(partition -> partsToAssign.add(new TopicPartition(topic, partition)));
                    });
                }    
                assign (partsToAssign);
                seekToPosition (partsToAssign, startPosition);
            }
        }
    }

    /**
     * assigns to topic partitions and seeks to the nearest offset given by a timestamp.
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
        assign (topicPartitionTimestampMap.keySet());
        seekToTimestamp (topicPartitionTimestampMap);
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
    public void subscribeToTopicsWithOffsets (String topic, List<Integer> partitions, List<Long> startOffsets) throws Exception {
        if(partitions.size() != startOffsets.size())
            throw new IllegalArgumentException("The number of partitions and the number of offsets must be equal");

        Map<TopicPartition, Long> topicPartitionOffsetMap = new HashMap<TopicPartition, Long>();
        int i = 0;
        for (int partitionNo: partitions) {
            topicPartitionOffsetMap.put(new TopicPartition(topic, partitionNo), startOffsets.get(i++));
        }
        assignToPartitionsWithOffsets(topicPartitionOffsetMap);
    }


    /**
     * Callback method of the ConsumerRebalanceListener
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        getOperatorContext().getMetrics().getCustomMetric (N_PARTITION_REBALANCES).increment();
        logger.info("onPartitionsRevoked: old partition assignment = " + partitions);
    }

    /**
     * Callback method of the ConsumerRebalanceListener
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsAssigned: new partition assignment = " + partitions);
        Set<TopicPartition> previousAssignment = new HashSet<>(getAssignedPartitions());
        getAssignedPartitions().clear();
        getAssignedPartitions().addAll(partitions);
        nAssignedPartitions.setValue(partitions.size());
        // When auto-commit is disabled and the assigned partitions change, 
        // we should remove the messages from the revoked partitions from the message queue.
        // The queue contains only uncommitted messages in this case.
        // With auto-commit enabled, the message queue can also contain committed messages - do not remove!
        if (!autoCommitEnabled) {
            Set<TopicPartition> gonePartitions = new HashSet<>(previousAssignment);
            gonePartitions.removeAll(partitions);
            logger.info("topic partitions that are not assigned anymore: " + gonePartitions);
            if (!gonePartitions.isEmpty()) {
                logger.info("removing consumer records from gone partitions from message queue");
                getMessageQueue().removeIf(queuedRecord -> belongsToPartition (queuedRecord, gonePartitions));
                // remove the topic partition also from the offset manager
                synchronized (offsetManager) {
                    for (TopicPartition tp: gonePartitions) {
                        offsetManager.remove(tp.topic(), tp.partition());
                    }
                }
            }
        }
        try {
            checkSpaceInMessageQueueAndPauseFetching (true);
        } catch (IllegalStateException | InterruptedException e) {
            // IllegalStateException cannot happen
            // On Interruption, do nothing
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processUpdateAssignmentEvent(com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate)
     */
    @Override
    protected void processUpdateAssignmentEvent(TopicPartitionUpdate update) {
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
                // No need to update offset manager here, like adding topics, etc. Missing topics are auto-created
                break;
            case REMOVE:
                update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
                    currentTopicPartitionOffsets.remove(tp);
                });
                // remove messages of removed topic partitions from the message queue
                getMessageQueue().removeIf (record -> belongsToPartition (record, update.getTopicPartitionOffsetMap().keySet()));
                if (offsetManager != null) {
                    // when auto-commit is enabled, there is no offset manager
                    // remove removed partitions from offset manager. We can't commit offsets for those partitions we are not assigned any more.
                    synchronized (offsetManager) {
                        update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
                            offsetManager.remove (tp.topic(), tp.partition());
                        });
                    }
                }
                // we can end up here with an empty map after removal of assignments.
                assignToPartitionsWithOffsets (currentTopicPartitionOffsets);
                break;
            default:
                throw new Exception ("processUpdateAssignmentEvent(): unimplemented action: " + update.getAction());
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new RuntimeException (e);
        }
    }

    /**
     * This implementation counts submitted tuples and commits the offsets when {@link #commitCount} has reached and auto-commit is disabled. 
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#postSubmit(org.apache.kafka.clients.consumer.ConsumerRecord)
     */
    @Override
    public void postSubmit (ConsumerRecord<?, ?> submittedRecord) {
        if (autoCommitEnabled) return;
        // collect submitted offsets per topic partition for periodic commit.
        try {
            synchronized (offsetManager) {
                offsetManager.savePosition(submittedRecord.topic(), submittedRecord.partition(), submittedRecord.offset() +1l, /*autoCreateTopci=*/true);
            }
        } catch (Exception e) {
            // is not caught when autoCreateTopic is 'true'
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException (e);
        }
        if (++nSubmittedRecords  >= commitCount) {
            if (logger.isDebugEnabled()) {
                logger.debug("commitCount (" + commitCount + ") reached. Preparing to commit offsets ...");
            }
            // commit asynchronous, partition by partition.
            // asynchronous commit implies that the operator is not restarted when commit fails.
            final boolean commitSync = false;
            final boolean commitPartitionWise = true;
            CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);

            synchronized (offsetManager) {
                for (TopicPartition tp: offsetManager.getMappedTopicPartitions()) {
                    offsets.put (tp, offsetManager.getOffset(tp.topic(), tp.partition()));
                }
                offsetManager.clear();
            }
            try {
                nSubmittedRecords = 0l;
                if (!offsets.isEmpty()) {
                    // sendCommitEvent terminates the poll loop, throws InterruptedException:
                    sendCommitEvent (offsets);
                    // when committing offsets for one partition fails, the reason can be that we are not 
                    // assigned to the partition any more when building a consumer group.
                    // Then a different (or the same) consumer starts reading the records again creating duplicates within the application.
                    // This is normal Kafka methodology.
                    sendStartPollingEvent();
                }
            } catch (InterruptedException e) {
                // is not thrown when asynchronously committed; can be silently ignored.
                // Only when we decide to change to synchronous commit, we can end up here.
                // Then it is ok, NOT to start polling again. 
            }
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long)
     */
    @Override
    protected int pollAndEnqueue (long pollTimeout, boolean isThrottled) throws InterruptedException, SerializationException {
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
     * The builder for the consumer client following the builder pattern.
     */
    public static class Builder {

        private OperatorContext operatorContext;
        private Class<?> keyClass;
        private Class<?> valueClass;
        private KafkaOperatorProperties kafkaProperties;
        private long pollTimeout;
        private long commitCount;
        private int numTopics = 0;

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

        public final Builder setValueClass(Class<?> c) {
            this.valueClass = c;
            return this;
        }

        public final Builder setPollTimeout (long t) {
            this.pollTimeout = t;
            return this;
        }

        public final Builder setCommitCount (long t) {
            this.commitCount = t;
            return this;
        }

        public final Builder setNumTopics (int n) {
            this.numTopics = n;
            return this;
        }

        public ConsumerClient build() throws Exception {
            NonCrKafkaConsumerClient client = new NonCrKafkaConsumerClient (operatorContext, keyClass, valueClass, kafkaProperties, numTopics);
            client.setPollTimeout (pollTimeout);
            client.setCommitCount (commitCount);
            return client;
        }
    }
}
