/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Kafka consumer client to be used when group management is active, and the operator is not in a consistent region.
 *
 * @author The IBM Kafka toolkit team
 */
public class NonCrKafkaConsumerGroupClient extends AbstractNonCrKafkaConsumerClient implements ConsumerRebalanceListener {

    private static final Logger trace = Logger.getLogger(NonCrKafkaConsumerGroupClient.class);


    /**
     * Creates a new NonCrKafkaConsumerGroupClient instance
     * 
     * @param operatorContext the operator context
     * @param keyClass the key class for Kafka messages
     * @param valueClass the value class for Kafka messages
     * @param commitCount the tuple count after which offsets are committed. This parameter is ignored when auto-commit is explicitly enabled.
     * @param kafkaProperties Kafka properties
     * @param nTopics the number of subscribed topics
     * 
     * @throws KafkaConfigurationException
     */
    private <K, V> NonCrKafkaConsumerGroupClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, int nTopics) throws KafkaConfigurationException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);

        // if no partition assignment strategy is specified, set the round-robin when nTopics > 1
        if (!kafkaProperties.containsKey (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG) && nTopics > 1) {
            String assignmentStrategy = RoundRobinAssignor.class.getCanonicalName();
            kafkaProperties.put (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
            trace.info (MessageFormat.format ("Multiple topics specified. Using the ''{0}'' partition assignment strategy for group management", assignmentStrategy));
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
        trace.info (MessageFormat.format ("subscribeToTopics: topics = {0}, partitions = {1}, startPosition = {2}",
                topics, partitions, startPosition));
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        if (partitions != null && !partitions.isEmpty()) {
            trace.error("When the " + getThisClassName() + " consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }
        // the operator instantiates this client only, when startPosition is Default.
        // Other startPositions (Begin, End) must be implemented.
        assert startPosition == StartPosition.Default;
        subscribe (topics, this);
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
        // Never called. Group management with startPosition == StartPosition.Time must be implemented
        trace.info ("subscribeToTopicsWithTimestamp: topic = " + topics + ", partitions = " + partitions + ", timestamp = " + timestamp);
        Map<TopicPartition, Long /* timestamp */> topicPartitionTimestampMap = new HashMap<TopicPartition, Long>();
        if(partitions == null || partitions.isEmpty()) {
            Set<TopicPartition> topicPartitions = getAllTopicPartitionsForTopic(topics);
            topicPartitions.forEach(tp -> topicPartitionTimestampMap.put(tp, timestamp));
        } else {
            topics.forEach(topic -> {
                partitions.forEach(partition -> topicPartitionTimestampMap.put(new TopicPartition(topic, partition), timestamp));
            });
        }
        trace.debug("subscribeToTopicsWithTimestamp: topicPartitionTimestampMap = " + topicPartitionTimestampMap);
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
        trace.debug ("subscribeToTopicsWithOffsets: topic = " + topic + ", partitions = " + partitions + ", startOffsets = " + startOffsets);
        throw new KafkaConfigurationException ("Subscription (assignment of partitions) with offsets is not supported by this client: " + getThisClassName());
    }


    /**
     * Callback method of the ConsumerRebalanceListener
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
     */
    @Override
    public void onPartitionsRevoked (Collection<TopicPartition> partitions) {
        getOperatorContext().getMetrics().getCustomMetric (N_PARTITION_REBALANCES).increment();
        trace.info("onPartitionsRevoked: old partition assignment = " + partitions);
        // remove the content of the queue. It contains uncommitted messages.
        // They will fetched again after rebalance.
        getMessageQueue().clear();
        OffsetManager offsetManager = getOffsetManager();
        try {
            awaitMessageQueueProcessed();
            // the post-condition is, that all messages from the queue have submitted as 
            // tuples and its offsets +1 are stored in OffsetManager.
            final boolean commitSync = true;
            final boolean commitPartitionWise = false;
            CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);
            synchronized (offsetManager) {
                Set <TopicPartition> partitionsInOffsetManager = offsetManager.getMappedTopicPartitions();
                for (TopicPartition tp: partitions) {
                    if (partitionsInOffsetManager.contains (tp)) {
                        offsets.put (tp, offsetManager.getOffset (tp.topic(), tp.partition()));
                    }
                }
            }
            if (!offsets.isEmpty()) {
                commitOffsets (offsets);
            }
            // reset the counter for periodic commit
            setnSubmittedRecords (0l);
        }
        catch (InterruptedException | RuntimeException e) {
            // Ignore InterruptedException, RuntimeException from commitOffsets is already traced.
        }
        finally {
            offsetManager.clear();
        }
    }

    /**
     * Callback method of the ConsumerRebalanceListener
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
     */
    @Override
    public void onPartitionsAssigned (Collection<TopicPartition> partitions) {
        trace.info("onPartitionsAssigned: new partition assignment = " + partitions);
        getAssignedPartitions().clear();
        getAssignedPartitions().addAll(partitions);
        nAssignedPartitions.setValue(partitions.size());
        OffsetManager offsetManager = getOffsetManager();
        offsetManager.clear();
        try {
            checkSpaceInMessageQueueAndPauseFetching (true);
        } catch (IllegalStateException | InterruptedException e) {
            // IllegalStateException cannot happen
            // On Interruption, do nothing
        }
    }


    /**
     * Assignments cannot be updated.
     * This method should not be called because operator control port and this client implementation are incompatible.
     * A context check should exist to detect this mis-configuration.
     * We only log the method call. 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processUpdateAssignmentEvent(com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate)
     */
    @Override
    protected void processUpdateAssignmentEvent (TopicPartitionUpdate update) {
        trace.warn("processUpdateAssignmentEvent(): update = " + update + "; update of assignments not supported by this client: " + getThisClassName());
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
            NonCrKafkaConsumerGroupClient client = new NonCrKafkaConsumerGroupClient (operatorContext, keyClass, valueClass, kafkaProperties, numTopics);
            client.setPollTimeout (pollTimeout);
            client.setCommitCount (commitCount);
            return client;
        }
    }
}
