/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.kafka.KafkaOperatorException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Kafka consumer client to be used when not in a consistent region and group management is off, for example, 
 * when partitions are specified. This client assigns partitions manually.
 */
public class NonCrKafkaConsumerClient extends AbstractNonCrKafkaConsumerClient {

    private static final Logger trace = Logger.getLogger(NonCrKafkaConsumerClient.class);
    private static final long JCP_CONNECT_TIMEOUT_MILLIS = 15000;


    /**
     * Constructs a new NonCrKafkaConsumerClient.
     * 
     * @param operatorContext the operator context
     * @param keyClass the key class for Kafka messages
     * @param valueClass the value class for Kafka messages
     * @param commitCount the tuple count after which offsets are committed. This parameter is ignored when auto-commit is explicitly enabled.
     * @param kafkaProperties Kafka properties
     * @throws KafkaOperatorException 
     */
    private <K, V> NonCrKafkaConsumerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws KafkaOperatorException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);
    }


    /**
     * Assigns given or all topic partitions.
     * The partitions can be null or empty. Then the metadata of the topics is read to get all partitions of each topic.
     * After partitions are assigned, the consumer is seeked to the given start position, (i.e. the fetch offset is overridden).
     *
     * @param topics         the topics
     * @param partitions     partition numbers. When not null or empty, every given topic must have the given partition numbers.
     * @param startPosition  Must be StartPosition.Default, StartPosition.Beginning, or StartPosition.End.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.Collection, java.util.Collection, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
        trace.debug("subscribeToTopics: topics=" + topics + ", partitions=" + partitions + ", startPosition=" + startPosition);
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        if (topics == null || topics.isEmpty()) return;

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
        if (getInitialStartPosition() != StartPosition.Default) {

            if (!testJobControlConnection (JCP_CONNECT_TIMEOUT_MILLIS)) {
                trace.warn ("A JobControlPlane operator cannot be connected. After PE relaunch the assigned partitions will be seeked to the startPosition " + startPosition
                        + ". To support fetching from last committed offset after PE relaunch, add a JobControlPlane operator to the application graph.");
            }
            if (getOperatorContext().getPE().getRelaunchCount() == 0 || !canUseJobControlPlane()) {
                seekToPosition (partsToAssign, startPosition);
            }
            else {
                // relaunch count > 0 && JCP detected, seek when partition not yet committed
                for (TopicPartition tp: partsToAssign) {
                    if (!isCommittedForPartition (tp)) {
                        seekToPosition (tp, startPosition);
                    }
                }
            }
        }
    }


    /**
     * Assigns topic partitions and seeks to the nearest offset given by a timestamp.
     * The partitions can be null or empty. Then the metadata of the topics is read to get all partitions of each topic.
     * After partitions are assigned, the consumer is seeked to the offset nearest to the given timestamp.
     *
     * @param topics         the topics
     * @param partitions     partition numbers. When not null or empty, every given topic must have the given partition numbers.
     * @param timestamp      the timestamp where to start reading in milliseconds since Epoch.
     * @throws Exception 
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.Collection, java.util.Collection, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Collection<String> topics, Collection<Integer> partitions, long timestamp) throws Exception {
        trace.debug("subscribeToTopicsWithTimestamp: topic = " + topics + ", partitions = " + partitions + ", timestamp = " + timestamp);
        if (topics == null || topics.isEmpty()) return;
        Map<TopicPartition, Long /* timestamp */> topicPartitionTimestampMap = new HashMap<>();
        if (partitions == null || partitions.isEmpty()) {
            Set<TopicPartition> topicPartitions = getAllTopicPartitionsForTopic (topics);
            topicPartitions.forEach(tp -> topicPartitionTimestampMap.put (tp, timestamp));
        } else {
            topics.forEach (topic -> {
                partitions.forEach (partition -> topicPartitionTimestampMap.put (new TopicPartition(topic, partition), timestamp));
            });
        }
        trace.debug("subscribeToTopicsWithTimestamp: topicPartitionTimestampMap = " + topicPartitionTimestampMap);
        final Set<TopicPartition> topicPartitions = topicPartitionTimestampMap.keySet();

        assign (topicPartitions);
        if (!testJobControlConnection (JCP_CONNECT_TIMEOUT_MILLIS)) {
            trace.warn ("A JobControlPlane operator cannot be connected. After PE relaunch the assigned partitions will be seeked to the startTime " + timestamp
                    + ". To support fetching from last committed offset after PE relaunch, add a JobControlPlane operator to the application graph.");
        }
        if (getOperatorContext().getPE().getRelaunchCount() == 0 || !canUseJobControlPlane()) {
            seekToTimestamp (topicPartitionTimestampMap);
        }
        else {
            // relaunch count > 0 && JCP detected, seek when partition not yet committed
            for (TopicPartition tp: topicPartitions) {
                if (!isCommittedForPartition (tp)) {
                    seekToTimestamp (tp, timestamp);
                }
            }
        }
    }


    /**
     * Assigns to topic partitions and seeks to the given offsets.
     * Only a single topic can be specified. The collections for partitions and offsets must have equal size.
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

        if (partitions == null || partitions.isEmpty()) return;

        Map<TopicPartition, Long> topicPartitionOffsetMap = new HashMap<TopicPartition, Long>();
        int i = 0;
        for (int partitionNo: partitions) {
            topicPartitionOffsetMap.put (new TopicPartition (topic, partitionNo), startOffsets.get(i++));
        }

        if (!testJobControlConnection (JCP_CONNECT_TIMEOUT_MILLIS)) {
            trace.warn ("A JobControlPlane operator cannot be connected. After PE relaunch the partitions will be seeked to the startOffsets. "
                    + "To support fetching from last committed offset after PE relaunch, add a JobControlPlane operator to the application graph.");
        }
        if (getOperatorContext().getPE().getRelaunchCount() == 0 || !canUseJobControlPlane()) {
            assignToPartitionsWithOffsets (topicPartitionOffsetMap);
        }
        else {
            // relaunch count > 0 && JCP detected, seek when partition not yet committed
            assign (topicPartitionOffsetMap.keySet());
            for (TopicPartition tp: topicPartitionOffsetMap.keySet()) {
                if (!isCommittedForPartition (tp)) {
                    getConsumer().seek (tp, topicPartitionOffsetMap.get (tp).longValue());
                }
            }
        }
    }




    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processUpdateAssignmentEvent(com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate)
     */
    @Override
    protected void processUpdateAssignmentEvent(TopicPartitionUpdate update) {
        try {
            // create a map of current topic partitions and their fetch offsets for next record
            Map<TopicPartition, Long /* offset */> currentTopicPartitionOffsets = new HashMap<TopicPartition, Long>();

            Set<TopicPartition> topicPartitions = getConsumer().assignment();
            topicPartitions.forEach(tp -> currentTopicPartitionOffsets.put(tp, getConsumer().position(tp)));
            OffsetManager offsetManager = getOffsetManager();
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
                // TODO: commit offsets of the removed partition(s)
                // For now, the problem is not so urgent as a 'subscription' with Default start position is not yet possible.
                // Whe we need to commit offsets here, the flow would be:
                // 1. remove messages of the removed topic partitions from the queue - they are all uncommitted
                // 2. wait that the queue gets processed - awaitMessageQueueProcessed();
                // 3. commit the offsets of the removed topic partitions
                // 4. remove the unassigned topic partitions from the offsetManager
                // 5. update the partition assignment in the consumer
                // remove messages of removed topic partitions from the message queue
                getMessageQueue().removeIf (record -> belongsToPartition (record, update.getTopicPartitionOffsetMap().keySet()));
                // remove removed partitions from offset manager. We can't commit offsets for those partitions we are not assigned any more.
                synchronized (offsetManager) {
                    update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
                        offsetManager.remove (tp.topic(), tp.partition());
                    });
                }
                // we can end up here with an empty map after removal of assignments.
                assignToPartitionsWithOffsets (currentTopicPartitionOffsets);
                break;
            default:
                throw new Exception ("processUpdateAssignmentEvent(): unimplemented action: " + update.getAction());
            }
        } catch (Exception e) {
            trace.error(e.getLocalizedMessage(), e);
            throw new RuntimeException (e);
        }
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
        private StartPosition initialStartPosition;
        private CommitMode commitMode;
        private long commitPeriodMillis;

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

        public final Builder setCommitMode (CommitMode m) {
            this.commitMode = m;
            return this;
        }

        public final Builder setCommitPeriod (double p) {
            this.commitPeriodMillis = (long) (p * 1000.0);
            return this;
        }

        public final Builder setInitialStartPosition (StartPosition p) {
            this.initialStartPosition = p;
            return this;
        }

        public ConsumerClient build() throws Exception {
            NonCrKafkaConsumerClient client = new NonCrKafkaConsumerClient (operatorContext, keyClass, valueClass, kafkaProperties);
            client.setPollTimeout (pollTimeout);
            client.setCommitMode (commitMode);
            client.setCommitCount (commitCount);
            client.setCommitPeriodMillis (commitPeriodMillis); 
            client.setInitialStartPosition (initialStartPosition);
            return client;
        }
    }
}
