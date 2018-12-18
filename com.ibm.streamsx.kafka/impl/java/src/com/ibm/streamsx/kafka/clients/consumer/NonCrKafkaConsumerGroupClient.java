/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.KafkaOperatorException;
import com.ibm.streamsx.kafka.MissingJobControlPlaneException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Kafka consumer client to be used when group management is active, and the operator is not in a consistent region.
 *
 * @author The IBM Kafka toolkit team
 */
public class NonCrKafkaConsumerGroupClient extends AbstractNonCrKafkaConsumerClient implements ConsumerRebalanceListener {

    private static final Logger trace = Logger.getLogger(NonCrKafkaConsumerGroupClient.class);
    private static final long JCP_CONNECT_TIMEOUT_MILLIS = 20000;
    private long initialStartTimestamp = 0l;

    /**
     * Creates a new NonCrKafkaConsumerGroupClient instance
     * 
     * @param operatorContext the operator context
     * @param keyClass the key class for Kafka messages
     * @param valueClass the value class for Kafka messages
     * @param commitCount the tuple count after which offsets are committed. This parameter is ignored when auto-commit is explicitly enabled.
     * @param kafkaProperties Kafka properties
     * @param nTopics the number of subscribed topics
     * @throws KafkaOperatorException 
     */
    private <K, V> NonCrKafkaConsumerGroupClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, int nTopics) throws KafkaOperatorException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);

        // if no partition assignment strategy is specified, set the round-robin when nTopics > 1
        if (!kafkaProperties.containsKey (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG) && nTopics > 1) {
            String assignmentStrategy = RoundRobinAssignor.class.getCanonicalName();
            kafkaProperties.put (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
            trace.info (MessageFormat.format ("Multiple topics specified. Using the ''{0}'' partition assignment strategy for group management", assignmentStrategy));
        }
        if (getInitialStartPosition() != StartPosition.Default && getJcpContext() == null) {
            throw new KafkaOperatorException (Messages.getString ("JCP_REQUIRED_NOCR_STARTPOS_NOT_DEFAULT", getInitialStartPosition()));
        }
    }

    /**
     * Tests for a connection establishment with the JCP operator and throws an exception if it cannot be connected. 
     * @param connectTimeoutMillis The connect timeout in milliseconds
     * @param startPos the initial startposition, used for the exception message only
     * @throws MissingJobControlPlaneException The connection cannot be created
     */
    private void testForJobControlPlaneOrThrow (long connectTimeoutMillis, StartPosition startPos) throws MissingJobControlPlaneException {
        if (!testJobControlConnection (connectTimeoutMillis)) {
            trace.error (MessageFormat.format ("Could not connect to the JobControlPlane "
                    + "within {0} milliseconds. Make sure that the operator graph contains "
                    + "a JobControlPlane operator to support group management with startPosition {1}.", connectTimeoutMillis, startPos));
            throw new MissingJobControlPlaneException (Messages.getString ("JCP_REQUIRED_NOCR_STARTPOS_NOT_DEFAULT", startPos));
        }
    }


    /**
     * Subscribes to topics.
     * 
     * @param topics     the topics
     * @param partitions The partitions. Must be null or empty. Partitions are assigned by Kafka.
     * @param startPosition Must be StartPosition.Default, StartPosition.Beginning, or StartPosition.End.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.Collection, java.util.Collection, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
        trace.info (MessageFormat.format ("subscribeToTopics: topics = {0}, partitions = {1}, startPosition = {2}",
                topics, partitions, startPosition));
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        assert getInitialStartPosition() == startPosition;
        if (partitions != null && !partitions.isEmpty()) {
            trace.error("When the " + getThisClassName() + " consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }
        subscribe (topics, this);
        // we seek in onPartitionsAssigned()
        if (startPosition != StartPosition.Default) {
            testForJobControlPlaneOrThrow (JCP_CONNECT_TIMEOUT_MILLIS, startPosition);
        }
    }

    /**
     * Subscribes to topics.
     * 
     * @param topics     the topics
     * @param partitions The partitions. Must be null or empty. Partitions are assigned by Kafka.
     * @param timestamp  The timestamp where to start reading in milliseconds since Epoch.
     * @throws Exception 
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.Collection, java.util.Collection, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Collection<String> topics, Collection<Integer> partitions, long timestamp) throws Exception {
        trace.info ("subscribeToTopicsWithTimestamp: topic = " + topics + ", partitions = " + partitions + ", timestamp = " + timestamp);
        assert getInitialStartPosition() == StartPosition.Time;
        if (partitions != null && !partitions.isEmpty()) {
            trace.error("When the " + getThisClassName() + " consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }
        this.initialStartTimestamp = timestamp;
        subscribe (topics, this);
        // we seek in onPartitionsAssigned()
        testForJobControlPlaneOrThrow (JCP_CONNECT_TIMEOUT_MILLIS, StartPosition.Time);
    }


    /**
     * Not supported in this implementation. Throws an exception when invoked.
     * 
     * @param topic the topic
     * @param partitions the partitions of the topic
     * @param startOffsets the offsets to seek
     *  
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithOffsets(java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public void subscribeToTopicsWithOffsets (String topic, List<Integer> partitions, List<Long> startOffsets) throws Exception {
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
            resetCommitPeriod (System.currentTimeMillis());
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
        // override the fetch offset according to initialStartPosition for 
        // those partitions, which are never committed within the group
        final StartPosition startPos = getInitialStartPosition();
        try {
            for (TopicPartition tp: partitions) {
                switch (startPos) {
                case Default:
                    break;
                case Beginning:
                case End:
                    if (!isCommittedForPartition (tp)) {
                        seekToPosition (tp, startPos);
                    }
                    break;
                case Time:
                    if (!isCommittedForPartition (tp)) {
                        seekToTimestamp (tp, this.initialStartTimestamp);
                    }
                    break;
                default:
                    // unsupported start position, like 'Offset',  is already treated by initialization checks
                    final String msg = MessageFormat.format("onPartitionsAssigned(): {0} does not support startPosition {1}.", getThisClassName(), getInitialStartPosition());
                    trace.error (msg);
                    throw new RuntimeException (msg);
                }
            }
            try {
                checkSpaceInMessageQueueAndPauseFetching (true);
            } catch (IllegalStateException/* | InterruptedException*/ e) {
                // IllegalStateException cannot happen
                // On Interruption, do nothing
            }
        } catch (InterruptedException e) {
            trace.debug ("onPartitionsAssigned(): thread interrupted");
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
        trace.error("processUpdateAssignmentEvent(): update = " + update + "; update of assignments not supported by this client: " + getThisClassName());
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
        private int numTopics = 0;
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

        public final Builder setNumTopics (int n) {
            this.numTopics = n;
            return this;
        }

        public final Builder setInitialStartPosition (StartPosition p) {
            this.initialStartPosition = p;
            return this;
        }

        public ConsumerClient build() throws Exception {
            NonCrKafkaConsumerGroupClient client = new NonCrKafkaConsumerGroupClient (operatorContext, keyClass, valueClass, kafkaProperties, numTopics);
            client.setPollTimeout (pollTimeout);
            client.setCommitMode (commitMode);
            client.setCommitCount (commitCount);
            client.setCommitPeriodMillis (commitPeriodMillis); 
            client.setInitialStartPosition (initialStartPosition);
            return client;
        }
    }
}
