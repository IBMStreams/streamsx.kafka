/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.CheckpointContext.Kind;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.KafkaOperatorException;
import com.ibm.streamsx.kafka.KafkaOperatorResetFailedException;
import com.ibm.streamsx.kafka.KafkaOperatorRuntimeException;
import com.ibm.streamsx.kafka.MsgFormatter;
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
    private long initialStartTimestamp = 0l;

    /**
     * Creates a new NonCrKafkaConsumerGroupClient instance
     * 
     * @param operatorContext the operator context
     * @param keyClass the key class for Kafka messages
     * @param valueClass the value class for Kafka messages
     * @param kafkaProperties Kafka properties
     * @param singleTopic set to true, when the client subscribes to a single topic.
     *                    It affects the 'partition.assignment.strategy' consumer property.
     * @throws KafkaOperatorException 
     */
    private <K, V> NonCrKafkaConsumerGroupClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, boolean singleTopic) throws KafkaOperatorException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);

        // if no partition assignment strategy is specified, set the round-robin when multiple topics can be subscribed
        if (!(singleTopic || kafkaProperties.containsKey (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))) {
            String assignmentStrategy = RoundRobinAssignor.class.getCanonicalName();
            kafkaProperties.put (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
            trace.info (MsgFormatter.format ("Multiple topics specified or possible by using a pattern. Using the ''{0}'' partition assignment strategy for group management", assignmentStrategy));
        }
        if (getInitialStartPosition() != StartPosition.Default && getJcpContext() == null) {
            throw new KafkaOperatorException (Messages.getString ("JCP_REQUIRED_NOCR_STARTPOS_NOT_DEFAULT", getInitialStartPosition()));
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#supports(com.ibm.streamsx.kafka.clients.consumer.ControlPortAction)
     */
    @Override
    public boolean supports (ControlPortAction action) {
        switch (action.getActionType()) {
        case ADD_SUBSCRIPTION:
        case REMOVE_SUBSCRIPTION:
            return true;
        default:
            return false;
        }
    }


    /**
     * Subscription with pattern not supported by this client implementation.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.regex.Pattern, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Pattern pattern, long timestamp) throws Exception {
        trace.info (MsgFormatter.format ("subscribeToTopicsWithTimestamp: pattern = {0}, timestamp = {1}",
                pattern == null? "null": pattern.pattern(), timestamp));
        assert getInitialStartPosition() == StartPosition.Time;
        this.initialStartTimestamp = timestamp;
        subscribe (pattern, this);
        // we seek in onPartitionsAssigned()
        testForJobControlPlaneOrThrow (JCP_CONNECT_TIMEOUT_MILLIS, StartPosition.Time);
        resetCommitPeriod (System.currentTimeMillis());
    }


    /**
     * Subscription with pattern not supported by this client implementation.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.regex.Pattern, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Pattern pattern, StartPosition startPosition) throws Exception {
        trace.info (MsgFormatter.format ("subscribeToTopics: pattern = {0}, startPosition = {1}",
                pattern == null? "null": pattern.pattern(), startPosition));
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        assert getInitialStartPosition() == startPosition;
        subscribe (pattern, this);
        // we seek in onPartitionsAssigned()
        if (startPosition != StartPosition.Default) {
            testForJobControlPlaneOrThrow (JCP_CONNECT_TIMEOUT_MILLIS, startPosition);
        }
        resetCommitPeriod (System.currentTimeMillis());
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
        trace.info (MsgFormatter.format ("subscribeToTopics: topics = {0}, partitions = {1}, startPosition = {2}",
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
        resetCommitPeriod (System.currentTimeMillis());
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
        resetCommitPeriod (System.currentTimeMillis());
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
        setConsumedTopics (null);
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
        setConsumedTopics (partitions);
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
                    final String msg = MsgFormatter.format("onPartitionsAssigned(): {0} does not support startPosition {1}.", getThisClassName(), getInitialStartPosition());
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
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processControlPortActionEvent(com.ibm.streamsx.kafka.clients.consumer.ControlPortAction)
     */
    @Override
    protected void processControlPortActionEvent (ControlPortAction action) {
        try {
            final ControlPortActionType actionType = action.getActionType();
            if (actionType == ControlPortActionType.ADD_SUBSCRIPTION || actionType == ControlPortActionType.REMOVE_SUBSCRIPTION) {
                trace.info ("action: " + action);
            } else if (trace.isDebugEnabled()) {
                trace.debug ("action: " + action);
            }

            final Set<String> oldSubscription = getConsumer().subscription();
            final Set<String> newSubscription = new HashSet<>(oldSubscription);
            trace.info ("current topic subscription: " + newSubscription);
            boolean subscriptionChanged = false;
            switch (actionType) {
            case ADD_SUBSCRIPTION:
                action.getTopics().forEach (tpc -> {
                    newSubscription.add (tpc);
                });
                break;
            case REMOVE_SUBSCRIPTION:
                action.getTopics().forEach (tpc -> {
                    newSubscription.remove (tpc);
                });
                break;
            default:
                throw new Exception ("processControlPortActionEvent(): unimplemented action: " + actionType);
            }
            subscriptionChanged = !newSubscription.equals (oldSubscription);
            if (!subscriptionChanged) {
                trace.info("subscriptiopn has not changed: " + newSubscription);
            } else {
                if (newSubscription.isEmpty()) {
                    // no partition rebalance will happen, where we ususally commit offsets. Commit now.
                    // remove the content of the queue. It contains uncommitted messages.
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
                            Set <TopicPartition> currentAssignment = getAssignedPartitions();
                            for (TopicPartition tp: partitionsInOffsetManager) {
                                if (currentAssignment.contains (tp)) {
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
                    offsetManager.clear();
                }
                subscribe (newSubscription, this);
                // getChkptContext().getKind() is not reported properly. Streams Build 20180710104900 (4.3.0.0) never returns OPERATOR_DRIVEN
                if (getCheckpointKind() == Kind.OPERATOR_DRIVEN) {
                    trace.info ("initiating checkpointing with current topic subscription");
                    // createCheckpoint() throws IOException
                    boolean result = getChkptContext().createCheckpoint();
                    trace.info ("createCheckpoint() result: " + result);
                }
            }
        } catch (Exception e) {
            trace.error(e.getLocalizedMessage(), e);
            throw new KafkaOperatorRuntimeException (e.getMessage(), e);
        }
    }

    /**
     * Checkpoints the current subscription of the consumer.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onCheckpoint (Checkpoint checkpoint) throws InterruptedException {
        if (getOperatorContext().getNumberOfStreamingInputs() == 0 || !isCheckpointEnabled()) {
            trace.debug ("onCheckpoint() - ignored");
            return;
        }
        trace.log (DEBUG_LEVEL, "onCheckpoint() - entering. seq = " + checkpoint.getSequenceId());
        if (getCheckpointKind() == Kind.OPERATOR_DRIVEN) {
            try {
                // do not send an event here. In case of operator driven checkpoint it will never be processed (deadlock)
                final ObjectOutputStream outputStream = checkpoint.getOutputStream();
                final Set<String> subscription = getConsumer().subscription();
                outputStream.writeObject (subscription);
                trace.info ("topics written into checkpoint: " + subscription);
            } catch (IOException e) {
                throw new RuntimeException (e.getMessage(), e);
            }
        }
        else {
            // periodic checkpoint - create the checkpoint by the event thread
            sendStopPollingEvent();
            Event event = new Event (Event.EventType.CHECKPOINT, checkpoint, true);
            sendEvent (event);
            event.await();
            if (isSubscribedOrAssigned()) sendStartPollingEvent();
        }
    }


    /**
     * Empty default implementation which ensures that 'config checkpoint' is at least ignored
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onReset(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onReset (Checkpoint checkpoint) throws InterruptedException {
        trace.info ("onReset() - entering. seq = " + checkpoint.getSequenceId());
        if (getOperatorContext().getNumberOfStreamingInputs() == 0 || !isCheckpointEnabled()) {
            trace.debug ("onReset() - ignored");
            return;
        }
        sendStopPollingEvent();
        Event event = new Event (Event.EventType.RESET, checkpoint, true);
        sendEvent (event);
        event.await();
        // do not start polling; reset happens before allPortsReady(), which starts polling
    }

    /**
     * Resets the client by restoring the checkpointed subscription.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetEvent(Checkpoint)
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void processResetEvent (Checkpoint checkpoint) {
        final long chkptSeqId = checkpoint.getSequenceId();
        trace.log (DEBUG_LEVEL, "processResetEvent() - entering. seq = " + chkptSeqId);
        try {
            final Set <String> topics = (Set <String>) checkpoint.getInputStream().readObject();
            trace.info ("topics from checkpoint = " + topics);
            // subscribe, fetch offset is last committed offset.
            subscribe (topics, this);
        } catch (IllegalStateException | ClassNotFoundException | IOException e) {
            trace.error ("reset failed: " + e.getLocalizedMessage());
            throw new KafkaOperatorResetFailedException (MsgFormatter.format ("resetting operator {0} to checkpoint sequence ID {1,number,#} failed: {2}", getOperatorContext().getName(), chkptSeqId, e.getLocalizedMessage()), e);
        }
    }

    /**
     * Empty default implementation which ensures that 'config checkpoint' is at least ignored
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processCheckpointEvent(Checkpoint)
     */
    @Override
    protected void processCheckpointEvent (Checkpoint checkpoint) {
        try {
            final Set<String> subscription = getConsumer().subscription();
            checkpoint.getOutputStream().writeObject (subscription);
            trace.log (DEBUG_LEVEL, "topics written into checkpoint: " + subscription);
        } catch (IOException e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
    }




    /**
     * The builder for the consumer client following the builder pattern.
     */
    public static class Builder implements ConsumerClientBuilder {

        private OperatorContext operatorContext;
        private Class<?> keyClass;
        private Class<?> valueClass;
        private KafkaOperatorProperties kafkaProperties;
        private long pollTimeout;
        private long commitCount;
        private StartPosition initialStartPosition;
        private boolean singleTopic = false;   // safest default
        private CommitMode commitMode;
        private long commitPeriodMillis;

        public final Builder setOperatorContext(OperatorContext c) {
            this.operatorContext = c;
            return this;
        }

        public final Builder setKafkaProperties (KafkaOperatorProperties p) {
            this.kafkaProperties = new KafkaOperatorProperties();
            this.kafkaProperties.putAll (p);
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

        public final Builder setSingleTopic (boolean s) {
            this.singleTopic = s;
            return this;
        }

        public final Builder setInitialStartPosition (StartPosition p) {
            this.initialStartPosition = p;
            return this;
        }

        @Override
        public ConsumerClient build() throws Exception {
            KafkaOperatorProperties p = new KafkaOperatorProperties();
            p.putAll (this.kafkaProperties);
            NonCrKafkaConsumerGroupClient client = new NonCrKafkaConsumerGroupClient (operatorContext, keyClass, valueClass, p, singleTopic);
            client.setPollTimeout (pollTimeout);
            client.setCommitMode (commitMode);
            client.setCommitCount (commitCount);
            client.setCommitPeriodMillis (commitPeriodMillis); 
            client.setInitialStartPosition (initialStartPosition);
            return client;
        }

        @Override
        public int getImplementationMagic() {
            return NonCrKafkaConsumerGroupClient.class.getName().hashCode();
        }
    }
}
