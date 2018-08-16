package com.ibm.streamsx.kafka.clients.consumer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.Controllable;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.KafkaOperatorResetFailedException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinator.TP;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.operators.AbstractKafkaConsumerOperator;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class represents a Consumer client that can be used in consumer groups within a consistent region.
 */
public class CrKafkaConsumerGroupClient extends AbstractKafkaConsumerClient implements ConsumerRebalanceListener, Controllable, NotificationListener {

    private static final Logger logger = Logger.getLogger(CrKafkaConsumerGroupClient.class);
    private static final int MESSAGE_QUEUE_SIZE_MULTIPLIER = 10;
    private static final long RESET_MERGE_TIMEOUT_MILLIS = 30000l;
    private static final String MBEAN_DOMAIN_NAME = "com.ibm.streamsx.kafka";

    private static enum ClientState {
        INITIALIZED, STARTED, SUBSCRIBED,
        DRAINING,
        DRAINED,
        CHECKPOINTED,
        POLLING,
        POLLING_STOPPED,
        //        RESET_AWAIT_MERGE_COMPLETE,
        //        RESET_MERGE_COMPLETE,
        RESET_COMPLETE,
    }

    private ConsistentRegionContext crContext = null;
    private ControlPlaneContext jcpContext = null;
    /** Kafka group ID */
    private String groupId = null;
    /** The topics we are subscribed */
    private Collection <String> subscribedTopics;
    /** Partitions that can be theoretically assigned; sum of all partitions of all subscribed topics.
     * assignablePartitions is updated on every onPartitionsAssigned() and is used for checkpoint consolidation
     * in the MXBean. The MXBean needs this information to decide that all consumers of the group have contributed
     * to the group's consolidated checkpoint.
     * This variable is checkpointed. 
     */
    private Set<CrConsumerGroupCoordinator.TP> assignablePartitions = new HashSet<>();
    /** counts the tuples to trigger operator driven CR */
    private long nSubmittedRecords = 0l;
    /** threshold of nSubmittedRecords for operator driven CR */
    private long triggerCount = 5000l; 
    /** Start position where each subscribed topic is consumed from */
    private StartPosition initialStartPosition = StartPosition.Default;
    private long initialStartTimestamp = -1l;
    private CountDownLatch jmxSetupLatch;

    /** Lock for setting/checking the JMX notification */
    private final ReentrantLock jmxNotificationConditionLock = new ReentrantLock();
    /** Condition for setting/checking/waiting for the JMX notification */
    private final Condition jmxNotificationCondition = jmxNotificationConditionLock.newCondition();
    /** group's checkpoint merge complete JMX notification */
    private Notification jmxNotificationMergeCompleted = null;

    /** canonical name of the MXBean. Note: If you need a javax.management.ObjectName instance, use createMBeanObjectName() */
    private String crGroupCoordinatorMXBeanName = null;
    /** MXBean proxy for coordinating the group's checkpoint */
    private CrConsumerGroupCoordinatorMXBean crGroupCoordinator = null;
    /** stores the initial offsets of all partitions of all topics. Written to CV and used for reset to initial state */
    private OffsetManager initialOffsets;
    /** JCP control variable to persist the initialOffsets to JCP */
    private ControlVariableAccessor<String> initialOffsetsCV;
    /** stores the offsets of the submitted tuples - should contain only assignedPartitions - is checkpointed */
    private OffsetManager offsetManager; // TODO: find a better name for this variable
    /** the map that is used to seek the consumer after reset, resetToInitialState or in onPartitionsAssigned.
     * The map must contain mappings for all partitions of all topics because we do not know which partitions we get for consuming.
     */
    private Map<TopicPartition, Long> seekOffsetMap;
    /** current state of the consumer client */
    private ClientState state = null;

    /**
     * Constructs a new CrKafkaConsumerGroupClient object.
     * @throws KafkaConfigurationException 
     */
    private <K, V> CrKafkaConsumerGroupClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {

        super (operatorContext, keyClass, valueClass, kafkaProperties);

        this.crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        this.jcpContext = operatorContext.getOptionalContext (ControlPlaneContext.class);
        if (crContext == null || jcpContext == null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' must be used in a consistent region. This consumer client implementation (" 
                    + getThisClassName() + ") requires a consistent region context and a Control Plane context.");
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
        kafkaProperties.put (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.groupId = kafkaProperties.getProperty (ConsumerConfig.GROUP_ID_CONFIG);
        this.crGroupCoordinatorMXBeanName = createMBeanObjectName().getCanonicalName();
        this.initialOffsets = new OffsetManager();
        this.offsetManager = new OffsetManager();
        ClientState newState = ClientState.INITIALIZED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
    }


    /**
     * For this consumer client the message queue size shall be smaller than default to ensure fast drain.
     * @return {@value #MESSAGE_QUEUE_SIZE_MULTIPLIER}
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#getMessageQueueSizeMultiplier()
     */
    @Override
    protected int getMessageQueueSizeMultiplier() {
        return MESSAGE_QUEUE_SIZE_MULTIPLIER;
    }

    /**
     * Processes JMX connection related events.
     * @see com.ibm.streams.operator.control.Controllable#event(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext, com.ibm.streams.operator.control.Controllable.EventType)
     */
    @Override
    public void event (MBeanServerConnection jcp, OperatorContext context, EventType eventType) {
        logger.info ("JMX connection related event received: " + eventType);
        // TODO event(): any action required here?
    }

    /**
     * Returns always true.
     * @see com.ibm.streams.operator.control.Controllable#isApplicable(com.ibm.streams.operator.OperatorContext)
     */
    @Override
    public boolean isApplicable (OperatorContext context) {
        return true;
    }

    /**
     * JMX setup of MBeans, Proxy, and Notification listener
     * This method is called every time a connection to the Job Control Plane is made.
     * 
     * @throws IOException
     * @throws MBeanException
     * @throws ReflectionException
     * @throws NotCompliantMBeanException
     * @throws MBeanRegistrationException
     * @see com.ibm.streams.operator.control.Controllable#setup(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext)
     */
    @Override
    public void setup (MBeanServerConnection jcp, OperatorContext context) throws InstanceNotFoundException, MBeanRegistrationException, NotCompliantMBeanException, ReflectionException, MBeanException, IOException {
        try {
            ObjectName groupMbeanName = createMBeanObjectName();
            this.crGroupCoordinatorMXBeanName = groupMbeanName.getCanonicalName();
            // Try to register the MBean for checkpoint coordination in JCP. One of the operators in the consumer group wins.
            logger.info ("Trying to register MBean in JCP: " + crGroupCoordinatorMXBeanName);
            if (jcp.isRegistered (groupMbeanName)) {
                logger.info (crGroupCoordinatorMXBeanName + " already registered");
            }
            else {
                try {
                    // Constructor signature: (String groupId, Integer consistentRegionIndex)
                    jcp.createMBean (CrConsumerGroupCoordinator.class.getName(), groupMbeanName, 
                            new Object[]{this.groupId, new Integer(crContext.getIndex())},
                            new String[] {"java.lang.String", "java.lang.Integer"});
                    logger.info ("MBean registered: " + crGroupCoordinatorMXBeanName);
                }
                catch (InstanceAlreadyExistsException e) {
                    // another operator managed to create it first. that is ok, just use that one.
                    logger.info (MessageFormat.format ("another operator just created {0}: {1}", crGroupCoordinatorMXBeanName, e.getMessage()));
                }
            }
            logger.info("adding client as notification listener to " + crGroupCoordinatorMXBeanName);
            jcp.addNotificationListener (groupMbeanName, this, /*filter=*/null, /*handback=*/null);
            // TODO: Do we need to ensure not be registered twice as a listener, for example after region reset? or connection re-establishment?

            logger.info ("creating Proxy ...");
            crGroupCoordinator = JMX.newMXBeanProxy (jcp, groupMbeanName, CrConsumerGroupCoordinatorMXBean.class, /*notificationEmitter=*/true);
            logger.debug ("MBean Proxy get test: group-ID = " + crGroupCoordinator.getGroupId() + "; CR index = " + crGroupCoordinator.getConsistentRegionIndex());
        }
        finally {
            jmxSetupLatch.countDown();
        }
    }

    /**
     * Creates the object name for the group MBean.
     * The name is {@value #MBEAN_DOMAIN_NAME}:type=consumergroup,groupIdHash=<i>hashCode(our group-ID)</i>
     * @return An ObjectName instance
     */
    private ObjectName createMBeanObjectName() {
        Hashtable <String, String> props = new Hashtable<>();
        props.put ("type", "consumergroup");
        final int hash = this.groupId.hashCode();
        props.put ("groupIdHash", (hash < 0? "N": "P") + Math.abs (hash));

        try {
            return ObjectName.getInstance (MBEAN_DOMAIN_NAME, props);
        } catch (MalformedObjectNameException e) {
            // here we never end with our naming scheme
            e.printStackTrace();
            return null;
        }
    }


    /**
     * Processes JMX notifications from the CrGroupCoordinator MXBean.
     * The JMX notification is fired by the MXBean when the MXBean considers the group's checkpoint complete.
     * This is triggered by any operator instance of the consumer group that contributes to the group's checkpoint by
     * calling {@link CrConsumerGroupCoordinatorMXBean#mergeConsumerCheckpoint(long, Set, Map)}.
     * <br><br>
     * When one of the consumers has no partitions assigned at checkpoint time, for example, when we have more consumers in the
     * group than Kafka partitions, this consumers checkpoint does not contribute to the group's checkpoint on reset.
     * Then the other operator instances (which had partitions) only contribute to the checkpoint and cause the MXBean to fire the
     * 'merge complete' notification. An operator that does not contribute to the group's checkpoint can therefore receive
     * the notification at any time during its reset phase, perhaps also before {@link #reset(Checkpoint)} has been invoked.
     * Therefore, the implementation of {@link #handleNotification(Notification, Object)} must not expect a particular client state to succeed.
     * 
     * @see javax.management.NotificationListener#handleNotification(javax.management.Notification, java.lang.Object)
     */
    @Override
    public void handleNotification (Notification notification, Object handback) {
        logger.info (MessageFormat.format ("handleNotification() [{0}]; notification = {1}", this.state, notification));
        if (notification.getType().equals(CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE)) {
            logger.info (MessageFormat.format("handleNotification(): notification sequence number = {0} (must match the checkpoint sequence ID)", notification.getSequenceNumber()));
            jmxNotificationConditionLock.lock();
            jmxNotificationMergeCompleted = notification;
            jmxNotificationCondition.signalAll();
            jmxNotificationConditionLock.unlock();
        }
        else {
            logger.warn ("unexpected notification type (ignored): " + notification.getType());
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        logger.info (MessageFormat.format ("startConsumer() [{0}] - consumer start initiated", state));
        // Connect the PE for this operator to the Job Control Plane. 
        // A single connection is maintained to the Job Control Plane. The connection occurs asynchronously.
        // Does this mean that we might not yet be connected after connect()?
        jmxSetupLatch = new CountDownLatch (1);
        jcpContext.connect (this);
        // wait that MBeans are registered and notification listeners are set up, i.e. setup (...) of the Controllable interface is run:
        jmxSetupLatch.await();
        // calls our validate(), which uses the MBean ...
        super.startConsumer();
        // now we have a consumer object.
        offsetManager.setOffsetConsumer (getConsumer());
        initialOffsets.setOffsetConsumer (getConsumer());
        ClientState newState = ClientState.STARTED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info ("consumer started");
    }


    /**
     * This method subscribes to the given topics.
     * Before it can do this, the consumer assigns to all topic partitions of all topics, seeks to the given start position
     * (Begin, End) and fetches the read positions of all topic partitions the consumer can be assigned later on by Kafka's 
     * group coordinator. These read positions go into a separate OffsetManager and get persisted as a control variable in the JCP.
     * From there the initial offsets are restored in case of resetToInitialState().
     * After the initial read offsets are stored, the consumer is unassigned from all topic partitions and subscribes to all topics.
     * 
     * @param topics The topics that are subscribed
     * @param partitions must be empty or null for this ConsumerClient implementation
     * @param startPosition the start position for partitions that are initially assigned to the consumer. 
     *                      Must be one of {@link StartPosition#Beginning}, {@link StartPosition#End}, or {@link StartPosition#Default}.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.Collection, java.util.Collection, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
        logger.info (MessageFormat.format ("subscribeToTopics [{0}]: topics = {1}, partitions = {2}, startPosition = {3}",
                state, topics, partitions, startPosition));
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        assert startPosition == this.initialStartPosition;
        if (partitions != null && !partitions.isEmpty()) {
            logger.error("When the " + getThisClassName() + " consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            logger.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }

        // get initial start offsets of ALL partitions of all our topics - we do not yet know which partition we get later on
        // before we can get them, we must assign and seek
        Set<TopicPartition> topicPartitionCandidates;
        // read meta data of the given topics to fetch all topic partitions
        topicPartitionCandidates = getAllTopicPartitionsForTopic (topics);
        assign (topicPartitionCandidates);
        if (startPosition != StartPosition.Default) {
            seekToPosition (topicPartitionCandidates, startPosition);
        }
        // register the topic partitions with the initial offsets manager
        initialOffsets.addTopics (topicPartitionCandidates);
        // fetch the consumer offsets of initial positions
        initialOffsets.savePositionFromCluster();
        createJcpCvFromInitialOffsets();
        // create seekOffsetMap, so that we can seek in onPartitionsAssigned()
        initSeekOffsetMap();
        for (TopicPartition tp: initialOffsets.getMappedTopicPartitions()) {
            this.seekOffsetMap.put (tp, initialOffsets.getOffset(tp.topic(), tp.partition()));
        }
        // unassign all/unsubscribe:
        assign (null);
        subscribe (topics, this);
        setAssignablePartitions (topicPartitionCandidates);
        this.subscribedTopics = new ArrayList<String> (topics);
        // when later partitions are assigned dynamically, we seek using the seekOffsetMap
        ClientState newState = ClientState.SUBSCRIBED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info ("subscribed to " + topics);
    }



    /**
     * This method subscribes to the given topics.
     * Before it can do this, the consumer assigns to all topic partitions of all topics, seeks to the start position given by the timestamp
     * and fetches the read positions of all topic partitions the consumer can be assigned later on by Kafka's 
     * group coordinator. These read positions go into a separate OffsetManager and get persisted as a control variable in the JCP.
     * From there the initial offsets are restored in case of resetToInitialState().
     * After the initial read offsets are stored, the consumer is unassigned from all topic partitions and subscribes to all topics.
     * 
     * @param topics The topics that are subscribed
     * @param partitions must be empty or null for this ConsumerClient implementation
     * @param timestamp the timestamp in milliseconds since Unix Epoch
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.Collection, java.util.Collection, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Collection<String> topics, Collection<Integer> partitions, long timestamp) throws Exception {
        logger.info (MessageFormat.format ("subscribeToTopicsWithTimestamp [{0}]: topics = {1}, partitions = {2}, timestamp = {3}",
                state, topics, partitions, timestamp));
        assert this.initialStartPosition == StartPosition.Time;
        // partitions must be null or empty
        if (partitions != null && !partitions.isEmpty()) {
            logger.error("When the " + getThisClassName() + " Consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            logger.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }

        // get initial start offsets of ALL partitions of all our topics - we do not yet know which partition we get later on
        // before we can get them, we must assign and seek
        Map <TopicPartition, Long /* timestamp */> topicPartitionTimestampMap = new HashMap<TopicPartition, Long>();
        Set<TopicPartition> topicPartitions = getAllTopicPartitionsForTopic (topics);
        topicPartitions.forEach (tp -> topicPartitionTimestampMap.put (tp, timestamp));

        logger.debug ("subscribeToTopicsWithTimestamp: topicPartitionTimestampMap = " + topicPartitionTimestampMap);
        //        Set<TopicPartition> topicPartitionCandidates = topicPartitionTimestampMap.keySet();
        assign (topicPartitions);
        seekToTimestamp (topicPartitionTimestampMap);

        // register the partitions with the initial offsets manager
        initialOffsets.addTopics (topicPartitions);
        // fetch the consumer offsets of initial positions
        initialOffsets.savePositionFromCluster();
        createJcpCvFromInitialOffsets();
        // create seekOffsetMap, so that we can seek in onPartitionsAssigned()
        initSeekOffsetMap();
        for (TopicPartition tp: initialOffsets.getMappedTopicPartitions()) {
            this.seekOffsetMap.put (tp, initialOffsets.getOffset(tp.topic(), tp.partition()));
        }
        // unassign all/unsubscribe:
        assign (null);
        subscribe (topics, this);
        setAssignablePartitions (topicPartitions);
        this.subscribedTopics = new ArrayList<String> (topics);
        // when later partitions are assigned dynamically, we seek using the seekOffsetMap
        ClientState newState = ClientState.SUBSCRIBED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info ("subscribed to " + topics);
    }



    /**
     * Assignment to specific partitions and seek to specific offsets is not supported by this client.
     * This method should never be called.
     * @throws KafkaConfigurationException always thrown to indicate failure. This exception should be treated as a programming error.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithOffsets(java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public void subscribeToTopicsWithOffsets(String topic, List<Integer> partitions, List<Long> startOffsets) throws Exception {
        logger.info ("subscribeToTopicsWithOffsets: topic = " + topic + ", partitions = " + partitions + ", startOffsets = " + startOffsets);
        throw new KafkaConfigurationException ("Subscription (assignment of partitions) with offsets is not supported by this client: " + getThisClassName());
    }



    /**
     * Called when the consistent region is drained.
     * On drain, polling is stopped, when not CR trigger, the function waits until 
     * the message queue becomes empty, then it commits offsets.
     */
    @Override
    public void onDrain () throws Exception {
        logger.info (MessageFormat.format("onDrain() [{0}] - entering", state));
        try {
            // stop filling the message queue with more messages, this method returns when polling has stopped - not fire and forget
            sendStopPollingEvent();
            ClientState newState = ClientState.DRAINING;
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
            // when CR is operator driven, do not wait for queue to be emptied.
            // This would never happen because the tuple submitter thread is blocked in makeConsistent() in postSubmit(...) 
            // and cannot empty the queue
            if (!crContext.isTriggerOperator() && !getMessageQueue().isEmpty()) {
                // here we are only when we are NOT the CR trigger (for example, periodic CR) and the queue contains consumer records
                logger.debug("onDrain() waiting for message queue to become empty ...");
                long before = System.currentTimeMillis();
                awaitEmptyMessageQueue();
                logger.debug("onDrain() message queue empty after " + (System.currentTimeMillis() - before) + " milliseconds");
            }
            final boolean commitSync = true;
            final boolean commitPartitionWise = false;   // commit all partitions in one server request
            CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);

            synchronized (offsetManager) {
                for (TopicPartition tp: offsetManager.getMappedTopicPartitions()) {
                    offsets.put (tp, offsetManager.getOffset(tp.topic(), tp.partition()));
                }
            }
            if (!offsets.isEmpty()) {
                sendCommitEvent (offsets);
            }
            else {
                logger.info("onDrain(): no offsets to commit");
            }
            newState = ClientState.DRAINED;
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
            // drain is followed by checkpoint.
            // Don't poll for new messages in the meantime. - Don't send a 'start polling event'
        } catch (InterruptedException e) {
            logger.info("Interrupted waiting for empty queue or committing offsets");
            // NOT to start polling for Kafka messages again, is ok after interruption
        }
        logger.debug("onDrain() - exiting");
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
        } catch (Exception topicPartitionUnknown) {
            // should never happen in this client
            logger.warn (topicPartitionUnknown.getLocalizedMessage());
        }
        if (crContext.isTriggerOperator() && ++nSubmittedRecords >= triggerCount) {
            logger.debug("Making region consistent..."); //$NON-NLS-1$
            // makeConsistent blocks until all operators in the CR have drained and checkpointed
            boolean isSuccess = crContext.makeConsistent();
            nSubmittedRecords = 0l;
            logger.debug("Completed call to makeConsistent: isSuccess = " + isSuccess); //$NON-NLS-1$
        }
    }



    /**
     * TODO: document what happens in onPartitionsRevoked(...)
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
     */
    @Override
    public void onPartitionsRevoked (Collection<TopicPartition> partitions) {
        logger.info (MessageFormat.format("onPartitionsRevoked() [{0}]: old partition assignment = {1}", state, partitions));
        if (!(state == ClientState.SUBSCRIBED || state == ClientState.RESET_COMPLETE)) {
            logger.warn ("onPartitionsRevoked: ============= TODO: wrong state for ignoring onPartitionsRevoked. Not yet implemented: reset consistent region");
        }
        // TODO onPartitionsRevoked() implementation missing
    }



    /**
     * TODO: document what happens in onPartitionsAssigned(...)
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
     */
    @Override
    public void onPartitionsAssigned (Collection<TopicPartition> partitions) {
        logger.info (MessageFormat.format("onPartitionsAssigned() [{0}]: new partition assignment = {1}", state, partitions));

        // get meta data of potentially assignable partitions for all subscribed topics
        setAssignablePartitionsFromMetadata();
        logger.info ("onPartitionsAssigned(): assignable partitions for topics " + this.subscribedTopics + ": " + this.assignablePartitions);

        Set<TopicPartition> previousAssignment = new HashSet<>(getAssignedPartitions());
        getAssignedPartitions().clear();
        getAssignedPartitions().addAll (partitions);
        Set<TopicPartition> gonePartitions = new HashSet<>(previousAssignment);
        gonePartitions.removeAll (partitions);
        logger.info("topic partitions that are not assigned anymore: " + gonePartitions);

        if (!gonePartitions.isEmpty()) {
            logger.info("removing consumer records from gone partitions from message queue");
            getMessageQueue().removeIf (queuedRecord -> belongsToPartition (queuedRecord, gonePartitions));
            // remove the topic partition also from the offset manager
            synchronized (offsetManager) {
                for (TopicPartition tp: gonePartitions) {
                    offsetManager.remove (tp.topic(), tp.partition());
                }
            }
        }
        if (state == ClientState.SUBSCRIBED || state == ClientState.RESET_COMPLETE) {
            // seek the new assigned partitions
            KafkaConsumer <?, ?> consumer = getConsumer();
            offsetManager.updateTopics (partitions);
            // collection and map for seeking to inital startposition
            Collection <TopicPartition> tp1 = new ArrayList<>(1);
            Map <TopicPartition, Long> tpTimestampMap1 = new HashMap<>();
            for (TopicPartition tp: partitions) {
                // in 'subscribe', the 'seekOffsetMap' has been filled with initial offsets dependent
                // on the startPosition parameter or has been restored from checkpoints via MXBean
                if (seekOffsetMap.containsKey (tp)) {
                    final long seekToOffset = seekOffsetMap.get(tp);
                    logger.info (MessageFormat.format ("onPartitionsAssigned() seeking {0} to offset {1}", tp, seekToOffset));
                    consumer.seek (tp, seekToOffset);
                }
                else {
                    // partition for which we have no offset, for example, a partition that has 
                    // been added to the topic after last checkpoint.
                    // Seek to startPosition given as operator parameter(s)
                    switch (initialStartPosition) {
                    case Beginning:
                    case End:
                        tp1.clear();
                        tp1.add (tp);
                        logger.info (MessageFormat.format ("onPartitionsAssigned() seeking new topic partition {0} to {1}", tp, initialStartPosition));
                        seekToPosition (tp1, initialStartPosition);
                        break;
                    case Default:
                        // do not seek
                        break;
                    case Time:
                        tpTimestampMap1.clear();
                        tpTimestampMap1.put (tp, initialStartTimestamp);
                        logger.info (MessageFormat.format ("onPartitionsAssigned() seeking new topic partition {0} to timestamp {1}", tp, initialStartTimestamp));
                        seekToTimestamp (tpTimestampMap1);
                        break;
                    default:
                        // unsupported start position, like 'Offset' is already treated by initialization checks
                        final String msg = "onPartitionsAssigned: " + getThisClassName() + " does not support startPosition = " + initialStartPosition;
                        logger.error (msg);
                        throw new RuntimeException (msg);
                    }
                }
            }
            // update the fetch positions for all assigned partitions
            offsetManager.savePositionFromCluster();
        }
        else {
            logger.warn ("onPartitionsAssigned: ============= TODO: unexpected state '" + state + "' for partition assignment. Not yet implemented: What to do?");
            // TODO: What to do in onPartitionsAssigned(), if we are not in state SUBSCRIBED or RESET_COMPLETE?
        }
    }


    /**
     * initializes the assignablePartitions from the meta data of the subscribed topics.
     */
    private void setAssignablePartitionsFromMetadata() {
        if (this.assignablePartitions == null) this.assignablePartitions = new HashSet<>();
        this.assignablePartitions.clear();
        for (String topic: this.subscribedTopics) {
            List <PartitionInfo> partList = getConsumer().partitionsFor (topic);
            for (PartitionInfo part: partList) 
                this.assignablePartitions.add (new CrConsumerGroupCoordinator.TP (part.topic(), part.partition()));
        }
    }


    /**
     * initializes the assignablePartitions from a Collection of TopicPartitions.
     * @param topicPartitions the topic partitions
     */
    private void setAssignablePartitions (Collection<TopicPartition> topicPartitions) {
        if (this.assignablePartitions == null) this.assignablePartitions = new HashSet<>();
        this.assignablePartitions.clear();
        for (TopicPartition part: topicPartitions) 
            this.assignablePartitions.add (new CrConsumerGroupCoordinator.TP (part.topic(), part.partition()));
    }



    /**
     * For this consumer client it is required that all Consumer operators that belong to one consumer
     * group belong to a single consistent region. To validate this, the Consumer client tries to 
     * register a single consumer group specific MBean with the JCP (first operator wins). 
     * This MBean contains the CR index of the operator that registered the MBean as an attribute.
     * All other operators (which did not manage to register the MBean) must have the same CR index.
     * 
     * For consumer groups, a group.id must not be a random value generated by the consumer client.
     * This is also validated.
     * @throws Exception validation failed.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#validate()
     */
    @Override
    protected void validate() throws Exception {
        // test CR index against index in MBean
        int myCrIndex = crContext.getIndex();
        int groupCrIndex = crGroupCoordinator.getConsistentRegionIndex();
        logger.info("CR index from MBean = " + groupCrIndex + "; this opertor's CR index = " + myCrIndex);
        if (groupCrIndex != myCrIndex) {
            final String msg = Messages.getString("CONSUMER_GROUP_IN_MULTIPLE_CONSISTENT_REGIONS", getOperatorContext().getKind(), this.groupId);
            logger.error (msg);
            throw new KafkaConfigurationException (msg);
        }
        // test that group-ID has not default random value
        if (isGroupIdGenerated()) {
            final String msg = Messages.getString("GROUP_ID_REQUIRED_FOR_PARAM_VAL", AbstractKafkaConsumerOperator.CR_ASSIGNMENT_MODE_PARAM, ConsistentRegionAssignmentMode.GroupCoordinated);
            logger.error (msg);
            throw new KafkaConfigurationException (msg);
        }
        if (initialStartPosition == StartPosition.Offset) {
            throw new KafkaConfigurationException (getThisClassName() + " does not support startPosition = " + initialStartPosition);
        }
        logger.info("initial start position for first partition assignment = " + initialStartPosition);
        if (initialStartPosition == StartPosition.Time) {
            logger.info("start timestamp for first partition assignment = " + initialStartTimestamp);
        }
    }



    /**
     * Resets the consumer client to the initial state.
     * Resetting the client involves following steps:
     * <ul>
     * <li>clear the operator internal message queue</li>
     * <li>restore 'initialOffsets' from JCP Control variable. On subscribe we have saved them into JCP.
     *     These are the initial offsets of all assignable partitions (group's view).</li>
     * <li>initialize the 'seekOffsetMap' member variable from initial offsets.</li>
     * <li>seek all assigned partitions to what is stored in 'seekOffsetMap'</li>
     * <li>reset the offsetManager by setting the seekOffsets for all assigned partitions.</li>
     * </ul>
     * This method is run by the event thread. State must be POLLING_STOPPED.
     *
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#resetToInitialState()
     */
    @Override
    protected void resetToInitialState() {
        logger.info (MessageFormat.format("resetToInitialState() [{0}] - entering", state));
        getMessageQueue().clear();
        try {
            initialOffsets = getInitialOffsetsFromJcpCv();
            initialOffsets.setOffsetConsumer (getConsumer());
            logger.debug ("initialOffsets = " + initialOffsets); //$NON-NLS-1$
            initSeekOffsetMap();
            initialOffsets.getMappedTopicPartitions().forEach (tp -> {
                this.seekOffsetMap.put (tp, this.initialOffsets.getOffset(tp.topic(), tp.partition()));
            });

            // When no one of the KafkaConsumer in this group has been restarted before the region reset,
            // partition assignment will most likely not change and no onPartitionsRevoked()/onPartitionsAssigned will be fired on our
            // ConsumerRebalanceListener. That's why we must seek here to the partitions we think we are assigned to (can also be no partition).
            // If a consumer within our group - but not this operator - restarted, partition assignment may have changed, but we
            // are not yet notified about it. That's why we must catch IllegalArgumentException and ignore it.
            // When this operator is restarted and reset, getAssignedPartitions() will return an empty Set.

            // Reset also the offsetManager to the initial offsets of the assigned partitions. The offsetManager goes into the checkpoint,
            // and its offsets are used as the seek position when it comes to reset from a checkpoint. There must be a seek position also in
            // the case that no tuple has been submitted for a partition, which would update the offsetManager.
            offsetManager.clear();
            offsetManager.addTopics (getAssignedPartitions());
            KafkaConsumer <?, ?> consumer = getConsumer();
            for (TopicPartition tp: getAssignedPartitions()) {
                try {
                    long offset = this.seekOffsetMap.get (tp);
                    offsetManager.setOffset (tp.topic(), tp.partition(), offset);
                    logger.info (MessageFormat.format ("resetToInitialState() seeking {0} to offset {1}", tp, offset));
                    consumer.seek (tp, offset);
                }
                catch (IllegalArgumentException topicPartitionNotAssigned) {
                    // when this happens the ConsumerRebalanceListener will be called later. 
                    logger.info ("reset(): seek failed for " + tp + ": " + topicPartitionNotAssigned.getLocalizedMessage());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        ClientState newState = ClientState.RESET_COMPLETE;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info (MessageFormat.format("resetToInitialState() [{0}] - exiting", state));
    }



    /**
     * Resets the consumer client to a previous state.
     * Resetting the client involves following steps:
     * <ul>
     * <li>clear the operator internal message queue</li>
     * <li>restore 'assignablePartitions' from the checkpoint.
     *     This is the sum of all topic partitions the consumer group consumed at checkpoint time.</li>
     * <li>read the seek offsets from the checkpoint.
     *     These are the offsets of only those partitions the consumer was assigned at checkpoint time.</li>
     * <li>send the offsets of the prior partitions together with the 'assignablePartitions' to the CrGroupCoordinator MXBean.
     *     The other consumer operators will also send their prior partition-to-offset mappings.</li>
     * <li>wait for the JMX notification that the partition-to-offset map has merged to match 'assignablePartitions'</li>
     * <li>fetch the merged map from the MX bean so that the operator has the seek offsets of all partitions of
     *     all topics (the group's view) and store this in the 'seekOffsetMap' member variable.</li>
     * <li>seek all assigned partitions</li>
     * <li>reset the offsetManager by setting the seekOffsets for all assigned partitions.</li>
     * </ul>
     * This method is run by the event thread. State must be POLLING_STOPPED.
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#reset(com.ibm.streams.operator.state.Checkpoint)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void reset (Checkpoint checkpoint) {
        long chkptSeqId = checkpoint.getSequenceId();
        logger.info (MessageFormat.format("reset() [{0}] - entering. chkptSeqId = {1}", state, chkptSeqId));
        getMessageQueue().clear();
        try {
            final ObjectInputStream inputStream = checkpoint.getInputStream();
            this.assignablePartitions = (Set<TP>) inputStream.readObject();
            OffsetManager offsMgr = (OffsetManager) inputStream.readObject();
            logger.info(MessageFormat.format("reset(): assignablePartitions read from checkpoint: {0}", this.assignablePartitions));
            logger.info(MessageFormat.format("reset(): offset manager read from checkpoint: {0}", offsMgr));
            int nPartitionsTotal = this.assignablePartitions.size();
            int nPartitionsChckpt = offsMgr.size();
            logger.info (MessageFormat.format("contributing {0} partition=>offset mappings to the group's checkpoint for total {1} partitions", nPartitionsChckpt, nPartitionsTotal));
            if (!offsMgr.isEmpty()) {
                // send checkpoint data to CrGroupCoordinator MBean and wait for the notification
                // to fetch the group's complete checkpoint. Then, process this data.
                Map<CrConsumerGroupCoordinator.TP, Long> partialOffsetMap = new HashMap<>();
                for (TopicPartition tp: offsMgr.getMappedTopicPartitions()) {
                    final String topic = tp.topic();
                    final int partition = tp.partition();
                    final Long offset = offsMgr.getOffset(topic, partition);
                    partialOffsetMap.put (new TP (topic, partition), offset);
                }

                logger.info (MessageFormat.format("Merging my group's checkpoint contribution: partialOffsetMap = {0}, all consumable partitions = {1}",
                        partialOffsetMap, this.assignablePartitions));
                this.crGroupCoordinator.mergeConsumerCheckpoint (chkptSeqId, this.assignablePartitions, partialOffsetMap);
            }
            else {
                logger.info("reset(): no contribution to the group's checkpoint from this operator");
            }

            // check notification and wait
            jmxNotificationConditionLock.lock();
            logger.info(MessageFormat.format("checking receiption of JMX notification {0}", CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE));
            if (jmxNotificationMergeCompleted == null) {
                logger.info(MessageFormat.format("waiting for JMX notification from {0}", crGroupCoordinatorMXBeanName));
                jmxNotificationCondition.await (RESET_MERGE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            }

            if (jmxNotificationMergeCompleted == null) {
                logger.error ("timeout waiting for merge complete notification from MXBean " + crGroupCoordinatorMXBeanName);
                throw new KafkaOperatorResetFailedException (MessageFormat.format ("Timeout receiving JMX notification {0} from MXBean {1} in JCP. current timeout is {2} milliseconds.",
                        CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, crGroupCoordinatorMXBeanName, RESET_MERGE_TIMEOUT_MILLIS));
            }
            long groupChkptMergeSequenceId = jmxNotificationMergeCompleted.getSequenceNumber();
            if (groupChkptMergeSequenceId != chkptSeqId) {
                logger.error (MessageFormat.format ("Notification sequence number {0} does not match checkpoint sequence-ID {1}.", groupChkptMergeSequenceId, chkptSeqId));
                throw new KafkaOperatorResetFailedException (MessageFormat.format ("Consumer group checkpoint merge sequence ID ({0}) does not match checkpoint sequence ID ({1}).", groupChkptMergeSequenceId, chkptSeqId));
            }

            final Map <TP, Long> resetOffsets = crGroupCoordinator.getConsolidatedOffsetMap();
            logger.info ("resetOffsets (group's checkpoint) fetched from MXBean: " + resetOffsets);
            initSeekOffsetMap();
            resetOffsets.forEach ((tp, offset) -> {
                this.seekOffsetMap.put (new TopicPartition (tp.getTopic(), tp.getPartition()), offset);
            });

            // When no one of the KafkaConsumer in this group has been restarted before the region reset,
            // partition assignment will most likely not change and no onPartitionsRevoked()/onPartitionsAssigned will be fired on our
            // ConsumerRebalanceListener. That's why we must seek here to the partitions we think we are assigned to (can also be no partition).
            // If a consumer within our group - but not this operator - restarted, partition assignment may have changed, but we
            // are not yet notified about it. That's why we must catch IllegalArgumentException and ignore it.
            // When this operator is restarted and reset, getAssignedPartitions() will return an empty Set. 

            // Reset also the offsetManager to the initial offsets of the assigned partitions. The offsetManager goes into the checkpoint,
            // and its offsets are used as the seek position when it comes to reset from a checkpoint. There must be a seek position also in
            // the case that no tuple has been submitted for a partition, which would update the offsetManager.
            offsetManager.clear();
            offsetManager.addTopics (getAssignedPartitions());
            KafkaConsumer <?, ?> consumer = getConsumer();
            for (TopicPartition tp: getAssignedPartitions()) {
                try {
                    long offset = this.seekOffsetMap.get (tp);
                    offsetManager.setOffset (tp.topic(), tp.partition(), offset);
                    logger.info (MessageFormat.format ("reset() seeking {0} to offset {1}", tp, offset));
                    consumer.seek (tp, offset);
                }
                catch (IllegalArgumentException topicPartitionNotAssigned) {
                    // when this happens the ConsumerRebalanceListener will be called later. 
                    logger.info ("reset(): seek failed for " + tp + ": " + topicPartitionNotAssigned.getLocalizedMessage());
                }
            }

            ClientState newState = ClientState.RESET_COMPLETE;
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
            logger.info (MessageFormat.format("reset() [{0}] - exiting", state));
        }
        catch (InterruptedException e) {
            logger.debug ("reset(): interrupted waiting for the JMX notification");
            return;
        }
        catch (IOException | ClassNotFoundException e) {
            logger.error ("reset failed: " + e.getLocalizedMessage());
            throw new KafkaOperatorResetFailedException (MessageFormat.format ("resetting operator {0} to checkpoint sequence ID {1} failed: {2}", getOperatorContext().getName(), chkptSeqId, e.getLocalizedMessage()), e);
        }
        finally {
            jmxNotificationMergeCompleted = null;
            jmxNotificationConditionLock.unlock();
        }
    }


    /**
     * Initializes the seekOffsetMap - ensures that the map is not null and empty.
     */
    private void initSeekOffsetMap() {
        if (this.seekOffsetMap == null) {
            this.seekOffsetMap = new HashMap<>();
        }
        else this.seekOffsetMap.clear();
    }



    /**
     * Creates a checkpoint of the current state when used in consistent region.
     * Following data is included into the checkpoint in this sequence:
     * <ul>
     * <li>assignablePartitions</li>
     * <li>offsetManager</li>
     * </ul
     * @param checkpoint the reference of the checkpoint object
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#checkpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    protected void checkpoint (Checkpoint checkpoint) {
        logger.info (MessageFormat.format ("checkpoint() [{0}] sequenceId = {1}", state, checkpoint.getSequenceId()));
        try {
            ObjectOutputStream oStream = checkpoint.getOutputStream();
            oStream.writeObject (this.assignablePartitions);
            oStream.writeObject (this.offsetManager);
            //            if (logger.isDebugEnabled()) {
            if (logger.isInfoEnabled()) logger.info ("data written to checkpoint: assignablePartitions = " + this.assignablePartitions);
            if (logger.isInfoEnabled()) logger.info ("data written to checkpoint: offsetManager = " + this.offsetManager);
            //            }

        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        ClientState newState = ClientState.CHECKPOINTED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info ("checkpoint() - exiting.");
    }



    /**
     * Assignments cannot be updated.
     * This method should not be called because operator control port and this client implementation are incompatible.
     * A context check should exist to detect this mis-configuration.
     * We only log the method call. 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#updateAssignment(com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate)
     */
    @Override
    protected void updateAssignment (TopicPartitionUpdate update) {
        logger.warn("updateAssignment(): update = " + update + "; update of assignments not supported by this client: " + getThisClassName());
    }



    /**
     * Same implementation as in superclass, buts goes into state POLLING_STOPPED on exit
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#runPollLoop(java.lang.Long)
     */
    @Override
    protected void runPollLoop (Long pollTimeout) throws InterruptedException {
        super.runPollLoop(pollTimeout);
        ClientState newState = ClientState.POLLING_STOPPED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
    }


    /**
     * Polls for messages and enques them into the message queue.
     * In the context of this method call also {@link #onPartitionsRevoked(Collection)}
     * and {@link #onPartitionsAssigned(Collection)} can be called
     * @param pollTimeout the timeout in milliseconds to wait for availability of consumer records.
     * @return the number of enqueued consumer records.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long)
     */
    @Override
    protected int pollAndEnqueue (long pollTimeout) throws InterruptedException, SerializationException {

        if (logger.isInfoEnabled() && state != ClientState.POLLING) logger.info(MessageFormat.format("pollAndEnqueue() [{0}]: Polling for records...", state)); //$NON-NLS-1$
        if (logger.isTraceEnabled()) logger.trace("Polling for records..."); //$NON-NLS-1$
        ConsumerRecords<?, ?> records = getConsumer().poll (pollTimeout);
        int numRecords = records == null? 0: records.count();
        if (logger.isTraceEnabled() && numRecords == 0) logger.trace("# polled records: " + (records == null? "0 (records == null)": "0"));
        if (numRecords > 0) {
            if (logger.isDebugEnabled()) logger.debug("# polled records: " + numRecords);
            records.forEach(cr -> {
                //TODO: remove the info trace
                //                if (logger.isInfoEnabled()) logger.info(MessageFormat.format ("consumed [{3}]: p={0}, o={1}, t={2}", cr.partition(), cr.offset(), cr.timestamp(), state));
                if (logger.isTraceEnabled()) {
                    logger.trace (cr.topic() + "-" + cr.partition() + " key=" + cr.key() + " - offset=" + cr.offset()); //$NON-NLS-1$
                }
                getMessageQueue().add(cr);
            });
        }
        if (state != ClientState.POLLING) {
            ClientState newState = ClientState.POLLING;
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
        }
        return numRecords;
    }



    /**
     * @param triggerCount the triggerCount to set
     */
    public void setTriggerCount (long triggerCount) {
        this.triggerCount = triggerCount;
    }



    /**
     * @param initialStartPosition the initialStartPosition to set
     */
    public void setInitialStartPosition(StartPosition initialStartPosition) {
        this.initialStartPosition = initialStartPosition;
    }



    /**
     * @param initialStartTimestamp the initialStartTimestamp to set
     */
    public void setInitialStartTimestamp (long initialStartTimestamp) {
        this.initialStartTimestamp = initialStartTimestamp;
    }

    /**
     * Creates an operator-scoped JCP control variable if it not yet exists and stores the initial offsets manager in serialized format.
     * @throws Exception
     */
    private void createJcpCvFromInitialOffsets() throws Exception {
        logger.info ("createJcpCvFromInitialOffsets(). initialOffsets = " + initialOffsets); 
        initialOffsetsCV = jcpContext.createStringControlVariable (OffsetManager.class.getName() + ".initial",
                false, serializeObject (initialOffsets));
        OffsetManager mgr = getInitialOffsetsFromJcpCv();
        logger.debug ("Retrieved value for initialOffsetsCV = " + mgr); 
    }

    /**
     * Retrieves the initial offsets manager from the JCP control variable.
     * @return  an OffsetManager object
     * @throws Exception
     */
    private OffsetManager getInitialOffsetsFromJcpCv() throws Exception {
        return SerializationUtils.deserialize (Base64.getDecoder().decode (initialOffsetsCV.sync().getValue()));
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
        private StartPosition initialStartPosition;
        private long initialStartTimestamp;

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

        /**
         * @param initialStartPosition the initialStartPosition to set
         */
        public final Builder setInitialStartPosition(StartPosition initialStartPosition) {
            this.initialStartPosition = initialStartPosition;
            return this;
        }

        /**
         * @param initialStartTimestamp the initialStartTimestamp to set
         */
        public final Builder setInitialStartTimestamp(long initialStartTimestamp) {
            this.initialStartTimestamp = initialStartTimestamp;
            return this;
        }

        /**
         * builds a new ConsumerClient. 
         * Note, that also {@link ConsumerClient#startConsumer()} must be called after building the client.
         * @return A new ConsumerClient instance
         * @throws Exception
         */
        public ConsumerClient build() throws Exception {
            CrKafkaConsumerGroupClient client = new CrKafkaConsumerGroupClient (operatorContext, keyClass, valueClass, kafkaProperties);
            client.setPollTimeout (this.pollTimeout);
            client.setTriggerCount (this.triggerCount);
            client.setInitialStartPosition (this.initialStartPosition);
            client.setInitialStartTimestamp (this.initialStartTimestamp);
            return client;
        }
    }
}
