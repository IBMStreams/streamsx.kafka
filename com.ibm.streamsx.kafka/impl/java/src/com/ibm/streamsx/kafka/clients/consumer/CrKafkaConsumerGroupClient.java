package com.ibm.streamsx.kafka.clients.consumer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.Controllable;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.KafkaOperatorResetFailedException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinator.MergeKey;
import com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinator.TP;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.operators.AbstractKafkaConsumerOperator;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class represents a Consumer client that can be used in consumer groups within a consistent region.
 */
public class CrKafkaConsumerGroupClient extends AbstractCrKafkaConsumerClient implements ConsumerRebalanceListener, Controllable, NotificationListener {

    private static final long THROTTLED_POLL_SLEEP_MS = 100l;
    private static final Logger logger = Logger.getLogger(CrKafkaConsumerGroupClient.class);
    private static final String MBEAN_DOMAIN_NAME = "com.ibm.streamsx.kafka";

    // if set to true, the content of the message queue is temporarily drained into a buffer and restored on start of poll
    // if set to false, the message queue is drained by submitting tuples
    private static final boolean DRAIN_TO_BUFFER_ON_CR_DRAIN = true;

    private static enum ClientState {
        INITIALIZED, EVENT_THREAD_STARTED,
        SUBSCRIBED,
        POLLING,
        POLLING_THROTTLED,
        DRAINING,
        DRAINED,
        CHECKPOINTED,
        POLLING_STOPPED,
        POLLING_CR_RESET_PENDING,
        RESET_COMPLETE,
    }

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
    private Set<CrConsumerGroupCoordinator.TP> assignablePartitions = Collections.synchronizedSet (new HashSet<>());
    /** counts the tuples to trigger operator driven CR */
    private long nSubmittedRecords = 0l;
    /** threshold of nSubmittedRecords for operator driven CR */
    private long triggerCount = 5000l; 
    /** Start position where each subscribed topic is consumed from */
    private StartPosition initialStartPosition = StartPosition.Default;
    private long initialStartTimestamp = -1l;
    private CountDownLatch jmxSetupLatch;
    private AtomicBoolean pollingStartPending = new AtomicBoolean(false);

    /** Lock for setting/checking the JMX notification */
    private final ReentrantLock jmxNotificationConditionLock = new ReentrantLock();
    /** Condition for setting/checking/waiting for the JMX notification */
    private final Condition jmxNotificationCondition = jmxNotificationConditionLock.newCondition();
    /** group's checkpoint merge complete JMX notification */
    private Map<MergeKey, CrConsumerGroupCoordinator.CheckpointMerge> jmxMergeCompletedNotifMap = new HashMap<>();

    /** canonical name of the MXBean. Note: If you need a javax.management.ObjectName instance, use createMBeanObjectName() */
    private String crGroupCoordinatorMXBeanName = null;
    /** MXBean proxy for coordinating the group's checkpoint */
    private CrConsumerGroupCoordinatorMXBean crGroupCoordinatorMxBean = null;
    /** stores the initial offsets of all partitions of all topics. Written to CV and used for reset to initial state */
    private OffsetManager initialOffsets;
    /** JCP control variable to persist the initialOffsets to JCP */
    private ControlVariableAccessor<String> initialOffsetsCV;
    /** stores the offsets of the submitted tuples - should contain only assignedPartitions - is checkpointed */
    private OffsetManager assignedPartitionsOffsetManager;
    /** the map that is used to seek the consumer after reset, resetToInitialState or in onPartitionsAssigned.
     * The map must contain mappings for all partitions of all topics because we do not know which partitions we get for consumption.
     */
    private Map<TopicPartition, Long> seekOffsetMap;
    /** current state of the consumer client */
    private ClientState state = null;
    Gson gson;

    /**
     * Constructs a new CrKafkaConsumerGroupClient object.
     * @throws KafkaConfigurationException 
     */
    private <K, V> CrKafkaConsumerGroupClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {

        super (operatorContext, keyClass, valueClass, kafkaProperties);

        this.groupId = kafkaProperties.getProperty (ConsumerConfig.GROUP_ID_CONFIG);
        this.crGroupCoordinatorMXBeanName = createMBeanObjectName().getCanonicalName();
        this.initialOffsets = new OffsetManager();
        this.assignedPartitionsOffsetManager = new OffsetManager();
        this.gson = (new GsonBuilder()).enableComplexMapKeySerialization().create();
        ConsistentRegionContext crContext = getCrContext();
        logger.info(MessageFormat.format ("CR timeouts: reset: {0}, drain: {1}", crContext.getResetTimeout(), crContext.getDrainTimeout()));
        ClientState newState = ClientState.INITIALIZED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
    }

    /**
     * Processes JMX connection related events.
     * @see com.ibm.streams.operator.control.Controllable#event(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext, com.ibm.streams.operator.control.Controllable.EventType)
     */
    @Override
    public void event (MBeanServerConnection jcp, OperatorContext context, EventType eventType) {
        logger.info ("JMX connection related event received: " + eventType);
        if (eventType == EventType.LostNotifications) {
            logger.warn ("JMX notification lost");
        }
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
            ConsistentRegionContext crContext = getCrContext();
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
            try {
                jcp.removeNotificationListener (groupMbeanName, this);
            } catch (ListenerNotFoundException | InstanceNotFoundException e) {
                logger.info ("removeNotificationListener failed: " + e.getLocalizedMessage());
            }
            logger.info("adding client as notification listener to " + crGroupCoordinatorMXBeanName);
            jcp.addNotificationListener (groupMbeanName, this, /*filter=*/null, /*handback=*/null);

            logger.info ("creating Proxies ...");
            crGroupCoordinatorMxBean = JMX.newMXBeanProxy (jcp, groupMbeanName, CrConsumerGroupCoordinatorMXBean.class, /*notificationEmitter=*/true);
            logger.debug ("MBean Proxy get test: group-ID = " + crGroupCoordinatorMxBean.getGroupId() + "; CR index = " + crGroupCoordinatorMxBean.getConsistentRegionIndex());
            crGroupCoordinatorMxBean.setRebalanceResetPending (false);
            crGroupCoordinatorMxBean.registerConsumerOperator (getOperatorContext().getName());
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
     * the notification at any time during its reset phase, perhaps also before {@link #processResetEvent(Checkpoint)} has been invoked.
     * Therefore, the implementation of {@link #handleNotification(Notification, Object)} must not expect a particular client state to succeed.
     * 
     * @see javax.management.NotificationListener#handleNotification(javax.management.Notification, java.lang.Object)
     */
    @Override
    public void handleNotification (Notification notification, Object handback) {
        logger.info (MessageFormat.format ("handleNotification() [{0}]; notification = {1}", this.state, notification));
        if (notification.getType().equals (CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE)) {
            CrConsumerGroupCoordinator.CheckpointMerge merge = gson.fromJson (notification.getMessage(), CrConsumerGroupCoordinator.CheckpointMerge.class);
            MergeKey key = merge.getKey();
            jmxNotificationConditionLock.lock();
            jmxMergeCompletedNotifMap.put (key, merge);
            logger.debug (MessageFormat.format("handleNotification(): notification {0} stored, signalling waiting threads ...", key));
            jmxNotificationCondition.signalAll();
            jmxNotificationConditionLock.unlock();
        }
        else if (notification.getType().equals (CrConsumerGroupCoordinatorMXBean.PARTITIONS_META_CHANGED)) {
            String msg = notification.getMessage();
            ArrayList<TP> partitions = SerializationUtils.deserialize (Base64.getDecoder().decode (msg));
            logger.info ("handleNotification(): partitions from JMX notification: " + partitions);
            this.assignablePartitions.addAll (partitions);
            logger.info ("handleNotification(): assignable partitions for topics " + this.subscribedTopics + ": " + this.assignablePartitions);
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
        getJcpContext().connect (this);
        // wait that MBeans are registered and notification listeners are set up, i.e. setup (...) of the Controllable interface is run:
        jmxSetupLatch.await();
        // calls our validate(), which uses the MBean ...
        super.startConsumer();
        // now we have a consumer object.
        assignedPartitionsOffsetManager.setOffsetConsumer (getConsumer());
        initialOffsets.setOffsetConsumer (getConsumer());
        ClientState newState = ClientState.EVENT_THREAD_STARTED;
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
        //        setAssignablePartitions (topicPartitionCandidates);
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
        //        setAssignablePartitions (topicPartitions);
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
     * Initiates checkpointing of the consumer client.
     * Implementations ensure that checkpointing the client has completed when this method returns. 
     * @param checkpoint the checkpoint
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onCheckpoint (Checkpoint checkpoint) throws InterruptedException {
        Event event = new Event(com.ibm.streamsx.kafka.clients.consumer.Event.EventType.CHECKPOINT, checkpoint, true);
        sendEvent (event);
        event.await();
        // in this client, we start polling at full speed after getting a permit to submit tuples
        sendStartThrottledPollingEvent (THROTTLED_POLL_SLEEP_MS);
    }


    /**
     * Initiates resetting the client to a prior state. 
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onReset (final Checkpoint checkpoint) throws InterruptedException {
        resetPrepareData (checkpoint);
        Event event = new Event(com.ibm.streamsx.kafka.clients.consumer.Event.EventType.RESET, checkpoint, true);
        sendEvent (event);
        event.await();
        // in this client, we start polling at full speed after getting a permit to submit tuples
        sendStartThrottledPollingEvent (THROTTLED_POLL_SLEEP_MS);

    }

    /**
     * Initiates resetting the client to the initial state. 
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onResetToInitialState() throws InterruptedException {
        Event event = new Event (com.ibm.streamsx.kafka.clients.consumer.Event.EventType.RESET_TO_INIT, true);
        sendEvent (event);
        event.await();
        // in this client, we start polling at full speed after getting a permit to submit tuples
        sendStartThrottledPollingEvent (THROTTLED_POLL_SLEEP_MS);
    }

    /**
     * Called when the consistent region is drained.
     * On drain, polling is stopped, when not CR trigger, the function waits until 
     * the message queue becomes empty, then it commits offsets.
     * This runs within a runtime thread, not by the event thread.
     */
    @Override
    public void onDrain () throws Exception {
        logger.info (MessageFormat.format("onDrain() [{0}] - entering", state));
        try {
            // stop filling the message queue with more messages, this method returns when polling has stopped - not fire and forget
            ClientState newState = ClientState.DRAINING;
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
            sendStopPollingEvent();
            logger.info ("onDrain(): sendStopPollingEvent() exit: event processed by event thread");
            // when CR is operator driven, do not wait for queue to be emptied.
            // This would never happen because the tuple submitter thread is blocked in makeConsistent() in postSubmit(...) 
            // and cannot empty the queue
            if (!getCrContext().isTriggerOperator()) {
                // here we are only when we are NOT the CR trigger (for example, periodic CR)
                if (DRAIN_TO_BUFFER_ON_CR_DRAIN) {
                    drainMessageQueueToBuffer();
                }
                else {
                    long before = System.currentTimeMillis();
                    logger.info("onDrain() waiting for message queue to become empty ...");
                    awaitEmptyMessageQueue();
                    logger.info("onDrain() message queue empty after " + (System.currentTimeMillis() - before) + " milliseconds");
                }
            }
            final boolean commitSync = true;
            final boolean commitPartitionWise = false;   // commit all partitions in one server request
            CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);

            synchronized (assignedPartitionsOffsetManager) {
                for (TopicPartition tp: assignedPartitionsOffsetManager.getMappedTopicPartitions()) {
                    offsets.put (tp, assignedPartitionsOffsetManager.getOffset(tp.topic(), tp.partition()));
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
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#onCheckpointRetire(long)
     */
    @Override
    public void onCheckpointRetire (long id) {
        logger.info (MessageFormat.format("onCheckpointRetire() [{0}] - entering, id = {1}", state, id));
        Collection<MergeKey> retiredMergeKeys = new ArrayList<>(10);
        for (MergeKey k: jmxMergeCompletedNotifMap.keySet()) {
            if (k.getSequenceId() <= id) {   // remove also older (smaller) IDs
                retiredMergeKeys.add (k);
            }
        }
        for (MergeKey k: retiredMergeKeys) {
            jmxMergeCompletedNotifMap.remove(k);
        }
        try {
            this.crGroupCoordinatorMxBean.cleanupMergeMap (id);
        } catch (IOException e) {
            logger.warn ("onCheckpointRetire() failed (Retrying for next higher sequence ID): " + e.getLocalizedMessage());
        }
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
            synchronized (assignedPartitionsOffsetManager) {
                assignedPartitionsOffsetManager.savePositionWhenRegistered (submittedRecord.topic(), submittedRecord.partition(), submittedRecord.offset() +1l);
            }
        } catch (Exception topicPartitionUnknown) {
            // should never happen in this client
            logger.warn (topicPartitionUnknown.getLocalizedMessage());
        }
        ConsistentRegionContext crContext = getCrContext();
        if (crContext.isTriggerOperator() && ++nSubmittedRecords >= triggerCount) {
            logger.debug("Making region consistent..."); //$NON-NLS-1$
            // makeConsistent blocks until all operators in the CR have drained and checkpointed
            boolean isSuccess = crContext.makeConsistent();
            nSubmittedRecords = 0l;
            logger.debug("Completed call to makeConsistent: isSuccess = " + isSuccess); //$NON-NLS-1$
        }
    }



    /**
     * Callback function of the ConsumerRebalanceListener, which is called in the context of KafkaConsumer.poll(...)
     * before partitions are re-assigned. 
     * onPartitionsRevoked is ignored when the client has initially subscribed to topics or when the client has been reset.
     * In all other cases a reset of the consistent region is triggered. Polling for messsages is stopped.
     * @param partitions current partition assignment  
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
     */
    @Override
    public void onPartitionsRevoked (Collection<TopicPartition> partitions) {
        logger.info (MessageFormat.format("onPartitionsRevoked() [{0}]: old partition assignment = {1}", state, partitions));
        this.nPartitionRebalances.increment();
        if (!(state == ClientState.SUBSCRIBED || state == ClientState.RESET_COMPLETE)) {
            ClientState newState = ClientState.POLLING_CR_RESET_PENDING;
            logger.info (MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
//            sendStopPollingEventAsync();
            sendStartThrottledPollingEvent (THROTTLED_POLL_SLEEP_MS);

            boolean resetPending = false;
            try {
                resetPending = crGroupCoordinatorMxBean.getAndSetRebalanceResetPending (true);
            } catch (IOException ioe) {
                logger.warn ("Could not test for already pending rebalance caused consistent region reset: " + ioe.getLocalizedMessage());
            }
            if (resetPending) {
                logger.info (MessageFormat.format ("onPartitionsRevoked() [{0}]: consistent region reset already initiated", state));
            }
            else {
                logger.info (MessageFormat.format ("onPartitionsRevoked() [{0}]: initiating consistent region reset", state));
                ThreadFactory thf = getOperatorContext().getThreadFactory();
                thf.newThread (new Runnable() {

                    @Override
                    public void run() {
                        try {
                            // sleep 100 ms to give rebalance a chance to finish
                            Thread.sleep (100);
                            getCrContext().reset();
                        }
                        catch (InterruptedException e) {
                            // ignore
                        } catch (IOException e) {
                            // restart the operator 
                            e.printStackTrace();
                            throw new RuntimeException ("Failed to reset the consistent region: " + e.getLocalizedMessage(), e);
                        }
                    }
                }).start();
            }

            // this callback is called within the context of a poll() invocation.
            // onPartitionsRevoked() is followed by onPartitionsAssigned() with the new assignment within that poll().
            // We must process this assignment, but seeking is not required because this happens within the reset 
            // request, which will be enqueued into the event queue - and processed by this thread.
        }
    }



    /**
     * Callback function of the ConsumerRebalanceListener, which is called in the context of KafkaConsumer.poll(...)
     * after partitions are re-assigned. 
     * onPartitionsAssigned performs following:
     * <ul>
     * <li>
     * update the assignable partitions from meta data. On change (for example, when partitions have
     * been added to topics and the metadata in the client have been refreshed) synchronize the partitions
     * with the other consumers in the group via JMX</li>
     * <li>
     * save current assignment for this operator
     * </li>
     * <li>
     * remove the messages for the gone partitions from the message queue. The message queue contains only uncommitted consumer records.
     * </li>
     * <li>
     * update the offsetManager for the assigned partitions: remove gone partitions, update the topics with the newly assigned partitions
     * </li>
     * <li>
     * When the client has initially subscribed to topics or has been reset before, a 'seekOffsetMap' has been updated before. This map maps
     * topic partitions to offsets. for every assigned partition, the client seeks to the offset in the map. If the assigned partition 
     * cannot be found in the map, the initial offset is determined as given by operator parameter 'startPosition' and 
     * optionally 'startTimestamp'.
     * </li>
     * <li>
     * update the offset manager with the new fetch positions. This information goes into the next checkpoint.
     * </li>
     * <li>
     * When the client is in a different state, the client does not seek.
     * </li>
     * </ul>
     *
     * @param newAssignedPartitions new partition assignment  
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
     */
    @Override
    public void onPartitionsAssigned (Collection<TopicPartition> newAssignedPartitions) {
        logger.info (MessageFormat.format("onPartitionsAssigned() [{0}]: new partition assignment = {1}", state, newAssignedPartitions));

        // get meta data of potentially assignable partitions for all subscribed topics, added partitions may be
        // returned from meta data or not dependent on the refresh policy. Therefore broadcast our assignment to all consumers of the group
        // to make them aware of potentially added partitions.
        int nPartitions = assignablePartitions.size();
        updateAssignablePartitionsFromMetadata();
        logger.info ("onPartitionsAssigned(): assignable partitions for topics " + this.subscribedTopics + ": " + this.assignablePartitions);
        if (nPartitions > 0 && assignablePartitions.size() > nPartitions) {
            // we have discovered new partitions for the subscribed topics; number of partitions always increases
            try {
                ArrayList <TP> c = new ArrayList<>(assignablePartitions);
                logger.info ("onPartitionsAssigned(): partitions discovered from mata data changed. broadcasting as JMX notification within the group: " + c);
                this.crGroupCoordinatorMxBean.broadcastData (serializeObject (c), CrConsumerGroupCoordinatorMXBean.PARTITIONS_META_CHANGED);
            } catch (IOException e) {
                logger.warn ("onPartitionsAssigned(): changed partition metadata could not be broadcasted: " + e.getLocalizedMessage());
            }
        }
        Set<TopicPartition> previousAssignment = new HashSet<>(getAssignedPartitions());
        getAssignedPartitions().clear();
        getAssignedPartitions().addAll (newAssignedPartitions);
        nAssignedPartitions.setValue (newAssignedPartitions.size());
        Set<TopicPartition> gonePartitions = new HashSet<>(previousAssignment);
        gonePartitions.removeAll (newAssignedPartitions);
        logger.info("topic partitions that are not assigned anymore: " + gonePartitions);

        synchronized (assignedPartitionsOffsetManager) {
            if (!gonePartitions.isEmpty()) {
                logger.info("removing consumer records from gone partitions from message queue");
                getMessageQueue().removeIf (queuedRecord -> belongsToPartition (queuedRecord, gonePartitions));
                for (TopicPartition tp: gonePartitions) {
                    // remove the topic partition also from the offset manager
                    assignedPartitionsOffsetManager.remove (tp.topic(), tp.partition());
                }
            }
            assignedPartitionsOffsetManager.updateTopics (newAssignedPartitions);
        }
        assignedPartitionsOffsetManager.savePositionFromCluster();
        logger.info("onPartitionsAssigned() assignedPartitionsOffsetManager = " + assignedPartitionsOffsetManager);
        logger.info("onPartitionsAssigned() assignedPartitions = " + getAssignedPartitions());
        switch (state) {
        case SUBSCRIBED:
        case RESET_COMPLETE:
            seekPartitions (newAssignedPartitions, seekOffsetMap);
            // update the fetch positions for all assigned partitions - 
            break;

        case POLLING_CR_RESET_PENDING:
            // silently ignore; we have updated assigned partitions and assignedPartitionsOffsetManager before
            break;
        default:
            // ... not observed during tests
            logger.warn (MessageFormat.format("onPartitionsAssigned() [{0}]: unexpected state for onPartitionsAssigned()", state));
        }
    }

    /**
     * Seeks the given partitions to the offsets in the map. If the map does not contain a mapping, the partition is seeked to what is given as initial start position (End, Beginning, Time)
     * @param partitions the partitions
     * @param offsetMap the map with mappings from partition to offset
     * @return the topic partitions for which the seek failed because they are not assigned. 
     */
    private Collection<TopicPartition> seekPartitions (Collection <TopicPartition> partitions, Map<TopicPartition, Long> offsetMap) {
        KafkaConsumer <?, ?> consumer = getConsumer();
        seekOffsetMap.equals(consumer);
        // collection and map for seeking to inital startposition
        Collection <TopicPartition> tp1 = new ArrayList<>(1);
        Map <TopicPartition, Long> tpTimestampMap1 = new HashMap<>();
        Set<TopicPartition> seekFailedPartitions = new HashSet<>();
        List<TopicPartition> sortedPartitions = new LinkedList<> (partitions);
        Collections.sort (sortedPartitions, new Comparator<TopicPartition>() {
            @Override
            public int compare (TopicPartition o1, TopicPartition o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });

        for (TopicPartition tp: sortedPartitions) {
            try {
                // in 'subscribe', the 'seekOffsetMap' has been filled with initial offsets dependent
                // on the startPosition parameter or has been restored from checkpoints via MXBean
                if (offsetMap.containsKey (tp)) {
                    final long seekToOffset = offsetMap.get(tp);
                    logger.info (MessageFormat.format ("seekPartitions() seeking {0} to offset {1}", tp, seekToOffset));
                    consumer.seek (tp, seekToOffset);
                }
                else {
                    // partition for which we have no offset, for example, a partition that has 
                    // been added to the topic after last checkpoint.
                    // Seek to startPosition given as operator parameter(s)
                    switch (this.initialStartPosition) {
                    case Default:
                        // do not seek
                        break;
                    case Beginning:
                    case End:
                        tp1.clear();
                        tp1.add (tp);
                        logger.info (MessageFormat.format ("seekPartitions() seeking new topic partition {0} to {1}", tp, this.initialStartPosition));
                        seekToPosition (tp1, this.initialStartPosition);
                        break;
                    case Time:
                        tpTimestampMap1.clear();
                        tpTimestampMap1.put (tp, this.initialStartTimestamp);
                        logger.info (MessageFormat.format ("seekPartitions() seeking new topic partition {0} to timestamp {1}", tp, this.initialStartTimestamp));
                        seekToTimestamp (tpTimestampMap1);
                        break;
                    default:
                        // unsupported start position, like 'Offset',  is already treated by initialization checks
                        final String msg = MessageFormat.format("seekPartitions(): {0} does not support startPosition {1}.", getThisClassName(), this.initialStartPosition);
                        logger.error (msg);
                        throw new RuntimeException (msg);
                    }
                }
            }
            catch (IllegalArgumentException topicPartitionNotAssigned) {
                // when this happens the ConsumerRebalanceListener will be called later
                logger.info (MessageFormat.format ("seekPartitions(): seek failed for partition {0}: {1}", tp, topicPartitionNotAssigned.getLocalizedMessage()));
                seekFailedPartitions.add (tp);
            }
        }
        return seekFailedPartitions;
    }

    /**
     * updates the assignablePartitions from the meta data of the subscribed topics. Note that the meta data might not be refreshed by the client.
     * When the meta data has not yet fetched, a call to the Kafka server is made.
     */
    private void updateAssignablePartitionsFromMetadata() {
        for (String topic: this.subscribedTopics) {
            // partitionsFor might return outdated partition information.
            List <PartitionInfo> partList = getConsumer().partitionsFor (topic);
            for (PartitionInfo part: partList) 
                this.assignablePartitions.add (new CrConsumerGroupCoordinator.TP (part.topic(), part.partition()));
        }
    }


    /**
     * initializes the assignablePartitions from a Collection of TopicPartitions.
     * @param topicPartitions the topic partitions
     */
    @SuppressWarnings("unused")
    private void setAssignablePartitions (Collection<TopicPartition> topicPartitions) {
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
        int myCrIndex = getCrContext().getIndex();
        int groupCrIndex = crGroupCoordinatorMxBean.getConsistentRegionIndex();
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
     * <li>reset the assignedPartitionsOffsetManager by setting the seekOffsets for all assigned partitions.</li>
     * </ul>
     * This method is run by the event thread. State must be POLLING_STOPPED.
     *
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetToInitEvent()
     */
    @Override
    protected void processResetToInitEvent() {
        logger.info (MessageFormat.format("processResetToInitEvent() [{0}] - entering", state));
        clearDrainBuffer();
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
            // are not yet notified about it. That's why we must handle the failed seeks.
            // When this operator is restarted and reset, getAssignedPartitions() will return an empty Set.

            // Reset also the assignedPartitionsOffsetManager to the initial offsets of the assigned partitions. The assignedPartitionsOffsetManager goes into the checkpoint,
            // and its offsets are used as the seek position when it comes to reset from a checkpoint. There must be a seek position also in
            // the case that no tuple has been submitted for a partition, which would update the assignedPartitionsOffsetManager.
            assignedPartitionsOffsetManager.clear();
            assignedPartitionsOffsetManager.addTopics (getAssignedPartitions());
            Collection<TopicPartition> failedSeeks = seekPartitions (getAssignedPartitions(), this.seekOffsetMap);
            failedSeeks.forEach (tp -> assignedPartitionsOffsetManager.remove (tp.topic(), tp.partition()));
            assignedPartitionsOffsetManager.savePositionFromCluster();
        }
        catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        ClientState newState = ClientState.RESET_COMPLETE;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info (MessageFormat.format("processResetToInitEvent() [{0}] - exiting", state));
    }



    /**
     * prepares the reset by clearing queues and buffers and creating the seek offset map.
     * This method is run within a runtime thread.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractCrKafkaConsumerClient#resetPrepareData(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void resetPrepareData (Checkpoint checkpoint) throws InterruptedException {
        clearDrainBuffer();
        getMessageQueue().clear();
        createSeekOffsetMap (checkpoint);
    }

    /**
     * Resets the consumer client to a previous state. The seek offsets must have been created before from a checkpoint.
     * This method is run by the event thread.
     * Resetting the client involves following steps:
     * <ul>
     * <li>seek all assigned partitions</li>
     * <li>reset the assignedPartitionsOffsetManager by setting the seekOffsets for all assigned partitions.</li>
     * </ul>
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetEvent(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    protected void processResetEvent (Checkpoint checkpoint) {

        logger.info (MessageFormat.format("processResetEvent() [{0}] - entering", state));
        // When no one of the KafkaConsumer in this group has been restarted before the region reset,
        // partition assignment will most likely not change and no onPartitionsRevoked()/onPartitionsAssigned will be fired on our
        // ConsumerRebalanceListener. That's why we must seek here to the partitions we think we are assigned to (can also be no partition).
        // If a consumer within our group - but not this operator - restarted, partition assignment may have changed, but we
        // are not yet notified about it. That's why we must handle the partitions for which the seek failed.
        // When this operator is restarted and reset, getAssignedPartitions() will return an empty Set. 

        // Reset also the assignedPartitionsOffsetManager to the initial offsets of the assigned partitions. The assignedPartitionsOffsetManager goes into the checkpoint,
        // and its offsets are used as the seek position when it comes to reset from a checkpoint. There must be a seek position also in
        // the case that no tuple has been submitted for a partition, which would update the assignedPartitionsOffsetManager.
        assignedPartitionsOffsetManager.clear();
        assignedPartitionsOffsetManager.addTopics (getAssignedPartitions());
        Collection<TopicPartition> failedSeeks = seekPartitions (getAssignedPartitions(), this.seekOffsetMap);
        failedSeeks.forEach (tp -> assignedPartitionsOffsetManager.remove (tp.topic(), tp.partition()));
        assignedPartitionsOffsetManager.savePositionFromCluster();
        ClientState newState = ClientState.RESET_COMPLETE;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info (MessageFormat.format("processResetEvent() [{0}] - exiting", state));
    }

    /**
     * The seek offsets are created with following algorithm from the checkpoint:
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
     * </ul>
     * @param checkpoint
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    private void createSeekOffsetMap (Checkpoint checkpoint) throws InterruptedException {
        String operatorName = getOperatorContext().getName();
        long chkptSeqId = checkpoint.getSequenceId();
        int resetAttempt = getCrContext().getResetAttempt();
        MergeKey key = new MergeKey (chkptSeqId, resetAttempt);
        logger.info (MessageFormat.format("createSeekOffsetMap() [{0}] - entering. chkptSeqId = {1}, resetAttempt = {2}", state, chkptSeqId, resetAttempt));
        try {
            logger.info(MessageFormat.format ("createSeekOffsetMap() - merging {0} operator checkpoints into a single group checkpoint", crGroupCoordinatorMxBean.getNumRegisteredConsumers()));
            final ObjectInputStream inputStream = checkpoint.getInputStream();
            Set<TP> assignableParts = (Set<TP>) inputStream.readObject();
            OffsetManager offsMgr = (OffsetManager) inputStream.readObject();
            Map <TopicPartition, Long> committedOffsetsMap = (Map <TopicPartition, Long>) inputStream.readObject();
            logger.info(MessageFormat.format("createSeekOffsetMap(): assignablePartitions read from checkpoint: {0}", assignableParts));
            logger.info(MessageFormat.format("createSeekOffsetMap(): offset manager read from checkpoint: {0}", offsMgr));
            logger.info(MessageFormat.format("createSeekOffsetMap(): committedOffsetsMap read from checkpoint: {0}", committedOffsetsMap));
            this.assignablePartitions.addAll (assignableParts);
            int nPartitionsTotal = assignableParts.size();
            int nPartitionsChckpt = offsMgr.size();
            logger.info (MessageFormat.format("contributing {0} partition => offset mappings to the group''s checkpoint for total {1} partitions", nPartitionsChckpt, nPartitionsTotal));
            if (nPartitionsChckpt > 0) {
                // send checkpoint data to CrGroupCoordinator MXBean and wait for the notification
                // to fetch the group's complete checkpoint. Then, process the group's checkpoint.
                Map<CrConsumerGroupCoordinator.TP, Long> partialOffsetMap = new HashMap<>();
                for (TopicPartition tp: offsMgr.getMappedTopicPartitions()) {
                    final String topic = tp.topic();
                    final int partition = tp.partition();
                    final Long offset = offsMgr.getOffset (topic, partition);
                    partialOffsetMap.put (new TP (topic, partition), offset);
                }

                logger.info (MessageFormat.format("Merging my group''s checkpoint contribution: partialOffsetMap = {0}, all consumable partitions = {1}",
                        partialOffsetMap, assignableParts));
                this.crGroupCoordinatorMxBean.mergeConsumerCheckpoint (chkptSeqId, resetAttempt, assignableParts, partialOffsetMap, operatorName);
            }
            else {
                logger.info("createSeekOffsetMap(): no contribution to the group's checkpoint from this operator");
            }

            // check JMX notification and wait for notification
            jmxNotificationConditionLock.lock();
            long waitStartTime= System.currentTimeMillis();
            long timeoutMillis = timeouts.getJmxResetNotificationTimeout();
            boolean waitTimeLeft = true;
            int nWaits = 0;
            long timeElapsed = 0;
            logger.info(MessageFormat.format("checking receiption of JMX notification {0} for sequenceId {1}. timeout = {2} ms.",
                    CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, key, timeoutMillis));
            while (!jmxMergeCompletedNotifMap.containsKey(key) && waitTimeLeft) {
                long remainingTime = timeoutMillis - timeElapsed;
                waitTimeLeft = remainingTime > 0;
                if (waitTimeLeft) {
                    if (nWaits++ %50 == 0) logger.info(MessageFormat.format("waiting for JMX notification {0} for sequenceId {1}. Remaining time = {2} of {3} ms",
                            CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, key, remainingTime, timeoutMillis));
                    jmxNotificationCondition.await (100, TimeUnit.MILLISECONDS);
                }
                timeElapsed = System.currentTimeMillis() - waitStartTime;
            }
            CrConsumerGroupCoordinator.CheckpointMerge merge = jmxMergeCompletedNotifMap.get(key);
            if (merge == null) {
                final String msg = MessageFormat.format ("timeout receiving {0} JMX notification for {1} from MXBean {2} in JCP. Current timeout is {3} milliseconds.",
                        CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, key, crGroupCoordinatorMXBeanName, timeoutMillis);
                logger.warn (msg);
                logger.info ("will restore group's checkpoint by merge of partly merged checkpoints with last committed offsets");
                // JMX notification may have lost, an at least partly usable result may be available anyway...
            }
            else {
                logger.info(MessageFormat.format("waiting for JMX notification for sequenceId {0} took {1} ms", key, timeElapsed));
            }

            Map <TP, Long> mergedOffsetMap = merge != null? merge.getConsolidatedOffsetMap(): crGroupCoordinatorMxBean.getConsolidatedOffsetMap (chkptSeqId, resetAttempt, operatorName);
            logger.info ("reset offsets (group's checkpoint) received/fetched from MXBean: " + mergedOffsetMap);

            // reset offsets from MXBean may be incomplete when the checkpoint was taken in the middle of rebalancing the group.
            // In order to have an offset to which we can seek, merge with the committedOffsetsMap - which might be older.
            initSeekOffsetMap();
            mergedOffsetMap.forEach ((tp, offset) -> {
                this.seekOffsetMap.put (new TopicPartition (tp.getTopic(), tp.getPartition()), offset);
            });
            committedOffsetsMap.forEach ((tp, committedOffset) -> {
                if (!this.seekOffsetMap.containsKey (tp)) {
                    logger.warn (MessageFormat.format ("createSeekOffsetMap(): consolidated offset map is missing offset for topic partition {0}. Using last committed offset at drain time: {1}", tp, committedOffset));
                    this.seekOffsetMap.put (tp, committedOffset);
                }
                else {
                    // compare offsets and log when they are different
                    long offset = seekOffsetMap.get(tp).longValue();
                    if (committedOffset.longValue() != offset) {
                        logger.info (MessageFormat.format("seek offsets for {0} from merged checkpoint {1} and committed at drain {2} differ. "
                                + " Assertion {1} > {2} must be true.", tp, offset, committedOffset));
                    }
                    else {
                        logger.debug (MessageFormat.format("seek offsets for {0} from merged checkpoint {1} and committed at drain {2} match.",
                                tp, offset, committedOffset));
                    }
                }
            });
        }
        catch (InterruptedException e) {
            logger.debug ("createSeekOffsetMap(): interrupted waiting for the JMX notification");
            return;
        }
        catch (IOException | ClassNotFoundException e) {
            logger.error ("reset failed: " + e.getLocalizedMessage());
            throw new KafkaOperatorResetFailedException (MessageFormat.format ("resetting operator {0} to checkpoint sequence ID {1} failed: {2}", getOperatorContext().getName(), chkptSeqId, e.getLocalizedMessage()), e);
        }
        finally {
            jmxNotificationConditionLock.unlock();
        }
        logger.info ("createSeekOffsetMap(): seekOffsetMap = " + this.seekOffsetMap);
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
     * <li>assignedPartitionsOffsetManager</li>
     * <li>current committed offsets fetched from the cluster. Gathering this data is expensive as it involves a Kafka server request.
     * </ul
     * @param checkpoint the reference of the checkpoint object
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processCheckpointEvent(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    protected void processCheckpointEvent (Checkpoint checkpoint) {
        logger.info (MessageFormat.format ("processCheckpointEvent() [{0}] sequenceId = {1}", state, checkpoint.getSequenceId()));
        try {
            // get committed Offsets for all Partitions.
            // Note, that checkpoint() can be called before all consumers in the group have committed.
            // This means that this map may contain offsets committed next to last drain.
            // On reset to such a checkpoint there is a chance that more tuples are replayed when 
            // checkpoint merge over MXBen fails for any reason - but only then.
            KafkaConsumer <?, ?> consumer = getConsumer();
            Set <TopicPartition> topicPartitions = new HashSet<>();
            for (String topic: subscribedTopics) {
                List <PartitionInfo> partitionInfos = consumer.partitionsFor (topic);
                for (PartitionInfo pi: partitionInfos) {
                    topicPartitions.add (new TopicPartition (pi.topic(), pi.partition()));
                }
            }
            // partitionsFor(..) returns the info from the meta data, which may be out-dated in our consumer.
            // merge with assignablePartitions, which is synchronized via JMX over the consumers of the group 
            for (TP tpa: this.assignablePartitions) {
                TopicPartition tp = new TopicPartition (tpa.getTopic(), tpa.getPartition());
                if (!topicPartitions.contains (tp)) {
                    topicPartitions.add (tp);
                }
            }

            Map <TopicPartition, Long> committedOffsetsMap = new HashMap<>();
            for (TopicPartition tp: topicPartitions) {
                OffsetAndMetadata ofsm = consumer.committed (tp);
                committedOffsetsMap.put (tp, ofsm.offset());
            }
            ObjectOutputStream oStream = checkpoint.getOutputStream();
            oStream.writeObject (this.assignablePartitions);
            oStream.writeObject (this.assignedPartitionsOffsetManager);
            oStream.writeObject (committedOffsetsMap);
            //            if (logger.isDebugEnabled()) {
            if (logger.isInfoEnabled()) logger.info ("data written to checkpoint: assignablePartitions = " + this.assignablePartitions);
            if (logger.isInfoEnabled()) logger.info ("data written to checkpoint: assignedPartitionsOffsetManager = " + this.assignedPartitionsOffsetManager);
            if (logger.isInfoEnabled()) logger.info ("data written to checkpoint: current committed offsets = " + committedOffsetsMap);
            //            }
        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        ClientState newState = ClientState.CHECKPOINTED;
        logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
        state = newState;
        logger.info ("processCheckpointEvent() - exiting.");
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
        logger.warn("processUpdateAssignmentEvent(): update = " + update + "; update of assignments not supported by this client: " + getThisClassName());
    }



    /**
     * Same implementation as in superclass, but maintains consumer client state
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#runPollLoop(long, long)
     */
    @Override
    protected void runPollLoop(long pollTimeout, long throttleSleepMillis) throws InterruptedException {
        pollingStartPending.set (false);
        // do not change the state beforehand because the ConsumerRebalanceListener callbacks might be called, which behave state dependent
        super.runPollLoop (pollTimeout, throttleSleepMillis);
        // Don't change state, when state is something CR related, like DRAINING, RESETTING, ... 
        if (state == ClientState.POLLING || state == ClientState.POLLING_CR_RESET_PENDING || state == ClientState.POLLING_THROTTLED) {
            ClientState newState = ClientState.POLLING_STOPPED;
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
        }
        else {
            logger.info (MessageFormat.format ("runPollLoop() [{0}]: polling stopped", state));
        }
    }


    /**
     * Polls for messages and enqueues them into the message queue.
     * In the context of this method call also {@link #onPartitionsRevoked(Collection)}
     * and {@link #onPartitionsAssigned(Collection)} can be called
     * @param pollTimeout the timeout in milliseconds to wait for availability of consumer records.
     * @param isThrottled true, when polling is throttled, false otherwise
     * @return the number of enqueued consumer records.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long)
     */
    @Override
    protected int pollAndEnqueue (long pollTimeout, boolean isThrottled) throws InterruptedException, SerializationException {

        if (logger.isInfoEnabled() && !(state == ClientState.POLLING || state == ClientState.POLLING_THROTTLED)) {
            logger.info(MessageFormat.format ("pollAndEnqueue() [{0}]: Polling for records, Kafka poll timeout = {1}", state, pollTimeout)); //$NON-NLS-1$
        }
        if (logger.isTraceEnabled()) logger.trace ("Polling for records..."); //$NON-NLS-1$
        // Note: within poll(...) the ConsumerRebalanceListener might be called, changing the state to POLLING_CR_RESET_PENDING
        long before = 0;
        if (logger.isDebugEnabled()) before = System.currentTimeMillis();
        ConsumerRecords<?, ?> records = getConsumer().poll (pollTimeout);
        int numRecords = records == null? 0: records.count();
        if (logger.isTraceEnabled() && numRecords == 0) logger.trace("# polled records: " + (records == null? "0 (records == null)": "0"));
        if (logger.isDebugEnabled()) {
            logger.debug (MessageFormat.format ("consumer.poll took {0} ms, numRecords = {1}", (System.currentTimeMillis() - before), numRecords));
        }
        if (state == ClientState.POLLING_CR_RESET_PENDING) {
            logger.info (MessageFormat.format ("pollAndEnqueue() [{0}]: Stop enqueuing fetched records", state));
            return 0;
        }
        // state transition
        ClientState newState = state;
        if (state != ClientState.POLLING) {
            newState = isThrottled? ClientState.POLLING_THROTTLED: ClientState.POLLING;
        }
        if (state != ClientState.POLLING_THROTTLED) {
            newState = isThrottled? ClientState.POLLING_THROTTLED: ClientState.POLLING;
        }
        if (state != newState) {
            logger.info(MessageFormat.format("client state transition: {0} -> {1}", state, newState));
            state = newState;
        }
        // add records to message queue
        if (numRecords > 0) {
            if (logger.isDebugEnabled()) logger.debug("# polled records: " + numRecords);
            records.forEach(cr -> {
                if (logger.isTraceEnabled()) {
                    logger.trace (MessageFormat.format ("consumed [{0}]: tp={1}, pt={2}, of={3}, ts={4}, ky={5}",
                            state,  cr.topic(), cr.partition(), cr.offset(), cr.timestamp(), cr.key()));
                }
                getMessageQueue().add(cr);
            });
        }
        return numRecords;
    }



    /**
     * The method is called by the worker thread that submits tuples after acquiring a permit when in consistent region.
     * When the state is RESET_COMPLETE or CHECKPOINTED, it initiates polling for Kafka messages and calls the default
     * implementation from the base class.
     * 
     * @param timeout    the timeout to wait for records
     * @param timeUnit   the unit of time for the timeout
     * @return the next consumer record or `null` if there was no record within the timeout.
     * @throws InterruptedException The thread waiting for records has been interrupted.
     *
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#getNextRecord(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public ConsumerRecord<?, ?> getNextRecord (long timeout, TimeUnit timeUnit) throws InterruptedException {
        //  if ((state == ClientState.RESET_COMPLETE || state == ClientState.CHECKPOINTED) && pollingStartPending.get() == false) {
        if (!(state == ClientState.POLLING || state == ClientState.DRAINING || state == ClientState.POLLING_CR_RESET_PENDING) && pollingStartPending.get() == false) {
            logger.info(MessageFormat.format("getNextRecord() [{0}] - Acquired permit - initiating polling for records", state)); 
            pollingStartPending.set (true);
            try {
                crGroupCoordinatorMxBean.setRebalanceResetPending (false);
            } catch (IOException e) {
                // connection to MX bean is broken. state is reset on JMX initialization in setup()
                e.printStackTrace();
            }
            sendStartPollingEvent();
            Thread.sleep(50);
        }
        return super.getNextRecord (timeout, timeUnit);
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
        initialOffsetsCV = getJcpContext().createStringControlVariable (OffsetManager.class.getName() + ".initial",
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
