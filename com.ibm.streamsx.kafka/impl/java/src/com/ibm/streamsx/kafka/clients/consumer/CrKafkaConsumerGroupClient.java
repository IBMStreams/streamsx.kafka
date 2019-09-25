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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ConsistentRegionMXBean;
import com.ibm.streams.operator.control.Controllable;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.KafkaOperatorResetFailedException;
import com.ibm.streamsx.kafka.KafkaOperatorRuntimeException;
import com.ibm.streamsx.kafka.MsgFormatter;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinator.MergeKey;
import com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinator.TP;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class represents a Consumer client that can be used in consumer groups within a consistent region.
 */
public class CrKafkaConsumerGroupClient extends AbstractCrKafkaConsumerClient implements ConsumerRebalanceListener, Controllable, NotificationListener {

    private static final long THROTTLED_POLL_SLEEP_MS = 100l;
    private static final Logger trace = Logger.getLogger(CrKafkaConsumerGroupClient.class);
    private static final String MBEAN_DOMAIN_NAME = "com.ibm.streamsx.kafka";

    /**
     * if set to true, the content of the message queue is temporarily drained into a buffer and restored on start of poll
     * if set to false, the message queue is drained by submitting tuples
     */
    private static final boolean DRAIN_TO_BUFFER_ON_CR_DRAIN = true;
    private static final boolean ENABLE_CHECK_REGISTERED_ON_CHECKPOINT = true;

    private static enum ClientState {
        INITIALIZED,
        EVENT_THREAD_STARTED,
        SUBSCRIBED,
        RESET_COMPLETE,
        RECORDS_FETCHED,
        CR_RESET_PENDING
    }

    /** counts the tuples to trigger operator driven CR */
    private long nSubmittedRecords = 0l;
    /** threshold of nSubmittedRecords for operator driven CR */
    private long triggerCount = 5000l; 
    /** Start position where each subscribed topic is consumed from */
    private StartPosition initialStartPosition = StartPosition.Default;
    private long initialStartTimestamp = -1l;
    private CountDownLatch jmxSetupLatch;
    private AtomicBoolean startPollingRequired = new AtomicBoolean (false);

    /** Lock for setting/checking the JMX notification */
    private final ReentrantLock jmxNotificationConditionLock = new ReentrantLock();
    /** Condition for setting/checking/waiting for the JMX notification */
    private final Condition jmxNotificationCondition = jmxNotificationConditionLock.newCondition();
    /** group's checkpoint merge complete JMX notification */
    private Map<CrConsumerGroupCoordinator.MergeKey, CrConsumerGroupCoordinator.CheckpointMerge> jmxMergeCompletedNotifMap = new HashMap<>();

    /** canonical name of the MXBean. Note: If you need a javax.management.ObjectName instance, use createMBeanObjectName() */
    private String crGroupCoordinatorMXBeanName = null;
    /** MXBean proxy for coordinating the group's checkpoint */
    private CrConsumerGroupCoordinatorMXBean crGroupCoordinatorMxBean = null;
    private ConsistentRegionMXBean crMxBean = null;
    private CVOffsetAccessor initialOffsets;
    /** stores the offsets of the submitted tuples - should contain only assignedPartitions - is checkpointed */
    private OffsetManager assignedPartitionsOffsetManager;
    /** the map that is used to seek the consumer after reset, resetToInitialState or in onPartitionsAssigned.
     * The map must contain mappings for all partitions of all topics because we do not know which partitions we get for consumption.
     */
    private Map<TopicPartition, Long> seekOffsetMap = null;
    /** current state of the consumer client */
    private ClientState state = null;
    private Gson gson;

    /**
     * Constructs a new CrKafkaConsumerGroupClient object.
     * @throws KafkaConfigurationException 
     */
    private <K, V> CrKafkaConsumerGroupClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, boolean singleTopic) throws KafkaConfigurationException {

        super (operatorContext, keyClass, valueClass, kafkaProperties);
        this.initialOffsets = new CVOffsetAccessor (getJcpContext(), getGroupId());
        this.crGroupCoordinatorMXBeanName = createMBeanObjectName ("consumergroup").getCanonicalName();
        this.assignedPartitionsOffsetManager = new OffsetManager();
        this.gson = (new GsonBuilder()).enableComplexMapKeySerialization().create();
        ConsistentRegionContext crContext = getCrContext();
        // if no partition assignment strategy is specified, set the round-robin when multiple topics can be subscribed
        if (!(singleTopic || kafkaProperties.containsKey (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))) {
            String assignmentStrategy = RoundRobinAssignor.class.getCanonicalName();
            kafkaProperties.put (ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
            trace.info (MsgFormatter.format ("Multiple topics specified or possible by using a pattern. Using the ''{0}'' partition assignment strategy for group management", assignmentStrategy));
        }
        trace.info (MsgFormatter.format ("CR timeouts: reset: {0}, drain: {1}", crContext.getResetTimeout(), crContext.getDrainTimeout()));
        ClientState newState = ClientState.INITIALIZED;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
    }

    /**
     * Processes JMX connection related events.
     * @see com.ibm.streams.operator.control.Controllable#event(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext, com.ibm.streams.operator.control.Controllable.EventType)
     */
    @Override
    public void event (MBeanServerConnection jcp, OperatorContext context, EventType eventType) {
        trace.log (DEBUG_LEVEL, "JMX connection related event received: " + eventType);
        if (eventType == EventType.LostNotifications) {
            trace.warn ("JMX notification lost");
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
            ConsistentRegionContext crContext = getCrContext();
            ObjectName groupMbeanName = createMBeanObjectName ("consumergroup");
            this.crGroupCoordinatorMXBeanName = groupMbeanName.getCanonicalName();
            // Try to register the MBean for checkpoint coordination in JCP. One of the operators in the consumer group wins.
            trace.log (DEBUG_LEVEL, "Trying to register MBean in JCP: " + crGroupCoordinatorMXBeanName);
            if (jcp.isRegistered (groupMbeanName)) {
                trace.log (DEBUG_LEVEL, crGroupCoordinatorMXBeanName + " already registered");
            }
            else {
                try {
                    // Constructor signature: (String groupId, Integer consistentRegionIndex, String traceLevel)
                    jcp.createMBean (CrConsumerGroupCoordinator.class.getName(), groupMbeanName, 
                            new Object[] {getGroupId(), new Integer(crContext.getIndex()), DEBUG_LEVEL.toString()},
                            new String[] {"java.lang.String", "java.lang.Integer", "java.lang.String"});
                    trace.log (DEBUG_LEVEL, "MBean registered: " + crGroupCoordinatorMXBeanName);
                }
                catch (InstanceAlreadyExistsException e) {
                    // another operator managed to create it first. that is ok, just use that one.
                    trace.log (DEBUG_LEVEL, MsgFormatter.format ("another operator just created {0}: {1}", crGroupCoordinatorMXBeanName, e.getMessage()));
                }
            }
            try {
                jcp.removeNotificationListener (groupMbeanName, this);
            } catch (ListenerNotFoundException | InstanceNotFoundException e) {
                trace.log (DEBUG_LEVEL, "removeNotificationListener failed: " + e.getLocalizedMessage());
            }
            trace.log (DEBUG_LEVEL, "adding client as notification listener to " + crGroupCoordinatorMXBeanName);
            jcp.addNotificationListener (groupMbeanName, this, /*filter=*/null, /*handback=*/null);

            trace.log (DEBUG_LEVEL, "creating Proxies ...");
            crMxBean = JMX.newMXBeanProxy (jcp, getCrContext().getConsistentRegionMXBeanName(), ConsistentRegionMXBean.class);
            crGroupCoordinatorMxBean = JMX.newMXBeanProxy (jcp, groupMbeanName, CrConsumerGroupCoordinatorMXBean.class, /*notificationEmitter=*/true);
            trace.log (DEBUG_LEVEL, "MBean Proxy get test: group-ID = " + crGroupCoordinatorMxBean.getGroupId() + "; CR index = " + crGroupCoordinatorMxBean.getConsistentRegionIndex());
            crGroupCoordinatorMxBean.registerConsumerOperator (getOperatorContext().getName());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            jmxSetupLatch.countDown();
        }
    }

    /**
     * Creates the object name for the group MBean.
     * The name is {@value #MBEAN_DOMAIN_NAME}:type=<i>mBeanType</i>,groupIdHash=<i>hashCode(our group-ID)</i>
     * @param mBeanType The 'type' property value of the object name
     * @return An ObjectName instance
     */
    private ObjectName createMBeanObjectName (String mBeanType) {
        Hashtable <String, String> props = new Hashtable<>();
        props.put ("type", mBeanType);
        final int hash = getGroupId().hashCode();
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
     * calling {@link CrConsumerGroupCoordinatorMXBean#mergeConsumerCheckpoint(long, int, int, Map, String)}.
     * <br><br>
     * When we have more consumers than topic partitions, it is obvious that not all operator checkpoints contribute to
     * the group's checkpoint. We should take into account that {@link #handleNotification(Notification, Object)} can 
     * be called at any time. The decision, when a group's checkpoint is complete (and therefore when the JMX 
     * notification is fired) is up to the MXBean implementation. An operator that does not contribute to the 
     * group's checkpoint can (in theory) therefore receive the notification at any time during its reset phase, 
     * perhaps also before {@link #processResetEvent(Checkpoint)} has been invoked.
     * 
     * @see javax.management.NotificationListener#handleNotification(javax.management.Notification, java.lang.Object)
     */
    @Override
    public void handleNotification (Notification notification, Object handback) {
        trace.info (MsgFormatter.format ("handleNotification() [{0}]; notification = {1}", this.state, notification));
        if (notification.getType().equals (CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE)) {
            CrConsumerGroupCoordinator.CheckpointMerge merge = gson.fromJson (notification.getMessage(), CrConsumerGroupCoordinator.CheckpointMerge.class);
            MergeKey key = merge.getKey();
            jmxNotificationConditionLock.lock();
            jmxMergeCompletedNotifMap.put (key, merge);
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("handleNotification(): notification {0} stored, signalling waiting threads ...", key));
            jmxNotificationCondition.signalAll();
            jmxNotificationConditionLock.unlock();
        }
        else {
            trace.warn ("unexpected notification type (ignored): " + notification.getType());
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        trace.info (MsgFormatter.format ("startConsumer() [{0}] - consumer start initiated", state));
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
        ClientState newState = ClientState.EVENT_THREAD_STARTED;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
        trace.info ("consumer started");
    }


    /**
     * Subscribes with a pattern and start consuming offsets with a given timestamp in milliseconds since Epoch.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.regex.Pattern, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Pattern pattern, long timestamp) throws Exception {
        trace.info (MsgFormatter.format ("subscribeToTopicsWithTimestamp [{0}]: pattern = {1}, timestamp = {2,number,#}",
                state, (pattern == null? "null": pattern.pattern()), timestamp));
        assert this.initialStartPosition == StartPosition.Time;
        if (pattern == null) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics or apattern must be specified.");
            throw new KafkaConfigurationException ("pattern == null");
        }
        initSeekOffsetMap();
        subscribe (pattern, this);
        // when later partitions are assigned dynamically, we seek in onPartitionsAssigned
        ClientState newState = ClientState.SUBSCRIBED;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
        trace.info ("subscribed to pattern: " + pattern.pattern());
    }


    /**
     * Subscribes with a pattern and start consuming at a given start position (Beginning, End, Default).
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.regex.Pattern, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Pattern pattern, StartPosition startPosition) throws Exception {
        trace.info (MsgFormatter.format ("subscribeToTopics [{0}]: pattern = {1}, startPosition = {2}",
                state, (pattern == null? "null": pattern.pattern()), startPosition));
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        assert startPosition == this.initialStartPosition;
        if (pattern == null) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics or apattern must be specified.");
            throw new KafkaConfigurationException ("pattern == null");
        }
        initSeekOffsetMap();
        subscribe (pattern, this);
        // when later partitions are assigned dynamically, we seek in onPartitionsAssigned
        ClientState newState = ClientState.SUBSCRIBED;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
        trace.info ("subscribed to pattern: " + pattern.pattern());
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
        trace.info (MsgFormatter.format ("subscribeToTopics [{0}]: topics = {1}, partitions = {2}, startPosition = {3}",
                state, topics, partitions, startPosition));
        assert startPosition != StartPosition.Time && startPosition != StartPosition.Offset;
        assert startPosition == this.initialStartPosition;
        if (partitions != null && !partitions.isEmpty()) {
            trace.error("When the " + getThisClassName() + " consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }
        initSeekOffsetMap();
        subscribe (topics, this);
        // when later partitions are assigned dynamically, we seek in onPartitionsAssigned
        ClientState newState = ClientState.SUBSCRIBED;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
        trace.info ("subscribed to " + topics);
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
        trace.info (MsgFormatter.format ("subscribeToTopicsWithTimestamp [{0}]: topics = {1}, partitions = {2}, timestamp = {3,number,#}",
                state, topics, partitions, timestamp));
        assert this.initialStartPosition == StartPosition.Time;
        // partitions must be null or empty
        if (partitions != null && !partitions.isEmpty()) {
            trace.error("When the " + getThisClassName() + " Consumer client is used, no partitions must be specified. partitions: " + partitions);
            throw new KafkaConfigurationException ("Partitions for assignment must not be specified. Found: " + partitions);
        }
        if (topics == null || topics.isEmpty()) {
            trace.error ("When the " + getThisClassName() + " consumer client is used, topics must be specified. topics: " + topics);
            throw new KafkaConfigurationException ("topics must not be null or empty. topics = " + topics);
        }
        initSeekOffsetMap();
        subscribe (topics, this);
        // when later partitions are assigned dynamically, we seek using the seekOffsetMap
        ClientState newState = ClientState.SUBSCRIBED;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
        trace.info ("subscribed to " + topics);
    }



    /**
     * Assignment to specific partitions and seek to specific offsets is not supported by this client.
     * This method should never be called.
     * @throws KafkaConfigurationException always thrown to indicate failure. This exception should be treated as a programming error.
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithOffsets(java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public void subscribeToTopicsWithOffsets(String topic, List<Integer> partitions, List<Long> startOffsets) throws Exception {
        trace.log (DEBUG_LEVEL, "subscribeToTopicsWithOffsets: topic = " + topic + ", partitions = " + partitions + ", startOffsets = " + startOffsets);
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
        if (ENABLE_CHECK_REGISTERED_ON_CHECKPOINT) {
            final String myOperatorName = getOperatorContext().getName();
            try {
                Set<String> registeredConsumers = this.crGroupCoordinatorMxBean.getRegisteredConsumerOperators();
                if (!registeredConsumers.contains (myOperatorName)) {
                    trace.warn (MsgFormatter.format ("My operator name not registered in group MXBean: {0}, trying to register", myOperatorName));
                    this.crGroupCoordinatorMxBean.registerConsumerOperator (myOperatorName);
                }
            } catch (IOException e) {
                trace.error (e.getMessage());
            }
        }
        Event event = new Event(com.ibm.streamsx.kafka.clients.consumer.Event.EventType.CHECKPOINT, checkpoint, true);
        sendEvent (event);
        event.await();
        // in this client, we start polling at full speed after getting a permit to submit tuples
        sendStartThrottledPollingEvent (THROTTLED_POLL_SLEEP_MS);
        startPollingRequired.set (true);
    }


    /**
     * Initiates resetting the client to a prior state. 
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onReset (final Checkpoint checkpoint) throws InterruptedException {
        resetPrepareDataBeforeStopPolling(checkpoint);
        sendStopPollingEvent();
        resetPrepareDataAfterStopPolling (checkpoint);
        Event event = new Event(com.ibm.streamsx.kafka.clients.consumer.Event.EventType.RESET, checkpoint, true);
        sendEvent (event);
        event.await();
        // in this client, we start polling at full speed after getting a permit to submit tuples
        sendStartThrottledPollingEvent (THROTTLED_POLL_SLEEP_MS);
        startPollingRequired.set (true);
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
        startPollingRequired.set (true);
    }

    /**
     * Called when the consistent region is drained.
     * On drain, polling is stopped, when not CR trigger, the function waits until 
     * the message queue becomes empty, then it commits offsets.
     * This runs within a runtime thread, not by the event thread.
     */
    @Override
    public void onDrain () throws Exception {
        trace.info (MsgFormatter.format ("onDrain() [{0}] - entering", state));
        try {
            // stop filling the message queue with more messages, this method returns when polling has stopped - not fire and forget
            sendStopPollingEvent();
            trace.log (DEBUG_LEVEL, "onDrain(): sendStopPollingEvent() exit: event processed by event thread");
            // when CR is operator driven, do not wait for queue to be emptied.
            // This would never happen because the tuple submitter thread is blocked in makeConsistent() in postSubmit(...) 
            // and cannot empty the queue
            if (!getCrContext().isTriggerOperator()) {
                // here we are only when we are NOT the CR trigger (for example, periodic CR)
                if (DRAIN_TO_BUFFER_ON_CR_DRAIN) {
                    drainMessageQueueToBuffer();
                    // also, when we drain the queue into a buffer, there may be in-flight messages
                    // wait for them to be processed.
                }
                long before = System.currentTimeMillis();
                trace.log (DEBUG_LEVEL, "onDrain() waiting for message queue to become empty ...");
                awaitMessageQueueProcessed();
                trace.log (DEBUG_LEVEL, "onDrain() message queue empty after " + (System.currentTimeMillis() - before) + " milliseconds");
            }
            final boolean commitSync = true;
            final boolean commitPartitionWise = false;   // commit all partitions in one server request
            CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);
            offsets.setThrowOnSynchronousCommitFailure (false);
            synchronized (assignedPartitionsOffsetManager) {
                for (TopicPartition tp: assignedPartitionsOffsetManager.getMappedTopicPartitions()) {
                    offsets.put (tp, assignedPartitionsOffsetManager.getOffset(tp.topic(), tp.partition()));
                }
            }
            if (!offsets.isEmpty()) {
                sendCommitEvent (offsets);
            }
            else {
                trace.info ("onDrain(): no offsets to commit");
            }
            // drain is followed by checkpoint.
            // Don't poll for new messages in the meantime. - Don't send a 'start polling event'
        } catch (InterruptedException e) {
            trace.log (DEBUG_LEVEL, "Interrupted waiting for empty queue or committing offsets");
            // NOT to start polling for Kafka messages again, is ok after interruption
        }
        trace.log (DEBUG_LEVEL, "onDrain() - exiting");
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#onCheckpointRetire(long)
     */
    @Override
    public void onCheckpointRetire (long id) {
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("onCheckpointRetire() [{0}] - entering, id = {1}", state, id));
        Collection<MergeKey> retiredMergeKeys = new ArrayList<>(10);
        for (MergeKey k: jmxMergeCompletedNotifMap.keySet()) {
            if (k.getSequenceId() <= id) {   // remove also older (smaller) IDs
                retiredMergeKeys.add (k);
            }
        }
        for (MergeKey k: retiredMergeKeys) {
            jmxMergeCompletedNotifMap.remove (k);
        }
        try {
            this.crGroupCoordinatorMxBean.cleanupMergeMap (id);
        } catch (IOException e) {
            trace.warn ("onCheckpointRetire() failed (Retrying for next higher sequence ID): " + e.getMessage());
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
            // can happen when onPartitionsAssigned happened between fetching a record from the queue by this thread and postSubmit.
            // onPartitionsRevoked must have been called before, initiating a consistent region reset.
            trace.warn (topicPartitionUnknown.getLocalizedMessage());
        }
        ConsistentRegionContext crContext = getCrContext();
        if (crContext.isTriggerOperator() && ++nSubmittedRecords >= triggerCount) {
            trace.log (DEBUG_LEVEL, "Making region consistent..."); //$NON-NLS-1$
            // makeConsistent blocks until all operators in the CR have drained and checkpointed
            boolean isSuccess = crContext.makeConsistent();
            nSubmittedRecords = 0l;
            trace.log (DEBUG_LEVEL, "Completed call to makeConsistent: isSuccess = " + isSuccess); //$NON-NLS-1$
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
        trace.info (MsgFormatter.format ("onPartitionsRevoked() [{0}]: old partition assignment = {1}", state, partitions));
        getOperatorContext().getMetrics().getCustomMetric (N_PARTITION_REBALANCES).increment();
        // remove the content of the queue. It contains uncommitted messages.
        // They will be fetched again after rebalance.
        getMessageQueue().clear();
        setConsumedTopics (null);
        if (state == ClientState.RECORDS_FETCHED) {
            ClientState newState = ClientState.CR_RESET_PENDING;
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
            state = newState;
            sendStopPollingEventAsync();
            trace.info (MsgFormatter.format ("onPartitionsRevoked() [{0}]: initiating consistent region reset", state));
            try {
                crMxBean.reset (true);
            } catch (Exception e) {
                throw new KafkaOperatorRuntimeException ("Failed to reset the consistent region: " + e.getMessage(), e);
            }
            // this callback is called within the context of a poll() invocation.
        }
    }



    /**
     * Callback function of the ConsumerRebalanceListener, which is called in the context of KafkaConsumer.poll(...)
     * after partitions are re-assigned. 
     * onPartitionsAssigned performs following:
     * <ul>
     * <li>
     * save current assignment for this operator
     * </li>
     * <li>
     * update the offsetManager for the assigned partitions: remove gone partitions, update the topics with the newly assigned partitions
     * </li>
     * <li>
     * When the client has has been reset before from a checkpoint, or has been reset to initial state with assigned partitions,
     * a 'seekOffsetMap' with content in it has been created before. This map maps
     * topic partitions to offsets. When partitions are assigned that have no mapping in the map, the control variable is accessed 
     * for the initial offset, and added to the map. If there is no control variable, the initial offset is determined as given
     * by operator parameter 'startPosition' and optionally 'startTimestamp' and written into a control variable for the topic partition.
     * For every assigned partition, the client seeks to the offset in the map.
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
        trace.info (MsgFormatter.format ("onPartitionsAssigned() [{0}]: new partition assignment = {1}", state, newAssignedPartitions));
        Set<TopicPartition> gonePartitions = new HashSet<>(getAssignedPartitions());
        gonePartitions.removeAll (newAssignedPartitions);
        getAssignedPartitions().clear();
        getAssignedPartitions().addAll (newAssignedPartitions);
        nAssignedPartitions.setValue (newAssignedPartitions.size());
        trace.info ("topic partitions that are not assigned anymore: " + gonePartitions);

        synchronized (assignedPartitionsOffsetManager) {
            if (!gonePartitions.isEmpty()) {
                for (TopicPartition tp: gonePartitions) {
                    // remove the topic partition also from the offset manager
                    assignedPartitionsOffsetManager.remove (tp.topic(), tp.partition());
                }
            }
            assignedPartitionsOffsetManager.updateTopics (newAssignedPartitions);
        }
        setConsumedTopics (newAssignedPartitions);
        trace.log (DEBUG_LEVEL, "onPartitionsAssigned() assignedPartitionsOffsetManager = " + assignedPartitionsOffsetManager);
        trace.log (DEBUG_LEVEL, "onPartitionsAssigned() assignedPartitions = " + getAssignedPartitions());
        switch (state) {
        case SUBSCRIBED:
        case RESET_COMPLETE:
            assert (seekOffsetMap != null);
            // the seek offset map can be empty - The consumer has been started for the first time or reset to initial state without assigned partitions
            // the seek offset map can contain offsets from a checkpoint or those from the CVs when reset to initial with assigned partitions
            // the seek offset map can contain offsets from a checkpoint, and a CV can exist for a partition that is not in the checkpoint. A partition may have added and assigned.
            // Add all offsets from the 'initialOffsets' to the seekOffset if not yet present for the topic partition
            try {
                final Map<TopicPartition, Long> initialOffsetsMap = initialOffsets.createOffsetMap (newAssignedPartitions);
                trace.info (MsgFormatter.format ("seekOffsetMap created from initial offsets: {0}", initialOffsetsMap));
                initialOffsetsMap.forEach ((tp, offs) -> {
                    seekOffsetMap.putIfAbsent (tp, offs);
                });
            } catch (InterruptedException e) {
                trace.log (DEBUG_LEVEL, "interrupted creating a seekOffsetMap from JCP control variables");
            } catch (IOException e) {
                throw new KafkaOperatorRuntimeException (e.getMessage());
            }
            seekPartitions (newAssignedPartitions, seekOffsetMap);
            // update the fetch positions in the offset manager for all assigned partitions - 
            assignedPartitionsOffsetManager.savePositionFromCluster();
            break;

        case CR_RESET_PENDING:
            // silently ignore; we have updated assigned partitions and assignedPartitionsOffsetManager before
            break;
        default:
            // ... not observed during tests
            trace.warn (MsgFormatter.format ("onPartitionsAssigned() [{0}]: unexpected state for onPartitionsAssigned()", state));
        }
        try {
            checkSpaceInMessageQueueAndPauseFetching (true);
        } catch (IllegalStateException | InterruptedException e) {
            ;
            // IllegalStateException cannot happen
            // On Interruption, do nothing
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
        // collection and map for seeking to inital startposition
        Collection <TopicPartition> tp1 = new ArrayList<>(1);
        Map <TopicPartition, Long> tpTimestampMap1 = new HashMap<>();
        Set<TopicPartition> seekFailedPartitions = new HashSet<> (partitions);
        List<TopicPartition> sortedPartitions = new LinkedList<> (partitions);
        Collections.sort (sortedPartitions, new Comparator<TopicPartition>() {
            @Override
            public int compare (TopicPartition o1, TopicPartition o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });

        for (TopicPartition tp: sortedPartitions) {
            try {
                if (offsetMap.containsKey (tp)) {
                    final long seekToOffset = offsetMap.get (tp);
                    trace.info (MsgFormatter.format ("seekPartitions() seeking {0} to offset {1}", tp, seekToOffset));
                    consumer.seek (tp, seekToOffset);
                }
                else {
                    // We have never seen the partition. Seek to startPosition given as operator parameter(s)
                    switch (this.initialStartPosition) {
                    case Default:
                        trace.info (MsgFormatter.format ("seekPartitions() new topic partition {0}; no need to seek to {1}", tp, this.initialStartPosition));
                        // do not seek
                        break;
                    case Beginning:
                    case End:
                        tp1.clear();
                        tp1.add (tp);
                        trace.info (MsgFormatter.format ("seekPartitions() seeking new topic partition {0} to {1}", tp, this.initialStartPosition));
                        seekToPosition (tp1, this.initialStartPosition);
                        break;
                    case Time:
                        tpTimestampMap1.clear();
                        tpTimestampMap1.put (tp, this.initialStartTimestamp);
                        trace.info (MsgFormatter.format ("seekPartitions() seeking new topic partition {0} to timestamp {1,number,#}", tp, this.initialStartTimestamp));
                        seekToTimestamp (tpTimestampMap1);
                        break;
                    default:
                        // unsupported start position, like 'Offset',  is already treated by initialization checks
                        final String msg = MsgFormatter.format ("seekPartitions(): {0} does not support startPosition {1}.", getThisClassName(), this.initialStartPosition);
                        trace.error (msg);
                        throw new KafkaOperatorRuntimeException (msg);
                    }
                    long initialFetchOffset = getConsumer().position (tp);
                    initialOffsets.saveOffset (tp, initialFetchOffset, false);
                }
                seekFailedPartitions.remove (tp);
            }
            catch (IllegalArgumentException topicPartitionNotAssigned) {
                // when this happens the ConsumerRebalanceListener will be called later
                trace.warn (MsgFormatter.format ("seekPartitions(): seek failed for partition {0}: {1}", tp, topicPartitionNotAssigned.getLocalizedMessage()));
            } catch (InterruptedException e) {
                trace.log (DEBUG_LEVEL, "interrupted creating or saving offset to JCP control variable");
                // leave for-loop
                break;
            } catch (IOException e) {
                throw new KafkaOperatorRuntimeException (e.getMessage());
            }
        }   // for
        trace.log (DEBUG_LEVEL, "partitions failed to seek: " + seekFailedPartitions);
        return seekFailedPartitions;
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
        trace.log (DEBUG_LEVEL, "CR index from MBean = " + groupCrIndex + "; this opertor's CR index = " + myCrIndex);
        if (groupCrIndex != myCrIndex) {
            final String msg = Messages.getString("CONSUMER_GROUP_IN_MULTIPLE_CONSISTENT_REGIONS", getOperatorContext().getKind(), getGroupId());
            trace.error (msg);
            throw new KafkaConfigurationException (msg);
        }
        // test that group-ID is not the generated (random) value
        if (isGroupIdGenerated()) {
            throw new KafkaConfigurationException (getThisClassName() + " cannot be used without specifying the groupId parameter or a group.id consumer property");
        }
        if (initialStartPosition == StartPosition.Offset) {
            throw new KafkaConfigurationException (getThisClassName() + " does not support startPosition = " + initialStartPosition);
        }
        trace.info ("initial start position for first partition assignment = " + initialStartPosition);
        if (initialStartPosition == StartPosition.Time) {
            trace.info ("start timestamp for first partition assignment = " + initialStartTimestamp);
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
        trace.info (MsgFormatter.format ("processResetToInitEvent() [{0}] - entering", state));
        try {
            clearDrainBuffer();
            getMessageQueue().clear();
            // When no one of the KafkaConsumer in this group has been restarted before the region reset,
            // partition assignment will most likely not change and no onPartitionsRevoked()/onPartitionsAssigned will be fired on our
            // ConsumerRebalanceListener. That's why we must seek here to the partitions we think we are assigned to (can also be no partition).
            // If a consumer within our group - but not this operator - restarted, partition assignment may have changed, but we
            // are not yet notified about it. That's why we must handle the failed seeks.
            // When this operator is restarted and reset, getAssignedPartitions() will return an empty Set.
            this.seekOffsetMap = initialOffsets.createOffsetMap (getAssignedPartitions());
            trace.info (MsgFormatter.format ("initial Offsets for assignment {0}: {1}", getAssignedPartitions(), this.seekOffsetMap));

            // Reset also the assignedPartitionsOffsetManager to the initial offsets of the assigned partitions. The assignedPartitionsOffsetManager goes into the checkpoint,
            // and its offsets are used as the seek position when it comes to reset from a checkpoint. There must be a seek position also in
            // the case that no tuple has been submitted for a partition, which would update the assignedPartitionsOffsetManager.
            assignedPartitionsOffsetManager.clear();
            assignedPartitionsOffsetManager.addTopics (getAssignedPartitions());
            Collection<TopicPartition> failedSeeks = seekPartitions (getAssignedPartitions(), this.seekOffsetMap);
            failedSeeks.forEach (tp -> assignedPartitionsOffsetManager.remove (tp.topic(), tp.partition()));
            assignedPartitionsOffsetManager.savePositionFromCluster();
            // reset tuple counter for operator driven CR
            nSubmittedRecords = 0l;
            ClientState newState = ClientState.RESET_COMPLETE;
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
            state = newState;
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("processResetToInitEvent() [{0}] - exiting", state));
        }
        catch (Exception e) {
            throw new KafkaOperatorRuntimeException (e.getMessage(), e);
        }
    }



    /**
     * creates the seek offset map.
     * This method is run within a runtime thread.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractCrKafkaConsumerClient#resetPrepareDataBeforeStopPolling(Checkpoint)
     */
    @Override
    public void resetPrepareDataBeforeStopPolling (Checkpoint checkpoint) throws InterruptedException {
        createSeekOffsetMap (checkpoint);
    }

    /**
     * clears the drain buffer and the message queue.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractCrKafkaConsumerClient#resetPrepareDataAfterStopPolling(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    protected void resetPrepareDataAfterStopPolling(Checkpoint checkpoint) throws InterruptedException {
        clearDrainBuffer();
        getMessageQueue().clear();
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

        trace.info (MsgFormatter.format ("processResetEvent() [{0}] - entering", state));

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
        // reset tuple counter for operator driven CR
        nSubmittedRecords = 0l;
        ClientState newState = ClientState.RESET_COMPLETE;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
        state = newState;
        trace.log (DEBUG_LEVEL, MsgFormatter.format ("processResetEvent() [{0}] - exiting", state));
    }

    /**
     * The seek offsets are created with following algorithm from the checkpoint:
     * <ul>
     * <li>read the contributing operator names from the checkpoint (operator names of the consumer group)
     * <li>read the seek offsets from the checkpoint.
     *     These are the offsets of only those partitions the consumer was assigned at checkpoint time.</li>
     * <li>send the offsets of the prior partitions together with the number of operators and the own operator name to the CrGroupCoordinator MXBean.
     *     The other consumer operators will also send their prior partition-to-offset mappings, and and their dsitinct operator name.</li>
     * <li>wait for the JMX notification that the partition-to-offset map has merged</li>
     * <li>fetch the merged map from the MX bean so that the operator has the seek offsets of all partitions of
     *     all topics (the group's view) and store this in the 'seekOffsetMap' member variable.</li>
     * </ul>
     * @param checkpoint
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    private void createSeekOffsetMap (Checkpoint checkpoint) throws InterruptedException {
        final String operatorName = getOperatorContext().getName();
        long chkptSeqId = checkpoint.getSequenceId();
        int resetAttempt = getCrContext().getResetAttempt();
        MergeKey key = new MergeKey (chkptSeqId, resetAttempt);
        trace.info (MsgFormatter.format ("createSeekOffsetMap() [{0}] - entering. chkptSeqId = {1,number,#}, resetAttempt = {2}", state, chkptSeqId, resetAttempt));
        try {
            final ObjectInputStream inputStream = checkpoint.getInputStream();
            final String myOperatorNameInCkpt = (String) inputStream.readObject();
            Set<String> contributingOperators = (Set<String>) inputStream.readObject();
            OffsetManager offsMgr = (OffsetManager) inputStream.readObject();
            trace.info (MsgFormatter.format ("createSeekOffsetMap() - merging {0} operator checkpoints into a single group checkpoint", contributingOperators.size()));

            if (trace.isEnabledFor (DEBUG_LEVEL)) {
                trace.log (DEBUG_LEVEL, MsgFormatter.format ("createSeekOffsetMap(): myOperatorName read from checkpoint: {0}", myOperatorNameInCkpt));
                trace.log (DEBUG_LEVEL, MsgFormatter.format ("createSeekOffsetMap(): contributingOperators read from checkpoint: {0}", contributingOperators));
                trace.log (DEBUG_LEVEL, MsgFormatter.format ("createSeekOffsetMap(): offset manager read from checkpoint: {0}", offsMgr));
            }
            if (!operatorName.equals (myOperatorNameInCkpt)) {
                trace.warn (MsgFormatter.format ("Operator name in checkpoint ({0}) differs from current operator name: {1}", myOperatorNameInCkpt, operatorName));
            }
            if (!contributingOperators.contains (operatorName)) {
                trace.error (MsgFormatter.format ("This operator''s name ({0}) not found in contributing operator names: {1}",
                        operatorName, contributingOperators));
            }
            trace.info (MsgFormatter.format ("contributing {0} partition => offset mappings to the group''s checkpoint.", offsMgr.size()));
            // send checkpoint data to CrGroupCoordinator MXBean and wait for the notification
            // to fetch the group's complete checkpoint. Then, process the group's checkpoint.
            Map<CrConsumerGroupCoordinator.TP, Long> partialOffsetMap = new HashMap<>();
            for (TopicPartition tp: offsMgr.getMappedTopicPartitions()) {
                final String topic = tp.topic();
                final int partition = tp.partition();
                final Long offset = offsMgr.getOffset (topic, partition);
                partialOffsetMap.put (new TP (topic, partition), offset);
            }

            trace.info (MsgFormatter.format ("Merging my group''s checkpoint contribution: partialOffsetMap = {0}, myOperatorName = {1}",
                    partialOffsetMap, operatorName));
            this.crGroupCoordinatorMxBean.mergeConsumerCheckpoint (chkptSeqId, resetAttempt, contributingOperators.size(), partialOffsetMap, operatorName);

            // check JMX notification and wait for notification
            jmxNotificationConditionLock.lock();
            long waitStartTime= System.currentTimeMillis();
            // increase timeout exponentially with every reset attempt by 20%
            // long timeoutMillis = (long)(Math.pow (1.2, resetAttempt) * (double)timeouts.getJmxResetNotificationTimeout());
            long timeoutMillis = timeouts.getJmxResetNotificationTimeout();
            boolean waitTimeLeft = true;
            int nWaits = 0;
            long timeElapsed = 0;
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("checking receiption of JMX notification {0} for sequenceId {1}. timeout = {2,number,#} ms.",
                    CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, key, timeoutMillis));
            while (!jmxMergeCompletedNotifMap.containsKey (key) && waitTimeLeft) {
                long remainingTime = timeoutMillis - timeElapsed;
                waitTimeLeft = remainingTime > 0;
                if (waitTimeLeft) {
                    if (nWaits++ %50 == 0) trace.log (DEBUG_LEVEL, MsgFormatter.format ("waiting for JMX notification {0} for sequenceId {1}. Remaining time = {2,number,#} of {3,number,#} ms",
                            CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, key, remainingTime, timeoutMillis));
                    jmxNotificationCondition.await (100, TimeUnit.MILLISECONDS);
                }
                timeElapsed = System.currentTimeMillis() - waitStartTime;
            }
            CrConsumerGroupCoordinator.CheckpointMerge merge = jmxMergeCompletedNotifMap.get (key);
            if (merge == null) {
                final String msg = MsgFormatter.format ("timeout receiving {0} JMX notification for {1} from MXBean {2} in JCP. Current timeout is {3,number,#} milliseconds.",
                        CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, key, crGroupCoordinatorMXBeanName, timeoutMillis);
                trace.error (msg);
                throw new KafkaOperatorResetFailedException (msg);
            }
            else {
                trace.info (MsgFormatter.format ("waiting for JMX notification for sequenceId {0} took {1} ms", key, timeElapsed));
            }

            Map <TP, Long> mergedOffsetMap = merge.getConsolidatedOffsetMap();
            trace.info ("reset offsets (group's checkpoint) received from MXBean: " + mergedOffsetMap);

            initSeekOffsetMap();
            mergedOffsetMap.forEach ((tp, offset) -> {
                this.seekOffsetMap.put (new TopicPartition (tp.getTopic(), tp.getPartition()), offset);
            });
        }
        catch (InterruptedException e) {
            trace.log (DEBUG_LEVEL, "createSeekOffsetMap(): interrupted waiting for the JMX notification");
            return;
        }
        catch (IOException | ClassNotFoundException e) {
            trace.error ("reset failed: " + e.getLocalizedMessage());
            throw new KafkaOperatorResetFailedException (MsgFormatter.format ("resetting operator {0} to checkpoint sequence ID {1} failed: {2}", getOperatorContext().getName(), chkptSeqId, e.getLocalizedMessage()), e);
        }
        finally {
            jmxNotificationConditionLock.unlock();
        }
        trace.log (DEBUG_LEVEL, "createSeekOffsetMap(): seekOffsetMap = " + this.seekOffsetMap);
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
        trace.info (MsgFormatter.format ("processCheckpointEvent() [{0}] sequenceId = {1}", state, checkpoint.getSequenceId()));
        try {
            Set<String> registeredConsumers = this.crGroupCoordinatorMxBean.getRegisteredConsumerOperators();
            final String myOperatorName = getOperatorContext().getName();
            if (ENABLE_CHECK_REGISTERED_ON_CHECKPOINT) {
                if (!registeredConsumers.contains (myOperatorName)) {
                    trace.error (MsgFormatter.format ("My operator name not registered in group MXBean: {0}", myOperatorName));
                }
            }
            ObjectOutputStream oStream = checkpoint.getOutputStream();
            oStream.writeObject (myOperatorName);
            oStream.writeObject (registeredConsumers);
            oStream.writeObject (this.assignedPartitionsOffsetManager);
            if (trace.isEnabledFor (DEBUG_LEVEL)) {
                trace.log (DEBUG_LEVEL, "data written to checkpoint: myOperatorName = " + myOperatorName);
                trace.log (DEBUG_LEVEL, "data written to checkpoint: contributingOperators = " + registeredConsumers);
                trace.log (DEBUG_LEVEL, "data written to checkpoint: assignedPartitionsOffsetManager = " + this.assignedPartitionsOffsetManager);
            }
        } catch (Exception e) {
            throw new RuntimeException (e.getLocalizedMessage(), e);
        }
        trace.log (DEBUG_LEVEL, "processCheckpointEvent() - exiting.");
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
     * Polls for messages and enqueues them into the message queue.
     * In the context of this method call also {@link #onPartitionsRevoked(Collection)}
     * and {@link #onPartitionsAssigned(Collection)} can be called
     * @param pollTimeout the timeout in milliseconds to wait for availability of consumer records.
     * @param isThrottled true, when polling is throttled, false otherwise
     * @return the number of enqueued consumer records.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long, boolean)
     */
    @Override
    protected EnqueResult pollAndEnqueue (long pollTimeout, boolean isThrottled) throws InterruptedException, SerializationException {
        if (trace.isTraceEnabled()) trace.trace ("Polling for records..."); //$NON-NLS-1$
        // Note: within poll(...) the ConsumerRebalanceListener might be called, changing the state to CR_RESET_PENDING
        long before = 0;
        if (trace.isDebugEnabled()) before = System.currentTimeMillis();
        ConsumerRecords<?, ?> records = getConsumer().poll (Duration.ofMillis (pollTimeout));
        int numRecords = records == null? 0: records.count();
        EnqueResult r = new EnqueResult (0);
        if (trace.isDebugEnabled()) {
            trace.debug (MsgFormatter.format ("consumer.poll took {0,number,#} ms, numRecords = {1,number,#}", (System.currentTimeMillis() - before), numRecords));
        }
        if (state == ClientState.CR_RESET_PENDING) {
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("pollAndEnqueue() [{0}]: Stop enqueuing fetched records", state));
            return r;
        }
        // add records to message queue
        if (numRecords > 0) {
            // state transition
            if (state != ClientState.RECORDS_FETCHED) {
                ClientState newState = ClientState.RECORDS_FETCHED;
                trace.log (DEBUG_LEVEL, MsgFormatter.format ("client state transition: {0} -> {1}", state, newState));
                state = newState;
            }
            r.setNumRecords (numRecords);
            if (trace.isDebugEnabled()) trace.debug ("# polled records: " + numRecords);
            final BlockingQueue<ConsumerRecord<?, ?>> messageQueue = getMessageQueue();
            records.forEach(cr -> {
                if (trace.isTraceEnabled()) {
                    trace.trace (MsgFormatter.format ("consumed [{0}]: tp={1}, pt={2}, of={3,number,#}, ts={4,number,#}, ky={5}",
                            state,  cr.topic(), cr.partition(), cr.offset(), cr.timestamp(), cr.key()));
                }
                final int vsz = cr.serializedValueSize();
                final int ksz = cr.serializedKeySize();
                if (vsz > 0) r.incrementSumValueSize (vsz);
                if (ksz > 0) r.incrementSumKeySize (ksz);
                messageQueue.add(cr);
            });
        }
        return r;
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
        if (startPollingRequired.getAndSet (false)) {
            trace.log (DEBUG_LEVEL, MsgFormatter.format ("getNextRecord() [{0}] - Acquired permit - initiating polling for Kafka messages", state));
            sendStartPollingEvent();
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
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#onShutdown(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public void onShutdown (long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (this.crGroupCoordinatorMxBean != null) {
            try {
                crGroupCoordinatorMxBean.deregisterConsumerOperator (getOperatorContext().getName());
            } catch (Exception e) {
                trace.warn (MsgFormatter.format ("deregister {0} operator from MXBean {1} failed: {2}",
                        getOperatorContext().getName(), this.crGroupCoordinatorMXBeanName, e.toString()));
            }
        }
        super.onShutdown (timeout, timeUnit);
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
        private boolean singleTopic = false;   // safest default

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

        public final Builder setSingleTopic (boolean s) {
            this.singleTopic = s;
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
            CrKafkaConsumerGroupClient client = new CrKafkaConsumerGroupClient (operatorContext, keyClass, valueClass, kafkaProperties, singleTopic);
            client.setPollTimeout (this.pollTimeout);
            client.setTriggerCount (this.triggerCount);
            client.setInitialStartPosition (this.initialStartPosition);
            client.setInitialStartTimestamp (this.initialStartTimestamp);
            return client;
        }
    }
}
