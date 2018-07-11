/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.io.IOException;
import java.io.ObjectInputStream;
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

    private final static String MBEAN_DOMAIN_NAME = "com.ibm.streamsx.kafka";
    private final ConsistentRegionAssignmentMode assignmentMode = ConsistentRegionAssignmentMode.GroupCoordinated;
    private ConsistentRegionContext crContext = null;
    private ControlPlaneContext jcpContext = null;
    private String groupId = null;
    private Collection <String> subscribedTopics;
    private Set<CrConsumerGroupCoordinator.TP> assignablePartitions = new HashSet<>();
    private long triggerCount = 5000l; 
    private long nSubmittedRecords = 0l;
    private StartPosition initialStartPosition = StartPosition.Default;
    private long initialStartTimestamp = -1l;
    private CountDownLatch jmxSetupLatch;
    private CrConsumerGroupCoordinatorMXBean crGroupCoordinator = null;
    private OffsetManager initialOffsets;
    private ControlVariableAccessor<String> initialOffsetsCV;
    // stores the offsets of the submitted tuples
    private OffsetManager offsetManager; // TODO: find a better name for this variable
    // the map that is used to seek the consumer after reset or resetToInitialState. Can be nullified after seeking.
    // The map must contain mappings for all partitions of all topics.
    private Map<TP, Long> resetOffsetMap;

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
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.groupId = kafkaProperties.getProperty (ConsumerConfig.GROUP_ID_CONFIG);
        this.initialOffsets = new OffsetManager();
        this.offsetManager = new OffsetManager();
    }


    /**
     * Processes JMX connection related events.
     * @see com.ibm.streams.operator.control.Controllable#event(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext, com.ibm.streams.operator.control.Controllable.EventType)
     */
    @Override
    public void event (MBeanServerConnection jcp, OperatorContext context, EventType eventType) {
        logger.info ("JMX connection related event received: " + eventType);
        // TODO Auto-generated method stub
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
     * @throws MalformedObjectNameException 
     * @see com.ibm.streams.operator.control.Controllable#setup(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext)
     */
    @Override
    public void setup (MBeanServerConnection jcp, OperatorContext context) throws InstanceNotFoundException, MBeanRegistrationException, NotCompliantMBeanException, ReflectionException, MBeanException, IOException, MalformedObjectNameException {
        try {
            ObjectName groupMbeanName = createMBeanObjectName();
            // Try to register the MBean for checkpoint coordination in JCP. One of the operators in the consumer group wins.
            logger.info ("Trying to register MBean in JCP: " + groupMbeanName);
            if (jcp.isRegistered (groupMbeanName)) {
                logger.info (groupMbeanName + " already registered");
            }
            else {
                try {
                    // Constructor signature: (String groupId, Integer consistentRegionIndex)
                    jcp.createMBean (CrConsumerGroupCoordinator.class.getName(), groupMbeanName, 
                            new Object[]{this.groupId, new Integer(crContext.getIndex())},
                            new String[] {"java.lang.String", "java.lang.Integer"});
                    logger.info ("MBean registered: " + groupMbeanName);
                }
                catch (InstanceAlreadyExistsException e) {
                    // another operator managed to create it first. that is ok, just use that one.
                    logger.info ("another operator just created " + groupMbeanName + ": " + e.getMessage());
                }
            }
            logger.info ("creating Proxy ...");
            crGroupCoordinator = JMX.newMXBeanProxy (jcp, groupMbeanName, CrConsumerGroupCoordinatorMXBean.class, /*notificationEmitter=*/true);
            //TODO: register this as a notification listener with a filter that filters only from this group-ID
            // Do we need to ensure not registered twice, for example after region reset? or connection re-establishment?
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
     * @throws MalformedObjectNameException Not thrown. All parts of the object name are conform to the JMX specification.
     */
    private ObjectName createMBeanObjectName() throws MalformedObjectNameException {
        Hashtable <String, String> props = new Hashtable<>();
        props.put ("type", "consumergroup");
        props.put ("groupIdHash", "" + this.groupId.hashCode());
        return ObjectName.getInstance (MBEAN_DOMAIN_NAME, props);
    }


    /**
     * Processes JMX notifications from the CrGroupCoordinator MXBean.
     * @see javax.management.NotificationListener#handleNotification(javax.management.Notification, java.lang.Object)
     */
    @Override
    public void handleNotification (Notification notification, Object handback) {
        // TODO Auto-generated method stub
        
        // change the state of the consumer and count down the latch.
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        logger.info ("consumer start initiated");
        // Connect the PE for this operator to the Job Control Plane. 
        // A single connection is maintained to the Job Control Plane. The connection occurs asynchronously.
        // Does this mean that we might not yet be connected after connect()?
        jmxSetupLatch = new CountDownLatch (1);
        jcpContext.connect(this);
        // wait that MBeans are registered and notification listeners are set up, i.e. setup (...) of the Controllable interface is run:
        jmxSetupLatch.await();
        // calls our validate(), which uses the MBean ...
        super.startConsumer();
        // now we have a consumer object.
        offsetManager.setOffsetConsumer (getConsumer());
        initialOffsets.setOffsetConsumer (getConsumer());
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
        logger.info ("subscribeToTopics: topics=" + topics + ", partitions=" + partitions + ", startPosition=" + startPosition);
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
        // unassign all/unsubscribe:
        assign (null);
        subscribe (topics, this);
        setAssignablePartitions (topicPartitionCandidates);
        this.subscribedTopics = new ArrayList<String> (topics);
        // when later partitions are assigned dynamically, we seek using the initial offsets
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
        logger.info ("subscribeToTopicsWithTimestamp: topic = " + topics + ", partitions = " + partitions + ", timestamp = " + timestamp);
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
        Set<TopicPartition> topicPartitionCandidates = topicPartitionTimestampMap.keySet();
        assign (topicPartitionCandidates);
        seekToTimestamp (topicPartitionTimestampMap);

        // register the partitions with the initial offsets manager
        initialOffsets.addTopics (topicPartitionCandidates);
        // fetch the consumer offsets of initial positions
        initialOffsets.savePositionFromCluster();
        createJcpCvFromInitialOffsets();
        // unassign all/unsubscribe:
        assign (null);
        subscribe (topics, this);
        setAssignablePartitions (topicPartitionCandidates);
        this.subscribedTopics = new ArrayList<String> (topics);
        // when later partitions are assigned dynamically, we seek using the initial offsets
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



    /* (non-Javadoc)
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onDrain()
     */
    @Override
    public void onDrain () throws Exception {
        // TODO Auto-generated method stub
        // commit offsets

    }



    /* (non-Javadoc)
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#postSubmit(org.apache.kafka.clients.consumer.ConsumerRecord)
     */
    @Override
    public void postSubmit (ConsumerRecord<?, ?> submittedRecord) {
        // TODO Auto-generated method stub
    }



    /* (non-Javadoc)
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
     */
    @Override
    public void onPartitionsRevoked (Collection<TopicPartition> partitions) {
        logger.info("onPartitionsRevoked: old partition assignment = " + partitions);
    }



    /* (non-Javadoc)
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
     */
    @Override
    public void onPartitionsAssigned (Collection<TopicPartition> partitions) {
        logger.info("onPartitionsAssigned: new partition assignment = " + partitions);

        // get meta data of potentially assignable partitions for all subscribed topics
        setAssignablePartitionsFromMetadata();
        logger.info("onPartitionsAssigned(): assignable partitions for topics " + this.subscribedTopics + ": " + this.assignablePartitions);

        Set<TopicPartition> previousAssignment = new HashSet<>(getAssignedPartitions());
        getAssignedPartitions().clear();
        getAssignedPartitions().addAll (partitions);
        //        for (TopicPartition tp: partitions) {
        //            this.crGroupCoordinator.registerAssignedPartition (tp.topic(), tp.partition());
        //        }
        //        logger.info ("Partitions in MBean: " + this.crGroupCoordinator.getAssignedPartitions());
        Set<TopicPartition> gonePartitions = new HashSet<>(previousAssignment);
        gonePartitions.removeAll (partitions);
        logger.info("topic partitions that are not assigned anymore: " + gonePartitions);

        //TODO: onPartitionsAssigned handling not completed
        // When the assigned partitions change, 
        // we should remove the messages from the revoked partitions from the message queue.
        // The queue contains only uncommitted messages in this case.
        // With auto-commit enabled, the message queue can also contain committed messages - do not remove!
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
     * @param topicPartitions
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
            final String msg = Messages.getString("GROUP_ID_REQUIRED_FOR_PARAM_VAL", AbstractKafkaConsumerOperator.CR_ASSIGNMENT_MODE_PARAM, this.assignmentMode);
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



    /* (non-Javadoc)
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#resetToInitialState()
     */
    @Override
    protected void resetToInitialState() {
        // We must fetch the initial start offsets for all partitions of all topics from JCP.
        // On subscribe we have saved them into JCP. 
        // TODO Auto-generated method stub

    }



    /* (non-Javadoc)
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#reset(com.ibm.streams.operator.state.Checkpoint)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void reset (Checkpoint checkpoint) {
        // Note: this method is run by the event thread.
        long chkptSeqId = checkpoint.getSequenceId();
        logger.debug("reset() - entering. seq = " + chkptSeqId);
        getMessageQueue().clear();
        try {
            final ObjectInputStream inputStream = checkpoint.getInputStream();
            this.assignablePartitions = (Set<TP>) inputStream.readObject();
            OffsetManager offsMgr = (OffsetManager) inputStream.readObject();
            // send checkpoint data to crGroupCoordinator MBean and wait for 
            // to fetch the complete checkpoint. Then, process this data.
            Map<CrConsumerGroupCoordinator.TP, Long> partialOffsetMap = new HashMap<>();
            for (TopicPartition tp: offsMgr.getMappedTopicPartitions()) {
                final String topic = tp.topic();
                final int partition = tp.partition();
                final Long offset = offsMgr.getOffset(topic, partition);
                partialOffsetMap.put (new TP (topic, partition), offset);

            }
            // TODO: we need a CountDownLatch for waiting here
            this.crGroupCoordinator.mergePartialCheckpoint (chkptSeqId, this.assignablePartitions, partialOffsetMap);

            // TODO: Now wait. handleNotification() counts down.
        }
        catch (Exception e) {
            //TODO
            e.printStackTrace();
        }
        // TODO: create the merge complete notification latch
        // wait for the merge complete notification and continue.

        // TODO Auto-generated method stub
    }



    /* (non-Javadoc)
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#checkpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    protected void checkpoint (Checkpoint data) {
        // TODO Auto-generated method stub
        // following data must go into the checkpoint:
        // - this.assignablePartitions
        // - this.offsetManager


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
     * Polls for messages and enques them into the message queue.
     * In the context of this method call also {@link #onPartitionsRevoked(Collection)}
     * and {@link #onPartitionsAssigned(Collection)} can be called
     * @param pollTimeout the timeout in milliseconds to wait for availability of consumer records.
     * @return the number of enqueued consumer records.
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
        public final Builder  setInitialStartPosition(StartPosition initialStartPosition) {
            this.initialStartPosition = initialStartPosition;
            return this;
        }

        /**
         * @param initialStartTimestamp the initialStartTimestamp to set
         */
        public final Builder  setInitialStartTimestamp(long initialStartTimestamp) {
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
