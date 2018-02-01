package com.ibm.streamsx.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.clients.consumer.Event.EventType;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public class KafkaConsumerClient extends AbstractKafkaClient implements ConsumerRebalanceListener {

    private static final Logger logger = Logger.getLogger(KafkaConsumerClient.class);
    private static final long EVENT_LOOP_PAUSE_TIME = 100;
    private static final long CONSUMER_TIMEOUT_MS = 2000;
    private static final int MESSAGE_QUEUE_SIZE_MULTIPLIER = 100;
    private static final int DEFAULT_MAX_POLL_RECORDS_CONFIG = 500;
    private static final long DEFAULT_MAX_POLL_INTERVAL_MS_CONFIG = 300000;
    private static final String GENERATED_GROUPID_PREFIX = "group-"; //$NON-NLS-1$
    private static final String GENERATED_CLIENTID_PREFIX = "client-"; //$NON-NLS-1$

    private KafkaConsumer<?, ?> consumer;
    private OffsetManager offsetManager;

    private KafkaOperatorProperties kafkaProperties;
    private ControlVariableAccessor<String> offsetManagerCV;

    private BlockingQueue<ConsumerRecord<?, ?>> messageQueue;
    private BlockingQueue<Event> eventQueue;
    private AtomicBoolean processing;

    private CountDownLatch consumerInitLatch;
    private CountDownLatch checkpointingLatch;
    private CountDownLatch resettingLatch;
    private CountDownLatch shutdownLatch;
    private CountDownLatch pollingStoppedLatch;
    private CountDownLatch updateAssignmentLatch;
    private OperatorContext operatorContext;
    private ConsistentRegionContext crContext;
    private Collection<Integer> partitions;
    private boolean isAssignedToTopics;
    private int maxPollRecords;
    private Exception initializationException;
    private long lastPollTimestamp = 0;
    private long maxPollIntervalMs;
    private boolean autoCommitEnabled = false;
    private Thread eventThread;

    
    /**
     * Callback to notify that topic partitions have been assigned by the group coordinator to the consumer.
     * Note that this callback is only used when the client subscribes to topics, but not, when the client assigns itself to topic partitions.
     * This method is invoked with the thread that calls `consumer.poll (long timeout)`. The group coordinator assures that
     * {@link #onPartitionsRevoked(Collection)} is always called before this method.
     * 
     * @param partitions The assigned topic partitions 
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
     */
    @Override
    public void onPartitionsAssigned (Collection<TopicPartition> partitions) {
        logger.info("onPartitionsAssigned: " + partitions);
    }

    /**
     * Callback to notify that topic partitions have been revoked by the group coordinator from the consumer.
     * Note that this callback is only used when the client subscribes to topics, but not, when the client assigns itself to topic partitions.
     * This method is invoked with the thread that calls `consumer.poll (long timeout)`. The group coordinator assures that
     * this method is always called before {@link #onPartitionsAssigned(Collection)}.
     * 
     * @param partitions The revoked topic partitions
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsRevoked: " + partitions);
    }
    

    private <K, V> KafkaConsumerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties)
                    throws Exception {
        this.kafkaProperties = kafkaProperties;
        if (!this.kafkaProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getDeserializer(keyClass));
        }

        if (!kafkaProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getDeserializer(valueClass));
        }

        // create a random group ID for the consumer if one is not specified
        if (!kafkaProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, getRandomId(GENERATED_GROUPID_PREFIX));
        }

        // Create a random client ID for the consumer if one is not specified.
        // This is important, otherwise running multiple consumers from the same
        // application will result in a KafkaException when registering the
        // client
        if (!kafkaProperties.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, getRandomId(GENERATED_CLIENTID_PREFIX));
        }

        // if not explicitly configured, disable auto commit
        if (!kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        autoCommitEnabled = this.kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true");

        maxPollRecords = getMaxPollRecords();
        maxPollIntervalMs = getMaxPollIntervalMs();
        messageQueue = new LinkedBlockingQueue<ConsumerRecord<?, ?>>(getMessageQueueSize());
        eventQueue = new LinkedBlockingQueue<Event>();
        processing = new AtomicBoolean(false);
        this.operatorContext = operatorContext;
        crContext = operatorContext.getOptionalContext(ConsistentRegionContext.class);
        this.partitions = partitions == null ? Collections.emptyList() : partitions;
        
        consumerInitLatch = new CountDownLatch(1);
        eventThread = operatorContext.getThreadFactory().newThread(new Runnable() {

            @Override
            public void run() {
                try {
                    consumer = new KafkaConsumer<>(kafkaProperties);
                    offsetManager = new OffsetManager(consumer);
                    
                    consumerInitLatch.countDown(); // consumer is ready
                    startEventLoop();
                } catch (Exception e) {
                	// store the exception...will be thrown from the calling operator
                	initializationException = e;
                	consumerInitLatch.countDown(); // remove lock
                }
            }
        });
        eventThread.setDaemon(false);
        
        eventThread.start();
        consumerInitLatch.await(); // wait for consumer to be created before returning
    }

    public Exception getInitializationException() {
		return initializationException;
	}
    
    private int getMaxPollRecords() {
    	return this.kafkaProperties.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)
				? Integer.valueOf(kafkaProperties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) : DEFAULT_MAX_POLL_RECORDS_CONFIG;
    }
    
    private long getMaxPollIntervalMs() {
        return this.kafkaProperties.containsKey(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)?
                Long.valueOf(kafkaProperties.getProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)):
                    DEFAULT_MAX_POLL_INTERVAL_MS_CONFIG;
    }
    
    private int getMessageQueueSize() {
		return MESSAGE_QUEUE_SIZE_MULTIPLIER * getMaxPollRecords();
    }
    
    private boolean isConsistentRegionEnabled() {
    	return crContext != null;
    }
    
    private List<TopicPartition> getAllTopicPartitionsForTopic(Collection<String> topics) {
    	List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
		topics.forEach(topic -> {
			List<PartitionInfo> partitions = consumer.partitionsFor(topic);
			partitions.forEach(p -> topicPartitions.add(new TopicPartition(topic, p.partition())));
		});
		
		return topicPartitions;
    }
    
    public void subscribeToTopics(Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
    	logger.debug("subscribeToTopics: topics=" + topics + ", partitions=" + partitions + ", startPosition=" + startPosition);
    	assert startPosition != StartPosition.Time;
    	
    	if(topics != null && !topics.isEmpty()) {
    		if(partitions == null || partitions.isEmpty()) {
    			// no partition information provided
    			if(!isConsistentRegionEnabled() && startPosition == StartPosition.Default) {
    				subscribe(topics);	
    			} else {
        			List<TopicPartition> topicPartitions = getAllTopicPartitionsForTopic(topics);
        			assign(topicPartitions);
        			seekToPosition(topicPartitions, startPosition);    				
    			}    			
    		} else {
    			List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
    	    	topics.forEach(topic -> {
    	    		partitions.forEach(partition -> topicPartitions.add(new TopicPartition(topic, partition)));
    	    	});
    	    	
    	    	assign(topicPartitions);
    	    	
    	    	if(startPosition != StartPosition.Default) {
        	    	seekToPosition(topicPartitions, startPosition);    	    		
    	    	}
    		}
    		
          if (isConsistentRegionEnabled()) {
	          // save the consumer offset after moving it's position
	          offsetManager.savePositionFromCluster();
	          saveOffsetManagerToJCP();
          }
    	}
    }

    public void subscribeToTopicsWithTimestamp(Collection<String> topics, Collection<Integer> partitions, Long timestamp) throws Exception {
    	logger.debug("subscribeToTopicsWithTimestamp: topic=" + topics + ", partitions=" + partitions + ", timestamp=" + timestamp);
    	Map<TopicPartition, Long /* timestamp */> topicPartitionTimestampMap = new HashMap<TopicPartition, Long>();
    	if(partitions == null || partitions.isEmpty()) {
    		List<TopicPartition> topicPartitions = getAllTopicPartitionsForTopic(topics);
    		topicPartitions.forEach(tp -> topicPartitionTimestampMap.put(tp, timestamp));
    	} else {
    		topics.forEach(topic -> {
    			partitions.forEach(partition -> topicPartitionTimestampMap.put(new TopicPartition(topic, partition), timestamp));
    		});
    	}

    	assign(topicPartitionTimestampMap.keySet());
    	seekToTimestamp(topicPartitionTimestampMap);
    	
    	if (isConsistentRegionEnabled()) {
    		// save the consumer offset after moving it's position
    		offsetManager.savePositionFromCluster();
    		saveOffsetManagerToJCP();
    	}
    }
    
    public void subscribeToTopicsWithOffsets(Map<TopicPartition, Long> topicPartitionOffsetMap) throws Exception {
    	logger.debug("subscribeToTopicsWithOffsets: topicPartitionOffsetMap=" + topicPartitionOffsetMap);
    	if(topicPartitionOffsetMap != null && !topicPartitionOffsetMap.isEmpty()) {
    		assign(topicPartitionOffsetMap.keySet());
    	}
    	
    	// seek to position
    	seekToOffset(topicPartitionOffsetMap);
    	
    	if (isConsistentRegionEnabled()) {
    		// save the consumer offset after moving it's position
    		offsetManager.savePositionFromCluster();
    		saveOffsetManagerToJCP();
    	}
    }
    
	public void subscribeToTopicsWithOffsets(List<String> topics, List<Integer> partitions, List<Long> startOffsets) throws Exception {
		if(partitions.size() != startOffsets.size())
			throw new IllegalArgumentException("The number of partitions and the number of offsets must be equal");			
			
		Map<TopicPartition, Long> topicPartitionOffsetMap = new HashMap<TopicPartition, Long>();
		
    	topics.forEach(topic -> {
    		for(int i = 0; i < partitions.size(); i++) {
    			int partition = partitions.get(i);
    			long offset = (startOffsets.size() > i) ? startOffsets.get(i) : -1l;
    			
    			topicPartitionOffsetMap.put(new TopicPartition(topic, partition), offset);
    		}
    	});
    	
    	subscribeToTopicsWithOffsets(topicPartitionOffsetMap);
	}
    
    private void saveOffsetManagerToJCP() throws Exception {
        ControlPlaneContext controlPlaneContext = operatorContext
                .getOptionalContext(ControlPlaneContext.class);
        offsetManagerCV = controlPlaneContext.createStringControlVariable(OffsetManager.class.getName(),
                false, serializeObject(offsetManager));
        OffsetManager mgr = getDeserializedOffsetManagerCV();
        logger.debug("Retrieved value for offsetManagerCV=" + mgr);	
    }
    
    private void subscribe(Collection<String> topics) {
        logger.info("Subscribing: topics=" + topics); //$NON-NLS-1$
        consumer.subscribe(topics, this);
        isAssignedToTopics = true;
    }
    
    private void assign(Collection<TopicPartition> topicPartitions) {
    	logger.info("Assigning topic-partitions: " + topicPartitions);
    	Map<String /* topic */, List<TopicPartition>> topicPartitionMap = new HashMap<>();

    	// update the offsetmanager
    	topicPartitions.forEach(tp -> {
    		if(!topicPartitionMap.containsKey(tp.topic())) {
    			topicPartitionMap.put(tp.topic(), new ArrayList<>());
    		}
    		topicPartitionMap.get(tp.topic()).add(tp);
    	});
    	
    	if(isConsistentRegionEnabled())
    		topicPartitionMap.forEach((topic, tpList) -> offsetManager.addTopic(topic, tpList));
    	
    	consumer.assign(topicPartitions);
    	isAssignedToTopics = true;
    }
    
    private void seekToPosition(Collection<TopicPartition> topicPartitions, StartPosition startPosition) {
    	if(startPosition == StartPosition.Beginning) {
    		consumer.seekToBeginning(topicPartitions);
    	} else if(startPosition == StartPosition.End){
    		consumer.seekToEnd(topicPartitions);
    	}
    }
    
    /*
     * If offset equals -1, seek to the end of the topic
     * If offset equals -2, seek to the beginning of the topic
     * Otherwise, seek to the specified offset
     */
    private void seekToOffset(Map<TopicPartition, Long> topicPartitionOffsetMap) {
    	topicPartitionOffsetMap.forEach((tp, offset) -> {
    		if(offset == -1l) {
    			consumer.seekToEnd(Arrays.asList(tp));
    		} else if(offset == -2) {
    			consumer.seekToBeginning(Arrays.asList(tp));
    		} else {
    			consumer.seek(tp, offset);	
    		}
    	});
    }

    private void seekToTimestamp(Map<TopicPartition, Long> topicPartitionTimestampMap) {
    	Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(topicPartitionTimestampMap);
    	logger.debug("offsetsForTimes=" + offsetsForTimes);
    	
    	topicPartitionTimestampMap.forEach((tp, timestamp) -> {
    		OffsetAndTimestamp ot = offsetsForTimes.get(tp);
    		if(ot != null) {
        		logger.debug("Seeking consumer for tp=" + tp + " to offsetAndTimestamp=" + ot);
        		consumer.seek(tp, ot.offset());
    		} else {
    			// nothing...consumer will move to the offset as determined by the 'auto.offset.reset' config
    		}
    	});
    }

    private void poll(long timeout) throws Exception {
        logger.debug("Initiating polling..."); //$NON-NLS-1$
        // continue polling for messages until a new event
        // arrives in the event queue
        while (eventQueue.isEmpty()) {
            int remainingCapacityMsgQ = messageQueue.remainingCapacity();
            if (remainingCapacityMsgQ >= maxPollRecords) {
                try {
                    long now = System.currentTimeMillis();
                    long timeBetweenPolls = now -lastPollTimestamp;
                    if (lastPollTimestamp > 0) {
                        // this is not the first 'poll'
                        if (timeBetweenPolls >= maxPollIntervalMs) {
                            logger.warn("Kafka client did'nt poll often enaugh for messages. "  //$NON-NLS-1$
                                    + "Maximum time between two polls is currently " + maxPollIntervalMs //$NON-NLS-1$
                                    + " milliseconds. Consider to set consumer property '" //$NON-NLS-1$
                                    + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG + "' to a value higher than " + timeBetweenPolls); //$NON-NLS-1$
                        }
                    }
                    if (logger.isTraceEnabled()) logger.trace("Polling for records..."); //$NON-NLS-1$
                    ConsumerRecords<?, ?> records = consumer.poll(timeout);
                    if (logger.isDebugEnabled()) logger.debug("# polled records: " + (records == null? 0: records.count()));
                    lastPollTimestamp = System.currentTimeMillis();
                    if (records != null) {
                        records.forEach(cr -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug(cr.topic() + "-" + cr.partition() + " key=" + cr.key() + " - offset=" + cr.offset()); //$NON-NLS-1$
                            }
                        	messageQueue.add(cr);
                        });
                        if (autoCommitEnabled) consumer.commitSync();
                    }
                } catch (SerializationException e) {
                    // The default deserializers of the operator do not 
                    // throw SerializationException, but custom deserializers may throw...
                    // cannot do anything else at the moment
                    // (may be possible to handle this in future Kafka releases
                    // https://issues.apache.org/jira/browse/KAFKA-4740)
                    throw e;
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug ("remaining capacity in message queue (" + remainingCapacityMsgQ //$NON-NLS-1$
                            + ") < maxPollRecords (" + maxPollRecords + "). Skipping poll cycle."); //$NON-NLS-1$
                }
                // prevent busy-wait
                Thread.sleep(100);
            }
        }
        logger.debug("Stop polling, message in event queue: " + eventQueue.peek().getEventType()); //$NON-NLS-1$
    }

    public boolean isAssignedToTopics() {
    	return isAssignedToTopics;
    }
    
    public void startEventLoop() throws Exception {
        logger.debug("Event loop started!"); //$NON-NLS-1$
        processing.set(true);
        while (processing.get()) {
            logger.debug("Checking event queue for message..."); //$NON-NLS-1$
            Event event = eventQueue.poll(1, TimeUnit.SECONDS);

            if (event == null) {
                Thread.sleep(EVENT_LOOP_PAUSE_TIME);
                continue;
            }

            logger.debug("Received event: " + event.getEventType().name()); //$NON-NLS-1$
            switch (event.getEventType()) {
            case START_POLLING:
                poll((Long) event.getData());
                break;
            case STOP_POLLING:
                pollingStoppedLatch.countDown(); // indicates that polling has stopped
                break;
            case UPDATE_ASSIGNMENT:
            	updateAssignment(event.getData());
            	break;
            case CHECKPOINT:
                checkpoint((Checkpoint) event.getData());
                break;
            case RESET:
                reset((Checkpoint) event.getData());
                break;
            case RESET_TO_INIT:
                resetToInitialState();
                break;
            case SHUTDOWN:
                shutdown();
            default:
                Thread.sleep(EVENT_LOOP_PAUSE_TIME);
                break;
            }
        }
    }

    private void updateAssignment(Object data) throws Exception {
    	try {
    		TopicPartitionUpdate update = (TopicPartitionUpdate)data;
    		
    		// get a map of current topic partitions and their offsets
    		Map<TopicPartition, Long /* offset */> currentTopicPartitionOffsets = new HashMap<TopicPartition, Long>();
    		
    		Set<TopicPartition> topicPartitions = consumer.assignment();
    		topicPartitions.forEach(tp -> currentTopicPartitionOffsets.put(tp, consumer.position(tp)));
    		
    		if(update.getAction() == TopicPartitionUpdateAction.ADD) {
    			update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
    				currentTopicPartitionOffsets.put(tp, offset);
    			});
    		} else if(update.getAction() == TopicPartitionUpdateAction.REMOVE) {
    			update.getTopicPartitionOffsetMap().forEach((tp, offset) -> {
    				currentTopicPartitionOffsets.remove(tp);
    			});
    		}
    		
    		subscribeToTopicsWithOffsets(currentTopicPartitionOffsets);	
    	} finally {
        	updateAssignmentLatch.countDown();
    	}
	}

	public void sendStartPollingEvent(long timeout) {
        logger.debug("Sending " + EventType.START_POLLING.name() + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add(new Event(EventType.START_POLLING, Long.valueOf(timeout)));
    }

    public void sendStopPollingEvent() throws Exception {
        logger.debug("Sending " + EventType.STOP_POLLING + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        pollingStoppedLatch = new CountDownLatch(1);
        eventQueue.add(new Event(EventType.STOP_POLLING, null));
        pollingStoppedLatch.await();
    }

    public void sendUpdateTopicAssignmentEvent(TopicPartitionUpdate update) throws Exception {
    	logger.debug("Sending " + EventType.UPDATE_ASSIGNMENT + " event...");
    	updateAssignmentLatch = new CountDownLatch(1);
    	eventQueue.add(new Event(EventType.UPDATE_ASSIGNMENT, update));
    	updateAssignmentLatch.await();
    }
    
    public void sendCheckpointEvent(Checkpoint checkpoint) throws Exception {
        logger.debug("Sending " + EventType.CHECKPOINT + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        checkpointingLatch = new CountDownLatch(1);
        eventQueue.add(new Event(EventType.CHECKPOINT, checkpoint));
        checkpointingLatch.await();
    }

    public void sendResetEvent(Checkpoint checkpoint) throws Exception {
        logger.debug("Sending " + EventType.RESET + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        resettingLatch = new CountDownLatch(1);
        eventQueue.add(new Event(EventType.RESET, checkpoint));
        resettingLatch.await();
    }

    public void sendResetToInitEvent() throws Exception {
        logger.debug("Sending " + EventType.RESET_TO_INIT + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        resettingLatch = new CountDownLatch(1);
        eventQueue.add(new Event(EventType.RESET_TO_INIT, null));
        resettingLatch.await();
    }

    public void sendShutdownEvent(long timeout, TimeUnit timeUnit) throws Exception {
        logger.debug("Sending " + EventType.SHUTDOWN + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        shutdownLatch = new CountDownLatch(1);
        eventQueue.add(new Event(EventType.SHUTDOWN, null));
        shutdownLatch.await(timeout, timeUnit);
    }

    public ConsumerRecord<?, ?> getNextRecord() {
    	try {
            return messageQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Consumer interrupted while waiting for messages to arrive", e); //$NON-NLS-1$
        }
    }
    
//    public ConsumerRecords<?, ?> getRecords() {
//        try {
//            return messageQueue.poll(1, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new RuntimeException("Consumer interrupted while waiting for messages to arrive", e); //$NON-NLS-1$
//        }
//    }

    private void refreshFromCluster() {
        logger.debug("Refreshing from cluster..."); //$NON-NLS-1$
        List<String> topics = offsetManager.getTopics();
        Map<TopicPartition, Long> startOffsetMap = new HashMap<TopicPartition, Long>();
        for (String topic : topics) {
            List<PartitionInfo> parts = consumer.partitionsFor(topic);
            parts.forEach(pi -> {
            	// if the 'partitions' list is empty, retrieve offsets for all topic partitions,
            	// otherwise only retrieve offsets for the user-specified partitions
            	if(partitions.isEmpty() || partitions.contains(pi.partition())) {
                    TopicPartition tp = new TopicPartition(pi.topic(), pi.partition());
                    long startOffset = offsetManager.getOffset(pi.topic(), pi.partition());
                    if(startOffset > -1l) {
                    	startOffsetMap.put(tp, startOffset);
                    }
            	}
            });
        }
        logger.debug("startOffsets=" + startOffsetMap); //$NON-NLS-1$

        // assign the consumer to the partitions and seek to the
        // last saved offset
        consumer.assign(startOffsetMap.keySet());
        for (Entry<TopicPartition, Long> entry : startOffsetMap.entrySet()) {
            logger.debug("Consumer seeking: TopicPartition=" + entry.getKey() + ", new_offset=" + entry.getValue()); //$NON-NLS-1$ //$NON-NLS-2$

            consumer.seek(entry.getKey(), entry.getValue());
        }
    }

    private void shutdown() {
        logger.debug("Shutdown sequence started..."); //$NON-NLS-1$
        try {
            consumer.close(CONSUMER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            processing.set(false);
        } finally {
            shutdownLatch.countDown();
        }

    }

    public void drain() throws Exception {
        // nothing to drain
    }

    private void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.debug("Checkpointing seq=" + checkpoint.getSequenceId()); //$NON-NLS-1$
        try {
            // offsetManager.savePositionFromCluster();
            checkpoint.getOutputStream().writeObject(offsetManager);
            if (logger.isDebugEnabled()) {
                logger.debug("offsetManager=" + offsetManager); //$NON-NLS-1$
            }
        } finally {
            checkpointingLatch.countDown();
        }

    }

    public OffsetManager getOffsetManager() {
		return offsetManager;
	}
    
    private void reset(Checkpoint checkpoint) throws Exception {
        logger.debug("Resetting to seq=" + checkpoint.getSequenceId()); //$NON-NLS-1$
        try {
            offsetManager = (OffsetManager) checkpoint.getInputStream().readObject();
            offsetManager.setOffsetConsumer(consumer);

            refreshFromCluster();
            
            // remove records from queue
            messageQueue.clear();
        } finally {
            resettingLatch.countDown();
        }
    }

    private void resetToInitialState() throws Exception {
        logger.debug("Resetting to initial state..."); //$NON-NLS-1$
        try {
            offsetManager = getDeserializedOffsetManagerCV();
            offsetManager.setOffsetConsumer(consumer);
            logger.debug("offsetManager=" + offsetManager); //$NON-NLS-1$

            // refresh from the cluster as we may
            // have written to the topics
            refreshFromCluster();
            
            // remove records from queue
            messageQueue.clear();
        } finally {
            resettingLatch.countDown();
        }
    }

    private OffsetManager getDeserializedOffsetManagerCV() throws Exception {
    	return SerializationUtils.deserialize(Base64.getDecoder().decode(offsetManagerCV.sync().getValue()));
    }
    
    public static class KafkaConsumerClientBuilder {
    	private OperatorContext operatorContext;
    	private Class<?> keyClass;
    	private Class<?> valueClass;
        private KafkaOperatorProperties kafkaProperties;
        
        public KafkaConsumerClientBuilder setKafkaProperties(KafkaOperatorProperties kafkaProperties) {
			this.kafkaProperties = kafkaProperties;
			return this;
		}
        
        public KafkaConsumerClientBuilder setKeyClass(Class<?> keyClass) {
			this.keyClass = keyClass;
			return this;
		}
        
        public KafkaConsumerClientBuilder setOperatorContext(OperatorContext operatorContext) {
			this.operatorContext = operatorContext;
			return this;
		}
        
        public KafkaConsumerClientBuilder setValueClass(Class<?> valueClass) {
			this.valueClass = valueClass;
			return this;
		}
        
        public KafkaConsumerClient build() throws Exception {
        	return new KafkaConsumerClient(operatorContext, keyClass, valueClass, kafkaProperties);
        }
    }
}
