package com.ibm.streamsx.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

public class KafkaConsumerClient extends AbstractKafkaClient {

	private static final Logger logger = Logger.getLogger(KafkaConsumerClient.class);
	private static final long EVENT_LOOP_PAUSE_TIME = 100; 
	private static final long CONSUMER_TIMEOUT_MS = 2000;
	private static final int MESSAGE_QUEUE_SIZE = 100;
	private static final String GENERATED_GROUPID_PREFIX = "group-" ;
	private static final String GENERATED_CLIENTID_PREFIX = "client-" ;
	
	
	private KafkaConsumer<?, ?> consumer;
	private OffsetManager offsetManager;

	private KafkaOperatorProperties kafkaProperties;
	private ControlVariableAccessor<String> offsetManagerCV;

	private BlockingQueue<ConsumerRecords<?, ?>> messageQueue;
	private BlockingQueue<Event> eventQueue;
	private AtomicBoolean processing;

	private CountDownLatch checkpointingLatch;
	private CountDownLatch resettingLatch;
	private CountDownLatch shutdownLatch;
	private CountDownLatch pollingStoppedLatch;
	private ConsistentRegionContext crContext;
	
	private Thread eventThread;
	
	public <K, V> KafkaConsumerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
			KafkaOperatorProperties kafkaProperties, Collection<String> topics, StartPosition startPosition) throws Exception {
		this.kafkaProperties = kafkaProperties;
		if (!this.kafkaProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
			this.kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getDeserializer(keyClass));
		}

		if (!kafkaProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
			this.kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getDeserializer(valueClass));
		}
		
		// create a random group ID for the consumer if one is not specified
		if(!kafkaProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
			this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, getRandomId(GENERATED_GROUPID_PREFIX));
		}
		
		// Create a random client ID for the consumer if one is not specified.
		// This is important, otherwise running multiple consumers from the same
		// application will result in a KafkaException when registering the client
		if(!kafkaProperties.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
			this.kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, getRandomId(GENERATED_CLIENTID_PREFIX));
		}
		
		messageQueue = new LinkedBlockingQueue<ConsumerRecords<?,?>>(MESSAGE_QUEUE_SIZE);
		eventQueue = new LinkedBlockingQueue<Event>();
		processing = new AtomicBoolean(false);
		crContext = operatorContext.getOptionalContext(ConsistentRegionContext.class);
		
		eventThread = operatorContext.getThreadFactory().newThread(
				new Runnable() {
					
					@Override
					public void run() {
						try {
							consumer = new KafkaConsumer<>(kafkaProperties);							
							
							offsetManager = new OffsetManager(consumer);
							subscribeToTopics(topics, startPosition);
							
							// create control variables
							ControlPlaneContext controlPlaneContext = operatorContext.getOptionalContext(ControlPlaneContext.class);
							offsetManagerCV = controlPlaneContext.createStringControlVariable(OffsetManager.class.getName(), false,
									serializeObject(offsetManager));
							
							startEventLoop();
						} catch (Exception e) {
							e.printStackTrace();
							logger.error(e.getLocalizedMessage(), e);
							throw new RuntimeException(e);
						}
					}
				});
		eventThread.setDaemon(false);
		eventThread.start();
	}
	
	private void subscribeToTopics(Collection<String> topics, StartPosition startPosition) {
		// Kafka's group management feature can only be used in the following scenarios:
		//  - startPosition = End
		//  - none of the topics have partitions assignments
		//  - operator is not in a consistent region
		
		if(startPosition == StartPosition.Beginning || crContext != null) {
			assign(topics, startPosition);
		} else {
			boolean doSubscribe = true;
			// check if any of the topics are being assigned to a specific set of partitions
			for(String topic : topics) {
				if(topic.split(":").length > 1) { //$NON-NLS-1$
					doSubscribe = false;
					break;
				}
			}
			
			if(doSubscribe) {
				subscribe(topics);
			} else {
				assign(topics, startPosition);	
			}
		}
	}
	
	private void subscribe(Collection<String> topics) {
		logger.debug("Subscribing: topics=" + topics); //$NON-NLS-1$
		consumer.subscribe(topics);
	}
	
	/*
	 * Assigns the consumer to all partitions for each of the topics. To assign
	 * the consumer to all partitions in a topic, simply pass in the topic name.
	 * To assign the consumer to a specific set of partitions, pass in a string
	 * in the form:
	 * 
	 * topic_name:part#,part#,etc.
	 * 
	 * For example, to assign the consumer to all partitions in "topicA", pass
	 * in the following string:
	 * 
	 * "topicA"
	 * 
	 * To assign the consumer to partitions 2, 4 & 5 in "topicB", pass in the
	 * following string:
	 * 
	 * "topicB:2,4,5"
	 * 
	 */
	private void assign(Collection<String> topics, StartPosition startPosition) {
		logger.debug("Assigning: topics=" + topics + ", startPosition=" + startPosition); //$NON-NLS-1$ //$NON-NLS-2$
		for (String topicStr : topics) {
			String[] tokens = topicStr.split(":"); //$NON-NLS-1$
						
			String topic = tokens[0];
			offsetManager.addTopic(topic, false);

			List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
			// assign consumer to all partitions
			if (tokens.length == 1) {
				topicPartitions.addAll(offsetManager.getTopicPartitions(topic));
			} else {
				String[] partitions = tokens[1].split(","); //$NON-NLS-1$
				for (String partStr : partitions) {
					topicPartitions.add(new TopicPartition(topic, Integer.valueOf(partStr)));
				}
			}
			consumer.assign(topicPartitions);
		}

		// passing in an empty list means seek to 
		// the beginning/end of ALL assigned partitions
		switch(startPosition) {
		case Beginning:
			consumer.seekToBeginning(Collections.emptyList());
			break;
		case End:
			consumer.seekToEnd(Collections.emptyList());
			break;
		}

		if(crContext != null) {
			// save the consumer offset after moving it's position
			//consumer.commitSync();
			offsetManager.savePositionFromCluster();	
		}
	}

	private void poll(long timeout) throws Exception {
		logger.debug("Initiating polling..."); //$NON-NLS-1$
		// continue polling for messages until a new event
		// arrives in the event queue
		while(eventQueue.isEmpty()) {
			if(messageQueue.remainingCapacity() > 0) {
				logger.trace("Polling for records..."); //$NON-NLS-1$
				try {
					ConsumerRecords<?, ?> records = consumer.poll(timeout);
					if(records != null) {
						records.forEach(cr -> logger.debug(cr.key() + " - offset=" + cr.offset())); //$NON-NLS-1$
						messageQueue.add(records);
					}					
				} catch(SerializationException e) {
					// cannot do anything else at the moment
					// (may be possible to handle this in future Kafka release (v0.11)
					// https://issues.apache.org/jira/browse/KAFKA-4740)
					throw e;
				}
			} else {
				// prevent busy-wait
				Thread.sleep(100);
			}
		}
		logger.debug("Stop polling, message in event queue: " + eventQueue.peek().getEventType()); //$NON-NLS-1$
	}
	
	public void startEventLoop() throws Exception {
		logger.debug("Event loop started!"); //$NON-NLS-1$
		processing.set(true);
		while(processing.get()) {
			logger.debug("Checking event queue for message..."); //$NON-NLS-1$
			Event event = eventQueue.poll(1, TimeUnit.SECONDS);
			
			if(event == null) {
				Thread.sleep(EVENT_LOOP_PAUSE_TIME);
				continue;
			}
			
			logger.debug("Received event: " + event.getEventType().name()); //$NON-NLS-1$
			switch(event.getEventType()) {
			case START_POLLING:
				poll((Long)event.getData());
				break;
			case STOP_POLLING:
				pollingStoppedLatch.countDown(); // indicates that polling has stopped
				break;
			case CHECKPOINT:
				checkpoint((Checkpoint)event.getData());
				break;
			case RESET:
				reset((Checkpoint)event.getData());
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
	
	public ConsumerRecords<?, ?> getRecords() {
		try {
			return messageQueue.poll(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Consumer interrupted while waiting for messages to arrive", e); //$NON-NLS-1$
		}
	}
	
	private void refreshFromCluster() {
		logger.debug("Refreshing from cluster..."); //$NON-NLS-1$
		List<String> topics = offsetManager.getTopics();
		Map<TopicPartition, Long> startOffsetMap = new HashMap<TopicPartition, Long>();
		for(String topic : topics) {
			List<PartitionInfo> parts = consumer.partitionsFor(topic);
			parts.forEach(pi -> {
				TopicPartition tp = new TopicPartition(pi.topic(), pi.partition());
				long startOffset = offsetManager.getOffset(pi.topic(), pi.partition());
				startOffsetMap.put(tp, startOffset);
			});			
		}
		logger.debug("startOffsets=" + startOffsetMap); //$NON-NLS-1$
		
		// assign the consumer to the partitions and seek to the 
		// last saved offset
		consumer.assign(startOffsetMap.keySet());
		for(Entry<TopicPartition, Long> entry : startOffsetMap.entrySet()) {
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
			offsetManager.savePositionFromCluster();
			checkpoint.getOutputStream().writeObject(offsetManager);
			logger.debug("offsetManager=" + offsetManager);	 //$NON-NLS-1$
		} finally {
			checkpointingLatch.countDown();			
		}

	}

	private void reset(Checkpoint checkpoint) throws Exception {
		logger.debug("Resetting to seq=" + checkpoint.getSequenceId()); //$NON-NLS-1$
		try {
			offsetManager = (OffsetManager)checkpoint.getInputStream().readObject();
			offsetManager.setOffsetConsumer(consumer);
			
			refreshFromCluster();			
		} finally {
			resettingLatch.countDown();			
		}
	}

	private void resetToInitialState() throws Exception {
		logger.debug("Resetting to initial state..."); //$NON-NLS-1$
		try {
			offsetManager = SerializationUtils.deserialize(Base64.getDecoder().decode(offsetManagerCV.sync().getValue()));
			offsetManager.setOffsetConsumer(consumer);
			logger.debug("offsetManager=" + offsetManager); //$NON-NLS-1$
					
			// refresh from the cluster as we may
			// have written to the topics
			refreshFromCluster();			
		} finally {
			resettingLatch.countDown();			
		}
	}
}
