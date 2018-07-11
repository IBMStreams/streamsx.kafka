/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.clients.consumer.Event.EventType;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Base class of all Kafka consumer client implementations.
 */
public abstract class AbstractKafkaConsumerClient extends AbstractKafkaClient implements ConsumerClient, OffsetCommitCallback {

    private static final Logger logger = Logger.getLogger(AbstractKafkaConsumerClient.class);

    private static final String GENERATED_GROUPID_PREFIX = "group-"; //$NON-NLS-1$
    private static final String GENERATED_CLIENTID_PREFIX = "client-"; //$NON-NLS-1$
    private static final long EVENT_LOOP_PAUSE_TIME_MS = 100;
    private static final long CONSUMER_CLOSE_TIMEOUT_MS = 2000;

    private static final int DEFAULT_MAX_POLL_RECORDS_CONFIG = 500;
    private static final long DEFAULT_MAX_POLL_INTERVAL_MS_CONFIG = 300000l;
    private static final long DEFAULT_CONSUMER_POLL_TIMEOUT_MS = 100l;
    private static final int MESSAGE_QUEUE_SIZE_MULTIPLIER = 100;

    private OffsetCommitCallback offsetCommitCallback = this;
    private KafkaConsumer<?, ?> consumer;
    private KafkaOperatorProperties kafkaProperties;
    private BlockingQueue<Event> eventQueue;
    private BlockingQueue<ConsumerRecord<?, ?>> messageQueue;
    private OperatorContext operatorContext;
    private boolean groupIdGenerated = false;
    private long pollTimeout = DEFAULT_CONSUMER_POLL_TIMEOUT_MS;
    private int maxPollRecords;
    private long maxPollIntervalMs;
    private long lastPollTimestamp = 0;

    private AtomicBoolean processing;
    private Set<TopicPartition> assignedPartitions = new HashSet<>();
    private SubscriptionMode subscriptionMode = SubscriptionMode.NONE;

    private Exception initializationException;
    private CountDownLatch consumerInitLatch;

    private final Metric nPendingMessages;
    private final Metric nLowMemoryPause;
    private final Metric nQueueFullPause;

    // Lock/condition for when we pause processing due to
    // no space on the queue or low memory.
    private final ReentrantLock pausedLock = new ReentrantLock();
    private final Condition paused = pausedLock.newCondition();


    /**
     * Creates a new AbstractKafkaConsumerclient.
     * After the constructor the method {@link #startConsumer()} must be invoked to create the consumer and the consumer thread.
     * Separation of constructor and thread creation allows changing kafka properties before the consumer is initialized and started.
     *  
     * @param operatorContext  the operator context
     * @param keyClass         the class of the message key
     * @param valueClass       the class of the message value
     * @param kafkaProperties  kafka consumer properties
     * @throws KafkaConfigurationException 
     */
    protected <K, V> AbstractKafkaConsumerClient (final OperatorContext operatorContext, final Class<K> keyClass, final Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {

        this.kafkaProperties = kafkaProperties;
        if (!kafkaProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getDeserializer(keyClass));
        }

        if (!kafkaProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getDeserializer(valueClass));
        }

        // create a random group ID for the consumer if one is not specified
        if (!kafkaProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, getRandomId(GENERATED_GROUPID_PREFIX));
            groupIdGenerated = true;
        }

        // Create a random client ID for the consumer if one is not specified.
        // This is important, otherwise running multiple consumers from the same
        // application will result in a KafkaException when registering the
        // client
        if (!kafkaProperties.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, getRandomId(GENERATED_CLIENTID_PREFIX));
        }

        maxPollRecords = getMaxPollRecordsFromProperties (this.kafkaProperties);
        maxPollIntervalMs = getMaxPollIntervalMsFromProperties (this.kafkaProperties);
        //        messageQueue = new LinkedBlockingQueue<ConsumerRecord<?, ?>>(getMessageQueueSize());
        eventQueue = new LinkedBlockingQueue<Event>();
        processing = new AtomicBoolean (false);
        messageQueue = new LinkedBlockingQueue<ConsumerRecord<?, ?>> (MESSAGE_QUEUE_SIZE_MULTIPLIER * getMaxPollRecords());
        this.nPendingMessages = operatorContext.getMetrics().getCustomMetric("nPendingMessages");
        this.nLowMemoryPause = operatorContext.getMetrics().getCustomMetric("nLowMemoryPause");
        this.nQueueFullPause = operatorContext.getMetrics().getCustomMetric("nQueueFullPause");
        this.operatorContext = operatorContext;
    }


    /**
     * @return the assignedPartitions
     */
    public final Set<TopicPartition> getAssignedPartitions() {
        return assignedPartitions;
    }


    /**
     * Gets the OffsetCommitCallback, which is used as callback for asynchronous offset commit. 
     * When no callback is set, this method returns `this`.
     * You can implement an own callback or overwrite {@link #onComplete(Map, Exception)}.
     * 
     * @return the offsetCommitCallback
     */
    public OffsetCommitCallback getOffsetCommitCallback() {
        return offsetCommitCallback;
    }

    /**
     * Sets a callback that is invoked by asynchronous offset commit.
     * If no callback is set `this` is used as the callback, which performs only logging.
     * You can implement an own callback or overwrite {@link #onComplete(Map, Exception)}.
     *
     * @param offsetCommitCallback the offsetCommitCallback to set
     */
    public void setOffsetCommitCallback(OffsetCommitCallback offsetCommitCallback) {
        this.offsetCommitCallback = offsetCommitCallback;
    }

    /**
     * Validate the consumer client for valid settings.
     * This method is called as the first method in {@link #startConsumer()}.
     * Implementations should throw an exception when invalid settings are detected.
     * @throws Exception The consumer client has no valid 
     */
    protected abstract void validate() throws Exception;

    /**
     * Validates the setup of the consumer client by calling the {@link #validate()} method, 
     * creates the Kafka consumer object and starts the consumer and event thread.
     * This method ensures that the event thread is running when it returns.
     * Methods that overwrite this method must call super.startConsumer().
     * @throws InterruptedException The thread has been interrupted.
     * @throws KafkaClientInitializationException The client could not be initialized
     */
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        try {
            validate();
        } catch (Exception e) {
            throw new KafkaClientInitializationException (e.getLocalizedMessage(), e);
        }
        consumerInitLatch = new CountDownLatch(1);
        Thread eventThread = operatorContext.getThreadFactory().newThread(new Runnable() {

            @Override
            public void run() {
                try {
                    maxPollRecords = getMaxPollRecordsFromProperties(kafkaProperties);
                    maxPollIntervalMs = getMaxPollIntervalMsFromProperties(kafkaProperties);
                    consumer = new KafkaConsumer<>(kafkaProperties);
                    processing.set (true);
                }
                catch (Exception e) {
                    initializationException = e;
                }
                finally {
                    consumerInitLatch.countDown();  // notify that consumer is ready
                }
                try {
                    runEventLoop();
                }
                catch (InterruptedException e) {
                    logger.debug ("Event thread interrupted. Terminating thread.");
                    return;
                }
            }
        });
        eventThread.setDaemon(false);
        eventThread.start();
        // wait for consumer thread to be running before returning
        consumerInitLatch.await();
        if (initializationException != null)
            throw new KafkaClientInitializationException (initializationException.getLocalizedMessage(), initializationException);
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#isAssignedToTopics()
     */
    @Override
    public boolean isSubscribedOrAssigned() {
        return this.subscriptionMode != SubscriptionMode.NONE;
    }


    /**
     * Runs a loop and consumes the event queue until the processing flag is set to false.
     * @throws InterruptedException the thread has been interrupted
     */
    private void runEventLoop() throws InterruptedException {
        logger.debug("Event loop started"); //$NON-NLS-1$
        while (processing.get()) {
            if (logger.isTraceEnabled()) {
                logger.trace ("Checking event queue for message ..."); //$NON-NLS-1$
            }
            Event event = eventQueue.poll (1, TimeUnit.SECONDS);

            if (event == null) {
                Thread.sleep (EVENT_LOOP_PAUSE_TIME_MS);
                continue;
            }

            logger.debug("Received event: " + event.getEventType().name()); //$NON-NLS-1$
            switch (event.getEventType()) {
            case START_POLLING:
                runPollLoop ((Long) event.getData());
                break;
            case STOP_POLLING:
                event.countDownLatch();  // indicates that polling has stopped
                break;
            case UPDATE_ASSIGNMENT:
                try {
                    updateAssignment ((TopicPartitionUpdate) event.getData());
                } catch (Exception e) {
                    logger.error("The assignment '" + (TopicPartitionUpdate) event.getData() + "' update failed: " + e.getLocalizedMessage());
                } finally {
                    event.countDownLatch();
                }
                break;
            case CHECKPOINT:
                try {
                    checkpoint ((Checkpoint) event.getData());
                } finally {
                    event.countDownLatch();
                }
                break;
            case RESET:
                try {
                    reset ((Checkpoint) event.getData());
                } finally {
                    event.countDownLatch();
                }
                break;
            case RESET_TO_INIT:
                try {
                    resetToInitialState();
                } finally {
                    event.countDownLatch();
                }
                break;
            case SHUTDOWN:
                try {
                    shutdown();
                } finally {
                    event.countDownLatch();
                }
                break;
            case COMMIT_OFFSETS:
                try {
                    commitOffsets ((CommitInfo) event.getData());
                } finally {
                    event.countDownLatch();
                }
                break;
            default:
                logger.error("runEventLoop(): Unexpected event received: " + event.getEventType());
                Thread.sleep(EVENT_LOOP_PAUSE_TIME_MS);
                break;
            }
        }
    }

    /**
     * Commits the offsets given in the map of the CommitInfo instance with the given controls set within the object.
     * @param offsets the offsets per topic partition and control information. The offsets must be the last processed offsets +1.
     * @throws InterruptedException The thread has been interrupted while committing synchronously
     * @throws RuntimeException  All other kinds of unrecoverable exceptions
     */
    private void commitOffsets (CommitInfo offsets) throws RuntimeException {
        final Map<TopicPartition, OffsetAndMetadata> offsetMap = offsets.getMap();
        if (logger.isDebugEnabled()) {
            logger.debug("Going to commit offsets: " + offsets.toString());
            if (offsetMap.isEmpty()) {
                logger.debug ("no offsets to commit ...");
            }
        }
        if (offsetMap.isEmpty()) {
            return;
        }
        if (offsets.isCommitPartitionWise()) {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>(1);
            for (TopicPartition tp: offsetMap.keySet()) {
                map.clear();
                map.put(tp, offsetMap.get(tp));
                if (offsets.isCommitSynchronous()) {
                    try {
                        consumer.commitSync(map);
                    }
                    catch (CommitFailedException e) {
                        // the commit failed and cannot be retried. This can only occur if you are using 
                        // automatic group management with subscribe(Collection), or if there is an active
                        // group with the same groupId which is using group management.
                        logger.warn (Messages.getString("OFFSET_COMMIT_FAILED_FOR_PARTITION", tp, e.getLocalizedMessage()));
                        // expose the exception to the runtime. When committing synchronous, 
                        // we usually want the offsets really have committed or restart operator, for example when in a CR
                        throw new RuntimeException (e.getMessage(), e);
                    }
                }
                else {
                    consumer.commitAsync (map, this);
                }
            }
        }
        else {
            if (offsets.isCommitSynchronous()) {
                try {
                    // can succeed partially
                    consumer.commitSync (offsetMap);
                }
                catch (CommitFailedException e) {
                    //if the commit failed and cannot be retried. This can only occur if you are using 
                    // automatic group management with subscribe(Collection), or if there is an active
                    // group with the same groupId which is using group management.
                    logger.warn (Messages.getString("OFFSET_COMMIT_FAILED", e.getLocalizedMessage()));
                    // expose the exception to the runtime. When committing synchronous, 
                    // we usually want the offsets really have committed or restart operator, for example when in a CR
                    throw new RuntimeException (e.getMessage(), e);
                }
            }
            else {
                consumer.commitAsync (offsetMap, this);
            }
        }
    }


    /**
     * Implements the shutdown sequence.
     * This sequence includes 
     * * closing the consumer
     * * terminating the event thread by setting the end condition
     * 
     * When you overwrite this method, you must call `super.shutdown()` in your implementation, preferably at the end.
     */
    protected void shutdown() {
        logger.debug("Shutdown sequence started..."); //$NON-NLS-1$
        consumer.close(CONSUMER_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        processing.set(false);
    }

    /**
     * Resets the client to the initial state when used in consistent region.
     * Derived classes must overwrite this method, but can provide an empty implementation if consistent region is not supported.
     */
    protected abstract void resetToInitialState();

    /**
     * Resets the client to a previous state when used in consistent region.
     * Derived classes must overwrite this method, but can provide an empty implementation if consistent region is not supported.
     * @param checkpoint the checkpoint that contains the previous state
     */
    protected abstract void reset(Checkpoint checkpoint);

    /**
     * Creates a checkpoint of the current state when used in consistent region.
     * Derived classes must overwrite this method, but can provide an empty implementation if consistent region is not supported.
     * @param checkpoint A reference of a checkpoint object where the user provides the state to be saved.
     */
    protected abstract void checkpoint(Checkpoint data);

    /**
     * Updates the assignment of the client to topic partitions.
     * Derived classes must overwrite this method, but can provide an empty implementation 
     * if assignments of topic partitions cannot be updated.
     * @param update the update increment/decrement
     * @throws Exception 
     */
    protected abstract void updateAssignment (TopicPartitionUpdate update);

    /**
     * This method must be overwritten by concrete classes. 
     * Here you implement polling for records and typically enqueue them into the message queue.
     * @param pollTimeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     * @return number of records enqueued into the message queue
     * @throws InterruptedException The thread has been interrupted 
     * @throws SerializationException The value or the key from the message could not be deserialized
     * @see AbstractKafkaConsumerClient#getMessageQueue()
     */
    protected abstract int pollAndEnqueue (long pollTimeout) throws InterruptedException, SerializationException;


    /**
     * Gets the next consumer record that has been received. If there are no records, the method waits the specified timeout.
     * @param timeout    the timeout to wait for records
     * @param timeUnit   the unit of time for the timeout
     * @return the next consumer record or `null` if there was no record within the timeout.
     * @throws InterruptedException The thread waiting for records has been interrupted.
     *
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#getNextRecord(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public ConsumerRecord<?, ?> getNextRecord (long timeout, TimeUnit timeUnit) throws InterruptedException {
        final ConsumerRecord<?,?> record = messageQueue.poll (timeout, TimeUnit.SECONDS);
        if (record == null) {
            // no messages - queue is empty
            nPendingMessages.setValue(messageQueue.size());
            try {
                pausedLock.lock();
                paused.signalAll();
            } finally {
                pausedLock.unlock();
            }
        }
        return record;
    }

    /**
     * Gets a reference to the message queue for received Kafka messages.
     * @return the messageQueue
     */
    public BlockingQueue<ConsumerRecord<?, ?>> getMessageQueue() {
        return messageQueue;
    }

    /**
     * Runs the loop polling for Kafka messages until an event is received in the event queue.
     * @param pollTimeout the timeout in milliseconds used to wait for new Kafka messages if there are less than the maximum batch size.
     * 
     * @throws InterruptedException 
     */
    protected void runPollLoop (Long pollTimeout) throws InterruptedException {
        logger.debug("Initiating polling ..."); //$NON-NLS-1$
        // continue polling for messages until a new event
        // arrives in the event queue
        while (eventQueue.isEmpty()) {
            // can wait for 100 ms; throws InterruptedException:
            if (isSpaceInMsgQueueWait()) {
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
                    lastPollTimestamp = System.currentTimeMillis();
                    /*int nRecordsEnqueued = */pollAndEnqueue (pollTimeout.longValue());
                    nPendingMessages.setValue(messageQueue.size());
                } catch (SerializationException e) {
                    // The default deserializers of the operator do not 
                    // throw SerializationException, but custom deserializers may throw...
                    // cannot do anything else at the moment
                    // (may be possible to handle this in future Kafka releases
                    // https://issues.apache.org/jira/browse/KAFKA-4740)
                    throw e;
                }
            }
        }
        logger.debug("Stop polling. Message in event queue: " + eventQueue.peek().getEventType()); //$NON-NLS-1$
    }

    /**
     * Checks for available space in the message queue including memory consumption
     * and waits 100 ms if the space is not sufficient.
     * Maintains the metrics `nPendingMessages`, `nLowMemoryPause`, and `nQueueFullPause`.
     * 
     * @throws InterruptedException Thread interrupted while waiting.
     */
    private boolean isSpaceInMsgQueueWait() throws InterruptedException {
        boolean space = messageQueue.isEmpty();
        boolean lowMemory = false;
        if (!space) {
            if (messageQueue.size() <= 4 * maxPollRecords)
                space = true;
            else {
                lowMemory = isLowMemory();
                space = !lowMemory &&
                        messageQueue.remainingCapacity() >= maxPollRecords;
            }
        }
        if (!space) {
            if (logger.isDebugEnabled()) {
                if (lowMemory)
                    logger.debug ("low memory detected: messages queued (" + messageQueue.size() //$NON-NLS-1$
                    + "). Skipping poll cycle."); //$NON-NLS-1$
                else
                    logger.debug ("remaining capacity in message queue (" + messageQueue.remainingCapacity() //$NON-NLS-1$
                    + ") < maxPollRecords (" + maxPollRecords + "). Skipping poll cycle."); //$NON-NLS-1$
            }
            nPendingMessages.setValue(messageQueue.size());
            if (lowMemory)
                nLowMemoryPause.increment();
            else
                nQueueFullPause.increment();

            try {
                pausedLock.lock();
                paused.await(100, TimeUnit.MILLISECONDS);
            } finally {
                pausedLock.unlock();
            }
            nPendingMessages.setValue(messageQueue.size());
        }
        return space;
    }

    /**
     * @return the timeout for polling for Kafka messages in milliseconds. The default value is {@value #DEFAULT_CONSUMER_POLL_TIMEOUT_MS}.
     */
    protected long getPollTimeout() {
        return pollTimeout;
    }

    /**
     * @param pollTimeout the poll timeout to set in milliseconds
     */
    public void setPollTimeout (long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    /**
     * @return the consumer object. This method must not be called before {@link #startConsumer()} succeeded.
     */
    public KafkaConsumer<?, ?> getConsumer() {
        return consumer;
    }

    /**
     * Tests whether the group ID of the consumer is a random generated group-ID.
     * @return `true` if the group ID has a random value generated by the client, `false` 
     *         if the group ID has been given from outside via kafka properties.
     */
    public boolean isGroupIdGenerated() {
        return groupIdGenerated;
    }

    /**
     * @return the kafkaProperties
     */
    public KafkaOperatorProperties getKafkaProperties() {
        return kafkaProperties;
    }

    /**
     * @return the operatorContext
     */
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    /**
     * Returns the maximum number of records to be returned from Kafka at each invocation of poll() at the consumer object
     * @return the maxPollRecords
     */
    protected int getMaxPollRecords() {
        return maxPollRecords;
    }

    /**
     * Returns the maximum amount of time in milliseconds that the broker allows between 
     * each poll before kicking a consumer out of the consumer group.
     * @return the maxPollIntervalMs
     */
    public long getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    /**
     * Initiates committing offsets.
     * If committing offsets is set to synchronous, the implementation ensures that the offsets are committed when the method returns.
     * @param offsets the offsets to commit and an control indication for asynchronous or synchronous commit.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     * @see CommitInfo#isCommitSynchronous()
     */
    public void sendCommitEvent (CommitInfo offsets) throws InterruptedException {
        Event event = new Event (EventType.COMMIT_OFFSETS, offsets, offsets.isCommitSynchronous());
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add (event);
        event.await();
    }

    /**
     * Initiates start of polling for KafKa messages.
     * Implementations should ignore this event if the consumer is not subscribed or assigned to partitions.
     */
    @Override
    public void sendStartPollingEvent() {
        Event event = new Event (EventType.START_POLLING, new Long (pollTimeout), false);
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add (event);
    }

    /**
     * Initiates stop polling for Kafka messages.
     * Implementations ensure that polling has stopped when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void sendStopPollingEvent() throws InterruptedException {
        Event event = new Event (EventType.STOP_POLLING, true);
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add(event);
        event.await();
    }

    /**
     * Assigns the consumer to the given set of topic partitions manually. No group management.
     * @param topicPartitions The topic partitions. null or an empty set is equivalent to
     *                        unsubscribe from everything previously subscribed or assigned.
     */
    protected void assign (Set<TopicPartition> topicPartitions) {
        logger.info("Assigning. topic-partitions = " + topicPartitions);
        if (topicPartitions == null) topicPartitions = Collections.emptySet();
        consumer.assign(topicPartitions);
        this.assignedPartitions = new HashSet<TopicPartition> (topicPartitions);
        this.subscriptionMode = topicPartitions.isEmpty()? SubscriptionMode.NONE: SubscriptionMode.ASSIGNED;
    }

    /**
     * Subscribes the consumer to the given topics. Subscription enables dynamic group assignment.
     * @param topics The topics to subscribe. An empty list or null is treated as unsubscribe from all.
     * @param rebalanceListener an optional ConsumerRebalanceListener
     */
    protected void subscribe(Collection<String> topics, ConsumerRebalanceListener rebalanceListener) {
        logger.info("Subscribing. topics = " + topics); //$NON-NLS-1$
        if (topics == null) topics = Collections.emptyList();
        consumer.subscribe (topics, rebalanceListener);
        this.subscriptionMode = topics.isEmpty()? SubscriptionMode.NONE: SubscriptionMode.SUBSCRIBED;
    }

    /**
     * Initiates topic partition assignment update. When this method is called, the consumer must be assigned to topic partitions.
     * If the consumer is subscribed to topics, the request is ignored.
     * Implementations ensure assignments have been updated when this method returns. 
     * @param update The the partition update.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void sendUpdateTopicAssignmentEvent (final TopicPartitionUpdate update) throws InterruptedException {
        Event event = new Event(EventType.UPDATE_ASSIGNMENT, update, true);
        logger.debug("Sending " + event + " event: " + update);
        eventQueue.add(event);
        event.await();
    }

    /**
     * Initiates checkpointing of the consumer client.
     * Implementations ensure that checkpointing the client has completed when this method returns. 
     * @param checkpoint the checkpoint
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void sendCheckpointEvent (Checkpoint checkpoint) throws InterruptedException {
        Event event = new Event(EventType.CHECKPOINT, checkpoint, true);
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add(event);
        event.await();
    }

    /**
     * Initiates resetting the client to a prior state. 
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void sendResetEvent (final Checkpoint checkpoint) throws InterruptedException {
        Event event = new Event(EventType.RESET, checkpoint, true);
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add(event);
        event.await();
    }

    /**
     * Initiates resetting the client to the initial state. 
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void sendResetToInitEvent() throws InterruptedException {
        Event event = new Event(EventType.RESET_TO_INIT, true);
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add(event);
        event.await();
    }

    /**
     * Initiates a shutdown of the consumer client.
     * Implementations ensure that shutting down the client has completed when this method returns. 
     * @param timeout    the timeout to wait for shutdown completion
     * @param timeUnit   the unit of time for the timeout
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void sendShutdownEvent (long timeout, TimeUnit timeUnit) throws InterruptedException {
        Event event = new Event(EventType.SHUTDOWN, true);
        logger.debug("Sending " + event + " event..."); //$NON-NLS-1$ //$NON-NLS-2$
        eventQueue.add(event);
        event.await (timeout, timeUnit);
    }



    /**
     * Try to determine if memory is getting low and thus
     * avoid continuing to add read messages to message queue.
     * See issue streamsx.kafka #91
     */
    private static boolean isLowMemory() {
        Runtime rt = Runtime.getRuntime();
        final double maxMemory = rt.maxMemory();
        final double totalMemory = rt.totalMemory();

        // Is there still room to grow?
        if (totalMemory < (maxMemory * 0.95))
            return false;

        final double freeMemory = rt.freeMemory();

        // Low memory if free memory at less than 5% of max.
        return freeMemory < (maxMemory * 0.05);
    }

    /**
     * Tests whether a consumer record originates from one of the given topic partitions
     * @param r    a consumer record
     * @param tps  a set of topic partitions
     * @return     true, if the record's topic and partition is contained in the set 'tps', false otherwise
     */
    protected static boolean belongsToPartition (ConsumerRecord<?, ?> r, Set<TopicPartition> tps) {
        TopicPartition tp = new TopicPartition (r.topic(), r.partition());
        return tps.contains(tp);
    }

    /**
     * Returns the value of the `max.poll.records` property from given properties.
     * @param kafkaProperties the properties
     * @return the property value or the default value {@value #DEFAULT_MAX_POLL_RECORDS_CONFIG} if the property is not set.
     */
    private static int getMaxPollRecordsFromProperties (KafkaOperatorProperties kafkaProperties) {
        return kafkaProperties.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)?
                Integer.valueOf(kafkaProperties.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)):
                    DEFAULT_MAX_POLL_RECORDS_CONFIG;
    }

    /**
     * Returns the value of the `max.poll.interval.ms` property from given properties.
     * @param kafkaProperties the properties
     * @return the property value or the default value {@value #DEFAULT_MAX_POLL_INTERVAL_MS_CONFIG} if the property is not set.
     */
    private static long getMaxPollIntervalMsFromProperties (KafkaOperatorProperties kafkaProperties) {
        return kafkaProperties.containsKey(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)?
                Long.valueOf(kafkaProperties.getProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)):
                    DEFAULT_MAX_POLL_INTERVAL_MS_CONFIG;
    }

    /**
     * Returns the partitions for the given topics from the metadata.
     * This method will issue a remote call to the server if it does not already have any metadata about the given topic.
     * 
     * @param topics A collection of topics
     * @return The topic partitions from the meta data of the topics
     */
    protected Set<TopicPartition> getAllTopicPartitionsForTopic (Collection<String> topics) {
        Set<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
        topics.forEach(topic -> {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            partitions.forEach(p -> topicPartitions.add(new TopicPartition(topic, p.partition())));
        });
        return topicPartitions;
    }

    /**
     * Seeks to the given position. This method evaluates lazily on next poll() or position() call.
     * @param topicPartitions The partitions to seek. If no partitions are given, all assigned partitions are seeked.
     * @param startPosition one of `StartPosition.End` or `StartPosition.Beginning`.
     */
    protected void seekToPosition(Collection<TopicPartition> topicPartitions, StartPosition startPosition) {
        switch (startPosition) {
        case Beginning:
            consumer.seekToBeginning(topicPartitions);
            break;
        case End:
            consumer.seekToEnd(topicPartitions);
            break;
        case Default:
            logger.warn("seekToPosition: ignoring position " + startPosition);
            break;
        default:
            throw new IllegalArgumentException("seekToPosition: illegal position: " + startPosition);
        }
    }

    /**
     * Seek the consumer for a Set of topic partitions to the nearest offset for a timestamp.
     * If there is no such offset, the consumer will move to the offset as determined by the 'auto.offset.reset' config
     * @param topicPartitionTimestampMap mapping from topic partition to timestamp in milliseconds since epoch
     */
    protected void seekToTimestamp (Map<TopicPartition, Long> topicPartitionTimestampMap) {
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(topicPartitionTimestampMap);
        logger.debug("offsetsForTimes=" + offsetsForTimes);
        for (TopicPartition tp: topicPartitionTimestampMap.keySet()) {
            OffsetAndTimestamp ot = offsetsForTimes.get(tp);
            if(ot != null) {
                logger.debug("Seeking consumer for tp = " + tp + " to offsetAndTimestamp=" + ot);
                consumer.seek(tp, ot.offset());
            } else {
                // nothing...consumer will move to the offset as determined by the 'auto.offset.reset' config
            }
        }
    }

    /**
     * Seeks to the given offsets for the given topic partitions. This offset is the offset that will is consume next.
     * If offset equals -1, seek to the end of the topic
     * If offset equals -2, seek to the beginning of the topic
     * Otherwise, seek to the specified offset
     */
    private void seekToOffset(Map<TopicPartition, Long> topicPartitionOffsetMap) {
        topicPartitionOffsetMap.forEach((tp, offset) -> {
            if(offset == -1l) {
                getConsumer().seekToEnd(Arrays.asList(tp));
            } else if(offset == -2) {
                getConsumer().seekToBeginning(Arrays.asList(tp));
            } else {
                getConsumer().seek(tp, offset);  
            }
        });
    }

    /**
     * Assigns the consumer to topic partitions and seeks to the given offsets for each topic partition.
     * @param topicPartitionOffsetMap Mapping from topic partition to the offset to seek to.
     *        An empty map or null is equivalent to unsubscribe from everything.
     * @throws Exception
     */
    protected void assignToPartitionsWithOffsets (Map<TopicPartition, Long> topicPartitionOffsetMap) throws Exception {
        logger.debug("assignToTopicsWithOffsets: topicPartitionOffsetMap=" + topicPartitionOffsetMap);
        if(topicPartitionOffsetMap != null) {
            assign (topicPartitionOffsetMap.keySet());
            if (!topicPartitionOffsetMap.isEmpty()) {
                // seek to position
                seekToOffset (topicPartitionOffsetMap);
            }
        }
        else {
            assign (Collections.emptySet());
        }
    }

    /**
     * Callback method for asynchronous offset commit.
     * @param offsets the committed offsets - not sure whether it contains data when an exception is set
     * @param exception an exception that may have occurred. null on success.
     * @see org.apache.kafka.clients.consumer.OffsetCommitCallback#onComplete(java.util.Map, java.lang.Exception)
     */
    @Override
    public void onComplete (Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception == null) {
            if (logger.isDebugEnabled()) {
                logger.debug ("Offsets successfully committed async: " + offsets);
            }
        }
        else {
            logger.warn(Messages.getString("OFFSET_COMMIT_FAILED", exception.getLocalizedMessage()));
        }
    }
}