/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.text.MessageFormat;
import java.util.ArrayList;
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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.KafkaMetricException;
import com.ibm.streamsx.kafka.UnknownTopicException;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.clients.consumer.Event.EventType;
import com.ibm.streamsx.kafka.clients.metrics.MetricsFetcher;
import com.ibm.streamsx.kafka.clients.metrics.MetricsProvider;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Base class of all Kafka consumer client implementations.
 */
public abstract class AbstractKafkaConsumerClient extends AbstractKafkaClient implements ConsumerClient, OffsetCommitCallback {

    protected static final String N_PARTITION_REBALANCES = "nPartitionRebalances";

    private static final Logger logger = Logger.getLogger(AbstractKafkaConsumerClient.class);

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
    private List <ConsumerRecord<?, ?>> drainBuffer;
    private final String groupId;
    private final boolean groupIdGenerated;
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
    protected final Metric nAssignedPartitions;

    // Lock/condition for when we pause processing due to
    // no space on the queue or low memory.
    private final ReentrantLock msgQueueLock = new ReentrantLock();
    private final Condition msgQueueEmptyCondition = msgQueueLock.newCondition();
    private AtomicBoolean msgQueueProcessed = new AtomicBoolean (true);
    private boolean fetchPaused = false;
    protected final ConsumerTimeouts timeouts;
    private MetricsFetcher metricsFetcher;


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

        super (operatorContext, kafkaProperties, true);
        this.kafkaProperties = kafkaProperties;
        if (!kafkaProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getDeserializer(keyClass));
        }

        if (!kafkaProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getDeserializer(valueClass));
        }

        // create a random group ID for the consumer if one is not specified
        if (kafkaProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            this.groupIdGenerated = false;
            this.groupId = kafkaProperties.getProperty (ConsumerConfig.GROUP_ID_CONFIG);
        }
        else {
            ProcessingElement pe = operatorContext.getPE();
            this.groupId = "D" + pe.getDomainId().hashCode() + pe.getInstanceId().hashCode()
                    + pe.getJobId() + operatorContext.getName().hashCode();
            this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            this.groupIdGenerated = true;
        }
        // always disable auto commit 
        if (kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            if (kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true")) {
                logger.warn("consumer config '" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "' has been turned to 'false'.");
            }
        }
        else {
            logger.info("consumer config '" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "' has been set to 'false'");
        }
        this.kafkaProperties.put (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // add our metric reporter
        this.kafkaProperties.put (ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "10000");
        if (kafkaProperties.containsKey (ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG)) {
            String propVal = kafkaProperties.getProperty (ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG);
            this.kafkaProperties.put (ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, 
                    propVal + "," + ConsumerMetricsReporter.class.getCanonicalName());
        }
        else {
            this.kafkaProperties.put (ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, ConsumerMetricsReporter.class.getCanonicalName());
        }

        this.timeouts = new ConsumerTimeouts (operatorContext, this.kafkaProperties);
        timeouts.adjust (this.kafkaProperties);
        maxPollRecords = getMaxPollRecordsFromProperties (this.kafkaProperties);
        maxPollIntervalMs = getMaxPollIntervalMsFromProperties (this.kafkaProperties);
        eventQueue = new LinkedBlockingQueue<Event>();
        processing = new AtomicBoolean (false);
        messageQueue = new LinkedBlockingQueue<ConsumerRecord<?, ?>> (getMessageQueueSizeMultiplier() * getMaxPollRecords());
        drainBuffer = new ArrayList<ConsumerRecord<?, ?>> (messageQueue.remainingCapacity());
        this.nPendingMessages = operatorContext.getMetrics().getCustomMetric("nPendingMessages");
        this.nLowMemoryPause = operatorContext.getMetrics().getCustomMetric("nLowMemoryPause");
        this.nQueueFullPause = operatorContext.getMetrics().getCustomMetric("nQueueFullPause");
        this.nAssignedPartitions = operatorContext.getMetrics().getCustomMetric("nAssignedPartitions");
    }


    /**
     * Returns the Kafka consumer group identifier, i.e. the value of the consumer config 'group.id'.
     * @return the group Id
     * @see #isGroupIdGenerated()
     */
    public String getGroupId() {
        return groupId;
    }


    /**
     * Gets the multiplier for the size of the message queue.
     * The multiplier is those factor that is used to multiply the Kafka consumer property
     * `max.poll.records` with to size the internal message queue. The default implementation uses
     * {@value #MESSAGE_QUEUE_SIZE_MULTIPLIER} as the factor.
     * @return the factor that `max.poll.records` is multiplied with to size the message queue.
     */
    protected int getMessageQueueSizeMultiplier() {
        return MESSAGE_QUEUE_SIZE_MULTIPLIER;
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
        Thread eventThread = getOperatorContext().getThreadFactory().newThread(new Runnable() {

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
        if (this.metricsFetcher == null) {
            this.metricsFetcher = new MetricsFetcher (getOperatorContext(), new MetricsProvider() {

                @Override
                public Map<MetricName, ? extends org.apache.kafka.common.Metric> getMetrics() {
                    return consumer.metrics();
                }

                @Override
                public String createCustomMetricName (MetricName metricName) throws KafkaMetricException {
                    return ConsumerMetricsReporter.createOperatorMetricName (metricName);
                }
            }, ConsumerMetricsReporter.getMetricsFilter(), AbstractKafkaClient.METRICS_REPORT_INTERVAL);
        }
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
            if (logger.isDebugEnabled()) {
                logger.debug ("Checking event queue for control message ..."); //$NON-NLS-1$
            }
            Event event = eventQueue.poll (30, TimeUnit.SECONDS);

            if (event == null) {
                continue;
            }

            logger.debug (MessageFormat.format ("runEventLoop() - processing event: {0}", event.getEventType().name()));
            switch (event.getEventType()) {
            case START_POLLING:
                StartPollingEventParameters p = (StartPollingEventParameters) event.getData();
                runPollLoop (p.getPollTimeoutMs(), p.getThrottlePauseMs());
                break;
            case STOP_POLLING:
                event.countDownLatch();  // indicates that polling has stopped
                break;
            case UPDATE_ASSIGNMENT:
                try {
                    processUpdateAssignmentEvent ((TopicPartitionUpdate) event.getData());
                } catch (Exception e) {
                    logger.error("The assignment '" + (TopicPartitionUpdate) event.getData() + "' update failed: " + e.getLocalizedMessage());
                } finally {
                    event.countDownLatch();
                }
                break;
            case CHECKPOINT:
                try {
                    processCheckpointEvent ((Checkpoint) event.getData());
                } finally {
                    event.countDownLatch();
                }
                break;
            case RESET:
                try {
                    processResetEvent ((Checkpoint) event.getData());
                } finally {
                    event.countDownLatch();
                }
                break;
            case RESET_TO_INIT:
                try {
                    processResetToInitEvent();
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
    protected void commitOffsets (CommitInfo offsets) throws RuntimeException {
        final Map<TopicPartition, OffsetAndMetadata> offsetMap = offsets.getMap();
        if (logger.isDebugEnabled()) {
            logger.debug ("Going to commit offsets: " + offsets);
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
                        postOffsetCommit (map);
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
                    consumer.commitSync (offsetMap);
                    postOffsetCommit (offsetMap);
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
     * This method is a hook which is called <b>after <i>successful</i> commit</b> of offsets.
     * When offsets are committed synchronous, the hook is called within the thread that committed the offsets,
     * which is the thread that invokes also the calls of 'consumer.poll'.
     * When offsets are committed asynchronous, the method is invoked by the commit callback.
     * 
     * Concrete classes must provide an implementation for this method.
     * 
     * @param offsets The mapping from topic partition to offsets (and meta data)
     */
    protected abstract void postOffsetCommit (Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * Implements the shutdown sequence.
     * This sequence includes
     * * remove all pending messages from the message queue
     * * closing the consumer
     * * terminating the event thread by setting the end condition
     * 
     * When you overwrite this method, you must call `super.shutdown()` in your implementation, preferably at the end.
     */
    protected void shutdown() {
        logger.debug("Shutdown sequence started..."); //$NON-NLS-1$
        getMessageQueue().clear();
        consumer.close(CONSUMER_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        processing.set(false);
    }

    /**
     * Resets the client to the initial state when used in consistent region.
     * Derived classes must overwrite this method, but can provide an empty implementation if consistent region is not supported.
     */
    protected abstract void processResetToInitEvent();

    /**
     * Resets the client to a previous state when used in consistent region.
     * Derived classes must overwrite this method, but can provide an empty implementation if consistent region is not supported.
     * @param checkpoint the checkpoint that contains the previous state
     */
    protected abstract void processResetEvent(Checkpoint checkpoint);

    /**
     * Creates a checkpoint of the current state when used in consistent region.
     * Derived classes must overwrite this method, but can provide an empty implementation if consistent region is not supported.
     * @param checkpoint A reference of a checkpoint object where the user provides the state to be saved.
     */
    protected abstract void processCheckpointEvent(Checkpoint checkpoint);

    /**
     * Updates the assignment of the client to topic partitions.
     * Derived classes must overwrite this method, but can provide an empty implementation 
     * if assignments of topic partitions cannot be updated.
     * @param update the update increment/decrement
     * @throws Exception 
     */
    protected abstract void processUpdateAssignmentEvent (TopicPartitionUpdate update);

    /**
     * This method must be overwritten by concrete classes. 
     * Here you implement polling for records and typically enqueue them into the message queue.
     * @param pollTimeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     * @param isThrottled true, when polling is throttled, false otherwise
     * @return number of records enqueued into the message queue
     * @throws InterruptedException The thread has been interrupted 
     * @throws SerializationException The value or the key from the message could not be deserialized
     * @see AbstractKafkaConsumerClient#getMessageQueue()
     */
    protected abstract int pollAndEnqueue (long pollTimeout, boolean isThrottled) throws InterruptedException, SerializationException;


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
        ConsumerRecord<?,?> record = null;
        if (messageQueue.isEmpty()) {
            // assuming, that the queue is not filled concurrently...
            msgQueueProcessed.set (true);
            try {
                msgQueueLock.lock();
                msgQueueEmptyCondition.signalAll();
            } finally {
                msgQueueLock.unlock();
            }
        }
        else msgQueueProcessed.set (false);
        // if filling the queue is NOT stopped, we can, of cause,
        // fetch a record now from the queue, even when we have seen an empty queue, shortly before... 

        // messageQueue.poll throws InterruptedException
        record = messageQueue.poll (timeout, timeUnit);
        if (record == null) {
            // no messages - queue is empty
            if (logger.isTraceEnabled()) logger.trace("getNextRecord(): message queue is empty");
            nPendingMessages.setValue (messageQueue.size());
        }
        return record;
    }


    /**
     * drains the message queue into a buffer.
     * The content of the buffer is enqueued when polling for records is initiated, before records are read from Kafka.
     * @return the number of drained records
     */
    protected int drainMessageQueueToBuffer() {
        int nRecords = 0;
        int qSize = 0;
        int bufSize = 0;
        logger.debug ("drainMessageQueueToBuffer(): trying to acquire lock");
        synchronized (drainBuffer) {
            if (!drainBuffer.isEmpty()) {
                logger.warn (MessageFormat.format ("drainMessageQueueToBuffer(): buffer is NOT empty. Num records in buffer = {0}", drainBuffer.size()));
            }
            nRecords = messageQueue.drainTo (drainBuffer);
            bufSize = drainBuffer.size();
        }
        qSize = messageQueue.size();
        logger.debug (MessageFormat.format ("drainMessageQueueToBuffer(): {0} consumer records drained to buffer. bufSz = {1}, queueSz = {2}.",
                nRecords, bufSize, qSize));
        return nRecords;
    }


    protected void clearDrainBuffer() {
        synchronized (drainBuffer) {
            drainBuffer.clear();
        }
    }


    /**
     * Waits that the message queue becomes empty and has been processed by the tuple producer thread.
     * This method should not be called, when filling the queue with new messages has not been stopped before.
     * @throws InterruptedException The waiting thread has been interrupted waiting
     */
    protected void awaitMessageQueueProcessed() throws InterruptedException {
        while (!(messageQueue.isEmpty() && msgQueueProcessed.get())) {
            try {
                msgQueueLock.lock();
                msgQueueEmptyCondition.await (100l, TimeUnit.MILLISECONDS);
            }
            finally {
                msgQueueLock.unlock();
            }
        }
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
     * @param throttleSleepMillis the time in milliseconds the polling thread sleeps after each poll.
     * 
     * @throws InterruptedException 
     */
    protected void runPollLoop (long pollTimeout, long throttleSleepMillis) throws InterruptedException {
        if (throttleSleepMillis > 0l) {
            logger.debug (MessageFormat.format ("Initiating throttled polling (sleep time = {0} ms); maxPollRecords = {1}",
                    throttleSleepMillis, getMaxPollRecords()));
        }
        else {
            logger.debug (MessageFormat.format ("Initiating polling; maxPollRecords = {0}", getMaxPollRecords()));
        }
        synchronized (drainBuffer) {
            if (!drainBuffer.isEmpty()) {
                final int bufSz = drainBuffer.size();
                final int capacity = messageQueue.remainingCapacity();
                // restore records that have been put aside to the drain buffer
                if (capacity < bufSz) {
                    String msg = MessageFormat.format ("drain buffer size {0} > capacity of message queue {1}", bufSz, capacity);
                    logger.error ("runPollLoop() - " + msg);
                    // must restart operator.
                    throw new RuntimeException (msg);
                }
                messageQueue.addAll (drainBuffer);
                final int qSize = messageQueue.size();
                drainBuffer.clear();
                logger.debug (MessageFormat.format ("runPollLoop(): {0} consumer records added from drain buffer to the message queue. Message queue size is {1} now.", bufSz, qSize));
            }
        }
        // continue polling for messages until a new event
        // arrives in the event queue
        fetchPaused = consumer.paused().size() > 0;
        logger.debug ("previously paused partitions: " + consumer.paused());
        while (eventQueue.isEmpty()) {
            boolean doPoll = true;
            // can wait for 100 ms; throws InterruptedException:
            try {
                checkSpaceInMessageQueueAndPauseFetching (false);
            }
            catch (IllegalStateException e) {
                logger.warn ("runPollLoop(): " + e.getLocalizedMessage());
                // no space, could not pause - do not call poll
                doPoll = false;
            }
            if (doPoll) {
                try {
                    final long now = System.currentTimeMillis();
                    final long timeBetweenPolls = now -lastPollTimestamp;
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
                    /*int nRecordsEnqueued = */pollAndEnqueue (pollTimeout, throttleSleepMillis > 0l);
                    nPendingMessages.setValue (messageQueue.size());
                    if (throttleSleepMillis > 0l) {
                        Thread.sleep (throttleSleepMillis);
                    }
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
     * checks available space in the message queue and pauses fetching
     * if the space is not sufficient for a batch of consumer records.
     * This method may block up to 100 milliseconds waiting for space to become available.
     * 
     * @throws IllegalStateException fetching could not be paused because one or more
     * of the partitions to be paused are not assigned to the consumer any more.
     * When this exception is thrown, the client should not poll for new records. 
     * @throws InterruptedException Thread has been interrupted waiting for space become available
     */
    protected void checkSpaceInMessageQueueAndPauseFetching (boolean resetPausedState) throws IllegalStateException, InterruptedException {
        if (resetPausedState) {
            fetchPaused = consumer.paused().size() > 0;
        }
        if (!isSpaceInMsgQueueWait()) {
            if (!fetchPaused) {
                consumer.pause (assignedPartitions);
                if (logger.isDebugEnabled()) logger.debug ("runPollLoop() - no space in message queue, fetching paused");
                fetchPaused = true;
            }
        }
        else {
            // space in queue, resume fetching
            if (fetchPaused) {
                try {
                    // when not paused, 'resumed' is a no-op
                    consumer.resume (assignedPartitions);
                    if (logger.isDebugEnabled()) logger.debug ("runPollLoop() - fetching resumed");
                    fetchPaused = false;
                }
                catch (IllegalStateException e) {
                    logger.warn ("runPollLoop(): " + e.getLocalizedMessage());
                }
            }
        }
    }

    /**
     * Checks for available space in the message queue including memory consumption
     * and waits up to 100 ms if the space is not sufficient.
     * Maintains the metrics `nPendingMessages`, `nLowMemoryPause`, and `nQueueFullPause`.
     * 
     * @throws InterruptedException Thread interrupted while waiting.
     */
    private boolean isSpaceInMsgQueueWait() throws InterruptedException {
        boolean space = messageQueue.isEmpty();
        boolean lowMemory = false;
        int remainingCapacity = 0;
        int mqSize = 0;
        if (!space) {
            // queue not empty
            mqSize = messageQueue.size();
            if (mqSize <= 4 * maxPollRecords)
                space = true;
            else {
                lowMemory = isLowMemory();
                remainingCapacity = messageQueue.remainingCapacity();
                space = !lowMemory && remainingCapacity >= maxPollRecords;
            }
        }
        if (!space) {
            if (logger.isDebugEnabled()) {
                if (lowMemory) {
                    logger.debug (MessageFormat.format ("low memory detected: messages queued ({0}).", mqSize));
                } else {
                    logger.debug (MessageFormat.format ("remaining capacity in message queue ({0}) < max.poll.records ({1}).",
                            remainingCapacity, maxPollRecords));
                }
            }
            nPendingMessages.setValue(mqSize);
            if (lowMemory)
                nLowMemoryPause.increment();
            else
                nQueueFullPause.increment();
            try {
                msgQueueLock.lock();
                msgQueueEmptyCondition.await(100, TimeUnit.MILLISECONDS);
            } finally {
                msgQueueLock.unlock();
            }
            nPendingMessages.setValue(mqSize);
            if (logger.isTraceEnabled()) {
                logger.trace ("isSpaceInMsgQueueWait() returning 'false'");
            }
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
     * Returns the maximum number of records to be returned from Kafka at each invocation of poll() at the consumer object
     * @return the maxPollRecords
     */
    protected int getMaxPollRecords() {
        return maxPollRecords;
    }

    /**
     * Returns the maximum amount of time in milliseconds that the broker allows between 
     * each poll before kicking a consumer out of the consumer group.
     * @return the value of the consumer config max.poll.interval.ms
     */
    public long getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    /**
     * adds an event to the event queue
     * 
     * @param event the event
     */
    protected void sendEvent (Event event) {
        logger.debug (MessageFormat.format("Sending event: {0}", event));
        eventQueue.add (event);
        logger.debug(MessageFormat.format("Event {0} inserted into queue, q={1}", event, eventQueue));
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
        sendEvent(event);
        event.await();
    }

    /**
     * Initiates start of polling for KafKa messages.
     * Implementations should ignore this event if the consumer is not subscribed or assigned to partitions.
     */
    public void sendStartPollingEvent() {
        Event event = new Event (EventType.START_POLLING, new StartPollingEventParameters (pollTimeout), false);
        sendEvent(event);
    }

    /**
     * Initiates start of throttled polling for KafKa messages.
     * Implementations should ignore this event if the consumer is not subscribed or assigned to partitions.
     * @param throttlePauseMillis The time in milliseconds the consumer will sleep between the invocations of each consumer.poll().
     */
    public void sendStartThrottledPollingEvent (long throttlePauseMillis) {
        Event event = new Event (EventType.START_POLLING, new StartPollingEventParameters (0, throttlePauseMillis), false);
        logger.debug (MessageFormat.format("Sending event: {0}; throttled", event));
        sendEvent (event);
    }


    /**
     * Initiates stop polling for Kafka messages.
     * Implementations ensure that polling has stopped when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    public void sendStopPollingEvent() throws InterruptedException {
        Event event = new Event (EventType.STOP_POLLING, true);
        sendEvent(event);
        event.await();
    }

    /**
     * Initiates stop polling for Kafka messages without waiting that the event has been processed.
     * Effectively, it only inserts an event into the event queue.
     */
    protected void sendStopPollingEventAsync() {
        Event event = new Event (EventType.STOP_POLLING);
        sendEvent(event);
    }

    /**
     * Assigns the consumer to the given set of topic partitions manually. No group management.
     * This assignment will replace the previous assignment.
     * @param topicPartitions The topic partitions. null or an empty set is equivalent to
     *                        unsubscribe from everything previously subscribed or assigned.
     */
    protected void assign (Set<TopicPartition> topicPartitions) {
        logger.info("Assigning. topic-partitions = " + topicPartitions);
        if (topicPartitions == null) topicPartitions = Collections.emptySet();
        consumer.assign(topicPartitions);
        this.assignedPartitions = new HashSet<TopicPartition> (topicPartitions);
        // update metric:
        nAssignedPartitions.setValue (this.assignedPartitions.size());
        this.subscriptionMode = topicPartitions.isEmpty()? SubscriptionMode.NONE: SubscriptionMode.ASSIGNED;
    }

    /**
     * Subscribes the consumer to the given topics. Subscription enables dynamic group assignment.
     * This subscription unassigns all partitions and replaces a previous subscription.
     * @param topics The topics to subscribe. An empty list or null is treated as unsubscribe from all.
     * @param rebalanceListener an optional ConsumerRebalanceListener
     */
    protected void subscribe(Collection<String> topics, ConsumerRebalanceListener rebalanceListener) {
        logger.info("Subscribing. topics = " + topics); //$NON-NLS-1$
        if (topics == null) topics = Collections.emptyList();
        consumer.subscribe (topics, rebalanceListener);
        getOperatorContext().getMetrics().createCustomMetric (N_PARTITION_REBALANCES, "Number of partition rebalances within the consumer group", Metric.Kind.COUNTER);
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
    public void onTopicAssignmentUpdate (final TopicPartitionUpdate update) throws InterruptedException {
        Event event = new Event(EventType.UPDATE_ASSIGNMENT, update, true);
        sendEvent (event);
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
    public final void onShutdown (long timeout, TimeUnit timeUnit) throws InterruptedException {
        Event event = new Event(EventType.SHUTDOWN, true);
        sendEvent (event);
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
        if (totalMemory < (maxMemory * 0.90))
            return false;

        final double freeMemory = rt.freeMemory();

        // Low memory if free memory at less than 10% of max.
        return freeMemory < (maxMemory * 0.1);
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
     * @throws UnknownTopicException one of the given topics does not exist and cannot be automatically created by the broker
     */
    protected Set<TopicPartition> getAllTopicPartitionsForTopic (Collection<String> topics) throws UnknownTopicException {
        Set<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
        for (String topic: topics) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null) {
                throw new UnknownTopicException ("Could not get partition information for topic " + topic);
            }
            for (PartitionInfo p: partitions) topicPartitions.add (new TopicPartition (topic, p.partition()));
        }
        return topicPartitions;
    }

    /**
     * Seeks to the given position. This method evaluates lazily on next poll() or position() call.
     * @param topicPartitions The partitions to seek. If no partitions are given, all assigned partitions are seeked.
     * @param startPosition one of `StartPosition.End` or `StartPosition.Beginning`. `StartPosition.Default` is silently ignored.
     */
    protected void seekToPosition(Collection<TopicPartition> topicPartitions, StartPosition startPosition) {
        logger.info (MessageFormat.format ("seekToPosition() - {0}  -->  {1}", topicPartitions, startPosition));
        switch (startPosition) {
        case Beginning:
            consumer.seekToBeginning(topicPartitions);
            break;
        case End:
            consumer.seekToEnd(topicPartitions);
            break;
        case Default:
            logger.debug("seekToPosition: ignoring position " + startPosition);
            break;
        default:
            throw new IllegalArgumentException("seekToPosition: illegal position: " + startPosition);
        }
    }

    /**
     * Seeks a single topic partition to the given position. This method evaluates lazily on next poll() or position() call.
     * @param tp The partition to seek.
     * @param startPosition one of `StartPosition.End` or `StartPosition.Beginning`. `StartPosition.Default` is silently ignored.
     */
    protected void seekToPosition (TopicPartition tp, StartPosition startPosition) {
        logger.info (MessageFormat.format ("seekToPosition() - {0}  -->  {1}", tp, startPosition));
        switch (startPosition) {
        case Beginning:
            consumer.seekToBeginning (Collections.nCopies (1, tp));
            break;
        case End:
            consumer.seekToEnd (Collections.nCopies (1, tp));
            break;
        case Default:
            logger.debug("seekToPosition: ignoring position " + startPosition);
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
        logger.info (MessageFormat.format ("seekToTimestamp() - {0}", topicPartitionTimestampMap));
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
     * Seek the consumer for a single topic partitions to the nearest offset for a timestamp.
     * If there is no such offset, the consumer will move to the offset as determined by the 'auto.offset.reset' config
     * @param tp the topic partition
     * @param timestamp the timestamp in milliseconds since epoch
     */
    protected void seekToTimestamp (TopicPartition tp, long timestamp) {
        logger.info (MessageFormat.format ("seekToTimestamp() - {0}  --> {1}", tp, timestamp));
        Map <TopicPartition, Long> topicPartitionTimestampMap = new HashMap<>(1);
        topicPartitionTimestampMap.put (tp, new Long(timestamp));
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes (topicPartitionTimestampMap);
        logger.debug ("offsetsForTimes = " + offsetsForTimes);
        OffsetAndTimestamp ot = offsetsForTimes.get(tp);
        if (ot != null) {
            logger.info ("Seeking consumer for tp = " + tp + " to offsetAndTimestamp=" + ot);
            consumer.seek(tp, ot.offset());
        } else {
            // nothing...consumer will move to the offset as determined by the 'auto.offset.reset' config
        }
    }


    /**
     * Seeks to the given offsets for the given topic partitions. This offset is the offset that will is consume next.
     * If offset equals -1, seek to the end of the topic
     * If offset equals -2, seek to the beginning of the topic
     * Otherwise, seek to the specified offset
     */
    private void seekToOffset(Map<TopicPartition, Long> topicPartitionOffsetMap) {
        logger.info (MessageFormat.format ("seekToOffset() - {0}", topicPartitionOffsetMap));
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
        if (topicPartitionOffsetMap == null || topicPartitionOffsetMap.isEmpty()) {
            assign (Collections.emptySet());
        }
        else {
            assign (topicPartitionOffsetMap.keySet());
            seekToOffset (topicPartitionOffsetMap);
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
            if (logger.isInfoEnabled()) {
                logger.debug ("onComplete(): Offsets successfully committed async: " + offsets);
            }
            postOffsetCommit (offsets);
        }
        else {
            logger.warn(Messages.getString("OFFSET_COMMIT_FAILED", exception.getLocalizedMessage()));
        }
    }
}
