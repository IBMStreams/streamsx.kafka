package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;

/**
 * This interface represents the Kafka consumer client within the operator.
 */
public interface ConsumerClient {

    public static final String DRAIN_TIME_MILLIS_MAX_METRIC_NAME = "drainTimeMillisMax";
    public static final String DRAIN_TIME_MILLIS_METRIC_NAME = "drainTimeMillis";

    /**
     * Returns the client-ID, which is the value of the Kafka consumer property client.id
     * @return the client-ID
     */
    public String getClientId();
    
    /**
     * creates the Kafka consumer and starts the consumer and event thread.
     * This method ensures that the event thread is running when it returns.
     * @throws InterruptedException The thread has been interrupted waiting for the consumer thread to start
     * @throws KafkaClientInitializationException initialization of the Kafka consumer failed
     */
    void startConsumer() throws InterruptedException, KafkaClientInitializationException;

    /**
     * subscribes to topics or assigns to topic partitions and seeks to the given start position.
     * Seeking to the position can be done lazily.
     * @param topics         the topics
     * @param partitions     partition numbers. Every given topic must have the given partition numbers.
     * @param startPosition  start position. Must be one of {@link StartPosition#Default}, {@link StartPosition#Beginning}, {@link StartPosition#End}.
     * @throws Exception 
     */
    void subscribeToTopics (final Collection<String> topics, final Collection<Integer> partitions, final StartPosition startPosition)
            throws Exception;

    /**
     * subscribes to topics or assigns to topic partitions and seeks to the nearest offset given by a timestamp.
     * Seeking to the position can be done lazily.
     * @param topics         the topics
     * @param partitions     partition numbers. Every given topic must have the given partition numbers.
     * @param timestamp      the timestamp where to start reading in milliseconds since Epoch.
     * @throws Exception 
     */
    void subscribeToTopicsWithTimestamp(final Collection<String> topics, final Collection<Integer> partitions, final long timestamp)
            throws Exception;

    /**
     * Assigns the consumer to topic partitions of a single topic and seeks to the given offsets for each topic partition.
     * @param topic        the topic
     * @param partitions   the partition numbers
     * @param startOffsets the start offsets for the partition numbers, in which `startOffsets[i]` 
     *                     represents the start offset for partition number `partitions[i]`.
     *                     The size of the `startOffsets` list must be equal the size of `partitions`. 
     * @throws Exception
     */
    void subscribeToTopicsWithOffsets(final String topic, final List<Integer> partitions, final List<Long> startOffsets)
            throws Exception;

    /**
     * Tests whether the consumer is assigned to topic partitions or subscribed to topics.
     * Note: A consumer that subscribed to topics can have no assignment to partitions when the
     * group coordinator decides so. This method will also return `true` in this case.
     * @return `true` if the consumer has subscribed to at least one topic or has been assigned to at least one topic partition.
     */
    boolean isSubscribedOrAssigned();

    /**
     * Initiates start of polling for KafKa messages.
     * Implementations should ignore this event if the consumer is not subscribed or assigned to partitions.
     */
    void sendStartPollingEvent();

    /**
     * Initiates stop polling for Kafka messages.
     * Implementations ensure that polling has stopped when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interruped.
     */
    void sendStopPollingEvent() throws InterruptedException;

    /**
     * Initiates topic partition assignment update. When this method is called, the consumer must be assigned to topic partitions.
     * If the consumer is subscribed to topics, the request is ignored.
     * Implementations ensure assignments have been updated when this method returns. 
     * @param update The the partition update.
     * @throws InterruptedException The thread waiting for finished condition has been interruped.
     */
    void onTopicAssignmentUpdate (final TopicPartitionUpdate update) throws InterruptedException;
    
    /**
     * Action to be performed on consistent region drain.
     * @throws Exception
     */
    void onDrain() throws Exception;
    
    /**
     * Action to be performed when a checkpoint is retired.
     * @param id The checkpoint sequence ID
     */
    void onCheckpointRetire (long id);

    /**
     * Initiates checkpointing of the consumer client.
     * Implementations ensure that checkpointing the client has completed when this method returns. 
     * @param checkpoint the checkpoint
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    void onCheckpoint (Checkpoint checkpoint) throws InterruptedException;

    /**
     * Initiates resetting the client to a prior state.
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    void onReset (final Checkpoint checkpoint) throws InterruptedException;

    /**
     * Initiates resetting the client to the initial state. 
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    void onResetToInitialState() throws InterruptedException;

    /**
     * Initiates a shutdown of the consumer client.
     * Implementations ensure that shutting down the client has completed when this method returns. 
     * @param timeout    the timeout to wait for shutdown completion
     * @param timeUnit   the unit of time for the timeout
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    void onShutdown (long timeout, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Gets the next consumer record that has been received. If there are no records, the method waits the specified timeout.
     * @param timeout   the timeout to wait for records
     * @param timeUnit   the unit of time for the timeout
     * @return the next consumer record or `null` if there was no record within the timeout.
     * @throws InterruptedException The thread waiting for records has been interrupted.
     */
    ConsumerRecord<?, ?> getNextRecord (long timeout, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Implementations can implement an action that is called after the given consumer record has been submitted as a tuple.
     * For example, the consumer client can remember the offsets of the records for committing later. 
     */
    void postSubmit (ConsumerRecord<?, ?> submittedRecord);
}