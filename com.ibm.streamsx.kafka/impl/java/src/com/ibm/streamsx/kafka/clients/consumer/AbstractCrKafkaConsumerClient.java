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

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.clients.consumer.Event.EventType;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class is the base class for Consumer clients that can be used in a consistent region.
 */
public abstract class AbstractCrKafkaConsumerClient extends AbstractKafkaConsumerClient {

    @SuppressWarnings("unused")
    private static final Logger tracer = Logger.getLogger(AbstractCrKafkaConsumerClient.class);
    private static final int MESSAGE_QUEUE_SIZE_MULTIPLIER = 100;

    private final ConsistentRegionContext crContext;

    /**
     * Constructs a new AbstractCrKafkaConsumerClient and adjusts the Kafka properties for use in a consistent region.
     * @param operatorContext
     * @param keyClass
     * @param valueClass
     * @param kafkaProperties modifies enable.auto.commit, max.poll.interval.ms, and potentially others in the kafka properties
     * @throws KafkaConfigurationException
     */
    public <K, V> AbstractCrKafkaConsumerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass, KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);
        this.crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        if (crContext == null || getJcpContext() == null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' must be used in a consistent region. This consumer client implementation (" 
                    + getThisClassName() + ") requires a Consistent Region context and a Control Plane context.");
        }

        operatorContext.getMetrics().createCustomMetric (DRAIN_TIME_MILLIS_METRIC_NAME, "last drain time of this operator in milliseconds", Metric.Kind.GAUGE);
        operatorContext.getMetrics().createCustomMetric (DRAIN_TIME_MILLIS_MAX_METRIC_NAME, "maximum drain time of this operator in milliseconds", Metric.Kind.GAUGE);
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
     * @return the Consistent Region Context
     */
    public final ConsistentRegionContext getCrContext() {
        return crContext;
    }


    /**
     * Starts the consumer and event thread for controlling the consistent region.
     * This method ensures that the event thread is running when it returns.
     * Methods that overwrite this method must call super.startConsumer().
     * @throws InterruptedException The thread has been interrupted.
     * @throws KafkaClientInitializationException The client could not be initialized
     */
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        super.startConsumer();
    }



    /**
     * This is an empty default implementation.
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#postOffsetCommit(java.util.Map)
     */
    @Override
    protected void postOffsetCommit (Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    /**
     * Initiates checkpointing of the consumer client by sending an event to the event queue and initiates start of polling.
     * Implementations ensure that checkpointing the client has completed when this method returns. 
     * @param checkpoint the checkpoint
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onCheckpoint (Checkpoint checkpoint) throws InterruptedException {
        Event event = new Event(EventType.CHECKPOINT, checkpoint, true);
        sendEvent (event);
        event.await();
        sendStartPollingEvent();
    }


    /**
     * The consumer can prepare any data from the checkpoint. This method invocation should be followed by by
     * sending a reset event if not interrupted. This method is run by a runtime thread at 
     * reset of the consistent region.
     * Subclasses must have their own implementation.
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread has been interrupted.
     */
    protected abstract void resetPrepareDataAfterStopPolling (final Checkpoint checkpoint) throws InterruptedException;

    /**
     * The consumer can prepare any data from the checkpoint. This method invocation is called before polling is stopped. This method is run by a runtime thread at 
     * reset of the consistent region.
     * Subclasses must have their own implementation.
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread has been interrupted.
     */
    protected abstract void resetPrepareDataBeforeStopPolling (final Checkpoint checkpoint) throws InterruptedException;
    
    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#preDeQueueForSubmit()
     */
    @Override
    protected void preDeQueueForSubmit() {
    }

    /**
     * Initiates resetting the client to a prior state by sending an event to the event queue
     * and initiates start of polling.
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @param checkpoint the checkpoint that contains the state.
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onReset (final Checkpoint checkpoint) throws InterruptedException {
        resetPrepareDataBeforeStopPolling (checkpoint);
        sendStopPollingEvent();
        resetPrepareDataAfterStopPolling (checkpoint);
        Event event = new Event (EventType.RESET, checkpoint, true);
        sendEvent (event);
        event.await();
        sendStartPollingEvent();
    }

    /**
     * Initiates resetting the client to the initial state by sending an event to the event queue
     * and initiates start of polling.
     * Implementations ensure that resetting the client has completed when this method returns. 
     * @throws InterruptedException The thread waiting for finished condition has been interrupted.
     */
    @Override
    public void onResetToInitialState() throws InterruptedException {
        sendStopPollingEvent();
        Event event = new Event(EventType.RESET_TO_INIT, true);
        sendEvent (event);
        event.await();
        sendStartPollingEvent();
    }
}
