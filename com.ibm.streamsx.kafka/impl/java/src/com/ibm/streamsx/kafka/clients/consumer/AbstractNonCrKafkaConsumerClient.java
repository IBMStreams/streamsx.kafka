/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;


import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class is the base class for Consumer clients that can be used when not in a consistent region.
 * This class provides default implementations for all checkpoint and consistent region related methods from
 * @link {@link ConsumerClient}.
 */
public abstract class AbstractNonCrKafkaConsumerClient extends AbstractKafkaConsumerClient {

    private static final Logger trace = Logger.getLogger(AbstractNonCrKafkaConsumerClient.class);
    
    /**
     * @param operatorContext
     * @param keyClass
     * @param valueClass
     * @param kafkaProperties
     * @throws KafkaConfigurationException
     */
    public <K, V> AbstractNonCrKafkaConsumerClient (OperatorContext operatorContext, Class<K> keyClass,
            Class<V> valueClass, KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onDrain()
     */
    @Override
    public void onDrain() throws Exception {
        throw new Exception ("onDrain(): consistent region is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetToInitEvent()
     */
    @Override
    protected void processResetToInitEvent() {
        throw new RuntimeException ("resetToInitialState(): consistent region is not supported by this consumer client: " + getThisClassName());
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetEvent(Checkpoint)
     */
    @Override
    protected void processResetEvent(Checkpoint checkpoint) {
        trace.error ("'config checkpoint' is not supported by the " + getOperatorContext().getKind() + " operator.");
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processCheckpointEvent(Checkpoint)
     */
    @Override
    protected void processCheckpointEvent (Checkpoint data) {
        throw new RuntimeException ("onReset(): consistent region and 'config checkpoint' is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpointRetire(long)
     */
    @Override
    public void onCheckpointRetire(long id) {
        throw new RuntimeException ("onCheckpointRetire(): consistent region is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onCheckpoint (Checkpoint checkpoint) throws InterruptedException {
        throw new RuntimeException ("consistent region and 'config checkpoint' is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onReset(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onReset(Checkpoint checkpoint) throws InterruptedException {
        throw new RuntimeException ("onReset(): consistent region and 'config checkpoint' is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onResetToInitialState()
     */
    @Override
    public void onResetToInitialState() throws InterruptedException {
        throw new RuntimeException ("onResetToInitialState(): consistent region is not supported by this consumer client: " + getThisClassName());
    }
}
