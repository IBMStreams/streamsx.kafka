/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class represents a Consumer client that can be used in a consistent region.
 */
public abstract class AbstractCrKafkaConsumerClient extends AbstractKafkaConsumerClient {

    private static final Logger tracer = Logger.getLogger(AbstractCrKafkaConsumerClient.class);
    private final ConsistentRegionContext crContext;
    private final ControlPlaneContext jcpContext;

    /**
     * @param operatorContext
     * @param keyClass
     * @param valueClass
     * @param kafkaProperties the kafka properties are modified
     * @throws KafkaConfigurationException
     */
    public <K, V> AbstractCrKafkaConsumerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass, KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);
        this.crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        this.jcpContext = operatorContext.getOptionalContext (ControlPlaneContext.class);
        if (crContext == null || jcpContext == null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' must be used in a consistent region. This consumer client implementation (" 
                    + getThisClassName() + ") requires a Consistent Region context and a Control Plane context.");
        }
        // always disable auto commit - we commit on drain
        if (kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            if (kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true")) {
                tracer.warn("consumer config '" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "' has been turned to 'false'. In a consistent region, offsets are always committed when the region drains.");
            }
        }
        else {
            tracer.info("consumer config '" + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG + "' has been set to 'false' for CR.");
        }
        kafkaProperties.put (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    /**
     * @return the Consistent Region Context
     */
    public ConsistentRegionContext getCrContext() {
        return crContext;
    }

    /**
     * @return the Control Plane Context
     */
    public ControlPlaneContext getJcpContext() {
        return jcpContext;
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
}
