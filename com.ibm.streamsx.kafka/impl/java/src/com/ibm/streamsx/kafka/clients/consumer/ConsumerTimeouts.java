/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import com.ibm.icu.text.MessageFormat;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class resolves the timeout dependencies for the consumer.
 * @author IBM Kafka toolkit maintainers
 */
public class ConsumerTimeouts {

    private static final Logger trace = Logger.getLogger (ConsumerTimeouts.class);

    private static final int CR_TIMEOUT_MULTIPLIER = 3;
    private static final long MAX_POLL_INTERVAL_MILLIS = 300000;
    private static final long SESSION_TIMEOUT_MS = 120000;
//    auto.commit.interval.ms = 5000     -
//    connections.max.idle.ms = 540000   
//    fetch.max.wait.ms = 500            
//    heartbeat.interval.ms = 3000       
//    max.poll.interval.ms = 540000      x
//    metadata.max.age.ms = 300000       
//    metrics.sample.window.ms = 30000   
//    reconnect.backoff.max.ms = 1000    
//    reconnect.backoff.ms = 50          
//    request.timeout.ms = 125000        x
//    retry.backoff.ms = 100             
//    session.timeout.ms = 120000        x


    @SuppressWarnings("unused")
    private final OperatorContext opContext;
    @SuppressWarnings("unused")
    private final KafkaOperatorProperties kafkaProperties;
    private final ConsistentRegionContext crContext;
    private final boolean inConsistentRegion;
    private final long crResetTimeoutMs;
    private final long crDrainTimeoutMs;

    /**
     * 
     */
    public ConsumerTimeouts (OperatorContext operatorContext, KafkaOperatorProperties kafkaProperties) {
        this.opContext = operatorContext;
        this.crContext = operatorContext.getOptionalContext(ConsistentRegionContext.class);
        this.inConsistentRegion = this.crContext != null;
        if (inConsistentRegion) {
            crResetTimeoutMs = (long) (crContext.getResetTimeout() * 1000.0);
            crDrainTimeoutMs = (long) (crContext.getDrainTimeout() * 1000.0);
        }
        else {
            crResetTimeoutMs = 0;
            crDrainTimeoutMs = 0;
        }
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Returns the minimum timeout for max.poll.interval.ms in milliseconds, which is 3*max(CR_resetTimeout, CR_drainTimeout) 
     * @return the recommended value for max.poll.interval.ms
     */
    public long getMaxPollIntervalMs () {
        if (inConsistentRegion) {
            return CR_TIMEOUT_MULTIPLIER * (crResetTimeoutMs > crDrainTimeoutMs? crResetTimeoutMs: crDrainTimeoutMs);
        }
        return MAX_POLL_INTERVAL_MILLIS;
    }

    /**
     * Returns the minimum timeout for session.timeout.ms in milliseconds, which should be higher than the PE restart + reset time of an operator. 
     * @return the recommended value for session.timeout.ms #
     */
    public long getSessionTimeoutMs () {
        return SESSION_TIMEOUT_MS;
    }

    /**
     * Returns the minimum timeout for request.timeout.ms in milliseconds, which must be higher than session.timeout.ms.
     * @return the recommended value for request.timeout.ms #
     */
    public long getRequestTimeoutMs () {
        return SESSION_TIMEOUT_MS + 5000;
    }

    /** 
     * Adjusts some timeouts in the Kafka properties and mutates them.
     * @param kafkaProperties The kafka properties that are modified
     */
    public void adjust (KafkaOperatorProperties kafkaProperties) {
        adjustProperty (kafkaProperties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, getMaxPollIntervalMs());
        adjustProperty (kafkaProperties, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, getSessionTimeoutMs());
        adjustProperty (kafkaProperties, ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, getRequestTimeoutMs());
    }

    /**
     * @param kafkaProperties  The kafka properties
     * @param propertyName     the property name
     * @param minValue         the minimum value
     * @throws NumberFormatException
     */
    private void adjustProperty (KafkaOperatorProperties kafkaProperties, final String propertyName, final long minValue) throws NumberFormatException {
        boolean setProp = false;;
        if (kafkaProperties.containsKey (propertyName)) {
            long propValue = Long.valueOf (kafkaProperties.getProperty (propertyName));
            if (propValue < minValue) {
                trace.warn (MessageFormat.format ("consumer config ''{0}'' has been increased from {1} to {2}.",
                        propertyName, propValue, minValue));
                setProp = true;
            }
        }
        else {
            trace.info (MessageFormat.format ("consumer config ''{0}'' has been set to {1}.",
                    propertyName, minValue));
        }
        if (setProp) {
            kafkaProperties.put (propertyName, "" + minValue);
        }
    }
}
