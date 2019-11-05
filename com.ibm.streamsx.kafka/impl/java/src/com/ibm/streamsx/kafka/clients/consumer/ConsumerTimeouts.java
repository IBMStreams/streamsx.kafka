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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.MsgFormatter;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class resolves the timeout dependencies for the consumer.
 * @author IBM Kafka toolkit maintainers
 */
public class ConsumerTimeouts {

    private static final Logger trace = Logger.getLogger (ConsumerTimeouts.class);

    private static final int CR_TIMEOUT_MULTIPLIER = 3;
    private static final long MAX_POLL_INTERVAL_MILLIS = 300000;
    // we make this timeout larger than the expected PE restart time to avoid 
    // that the group coordinator rebalances the group among the not restarted
    // consumers while a consumer is restarted. When a consumer restarts (subscribes)
    // the partitions are re-assigned anyway.
    // When a consumer closes the client (graceful shutdown on stopPE) the group coordinator initializes re-balance immediately.
    private static final long SESSION_TIMEOUT_MS_DYNAMIC_GRP = 20000;
    private static final long SESSION_TIMEOUT_MS_STATIC_GRP = 120000;
    private static final long METADATA_MAX_AGE_MS = 2000;

    @SuppressWarnings("unused")
    private final OperatorContext opContext;
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
        // clone the Kafka properties to avoid they being changed when properties are setup.
        this.kafkaProperties = new KafkaOperatorProperties();
        this.kafkaProperties.putAll (kafkaProperties);
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
     * With set group.instance.id, the value is 900000, 15 minutes. 
     * @return the recommended value for session.timeout.ms
     */
    public long getSessionTimeoutMs () {
        final boolean isDynamicGroupMember = kafkaProperties.getProperty (ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "").trim().isEmpty();
        if (isDynamicGroupMember) return SESSION_TIMEOUT_MS_DYNAMIC_GRP;

        // default broker configs for maximum values:
        // group.max.session.timeout.ms = 300000 (5 min) for Kafka <= 2.2
        // group.max.session.timeout.ms = 1800000 (30 min) since Kafka 2.3
        final long crResetTo12 = (long) (1.2 * crResetTimeoutMs);
        return crResetTo12 > SESSION_TIMEOUT_MS_STATIC_GRP? crResetTo12: SESSION_TIMEOUT_MS_STATIC_GRP; 
    }

    /**
     * Returns the minimum timeout for request.timeout.ms in milliseconds, which must be higher than session.timeout.ms.
     * @return the recommended value for request.timeout.ms
     */
    public long getRequestTimeoutMs () {
        long sessionTimeoutMs = SESSION_TIMEOUT_MS_DYNAMIC_GRP;
        // when group.instance.id is set, the session timeout may have been set to a really high value.
        // In this case do NOT uses session.timeout.ms + 5s
        final boolean isDynamicGroupMember = kafkaProperties.getProperty (ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "").trim().isEmpty();
        if (kafkaProperties.containsKey (ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) && isDynamicGroupMember) {
            sessionTimeoutMs = Long.valueOf (kafkaProperties.getProperty (ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        }
        return sessionTimeoutMs + 5000;
    }

    /**
     * Returns the timeout for the JMX notification 'checkpoint merge complete'.
     * 
     * @return the minimum timeout in milliseconds, which is min (crResetTimeout/2, max.poll.interval.ms/2, 15 seconds)
     */
    public long getJmxResetNotificationTimeout() {
        // min (crResetTimeout/2, max.poll.interval.ms /2, 15 seconds)
        long timeout = crResetTimeoutMs / 2 < 15000? crResetTimeoutMs / 2: 15000;
        if (timeout > getMaxPollIntervalMs()/2) timeout = getMaxPollIntervalMs()/2;
        return timeout;
    }

    /** 
     * Adjusts some timeouts in the Kafka properties and mutates them.
     * @param kafkaProperties The kafka properties that are modified
     */
    public void adjust (KafkaOperatorProperties kafkaProperties) {
        adjustPropertyToMin (kafkaProperties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, getMaxPollIntervalMs());
        setPropertyIfUnset  (kafkaProperties, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "" + getSessionTimeoutMs());
        adjustPropertyToMin (kafkaProperties, ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, getRequestTimeoutMs());
        adjustPropertyToMax (kafkaProperties, ConsumerConfig.METADATA_MAX_AGE_CONFIG, METADATA_MAX_AGE_MS);
    }

    /**
     * Sets a property value if the key is not yet existing in given kafkaProperties
     * @param kafkaProperties  The kafka properties that get mutated
     * @param propertyName     the property name
     * @param value            the property value to set
     */
    private void setPropertyIfUnset (KafkaOperatorProperties kafkaProperties, final String propertyName, final String value) {
        if (!kafkaProperties.containsKey (propertyName)) {
            kafkaProperties.put (propertyName, value);
            trace.info (MsgFormatter.format ("consumer config ''{0}'' has been set to {1}.", propertyName, value));
        }
    }

    /**
     * Mutates a single numeric property to a minimum value.
     * @param kafkaProperties  The kafka properties that get mutated
     * @param propertyName     the property name
     * @param minValue         the minimum value
     * @throws NumberFormatException
     */
    private void adjustPropertyToMin (KafkaOperatorProperties kafkaProperties, final String propertyName, final long minValue) throws NumberFormatException {
        boolean setProp = false;
        if (kafkaProperties.containsKey (propertyName)) {
            long propValue = Long.valueOf (kafkaProperties.getProperty (propertyName));
            if (propValue < minValue) {
                trace.warn (MsgFormatter.format ("consumer config ''{0}'' has been increased from {1} to {2}.",
                        propertyName, propValue, minValue));
                setProp = true;
            }
        }
        else {
            trace.info (MsgFormatter.format ("consumer config ''{0}'' has been set to {1}.",
                    propertyName, minValue));
            setProp = true;
        }
        if (setProp) {
            kafkaProperties.put (propertyName, "" + minValue);
        }
    }

    /**
     * Mutates a single numeric property to a maximum value.
     * @param kafkaProperties  The kafka properties that get mutated
     * @param propertyName     the property name
     * @param maxValue         the maximum value
     * @throws NumberFormatException
     */
    private void adjustPropertyToMax (KafkaOperatorProperties kafkaProperties, final String propertyName, final long maxValue) throws NumberFormatException {
        boolean setProp = false;
        if (kafkaProperties.containsKey (propertyName)) {
            long propValue = Long.valueOf (kafkaProperties.getProperty (propertyName));
            if (propValue > maxValue) {
                trace.warn (MsgFormatter.format ("consumer config ''{0}'' has been decreased from {1} to {2}.",
                        propertyName, propValue, maxValue));
                setProp = true;
            }
        }
        else {
            trace.info (MsgFormatter.format ("consumer config ''{0}'' has been set to {1}.",
                    propertyName, maxValue));
            setProp = true;
        }
        if (setProp) {
            kafkaProperties.put (propertyName, "" + maxValue);
        }
    }
}
