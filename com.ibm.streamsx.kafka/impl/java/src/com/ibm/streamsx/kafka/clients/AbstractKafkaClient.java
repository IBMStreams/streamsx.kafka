package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.Base64;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.MsgFormatter;
import com.ibm.streamsx.kafka.SystemProperties;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;
import com.ibm.streamsx.kafka.serialization.DoubleDeserializerExt;
import com.ibm.streamsx.kafka.serialization.FloatDeserializerExt;
import com.ibm.streamsx.kafka.serialization.IntegerDeserializerExt;
import com.ibm.streamsx.kafka.serialization.LongDeserializerExt;
import com.ibm.streamsx.kafka.serialization.StringDeserializerExt;

public abstract class AbstractKafkaClient {

    private static final Logger logger = Logger.getLogger(AbstractKafkaClient.class);
    protected static final long METRICS_REPORT_INTERVAL = 2_000;
    protected static final Level DEBUG_LEVEL = SystemProperties.getDebugLevelOverride();
    protected static final Level DEBUG_LEVEL_METRICS = SystemProperties.getDebugLevelMetricsOverride();

    /**
     * when set to true, following properties are set when unset:
     * <ul>
     * <li>reconnect.backoff.max.ms = 10000</li>
     * <li>reconnect.backoff.ms = 250</li>
     * <li>retry.backoff.ms = 500</li>
     * </ul>
     * @param SLOW_RECONNECT the slowReconnect to set
     */
    private static final boolean SLOW_RECONNECT = true;

    private final String clientId;
    private final boolean clientIdGenerated;
    private final OperatorContext operatorContext;
    private final ControlPlaneContext jcpContext;


    /**
     * Constructs a new AbstractKafkaClient using Kafka properties
     * @param operatorContext the operator context
     * @param kafkaProperties the kafka properties - modifies a bunch of properties
     * @param isConsumer use true, if this is a consumer client, false otherwise
     */
    public AbstractKafkaClient (OperatorContext operatorContext, KafkaOperatorProperties kafkaProperties, boolean isConsumer) {

        this.operatorContext = operatorContext;
        logger.info ("instantiating client: " + getThisClassName());
        this.jcpContext = operatorContext.getOptionalContext (ControlPlaneContext.class);
        // Create a unique client ID for the consumer if one is not specified or add the UDP channel when specified and in UDP
        // This is important, otherwise running multiple consumers from the same
        // application will result in a KafkaException when registering the client
        final String clientIdConfig = isConsumer? ConsumerConfig.CLIENT_ID_CONFIG: ProducerConfig.CLIENT_ID_CONFIG;
        if (!kafkaProperties.containsKey (clientIdConfig)) {
            this.clientId = MsgFormatter.format ("{0}-J{1}-{2}",
                    (isConsumer? "C": "P"), "" + operatorContext.getPE().getJobId(), operatorContext.getName());
            logger.info("generated client.id: " + this.clientId);
            clientIdGenerated = true;
        }
        else {
            clientIdGenerated = false;
            int udpChannel = operatorContext.getChannel();
            if (udpChannel >= 0) {
                // we are in a parallel region
                this.clientId = kafkaProperties.getProperty (clientIdConfig) + "-" + udpChannel;
                logger.warn ("Operator in parallel region detected. modified client.id: " + this.clientId);
            }
            else {
                this.clientId = kafkaProperties.getProperty (clientIdConfig);
            }
        }
        kafkaProperties.put (clientIdConfig, this.clientId);
        kafkaProperties.expandApplicationDirectory (operatorContext.getPE().getApplicationDirectory().getAbsolutePath());
        final String clientDnsLookup = isConsumer? ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG: ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG;
        if (!kafkaProperties.containsKey (clientDnsLookup)) kafkaProperties.put (clientDnsLookup, "use_all_dns_ips");
        if (SLOW_RECONNECT) {
            final String reconnectBackoffMaxMs = isConsumer? ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG: ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
            final String reconnectBackoffMs = isConsumer? ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG: ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG;
            final String retryBackoffMs = isConsumer? ConsumerConfig.RETRY_BACKOFF_MS_CONFIG: ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
            if (!kafkaProperties.containsKey (reconnectBackoffMaxMs)) kafkaProperties.put (reconnectBackoffMaxMs, "10000");
            if (!kafkaProperties.containsKey (reconnectBackoffMs)) kafkaProperties.put (reconnectBackoffMs, "250");
            if (!kafkaProperties.containsKey (retryBackoffMs)) kafkaProperties.put (retryBackoffMs, "500");
        }
    }


    /**
     * returns the operator context.
     * @return the operator context
     */
    public final OperatorContext getOperatorContext() {
        return operatorContext;
    }

    /**
     * Returns the ControlPlaneContext if there is one.<br>
     * 
     * <b>Note:</b> There is always a ControlPlaneContext - whether there is a JobControlPlane operator in the application graph or not.
     *       Future implementations of IBM Streams may change this. Therefore it is a good practice to check the returned value for null.
     * @return the Control Plane Context
     */
    public final ControlPlaneContext getJcpContext() {
        return jcpContext;
    }

    /**
     * @return the clientId (client.id) of the Kafka client
     */
    public String getClientId() {
        return clientId;
    }


    /**
     * Returns true if the client ID has a random generated value, false otherwise
     * @return the clientIdGenerated
     */
    public boolean isClientIdGenerated() {
        return clientIdGenerated;
    }


    /**
     * Get the class name of this instance.
     * @return The class name of 'this'
     */
    public String getThisClassName() {
        return this.getClass().getName();
    }


    public static <T> String getSerializer(Class<T> clazz) throws KafkaConfigurationException {
        if (clazz == null) throw new KafkaConfigurationException ("Unable to find serializer for 'null'");
        if (clazz.equals(String.class) || clazz.equals(RString.class)) {
            return StringSerializer.class.getCanonicalName();
        } else if (clazz.equals(Long.class)) {
            return LongSerializer.class.getCanonicalName();
        } else if (clazz.equals(Float.class)) {
            return FloatSerializer.class.getCanonicalName();
        } else if (clazz.equals(Double.class)) {
            return DoubleSerializer.class.getCanonicalName();
        } else if (clazz.equals(Blob.class)) {
            return ByteArraySerializer.class.getCanonicalName();
        } else if (clazz.equals(Integer.class)) {
            return IntegerSerializer.class.getCanonicalName();
        } else {
            throw new KafkaConfigurationException("Unable to find serializer for: " + clazz.toString()); //$NON-NLS-1$
        }
    }

    public static String inferDeserializerFromSerializer(String serializerClassName) throws KafkaConfigurationException {
        if (serializerClassName == null) throw new KafkaConfigurationException ("Unable to infer deserializer from serializer 'null'");
        if (serializerClassName.equals(StringSerializer.class.getCanonicalName())) {
            return StringDeserializerExt.class.getCanonicalName();
        } else if (serializerClassName.equals(LongSerializer.class.getCanonicalName())) {
            return LongDeserializerExt.class.getCanonicalName();
        } else if (serializerClassName.equals(FloatSerializer.class.getCanonicalName())) {
            return FloatDeserializerExt.class.getCanonicalName();
        } else if (serializerClassName.equals(DoubleSerializer.class.getCanonicalName())) {
            return DoubleDeserializerExt.class.getCanonicalName();
        } else if (serializerClassName.equals(ByteArraySerializer.class.getCanonicalName())) {
            return ByteArrayDeserializer.class.getCanonicalName();
        } else if (serializerClassName.equals(IntegerSerializer.class.getCanonicalName())) {
            return IntegerDeserializerExt.class.getCanonicalName();
        } else {
            throw new KafkaConfigurationException("Unable to infer deserializer from serializer: " + serializerClassName); //$NON-NLS-1$
        }
    }

    /**
     * Gets the Built-In or Kafka provided Deserializer for types that are used as mapped attribute types, like String, com.ibm.streams.operator.types.RString,
     * Long, Float, Double, com.ibm.streams.operator.types.Blob
     *
     * @param clazz  The class for which the serializer is determined. This is a value or key class. 
     * @return The class name of a deserializer
     * @throws KafkaConfigurationException No matching deserializer found
     */
    public static <T> String getDeserializer(Class<T> clazz) throws KafkaConfigurationException {
        if (clazz == null) throw new KafkaConfigurationException ("Unable to find deserializer for 'null'");
        if (clazz.equals(String.class) || clazz.equals(RString.class)) {
            return StringDeserializerExt.class.getCanonicalName();
        } else if (clazz.equals(Long.class)) {
            return LongDeserializerExt.class.getCanonicalName();
        } else if (clazz.equals(Float.class)) {
            return FloatDeserializerExt.class.getCanonicalName();
        } else if (clazz.equals(Double.class)) {
            return DoubleDeserializerExt.class.getCanonicalName();
        } else if (clazz.equals(Blob.class)) {
            return ByteArrayDeserializer.class.getCanonicalName();
        } else if (clazz.equals(Integer.class)) {
            return IntegerDeserializerExt.class.getCanonicalName();
        } else {
            throw new KafkaConfigurationException("Unable to find deserializer for: " + clazz.toString()); //$NON-NLS-1$
        }
    }


    /**
     * Serializes a Serializable to a base64 encoded String
     * @param obj The object to be serialized
     * @return a base64 encoded String that represents the serialized object
     */
    protected static String serializeObject(Serializable obj) {
        return new String(Base64.getEncoder().encode(SerializationUtils.serialize(obj)));
    }

    /**
     * Creates a random String that can contain an optional prefix.
     * The length of the random part is 17 characters.
     * @param prefix A prefix. Can be null or empty if a prefix is not needed.
     * @param randomLength the length of the random part
     * @return The prefix + n random alpha numeric characters, where n is randomLength
     */
    protected static String getRandomId (String prefix, int randomLength) {
        String random = RandomStringUtils.randomAlphanumeric(randomLength);
        String id = prefix == null? random: prefix + random;
        logger.debug("Random id=" + id); //$NON-NLS-1$
        return id;
    }

    /**
     * Creates a random String that can contain an optional prefix.
     * The length of the random part is 17 characters.
     * @param prefix A prefix. Can be null or empty if a prefix is not needed.
     * @return The prefix + 17 random alpha numeric characters
     */
    protected static String getRandomId (String prefix) {
        return getRandomId (prefix, 17);
    }
}
