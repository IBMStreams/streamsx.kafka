package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.Base64;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.serialization.DoubleDeserializerExt;
import com.ibm.streamsx.kafka.serialization.FloatDeserializerExt;
import com.ibm.streamsx.kafka.serialization.IntegerDeserializerExt;
import com.ibm.streamsx.kafka.serialization.LongDeserializerExt;
import com.ibm.streamsx.kafka.serialization.StringDeserializerExt;

public abstract class AbstractKafkaClient {

    private static final Logger logger = Logger.getLogger(AbstractKafkaClient.class);

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
     * Get the class name of this instance.
     * @return The class name of 'this'
     */
    protected String getThisClassName() {
        return this.getClass().getName();
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
     * @return The prefix + 17 random alpha numeric characters
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
