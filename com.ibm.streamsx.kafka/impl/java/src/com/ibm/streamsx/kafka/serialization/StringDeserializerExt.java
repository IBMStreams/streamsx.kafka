package com.ibm.streamsx.kafka.serialization;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

/**
 * This class extends the `org.apache.kafka.common.serialization.StringDeserializer` 
 * so that its {@link #deserialize(String, byte[])} function does not throw a `SerializationException` in case of malformed data.
 * The Kafka client requires that the class has an argument-less public constructor.
 *
 * @since Toolkit v1.2.3
 */
public class StringDeserializerExt extends StringDeserializer {

    private static final Logger tracer = Logger.getLogger(StringDeserializerExt.class);
    /**
     * Constructs a new StringDeserializerExt. The class must have an argument-less constructor.
     */
    public StringDeserializerExt() {
        super();
    }

    /**
     * De-serializes an byte array into a value object of `java.lang.String` type by calling the `deserialize` method of the super class.
     * If the data cannot be deserialized, the SerializationException is caught, and `null` is returned.
     * 
     * @param topic  the topic
     * @param data   the serialized data
     * 
     * @return The value object or `null` if the data cannot be deserialized
     * 
     * @see org.apache.kafka.common.serialization.StringDeserializer#deserialize(java.lang.String, byte[])
     */
    @Override
    public String deserialize (String topic, byte[] data) {
        try {
            return super.deserialize (topic, data);
        }
        catch (org.apache.kafka.common.errors.SerializationException e) {
            tracer.error ("failed to deserialize data into String value from topic '" + topic + "': " + e.getLocalizedMessage(), e);
            return null;
        }
    }
}
