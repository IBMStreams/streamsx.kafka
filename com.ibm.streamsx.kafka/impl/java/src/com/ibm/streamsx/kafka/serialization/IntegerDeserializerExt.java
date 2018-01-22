package com.ibm.streamsx.kafka.serialization;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.log4j.Logger;

/**
 * This class extends the `org.apache.kafka.common.serialization.IntegerDeserializer` 
 * so that its {@link #deserialize(String, byte[])} function does not throw a `SerializationException` in case of malformed data.
 * The Kafka client requires that the class has an argument-less public constructor.
 *
 * @since Toolkit v1.2.3
 */
public class IntegerDeserializerExt extends IntegerDeserializer {

    private static final Logger tracer = Logger.getLogger(IntegerDeserializerExt.class);
    /**
     * Constructs a new IntegerDeserializerExt. The class must have an argument-less constructor.
     */
    public IntegerDeserializerExt() {
        super();
    }

    /**
     * De-serializes an byte array into a value object of `java.lang.Integer` type by calling the `deserialize` method of the super class.
     * If the data cannot be deserialized, the SerializationException is caught, and `null` is returned.
     * 
     * @param topic  the topic
     * @param data   the serialized data
     * 
     * @return The value object or `null` if the data cannot be deserialized
     * 
     * @see org.apache.kafka.common.serialization.IntegerDeserializer#deserialize(java.lang.String, byte[])
     */
    @Override
    public Integer deserialize (String topic, byte[] data) {
        try {
            return super.deserialize (topic, data);
        }
        catch (org.apache.kafka.common.errors.SerializationException e) {
            tracer.error ("failed to deserialize data into Integer value from topic '" + topic + "': " + e.getLocalizedMessage(), e);
            return null;
        }
    }
}
