package com.ibm.streamsx.kafka.serialization;

import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.log4j.Logger;

/**
 * This class extends the `org.apache.kafka.common.serialization.DoubleDeserializer` 
 * so that its {@link #deserialize(String, byte[])} function does not throw a `SerializationException` in case of malformed data.
 * The Kafka client requires that the class has an argument-less public constructor.
 *
 * @since Toolkit v1.2.3
 */
public class DoubleDeserializerExt extends DoubleDeserializer {

    private static final Logger tracer = Logger.getLogger(DoubleDeserializerExt.class);
    /**
     * Constructs a new DoubleDeserializerExt. The class must have an argument-less constructor.
     */
    public DoubleDeserializerExt() {
        super();
    }

    /**
     * De-serializes an byte array into a value object of `java.lang.Double` type by calling the `deserialize` method of the super class.
     * If the data cannot be deserialized, the SerializationException is caught, and `null` is returned.
     * 
     * @param topic  the topic
     * @param data   the serialized data
     * 
     * @return The value object or `null` if the data cannot be deserialized
     * 
     * @see org.apache.kafka.common.serialization.DoubleDeserializer#deserialize(java.lang.String, byte[])
     */
    @Override
    public Double deserialize (String topic, byte[] data) {
        try {
            return super.deserialize (topic, data);
        }
        catch (org.apache.kafka.common.errors.SerializationException e) {
            tracer.error ("failed to deserialize data into Double value from topic '" + topic + "': " + e.getLocalizedMessage(), e);
            return null;
        }
    }
}
