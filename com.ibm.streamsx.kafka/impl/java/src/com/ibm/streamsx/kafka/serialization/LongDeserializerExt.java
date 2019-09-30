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
package com.ibm.streamsx.kafka.serialization;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.log4j.Logger;

/**
 * This class extends the `org.apache.kafka.common.serialization.LongDeserializer` 
 * so that its {@link #deserialize(String, byte[])} function does not throw a `SerializationException` in case of malformed data.
 * The Kafka client requires that the class has an argument-less public constructor.
 *
 * @since Toolkit v1.2.3
 */
public class LongDeserializerExt extends LongDeserializer {

    private static final Logger tracer = Logger.getLogger(LongDeserializerExt.class);
    /**
     * Constructs a new LongDeserializerExt. The class must have an argument-less constructor.
     */
    public LongDeserializerExt() {
        super();
    }

    /**
     * De-serializes an byte array into a value object of `java.lang.Long` type by calling the `deserialize` method of the super class.
     * If the data cannot be deserialized, the SerializationException is caught, and `null` is returned.
     * 
     * @param topic  the topic
     * @param data   the serialized data
     * 
     * @return The value object or `null` if the data cannot be deserialized
     * 
     * @see org.apache.kafka.common.serialization.LongDeserializer#deserialize(java.lang.String, byte[])
     */
    @Override
    public Long deserialize (String topic, byte[] data) {
        try {
            return super.deserialize (topic, data);
        }
        catch (org.apache.kafka.common.errors.SerializationException e) {
            tracer.error ("failed to deserialize data into Long value from topic '" + topic + "': " + e.getLocalizedMessage(), e);
            return null;
        }
    }
}
