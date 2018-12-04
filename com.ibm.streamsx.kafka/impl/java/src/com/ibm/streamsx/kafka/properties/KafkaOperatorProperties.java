package com.ibm.streamsx.kafka.properties;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaOperatorProperties extends Properties {
    private static final long serialVersionUID = 1L;

    public KafkaOperatorProperties() {
        this(new Properties());
    }

    public KafkaOperatorProperties(Properties properties) {
        super(properties);
    }

    public void putIfNotPresent(Object key, Object value) {
        if (!containsKey(key)) {
            put(key, value);
        }
    }

    /**
     * Merges the properties with the given properties in such a way that only those properties from the given set of properties are merged, which are not present in this property instance.
     * @param properties The properties, from which the unknown properties are taken over.
     */
    public void putAllIfNotPresent(Properties properties) {
        if (properties == null)
            return;

        for (java.util.Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (!containsKey(entry.getKey())) {
                put(entry.getKey(), entry.getValue());
            }
        }
    }

    /*
     * Convenience methods
     */
    /**
     * Gets the 'buffer.memory' config or the default value of 33554432
     * @return the total buffer memory in bytes
     */
    public long getBufferMemory() {
        return Long.parseLong (getProperty (ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432").trim());
    }
    
    public String getBootstrapServers() {
        return getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    public String getJaasConfig() {
        return getProperty(JaasUtil.SASL_JAAS_PROPERTY);
    }

    public String getKeySerializer() {
        return getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    }

    public String getValueSerializer() {
        return getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    }

    public String getKeyDeserializer() {
        return getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    }

    public String getValueDeserializer() {
        return getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    }
}
