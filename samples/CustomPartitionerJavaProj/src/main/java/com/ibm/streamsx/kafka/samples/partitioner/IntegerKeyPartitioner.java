package com.ibm.streamsx.kafka.samples.partitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/*
 * This partitioner expects the keys to be of type Integer. The value of each
 * key will be used to assign the record to a partition. 
 * 
 * For example, records with key=1 will be assigned to partition 1,
 * records with key=2 will be assigned to partition 2, etc.
 */
public class IntegerKeyPartitioner implements Partitioner {

    @Override
    public void configure (Map<String, ?> configs) {

    }

    @Override
    public int partition (String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (!(key instanceof Integer)) {
            throw new RuntimeException("The key must of type Integer. Found: " + key.getClass().getCanonicalName());
        }
        return (Integer)key;
    }

    @Override
    public void close() {
    }
}
