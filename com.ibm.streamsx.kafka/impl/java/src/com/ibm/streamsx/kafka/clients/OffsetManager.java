package com.ibm.streamsx.kafka.clients;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class OffsetManager implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(OffsetManager.class);

    private Map<String /* topic */, TopicManager> managerMap;
    private transient KafkaConsumer<?, ?> offsetConsumer;

    public <K, V> OffsetManager(KafkaConsumer<K, V> offsetConsumer) {
        this.managerMap = Collections.synchronizedMap(new HashMap<String, TopicManager>());
        this.offsetConsumer = offsetConsumer;
    }

    private Object readResolve() throws ObjectStreamException {
        managerMap.values().forEach(mgr -> mgr.setOffsetConsumer(offsetConsumer));

        return this;
    }

    public void setOffsetConsumer(KafkaConsumer<?, ?> offsetConsumer) {
        this.offsetConsumer = offsetConsumer;
        managerMap.values().forEach(tm -> tm.setOffsetConsumer(offsetConsumer));
    }

    public boolean hasTopic(String topic) {
        return managerMap.containsKey(topic);
    }

    /*
     * Adds the topic to the offsetManager if it does not already exist.
     */
    public void addTopic(String topic, boolean forceUpdateOffsets) {
        TopicManager tm = new TopicManager(topic, offsetConsumer);
        TopicManager previousValue = managerMap.putIfAbsent(topic, tm);
        if (previousValue == null /* new topic added */) {
            if (forceUpdateOffsets)
                tm.saveEndOffsetsFromCluster();
            logger.debug("Added topic: " + topic); //$NON-NLS-1$
        }
    }

    public void saveEndOffsetsFromCluster() {
        for (Entry<String, TopicManager> entry : managerMap.entrySet()) {
            entry.getValue().saveEndOffsetsFromCluster();
        }
    }

    public void savePositionFromCluster() {
        for (Entry<String, TopicManager> entry : managerMap.entrySet()) {
            entry.getValue().savePositionFromCluster();
        }
    }

    public List<String> getTopics() {
        return new ArrayList<>(managerMap.keySet());
    }

    public void setOffset(String topic, int partition, long offset) {
        managerMap.get(topic).setOffset(partition, offset);
    }

    public long getOffset(String topic, int partition) {
        return managerMap.get(topic).getOffset(partition);
    }

    public Collection<TopicPartition> getTopicPartitions(String topic) {
        List<TopicPartition> tps = new ArrayList<TopicPartition>();
        List<PartitionInfo> partitionInfo = managerMap.get(topic).getPartitionInfo();
        for (PartitionInfo info : partitionInfo) {
            tps.add(new TopicPartition(topic, info.partition()));
        }

        return tps;
    }

    @Override
    public String toString() {
        return "OffsetManager [managerMap=" + managerMap + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}
