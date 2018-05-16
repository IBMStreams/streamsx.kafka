package com.ibm.streamsx.kafka.clients;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
/**
 * This class manages the offsets for partitions per topic.
 */
public class OffsetManager implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(OffsetManager.class);

    private Map<String /* topic */, TopicManager> managerMap;
    private transient KafkaConsumer<?, ?> offsetConsumer;

    public <K, V> OffsetManager(KafkaConsumer<K, V> offsetConsumer) {
        this.managerMap = Collections.synchronizedMap(new HashMap<String, TopicManager>());
        this.offsetConsumer = offsetConsumer;
    }

    /**
     * Does not work. Member variable 'offsetConsumer' is not serialized and is null after deserialization.
     * After Deserialization {@link #setOffsetConsumer(KafkaConsumer)} must be called.
     * @return `this`
     * @throws ObjectStreamException
     */
    private Object readResolve() throws ObjectStreamException {
        managerMap.values().forEach(mgr -> mgr.setOffsetConsumer(offsetConsumer));

        return this;
    }

    /**
     * Sets a consumer to the Manager. This is required for {@link #savePositionFromCluster()}.
     * @param offsetConsumer A kafka consumer instance.
     */
    public void setOffsetConsumer(KafkaConsumer<?, ?> offsetConsumer) {
        this.offsetConsumer = offsetConsumer;
        managerMap.values().forEach(tm -> tm.setOffsetConsumer(offsetConsumer));
    }

    /**
     * Tests whether there is a mapping from a topic to the secondary mapping partition-to-offset.
     * @param topic the topic to test
     * @return `true` if there is a mapping, `false` otherwise.
     */
    public boolean hasTopic(String topic) {
        return managerMap.containsKey(topic);
    }

    /**
     * Adds the topic to the offsetManager if it does not already exist.
     * It does not yet create secondary mappings from the given partitions to offsets.
     * 
     * @param topic the topic
     * @param topicPartitions the partitions to be included into the secondary mapping.
     *        It is assumed that the partitions in the list belong only to the given topic.
     */
    public void addTopic(String topic, List<TopicPartition> topicPartitions) {
        TopicManager tm = new TopicManager(topic, topicPartitions, offsetConsumer);
        TopicManager previousValue = managerMap.putIfAbsent(topic, tm);
        if (previousValue == null /* new topic added */) {
            logger.debug("Added topic: " + topic); //$NON-NLS-1$
        }
    }

    /**
     * Saves for every topic and partition given in {@link #addTopic(String, List)} the offsets of 
     * the next record that will be fetched (if a record with that offset exists).
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException - if no offset is currently defined for a partition.
     *         TODO: in this case -1 should be set or no secondary mapping be created. -- must check this with {@link KafkaConsumerClient#refreshFromCluster()}
     */
    public void savePositionFromCluster() {
        for (Entry<String, TopicManager> entry : managerMap.entrySet()) {
            entry.getValue().savePositionFromCluster();
        }
    }


    /**
     * Saves the offset for a given partition and topic.
     * The partition-to-offset mapping is also created for partition numbers not included in 
     * the `topicPartitions` argument of {@link #addTopic(String, List)}.
     * The topic must have been created with {@link #addTopic(String, List)} before.
     * Same implementation as {@link #setOffset(String, int, long)} except that this one throws Exception.
     * @param topic      the topic
     * @param partition  the partition number within the topic
     * @param offset     the offset
     * @throws Exception the topic has not been added before.
     */
    public void savePosition(String topic, int partition, long offset) throws Exception {
    	TopicManager topicManager = managerMap.get(topic);
    	if(topicManager == null) {
    		throw new Exception("TopicManager does not exist for topic: " + topic);
    	}
    	
    	topicManager.setOffset(partition, offset);
    }
    
    /**
     * Returns the topics that have been added with {@link #addTopic(String, List)}.
     * @return The list of topics, which are the primary keys
     */
    public List<String> getTopics() {
        return new ArrayList<>(managerMap.keySet());
    }

    /**
     * Saves the offset for a given partition and topic.
     * The partition-to-offset mapping is also created for partition numbers not included in 
     * the `topicPartitions` argument of {@link #addTopic(String, List)}.
     * If the topic has not been created with {@link #addTopic(String, List)} before, a NullPointerException is thrown.
     * @param topic      the topic
     * @param partition  the partition number within the topic
     * @param offset     the offset
     * @see #savePosition(String, int, long)
     */
    public void setOffset(String topic, int partition, long offset) {
        managerMap.get(topic).setOffset(partition, offset);
    }

    /**
     * Returns the offset for a given topic and partition number.
     * The topic must have been added before with {@link #addTopic(String, List)}. Otherwise a NullPointerException is thrown.
     * @param topic     the topic 
     * @param partition the partition number
     * @return The offset or -1 if there is no mapping from partition number to topic.
     */
    public long getOffset(String topic, int partition) {
    	TopicManager topicManager = managerMap.get(topic);
    	Long offset = topicManager.containsPartition(partition) ? topicManager.getOffset(partition) : -1l;
        return offset;
    }

    @Override
    public String toString() {
        return "OffsetManager [managerMap=" + managerMap + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
}
