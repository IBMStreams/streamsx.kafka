package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

    /**
     * Constructs a new OffsetManager without a consumer.
     * The consumer must be set later using {@link #setOffsetConsumer(KafkaConsumer)}.
     */
    public <K, V> OffsetManager() {
        this (null);
    }

    /**
     * Constructs a new OffsetManager with a consumer for using method {@link #savePositionFromCluster()}
     * 
     * @param offsetConsumer the consumer 
     */
    public <K, V> OffsetManager(KafkaConsumer<K, V> offsetConsumer) {
        this.managerMap = Collections.synchronizedMap(new HashMap<String, TopicManager>());
        this.offsetConsumer = offsetConsumer;
    }
    //
    //    /**
    //     * Does not work. Member variable 'offsetConsumer' is not serialized and is null after deserialization.
    //     * After Deserialization {@link #setOffsetConsumer(KafkaConsumer)} must be called.
    //     * @return `this`
    //     * @throws ObjectStreamException
    //     */
    //    private Object readResolve() throws ObjectStreamException {
    //        managerMap.values().forEach(mgr -> mgr.setOffsetConsumer(offsetConsumer));
    //
    //        return this;
    //    }

    /**
     * Sets a consumer to the Manager. This is required for {@link #savePositionFromCluster()}.
     * @param offsetConsumer A kafka consumer instance.
     */
    public void setOffsetConsumer(KafkaConsumer<?, ?> offsetConsumer) {
        logger.info ("setOffsetConsumer(): offsetConsumer = " + offsetConsumer);
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
     * Removes a partition from the offset manager.
     * When the last partition is removed from a topic, the topic is finally removed.
     * @param topic       the topic
     * @param partition   the partition number from the topic
     */
    public void remove (String topic, int partition) {
        if (!managerMap.containsKey (topic)) return;
        TopicManager tm = managerMap.get (topic);
        tm.remove (partition);
        if (tm.isEmpty()) managerMap.remove(topic);
    }

    /**
     * Removes all topics and partitions from the offset manager
     */
    public void clear() {
        logger.debug("clear() - removing all mappings"); //$NON-NLS-1$
        managerMap.clear();
    }

    /**
     * Adds the topic to the offsetManager if it does not already exist.
     * It does not yet create secondary mappings from the given partitions to offsets.
     * 
     * @param topic the topic
     * @param topicPartitions the partitions to be included into the secondary mapping.
     *        It is assumed that the partitions in the list belong only to the given topic.
     * @see #addTopics(Collection)
     */
    public void addTopic(String topic, List<TopicPartition> topicPartitions) {
        TopicManager tm = new TopicManager(topic, topicPartitions, offsetConsumer);
        TopicManager previousValue = managerMap.putIfAbsent(topic, tm);
        if (previousValue == null /* new topic added */) {
            logger.debug("Added topic: " + topic); //$NON-NLS-1$
        }
        else logger.debug("topic exists (not added): " + topic); //$NON-NLS-1$
    }

    /**
     * Adds topics to the offset manager. The topics and their partitions are given as a Collection of topic partitions.
     * Topics are not added or overwritten when they already exist in the offset manager.
     * @param topicPartitions The topic partitions
     * @see #addTopic(String, List)
     * @see #updateTopics(Collection)
     */
    public void addTopics (Collection <TopicPartition> topicPartitions) {
        // create a map from the flat list
        Map<String /* topic */, List<TopicPartition>> topicPartitionMap = new HashMap<>();
        topicPartitions.forEach(tp -> {
            if(!topicPartitionMap.containsKey(tp.topic())) {
                topicPartitionMap.put(tp.topic(), new ArrayList<>());
            }
            topicPartitionMap.get(tp.topic()).add(tp);
        });
        topicPartitionMap.forEach((topic, tpList) -> this.addTopic (topic, tpList));
    }

    /** 
     * updates all topics in the offset manager from the given collection of topic partitions.
     * Topics that are not yet present are created like they would be created by {@link #addTopics(Collection)}.
     * Topics that are already created get updated with the list of partitions for each topic,
     * so that {@link #savePositionFromCluster()} can be called by using the updated topic partitions.
     * @param topicPartitions The topic partitions
     */
    public void updateTopics (Collection <TopicPartition> topicPartitions) {
        // create a map from the flat list
        Map<String /* topic */, List<TopicPartition>> topicPartitionMap = new HashMap<>();
        topicPartitions.forEach(tp -> {
            if(!topicPartitionMap.containsKey(tp.topic())) {
                topicPartitionMap.put(tp.topic(), new ArrayList<>());
            }
            topicPartitionMap.get(tp.topic()).add(tp);
        });

        topicPartitionMap.forEach((topic, tpList) -> {
            TopicManager tm = managerMap.get(topic);
            if (tm == null) {
                addTopic (topic, tpList);
            }
            else {
                tm.setTopicPartitions (tpList);
            }
        });
    }

    /**
     * Saves for every topic and partition given in {@link #addTopic(String, List)}, 
     * {@link #addTopics(Collection)}, or {@link #updateTopics(Collection)} the offsets of 
     * the next record that will be fetched (if a record with that offset exists).
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
     * @param autoCreateTopic if true, the topic is automatically created with an empty partitions list in the offset manager
     * @throws Exception the topic has not been added before and autoCreateTopic is false.
     */
    public void savePosition(String topic, int partition, long offset, boolean autoCreateTopic) throws Exception {
        TopicManager topicManager = managerMap.get(topic);
        if(topicManager == null) {
            if (!autoCreateTopic)
                throw new Exception("TopicManager does not exist for topic: " + topic);
            topicManager = new TopicManager(topic, Collections.emptyList(), offsetConsumer);
            managerMap.put (topic, topicManager);
        }
        topicManager.setOffset(partition, offset);
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
     * @see #savePosition(String, int, long, boolean)
     */
    public void savePosition(String topic, int partition, long offset) throws Exception {
        savePosition(topic, partition, offset, /*autoCreateTopic=*/false);
    }

    /**
     * Saves the offset for a given partition and topic. The mapping is not created for partitions not registered for the topic.
     * @param topic      the topic
     * @param partition  the partition number within the topic
     * @param offset     the offset
     * @throws Exception the topic has not been added before or the partition number is not registered for the topic.
     */
    public void savePositionWhenRegistered (String topic, int partition, long offset) throws Exception {
        TopicManager topicManager = managerMap.get(topic);
        if(topicManager == null) {
            throw new Exception("TopicManager does not exist for topic: " + topic);
        }
        boolean offsetSaved = topicManager.setOffset(partition, offset, /*checkPartitionNumber=*/true);
        if (!offsetSaved)
            throw new Exception("TopicManager does not know partition number " + partition + "; known partitions = " + topicManager.getTopicPartitions());
    }

    /**
     * Returns the topics that have been added with {@link #addTopic(String, List)}.
     * @return The list of topics, which are the primary keys
     */
    public List<String> getTopics() {
        return new ArrayList<>(managerMap.keySet());
    }

    /**
     * Creates a Set of TopicPartition instances for all topics and partitions that have a mapping to an offset.
     * @return A new set of topic partitions. This set is mutable without side effects to the topic manager.
     */
    public Set<TopicPartition> getMappedTopicPartitions() {
        Set<TopicPartition> result = new HashSet<>();
        for (TopicManager tm: managerMap.values()) {
            String topic = tm.getTopic();
            for (Integer partition: tm.getMappedPartitions()) {
                result.add(new TopicPartition (topic, partition));
            }
        }
        return result;
    }

    /**
     * Returns the number of mappings from topics and partitions to offsets.
     * This call is logically identical to {@link #getMappedTopicPartitions()}.size(), nut of lower costs.
     * 
     * @return the number of mappings.
     */
    public int size() {
        int s = 0;
        for (TopicManager tm: managerMap.values()) {
            s += tm.size();
        }
        return s;
    }

    /**
     * Returns true if this OffsetManager contains no mappings from topics and partitions to offsets.
     * This call is logically equivalent to {@link #getMappedTopicPartitions()}.isEmpty(), but at lower costs.
     * @return true if this OffsetManager
     */
    public boolean isEmpty() {
        if (managerMap.isEmpty()) return true;
        for (String topic: managerMap.keySet()) {
            if (!managerMap.get (topic).isEmpty()) {
                return false;
            }
        }
        // all topic managers tested for emptiness - no one contained partition - offset mappings 
        return true;
    }

    /**
     * Creates a Set of partition numbers for a given topic.
     * These are the registered partition numbers registered via {@link #addTopic(String, List)},
     * {@link #addTopics(Collection)}, or {@link #updateTopics(Collection)}, not those, which have a mapping to an offset.
     * @return A new set of topic partitions numbers for the given topic. This set is mutable without side effects to the topic manager.
     */
    public Set<Integer> getRegisteredPartitionNumbers (String topic) {
        Set<Integer> result = new HashSet<>();
        TopicManager tm = managerMap.get (topic);
        if (tm != null) {
            tm.getTopicPartitions().forEach (tp -> result.add (tp.partition()));
        }
        return result;
    }


    /**
     * Saves the offset for a given partition and topic.
     * The partition-to-offset mapping is also created for partition numbers not included in 
     * the `topicPartitions` argument of {@link #addTopic(String, List)}.
     * If the topic has not been created with {@link #addTopic(String, List)} before, it is created with an empty partition list.
     * @param topic      the topic
     * @param partition  the partition number within the topic
     * @param offset     the offset
     * @see #savePosition(String, int, long)
     */
    public void setOffset(String topic, int partition, long offset) {
        if (managerMap.containsKey (topic))
            addTopic(topic, Collections.emptyList());
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

    /**
     * Adds the offsets from the given OffsetManager replacing potentially existing offsets for topic partitions.
     * @param ofsm The source offset manager
     */
    public void putOffsets (OffsetManager ofsm) {
        final Set<TopicPartition> ofsmMappedTopicPartitions = ofsm.getMappedTopicPartitions();
        Set<TopicPartition> mappedTopicPartitions = this.getMappedTopicPartitions();
        mappedTopicPartitions.addAll (ofsmMappedTopicPartitions);
        this.updateTopics (mappedTopicPartitions);
        for (TopicPartition tp: ofsmMappedTopicPartitions) {
            final String topic = tp.topic();
            final int partition = tp.partition();
            final long offset = ofsm.getOffset (topic, partition);
            this.setOffset (topic, partition, offset);
        }
    }
}
