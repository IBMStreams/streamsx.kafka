package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * Retrieves and stores the latest offsets for
 * each partition of a single topic.
 */
public class TopicManager implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(TopicManager.class);

    private String topic;
    private transient KafkaConsumer<?, ?> offsetConsumer;
    private Map<Integer /* partition */, Long /* offset */> offsetMap;
    private List<TopicPartition> topicPartitions;
    private Set<Integer> partitions;     // must be kept consistent with topicPartitions

    /**
     * Get the list of topic partitions used for {@link #savePositionFromCluster()}.
     * These are not the partitions that have a mapping to an offset.
     * @return the topicPartitions
     * @see #getMappedPartitions()
     */
    public final List<TopicPartition> getTopicPartitions() {
        return topicPartitions;
    }

    /**
     * Sets the list of topic partitions used for {@link #savePositionFromCluster()}.
     * @param topicPartitions the topicPartitions to set
     */
    public final void setTopicPartitions (List<TopicPartition> topicPartitions) {
        Set<Integer> partitionNumbers = new HashSet<Integer>(topicPartitions.size());
        topicPartitions.forEach(tp -> partitionNumbers.add(tp.partition()));
        this.topicPartitions = topicPartitions;
        this.partitions = partitionNumbers;
    }

    /**
     * Constructs a new TopicManager.
     * @param topic     the topic
     * @param topicPartitions the topic partitions. They must be partitions of the given topic. 
     * @param offsetConsumer A consumer to retrieve the offsets of next record to consume from the broker
     */
    public <K, V> TopicManager(String topic, List<TopicPartition> topicPartitions, KafkaConsumer<K, V> offsetConsumer) {
        this.topic = topic;
        this.offsetConsumer = offsetConsumer;
        this.offsetMap = new HashMap<Integer, Long>();
        setTopicPartitions (topicPartitions);
    }

    /**
     * Sets a consumer to the Manager. This is required for {@link #savePositionFromCluster()}.
     * @param offsetConsumer A kafka consumer instance.
     */
    public <K, V> void setOffsetConsumer(KafkaConsumer<K, V> offsetConsumer) {
        this.offsetConsumer = offsetConsumer;
    }

    /**
     * Returns the offset of a given partition number.
     * @param partition the partition number
     * @return the stored offset for the partition or `null` if there is no mapping for the given partition number.  
     */
    public Long getOffset(int partition) {
        return offsetMap.get(partition);
    }

    /**
     * Tests if there is a mapping to an offset for the given partition number.
     * @param partition the partition number
     * @return `true` if the partition number is mapped to an offset, `false` otherwise.
     */
    public boolean containsPartition(int partition) {
        return offsetMap.containsKey(partition);
    }

    /**
     * Saves for the partitions given in {@link #TopicManager(String, List, KafkaConsumer)} the offsets of 
     * the next record that will be fetched (if a record with that offset exists).
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException - if no offset is currently defined for a partition.
     */
    public void savePositionFromCluster() {
        topicPartitions.forEach(part -> {
            TopicPartition tp = new TopicPartition(topic, part.partition());
            // returns 0 if a partition is empty.
            long offset = offsetConsumer.position(tp);
            logger.debug("Saving offset for next record retrieved from topic partition '" + tp + "': " + offset); //$NON-NLS-1$
            setOffset(part.partition(), offset);
        });
    }

    /**
     * Create a mapping from partition number to offset.
     * The mapping is also created if the partition number is not contained in the `topicPartitions`
     * argument of the constructor {@link #TopicManager(String, List, KafkaConsumer)}, 
     * which can be retrieved with {@link #getTopicPartitions()}.
     * @param partition the partition number
     * @param offset the offset
     */
    public void setOffset(int partition, long offset) {
        setOffset (partition, offset, false);
    }

    /**
     * Create a mapping from partition number to offset.
     * Optionally the topicPartitionList can be checked for the given partition number to ignore creation of the mapping.
     * @param partition the partition number
     * @param offset the offset
     * @param checkPartitionNumber if true, the mapping from partition number to topic is 
     *        only created when the given partition number is registered in the topicPartitions list {@link #getTopicPartitions()}.
     * @return true if the offset has been updated, false otherwise (partition number not in topicPartitions list {@link #getTopicPartitions()}.
     */
    public boolean setOffset (int partition, long offset, boolean checkPartitionNumber) {
        if (checkPartitionNumber) {
            if (!partitions.contains (partition)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Update offset ignored (partition not in partition list): topic=" + topic + ", partition=" + partition + ", newOffset=" + offset);
                }
                return false;
            }
        }
        offsetMap.put(partition, offset);
        if (logger.isTraceEnabled()) {
            logger.trace("Updated offset: topic=" + topic + ", partition=" + partition + ", newOffset=" + offset);
        }
        return true;
    }

    /**
     * Returns the partition numbers that have a mapping to an offset.
     * @return a Set that contains the partition numbers
     */
    public Set<Integer> getMappedPartitions() {
        return offsetMap.keySet();
    }

    /**
     * Tests if there are mappings from partition number to offset. Equivalent to getPartitions().size() == 0.
     * @return true if there are no mappings, false otherwise
     */
    public boolean isEmpty() {
        return offsetMap.isEmpty();
    }
    /**
     * Removes the mapping from partition to offset and updates the partitions list.
     * @param partition  The partition number to be removed
     * @return true if there was such a mapping, false otherwise
     */
    public boolean remove (int partition) {
        topicPartitions.remove (new TopicPartition (this.topic, partition));
        partitions.remove (partition);
        return offsetMap.remove(partition) != null;
    }

    @Override
    public String toString() {
        return "TopicManager [topic=" + topic + ", offsetMap=" + offsetMap + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}
