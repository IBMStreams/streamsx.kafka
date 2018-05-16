package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	/**
	 * Constructs a new TopicManager.
	 * @param topic     the topic
	 * @param topicPartitions the topic partitions. They must be partitions of the given topic. 
	 * @param offsetConsumer A consumer to retrieve the offsets of next record to consume from the broker
	 */
    public <K, V> TopicManager(String topic, List<TopicPartition> topicPartitions, KafkaConsumer<K, V> offsetConsumer) {
        this.topic = topic;
        this.topicPartitions = topicPartitions;
        this.offsetConsumer = offsetConsumer;
        this.offsetMap = new HashMap<Integer, Long>();
    }

    /**
     * Sets a consumer to the Manager. This is required for {@link #savePositionFromCluster()}.
     * @param offsetConsumer A kafka consumer instance.
     */
    public <K, V> void setOffsetConsumer(KafkaConsumer<K, V> offsetConsumer) {
        this.offsetConsumer = offsetConsumer;
    }

    /**
     * returns the offset of a given partition number.
     * @param partition the partition number
     * @return the stored offset for the partition or `null` if there is no mapping for the given partition number.  
     */
    public Long getOffset(int partition) {
        return offsetMap.get(partition);
    }

    /**
     * Tests if there is a mapping for the given partition number.
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
     *         TODO: in this case no mapping should be created. -- must check this with {@link KafkaConsumerClient#refreshFromCluster()}
     */
    public void savePositionFromCluster() {
    	topicPartitions.forEach(part -> {
            TopicPartition tp = new TopicPartition(topic, part.partition());
            // TODO: throws org.apache.kafka.clients.consumer.InvalidOffsetException - f no offset is currently defined for the partition
            // set offset to -1 or simply do not create a mapping partition -> offset?
            long offset = offsetConsumer.position(tp);
            logger.debug("Saving offset for last record retrieved from cluster..."); //$NON-NLS-1$
            setOffset(part.partition(), offset);
        });
    }

    /**
     * Create a mapping from partition number to offset.
     * The mapping is also created if the partition number is not contained in the `topicPartitions`
     * argument of the constructor {@link #TopicManager(String, List, KafkaConsumer)}.
     * @param partition the partition number
     * @param offset the offset
     */
    public void setOffset(int partition, long offset) {
        offsetMap.put(partition, offset);
        logger.debug("Updated offset: topic=" + topic + ", partition=" + partition + ", newOffset=" + offset); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Override
    public String toString() {
        return "TopicManager [topic=" + topic + ", offsetMap=" + offsetMap + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}
