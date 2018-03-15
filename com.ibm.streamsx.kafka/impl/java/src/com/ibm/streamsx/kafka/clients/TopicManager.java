package com.ibm.streamsx.kafka.clients;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/*
 * Retrieves and stores the latest offsets for
 * each partition in the topic
 */
public class TopicManager implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(TopicManager.class);

    private String topic;
    private transient KafkaConsumer<?, ?> offsetConsumer;
    private Map<Integer /* partition */, Long /* offset */> offsetMap;
	private List<TopicPartition> topicPartitions;

    public <K, V> TopicManager(String topic, List<TopicPartition> topicPartitions, KafkaConsumer<K, V> offsetConsumer) {
        this.topic = topic;
        this.topicPartitions = topicPartitions;
        this.offsetConsumer = offsetConsumer;
        this.offsetMap = new HashMap<Integer, Long>();
    }

    public <K, V> void setOffsetConsumer(KafkaConsumer<K, V> offsetConsumer) {
        this.offsetConsumer = offsetConsumer;
    }

    public Long getOffset(int partition) {
        return offsetMap.get(partition);
    }
  
    public boolean containsPartition(int partition) {
    	return offsetMap.containsKey(partition);
    }
    
    public void savePositionFromCluster() {
    	topicPartitions.forEach(part -> {
            TopicPartition tp = new TopicPartition(topic, part.partition());
            long offset = offsetConsumer.position(tp);
            logger.debug("Saving offset for last record retrieved from cluster..."); //$NON-NLS-1$
            setOffset(part.partition(), offset);
        });
    }

    public void setOffset(int partition, long offset) {
        offsetMap.put(partition, offset);
        logger.debug("Updated offset: topic=" + topic + ", partition=" + partition + ", newOffset=" + offset); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Override
    public String toString() {
        return "TopicManager [topic=" + topic + ", offsetMap=" + offsetMap + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}
