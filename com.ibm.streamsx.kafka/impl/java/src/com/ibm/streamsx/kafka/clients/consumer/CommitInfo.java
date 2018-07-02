/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * This class represent the offsets to be committed.
 * Offsets should be those offsets that are to be consumed next, i.e. last processed offsets +1.
 */
public class CommitInfo {

    private Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
    private boolean commitSynchronous = false;
    private boolean commitPartitionWise = true;

    /**
     * Creates a new CommitInfo object
     * @param commitSync Set this to true, to commit synchronous
     * @param partitionWise commit partition by partition separately.
     */
    public CommitInfo (boolean commitSync, boolean partitionWise) {
        this.commitSynchronous = commitSync;
        this.commitPartitionWise = partitionWise;
    }

    /**
     * Puts an offset for given topic and partition number
     * @param topic     the topic
     * @param partition partition number
     * @param offset    the offset. Should be last processed +1
     */
    public void put (String topic, int partition, long offset) {
        map.put (new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
    }

    /**
     * Puts an offset for given topic and partition number
     * 
     * @param topicPartition the topic + partition
     * @param offset    the offset. Should be last processed +1
     */
    public void put (TopicPartition topicPartition, long offset) {
        map.put (topicPartition, new OffsetAndMetadata(offset));
    }

    /**
     * @return the map
     */
    public Map<TopicPartition, OffsetAndMetadata> getMap() {
        return map;
    }

    /**
     * @return the commitSynchronous
     */
    public boolean isCommitSynchronous() {
        return commitSynchronous;
    }

    /**
     * @return the commitPartitionWise
     */
    public boolean isCommitPartitionWise() {
        return commitPartitionWise;
    }

    /**
     * Tests for an empty topic partition to offset map
     * @return true if there are no offsets to be committed, false otherwise
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return map.toString()
                + "; sync = " + commitSynchronous
                + "; partition-wise = " + commitPartitionWise;
    }
}
