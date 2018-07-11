package com.ibm.streamsx.kafka.clients.consumer;

import java.util.Map;
import java.util.Set;

/**
 * This interface represents the MBean of a Kafka consumer group within a consistent region.
 * A Kafka consumer group can belong only to one consistent region. 
 * 
 * TODO: add throws IOException clause to all functions. See best practices 
 * http://www.oracle.com/technetwork/java/javase/tech/best-practices-jsp-136021.html#mozTocId805713
 * 
 * @author IBM Kafka toolkit maintainers
 */
public interface CrConsumerGroupCoordinatorMXBean {

    /**
     * Returns the index of the consistent region.
     * @return the consumer group index
     */
    public int getConsistentRegionIndex();

    /**
     * Gets the Kafka Group-ID, i.e. the value of the group.id Kafka property
     * @return the group-ID
     */
    public String getGroupId();

    /**
     * Merges the partial checkpoint data into the group checkpoint.
     * @param chkptSequenceId the checkpoint sequence ID.
     * @param allPartitions  the total set of all expected partitions
     * @param partialResetOffsetMap the partial set of offsets being partialResetOffsetMap.keySet() typically a subset of allPartitions.
     */
    public void mergePartialCheckpoint (long chkptSequenceId, Set<CrConsumerGroupCoordinator.TP> allPartitions, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap);

    /**
     * Gets the consolidated offset map that has been created by merging parts via {@link #mergePartialCheckpoint(long, Set, Map)}.
     * @return the consolidated map that maps topic partitions to offsets
     */
    public Map<CrConsumerGroupCoordinator.TP, Long> getConsolidatedOffsetMap();
    
    /*
     * Registers a topic partition in the Group MBean so that the MBean has 
     * the overview over all partitions that are assigned to all consumers in 
     * the Consumer group.
     * @param topic      the topic name 
     * @param partition  the partition number
    public void registerAssignedPartition (String topic, int partition);
     */

    //TODO: remove this. It is only for Debugging.
    /*
     * 
    public String getAssignedPartitions();
     */
}
