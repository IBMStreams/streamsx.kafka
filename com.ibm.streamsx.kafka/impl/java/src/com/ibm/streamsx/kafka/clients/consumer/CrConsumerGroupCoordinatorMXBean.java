package com.ibm.streamsx.kafka.clients.consumer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * This interface represents the MBean of a Kafka consumer group within a consistent region.
 * A Kafka consumer group can belong only to one consistent region. 
 * 
 * All functions are declared throwing IOException to force the client to handle communication problems.
 * 
 * @see <a href="http://www.oracle.com/technetwork/java/javase/tech/best-practices-jsp-136021.html#mozTocId805713"JMX Best Practices by Oracle/a>.
 * @author IBM Kafka toolkit maintainers
 */
public interface CrConsumerGroupCoordinatorMXBean {

    /**
     * Notification type offset map merge is completed.
     */
    public final static String MERGE_COMPLETE_NTF_TYPE = "OFFSET.MAP.MERGE.COMPLETE";

    /**
     * Returns the index of the consistent region.
     * @return the consumer group index
     */
    public int getConsistentRegionIndex() throws IOException;

    /**
     * Gets the Kafka Group-ID, i.e. the value of the group.id Kafka property
     * @return the group-ID
     */
    public String getGroupId() throws IOException;

    /**
     * Merges the checkpoint data of a single consumer into the consolidated group checkpoint.
     * @param chkptSequenceId the checkpoint sequence ID.
     * @param allPartitions  the total set of all expected partitions
     * @param partialResetOffsetMap the partial set of offsets being partialResetOffsetMap.keySet() typically a subset of allPartitions.
     */
    public void mergeConsumerCheckpoint (long chkptSequenceId, Set<CrConsumerGroupCoordinator.TP> allPartitions, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap) throws IOException;

    /**
     * Gets the consolidated offset map that has been created by merging parts via {@link #mergeConsumerCheckpoint(long, Set, Map)}.
     * @return the consolidated map that maps topic partitions to offsets
     */
    public Map<CrConsumerGroupCoordinator.TP, Long> getConsolidatedOffsetMap() throws IOException;
}
