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
 * @see <a href="http://www.oracle.com/technetwork/java/javase/tech/best-practices-jsp-136021.html#mozTocId805713">JMX Best Practices by Oracle</a>.
 * @author IBM Kafka toolkit maintainers
 */
public interface CrConsumerGroupCoordinatorMXBean {

    /**
     * JMX Notification type offset map merge is completed.
     */
    public final static String MERGE_COMPLETE_NTF_TYPE = "OFFSET.MAP.MERGE.COMPLETE";
    
    /**
     * JMX Notification type partitions have been assigned to a consumer in the group
     */
    public final static String PARTITIONS_META_CHANGED = "PARTITIONS.META.CHANGED";

    /**
     * Returns the index of the consistent region.
     * @return the consumer group index
     * @throws IOException
     */
    public int getConsistentRegionIndex() throws IOException;

    /**
     * Gets the Kafka Group-ID, i.e. the value of the group.id Kafka property
     * @return the group-ID
     * @throws IOException
     */
    public String getGroupId() throws IOException;

    /**
     * Merges the checkpoint data of a single consumer into the consolidated group checkpoint.
     * @param chkptSequenceId the checkpoint sequence ID.
     * @param resetAttempt the current number of attempts of resetting the CR
     * @param allPartitions  the total set of all expected partitions
     * @param partialResetOffsetMap the partial set of offsets being partialResetOffsetMap.keySet() typically a subset of allPartitions.
     * @param operatorName The unique name of the operator
     * @throws IOException
     */
    public void mergeConsumerCheckpoint (long chkptSequenceId, int resetAttempt, 
            Set<CrConsumerGroupCoordinator.TP> allPartitions, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap, String operatorName) throws IOException;

    /**
     * Gets the consolidated offset map that has been created by merging parts via {@link #mergeConsumerCheckpoint(long, int, Set, Map)}.
     * @param chkptSequenceId the checkpoint sequence ID.
     * @param resetAttempt the current number of attempts of resetting the CR
     * @param operatorName The unique name of the operator
     * @return the consolidated map that maps topic partitions to offsets
     * @throws IOException
     */
    public Map<CrConsumerGroupCoordinator.TP, Long> getConsolidatedOffsetMap (long chkptSequenceId, int resetAttempt, String operatorName) throws IOException;
    
    /**
     * Cleans the merge map for the given checkpoint sequence ID
     * @param chkptSequenceId the checkpoint sequence ID
     * @throws IOException
     */
    public void cleanupMergeMap (long chkptSequenceId) throws IOException;
    /**
     * broadcasts a JMX message of type all registered notification listeners that contains the given data
     * the partitions given in the partitions parameter.
     * @param data the data to be broadcasted. For example, this can be a deserialized object.
     * @param jmxNotificationType the notification type
     * @throws IOException
     */
    public void broadcastData (String data, String jmxNotificationType) throws IOException;
}
