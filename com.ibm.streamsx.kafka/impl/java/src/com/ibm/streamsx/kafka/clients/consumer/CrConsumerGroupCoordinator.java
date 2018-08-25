/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.log4j.Logger;

/**
 * This class represents the implementation of the consumer group MBean.
 * 
 * @author IBM Kafka toolkit maintainers
 * @see CrConsumerGroupCoordinatorMXBean
 */
public class CrConsumerGroupCoordinator extends NotificationBroadcasterSupport /*AbstractPersistentControlMBean<Set<CrConsumerGroupCoordinator.TP>>*/ implements CrConsumerGroupCoordinatorMXBean {

    private final static Logger trace = Logger.getLogger(CrConsumerGroupCoordinator.class);

    private final String groupId;
    private final int crIndex;
    private long notifSequenceNo = System.currentTimeMillis();
    private Map<MergeKey, CheckpointMerge> mergeMap;
    private Map<Long, Integer> highestResetAttempts;

    /**
     * constructs a new consumer group MBean
     */
    public CrConsumerGroupCoordinator (String groupId, Integer consistentRegionIndex) {
        super();
        this.groupId = groupId;
        this.crIndex = consistentRegionIndex.intValue();
        this.mergeMap = Collections.synchronizedMap (new HashMap<>());
        this.highestResetAttempts = new HashMap<>();
    }

    /**
     * @return the consistent region index that was used to create the MBean
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#getConsistentRegionIndex()
     */
    @Override
    public int getConsistentRegionIndex() {
        return this.crIndex;
    }

    /**
     * @return the Kafka group ID
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#getGroupId()
     */
    @Override
    public String getGroupId() {
        return this.groupId;
    }

    /**
     * Merges a checkpoint of a single consumer into the group checkpoint, which is the consumer group's view of the checkpointed data.
     * @param chkptSequenceId the checkpoint sequence ID.
     * @param resetAttempt the current number of attempts of resetting the CR
     * @param allPartitions  the total set of all expected partitions
     * @param partialResetOffsetMap the partial set of offsets being partialResetOffsetMap.keySet() typically a subset of allPartitions.
     * @param operatorName The unique name of the operator
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#mergeConsumerCheckpoint(long, int, Set, Map)
     */
    @Override
    public void mergeConsumerCheckpoint (long chkptSequenceId, int resetAttempt,
            Set<CrConsumerGroupCoordinator.TP> allPartitions, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap, String operatorName) {

        boolean completeBeforeMerge = false;
        boolean mergeComplete = false;
        MergeKey mergeKey = new MergeKey (chkptSequenceId, resetAttempt);

        synchronized (this) {
            trace.info (MessageFormat.format("mergePartialCheckpoint() [{4}][{3}]: seqId/resetAttempt = {0}, expectedPartitions = {1}, partialResetOffsetMap = {2}",
                    mergeKey, allPartitions, partialResetOffsetMap, operatorName, groupId));

            Integer highestResetAttemptObj = highestResetAttempts.get (chkptSequenceId);
            if (highestResetAttemptObj == null) {
                highestResetAttempts.put (chkptSequenceId, resetAttempt);
            }
            else {
                // try to detect resetAttempt reset
                int highestResetAttempt = highestResetAttemptObj.intValue();
                if (resetAttempt < highestResetAttempt) {
                    trace.info (MessageFormat.format("mergePartialCheckpoint() [{2}]: resetAttempt counter reset from {0} to {1} detected. Cleaning up ...",
                            highestResetAttemptObj, resetAttempt, groupId));
                    for (int attempt = 0; attempt <= highestResetAttempt; ++attempt) {
                        mergeMap.remove (new MergeKey(chkptSequenceId, attempt));
                    }
                    highestResetAttempts.put (chkptSequenceId, resetAttempt);
                }
            }

            CheckpointMerge merge = mergeMap.get(mergeKey);
            if (merge == null) {
                merge = new CheckpointMerge();
                mergeMap.put (mergeKey, merge);
            }
            else {
                if (merge.containsPartitionPart (partialResetOffsetMap)) {
                    trace.info(MessageFormat.format("[{1}] partial offsets {0} already seen here. Creating new merge.", partialResetOffsetMap.keySet(), groupId));
                    merge = new CheckpointMerge();
                    mergeMap.put (mergeKey, merge);
                }
            }
            completeBeforeMerge = merge.isComplete();
            mergeComplete = merge.addContribution (allPartitions, partialResetOffsetMap);
        }
        if (mergeComplete && !completeBeforeMerge) {
            trace.info(MessageFormat.format("mergeConsumerCheckpoint() [{1}]: offset merge became complete. Sending merge complete notification for seqId {0}", mergeKey, groupId));
            // the sequence number is the checkpoint sequence ID, the message contains the reset attempt
            Notification notif = new Notification (CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, this, chkptSequenceId, "" + resetAttempt);
            trace.info("sending JMX notification ...");
            sendNotification (notif);
            trace.info(MessageFormat.format("mergeConsumerCheckpoint() [{1}]: notification sent: {0}", notif, groupId));
        }
    }



    /**
     * Gets the consolidated offset map that has been created by merging parts via {@link #mergeConsumerCheckpoint(long, int, Set, Map)}.
     * @param chkptSequenceId the checkpoint sequence ID.
     * @param resetAttempt the current number of attempts of resetting the CR
     * @param operatorName The unique name of the operator
     * @return the consolidated map that maps topic partitions to offsets
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#getConsolidatedOffsetMap()
     */
    public Map<CrConsumerGroupCoordinator.TP, Long> getConsolidatedOffsetMap (long chkptSequenceId, int resetAttempt, String operatorName) {
        MergeKey mergeKey = new MergeKey (chkptSequenceId, resetAttempt);
        trace.info (MessageFormat.format("getConsolidatedOffsetMap() [{2}][{1}]: seqId/resetAttempt = {0}", mergeKey, operatorName, groupId));
        CheckpointMerge merge = mergeMap.get (mergeKey);
        if (merge == null) {
            trace.warn (MessageFormat.format("[{1}]offset map for seqId {0} not found. Returning an empty Map.", mergeKey, groupId));
            return (Collections.emptyMap());
        }
        assert (merge != null);
        if (!merge.isComplete()) {
            trace.warn (MessageFormat.format("[{1}] returning incomplete offset map for seqId {0}.", mergeKey, groupId));
        }
        return merge.getConsolidatedOffsetMap();
    }




    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#cleanupMergeMap(long)
     */
    @Override
    public void cleanupMergeMap (long chkptSequenceId) throws IOException {
        int removedKeys = 0;
        synchronized (this) {
            Collection<MergeKey> retiredMergeKeys = new ArrayList<>(10);
            for (MergeKey k: mergeMap.keySet()) {
                if (k.getSequenceId() <= chkptSequenceId) {   // remove also older (smaller) IDs
                    retiredMergeKeys.add (k);
                }
            }
            for (MergeKey k: retiredMergeKeys) {
                CheckpointMerge m = mergeMap.remove(k);
                if (m != null) ++removedKeys;
            }
            this.highestResetAttempts.remove (chkptSequenceId);
        }
        if (removedKeys > 0) 
            trace.info (MessageFormat.format ("cleanupMergeMap() {0} {1} removed for checkpoint sequence {2}",
                    removedKeys, (removedKeys == 1? "merge": "merges"), chkptSequenceId));
    }

    /**
     * broadcasts a JMX message of type all registered notification listeners that contains the given data
     * the partitions given in the partitions parameter.
     * @param data the data to be broadcasted. This can be a deserialized object, for example.
     * @param jmxNotificationType the notification type
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#broadcastData(java.lang.String, java.lang.String)
     */
    @Override
    public void broadcastData (String data, String jmxNotificationType) {
        trace.info ("broadcastData(): data = " + data + ", jmxNotificationType = " + jmxNotificationType);
        Notification notif = new Notification (jmxNotificationType, this, this.notifSequenceNo++, data);
        trace.info("sending JMX notification: " + notif);
        sendNotification (notif);
        trace.info("notification sent: " + notif);
    }

    /**
     * This class represents the key for the consolidation map. It is the combination of a checkpoint sequence ID and the reset attempt number.
     */
    public static class MergeKey implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long sequenceId;
        private final int resetAttempt;
        private final String toString;
        /**
         * @param notifSequenceNo
         * @param resetAttempt
         */
        public MergeKey (long sequenceId, int resetAttempt) {
            this.sequenceId = sequenceId;
            this.resetAttempt = resetAttempt;
            this.toString = MessageFormat.format("{0}-{1}", sequenceId, resetAttempt);
        }

        /**
         * @return the notifSequenceNo
         */
        public long getSequenceId() {
            return sequenceId;
        }

        /**
         * @return the resetAttempt
         */
        public int getResetAttempt() {
            return resetAttempt;
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + resetAttempt;
            result = prime * result + (int) (sequenceId ^ (sequenceId >>> 32));
            return result;
        }

        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MergeKey other = (MergeKey) obj;
            if (resetAttempt != other.resetAttempt)
                return false;
            if (sequenceId != other.sequenceId)
                return false;
            return true;
        }

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return toString;
        }
    }

    /**
     * This inner class represents a topic partition.
     * We do not use the TopicPartition class from the Kafka libs because this class must be available in the JCP.
     */
    public static class TP implements Serializable {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private final int partition;
        private final String toString;

        @ConstructorProperties ({"topic", "partition"})
        public TP (String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
            this.toString = topic + "-" + partition;
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + partition;
            result = prime * result + ((topic == null) ? 0 : topic.hashCode());
            return result;
        }

        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TP other = (TP) obj;
            if (partition != other.partition)
                return false;
            if (topic == null) {
                if (other.topic != null)
                    return false;
            } else if (!topic.equals(other.topic))
                return false;
            return true;
        }

        /**
         * @return the topic
         */
        public String getTopic() {
            return topic;
        }

        /**
         * @return the partition
         */
        public int getPartition() {
            return partition;
        }

        /**
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return toString;
        }
    }


    /**
     * This class represents a checkpoint merge being in progress.
     */
    private class CheckpointMerge implements Serializable {
        private static final long serialVersionUID = 1L;
        private Map<CrConsumerGroupCoordinator.TP, Long> consolidatedOffsetMap  = new HashMap<>();
        /** Set of topic partitions for which we expect offsets by several calls of {@link #mergeConsumerCheckpoint(long, Set, Map)}*/
        private Set<CrConsumerGroupCoordinator.TP> expectedPartitions = new HashSet<>();
        private Set<Set<CrConsumerGroupCoordinator.TP>> nonEmptyContributions = new HashSet<>();
        private int nContributions = 0;
        private boolean complete = false;

        public boolean addContribution (Set<CrConsumerGroupCoordinator.TP> allPartitions, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap) {
            if (!this.expectedPartitions.isEmpty()) {
                if (!this.expectedPartitions.equals (allPartitions)) {
                    trace.info ("operator checkpoints have different allPartition sets: " + this.expectedPartitions + " <--> " + allPartitions);
                }
            }
            this.expectedPartitions.addAll (allPartitions);
            if (!partialResetOffsetMap.isEmpty()) {
                Set<CrConsumerGroupCoordinator.TP> contributingPartitions = new HashSet<>(partialResetOffsetMap.keySet());
                nonEmptyContributions.add (contributingPartitions);
            }
            ++nContributions;
            this.consolidatedOffsetMap.putAll (partialResetOffsetMap);
            trace.info ("addContribution(): consolidated offset map: " + consolidatedOffsetMap + ", expected partition set: " + expectedPartitions);
            if (this.consolidatedOffsetMap.keySet().containsAll (this.expectedPartitions)) {
                trace.info("addContribution(): consolidated offset map treated complete");
                complete = true;
            }
            return complete;
        }

        /**
         * Tests if this set of topic partitions have already contributed.
         * This is for detection of reset with same resetAttemptCounter and checkpoint sequenceID.
         * @param partialResetOffsetMap
         * @return true if the keys of this partialOffsetMap already contributed.
         */
        boolean containsPartitionPart (Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap) {
            Set<CrConsumerGroupCoordinator.TP> contributingPartitions = new HashSet<>(partialResetOffsetMap.keySet());
            return nonEmptyContributions.contains (contributingPartitions);
        }

        /**
         * @return the consolidatedOffsetMap
         */
        public Map<CrConsumerGroupCoordinator.TP, Long> getConsolidatedOffsetMap() {
            return consolidatedOffsetMap;
        }

        /**
         * @return the expectedPartitions
         */
        @SuppressWarnings("unused")
        public Set<CrConsumerGroupCoordinator.TP> getExpectedPartitions() {
            return expectedPartitions;
        }

        /**
         * @return true if the merge is complete, false otherwise.
         */
        public boolean isComplete() {
            return complete;
        }

        /**
         * @return the number of contributions
         */
        @SuppressWarnings("unused")
        public int getNumContributions() {
            return nContributions;
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (complete ? 1231 : 1237);
            result = prime * result + ((consolidatedOffsetMap == null) ? 0 : consolidatedOffsetMap.hashCode());
            result = prime * result + ((expectedPartitions == null) ? 0 : expectedPartitions.hashCode());
            result = prime * result + nContributions;
            return result;
        }

        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CheckpointMerge other = (CheckpointMerge) obj;
            if (complete != other.complete)
                return false;
            if (consolidatedOffsetMap == null) {
                if (other.consolidatedOffsetMap != null)
                    return false;
            } else if (!consolidatedOffsetMap.equals(other.consolidatedOffsetMap))
                return false;
            if (expectedPartitions == null) {
                if (other.expectedPartitions != null)
                    return false;
            } else if (!expectedPartitions.equals(other.expectedPartitions))
                return false;
            if (nContributions != other.nContributions)
                return false;
            return true;
        }
    }
}
