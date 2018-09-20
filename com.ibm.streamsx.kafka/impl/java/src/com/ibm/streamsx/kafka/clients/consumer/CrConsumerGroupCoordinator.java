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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.log4j.Logger;

/**
 * This class represents the implementation of the consumer group MBean.
 * 
 * @author IBM Kafka toolkit maintainers
 * @see CrConsumerGroupCoordinatorMXBean
 */
public class CrConsumerGroupCoordinator extends NotificationBroadcasterSupport /*AbstractPersistentControlMBean<Map<MergeKey, CheckpointMerge>>*/ implements CrConsumerGroupCoordinatorMXBean {

    private final static Logger trace = Logger.getLogger(CrConsumerGroupCoordinator.class);
    private final static boolean INCLUDE_RESET_ATTEMPT_INTO_KEY = false;
    private final String groupId;
    private final int crIndex;
    private long notifSequenceNo = System.currentTimeMillis();
    private Map<MergeKey, CheckpointMerge> mergeMap;
    private Set<String> registeredConsumerOperators;
//    private Gson gson = (new GsonBuilder()).enableComplexMapKeySerialization().create();

    private AtomicBoolean rebalanceResetPending;

    /**
     * constructs a new consumer group MBean
     */
    public CrConsumerGroupCoordinator (String groupId, Integer consistentRegionIndex) {
        super();
        this.groupId = groupId;
        this.crIndex = consistentRegionIndex.intValue();
        this.mergeMap = Collections.synchronizedMap (new HashMap<>());
        this.registeredConsumerOperators = new HashSet<>();
        this.rebalanceResetPending = new AtomicBoolean(false);
    }

    @Override
    public void registerConsumerOperator (String id) {
        int sz = 0;
        synchronized (registeredConsumerOperators) {
            registeredConsumerOperators.add (id);
            sz = registeredConsumerOperators.size();
        }
        trace.info (MessageFormat.format("registerConsumerOperator: {0}, nRegistered consumers = {1}", id, sz));
    }

    @Override
    public int getNumRegisteredConsumers() {
        int sz = 0;
        synchronized (registeredConsumerOperators) {
            sz = registeredConsumerOperators.size();
        }
        return sz;
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

        boolean mergeComplete = false;
        MergeKey mergeKey = new MergeKey (chkptSequenceId, resetAttempt);
        CheckpointMerge merge = null;
        synchronized (this) {
            trace.debug (MessageFormat.format("mergeConsumerCheckpoint() - entering: [{0}, {1}] - seqId/resetAttempt = {2}, partialResetOffsetMap = {3}, expectedPartitions = {4}",
                    operatorName, groupId, mergeKey, partialResetOffsetMap, allPartitions));
            merge = mergeMap.get(mergeKey);
            if (merge == null) {
                merge = new CheckpointMerge (mergeKey);
                mergeMap.put (mergeKey, merge);
            }
            mergeComplete = merge.addContribution (operatorName, allPartitions, partialResetOffsetMap);
        }
        if (mergeComplete) {
            // the sequence number is the checkpoint sequence ID, the message contains the complete merge as JSON
//            Notification notif = new Notification (CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, this, chkptSequenceId, gson.toJson (merge));
            Notification notif = new Notification (CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, this, chkptSequenceId, merge.toJson());
            if (trace.isDebugEnabled()) {
                trace.debug(MessageFormat.format("mergeConsumerCheckpoint(): [{0}, {1}] - offset merge is complete. Sending merge complete notification for seqId {2}",
                        operatorName, groupId, mergeKey));
            }
            sendNotification (notif);
            if (trace.isDebugEnabled()) {
                trace.debug (MessageFormat.format("mergeConsumerCheckpoint(): [{0}, {1}] - JMX notification sent: {2}",
                        operatorName, groupId, notif));
            }
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
        trace.debug (MessageFormat.format("getConsolidatedOffsetMap(): [{0}, {1}] - seqId/resetAttempt = {2}",
                operatorName, groupId, mergeKey));
        CheckpointMerge merge = mergeMap.get (mergeKey);
        if (merge == null) {
            trace.warn (MessageFormat.format("getConsolidatedOffsetMap(): [{0}, {1}] - offset map for seqId {2} not found. Returning an empty Map.",
                    operatorName, groupId, mergeKey));
            return (Collections.emptyMap());
        }
        assert (merge != null);
        if (!merge.isComplete()) {
            trace.warn (MessageFormat.format("getConsolidatedOffsetMap(): [{0}, {1}] - returning incomplete offset map for seqId {2}.",
                    operatorName, groupId, mergeKey));
        }
        Map<CrConsumerGroupCoordinator.TP, Long> returnVal = merge.getConsolidatedOffsetMap();
        trace.debug (MessageFormat.format("getConsolidatedOffsetMap(): [{0}, {1}] - return = {2}",
                operatorName, groupId, returnVal));
        return returnVal;
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
        }
        if (removedKeys > 0) 
            trace.debug (MessageFormat.format ("cleanupMergeMap() {0} {1} removed for checkpoint sequence {2}",
                    removedKeys, (removedKeys == 1? "merge": "merges"), chkptSequenceId));
    }

    @Override
    public boolean getAndSetRebalanceResetPending (boolean pending, String operatorName) {
        boolean previousVal = this.rebalanceResetPending.getAndSet (pending);
        if (trace.isDebugEnabled()) 
            trace.debug (MessageFormat.format ("getAndSetRebalanceResetPending: old state = {0}; new state = {1}", previousVal, pending));
        if (previousVal != pending) {
            trace.info (MessageFormat.format ("[{0}, {1}] rebalance reset pending state toggled to {2}",
                    operatorName, groupId, pending));
        }
        return previousVal;
    }

    @Override
    public void setRebalanceResetPending (boolean pending, String operatorName) {
        boolean previousVal = this.rebalanceResetPending.getAndSet (pending);
        if (trace.isDebugEnabled()) 
            trace.debug (MessageFormat.format ("setRebalanceResetPending: old state = {0}; new state = {1}", previousVal, pending));
        if (previousVal != pending) {
            trace.info (MessageFormat.format ("[{0}, {1}] rebalance reset pending state toggled to {2}",
                    operatorName, groupId, pending));
        }
    }

    @Override
    public boolean isRebalanceResetPending() {
        boolean val = this.rebalanceResetPending.get();
        if (trace.isDebugEnabled()) 
            trace.debug (MessageFormat.format ("getRebalanceResetPending: state = {0}", val));
        return val;
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
         * @param sequenceId
         * @param resetAttempt
         */
        public MergeKey (long sequenceId, int resetAttempt) {
            this.sequenceId = sequenceId;
            this.resetAttempt = INCLUDE_RESET_ATTEMPT_INTO_KEY? resetAttempt: -1;
            this.toString = MessageFormat.format("{0}/{1}", sequenceId, resetAttempt);
        }

        /**
         * @return the sequenceId
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

        @Deprecated
        public String toJson() {
            String pattern = "'{'\"sequenceId\":{0},\"resetAttempt\":{1},\"toString\":\"{2}\"'}'";
            return MessageFormat.format (pattern, sequenceId, resetAttempt, toString);
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

        @Deprecated
        public String toJson() {
            String pattern = "'{'\"topic\":\"{0}\",\"partition\":{1},\"toString\":\"{2}\"'}'";
            return MessageFormat.format (pattern, topic, partition, toString);
        }
    }




    /**
     * This class represents a checkpoint merge being in progress.
     */
    public static class CheckpointMerge implements Serializable {
        private static final long serialVersionUID = 1L;
        private Map<CrConsumerGroupCoordinator.TP, Long> consolidatedOffsetMap  = new HashMap<>();
        /** Set of topic partitions for which we expect offsets by several calls of {@link CrConsumerGroupCoordinator#mergeConsumerCheckpoint(long, int, Set, Map, String)}*/
        private Set<CrConsumerGroupCoordinator.TP> expectedPartitions = new HashSet<>();
        private int nContributions = 0;
        private boolean complete = false;
        private final MergeKey key;

        /**
         * @param key
         */
        public CheckpointMerge (MergeKey key) {
            this.key = key;
        }

        /**
         * @return the key
         */
        public MergeKey getKey() {
            return key;
        }

        public boolean addContribution (String operatorName, Set<CrConsumerGroupCoordinator.TP> allPartitions, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap) {
            if (!this.expectedPartitions.isEmpty()) {
                if (!this.expectedPartitions.equals (allPartitions)) {
                    trace.debug (MessageFormat.format ("addContribution(): [{0}, {1}] - operator checkpoints have different expected partitions: {2} <--> {3}",
                            operatorName, key, this.expectedPartitions, allPartitions));
                }
            }
            this.expectedPartitions.addAll (allPartitions);
            ++nContributions;
            this.consolidatedOffsetMap.putAll (partialResetOffsetMap);
            Set<TP> missingPartitions = new HashSet<>(allPartitions);
            missingPartitions.removeAll (this.consolidatedOffsetMap.keySet());
            trace.debug (MessageFormat.format ("addContribution(): [{0}, {1}] - consolidated offset map = {2}, expected partitions = {3}",
                    operatorName, key, consolidatedOffsetMap, expectedPartitions));
            if (this.consolidatedOffsetMap.keySet().containsAll (this.expectedPartitions)) {
                trace.debug (MessageFormat.format ("addContribution(): [{0}, {1}] - consolidated offset map treated complete",
                        operatorName, key));
                complete = true;
            }
            else {
                trace.debug (MessageFormat.format ("addContribution(): [{0}, {1}] - still missing offset contribution(s) for partitions: {2}",
                        operatorName, key, missingPartitions));
            }
            return complete;
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

        private String offsetMap2Json() {
            String j = "[";
            int i = 0;
            for (Entry<TP, Long> e: consolidatedOffsetMap.entrySet()) {
                if (i++ > 0) j += ",";
                j += "[";
                j += e.getKey().toJson();
                j += ",";
                j += e.getValue();
                j += "]";
            }
            j += "]";
            return j;
        }

        private String expectedPartitionsToJson() {
            String j = "[";
            int i = 0;
            for (TP tp: expectedPartitions) {
                if (i++ > 0) j += ",";
                j += tp.toJson();
            }
            j += "]";
            return j;

        }

        @Deprecated
        public String toJson() {
            String pattern = "'{'\"consolidatedOffsetMap\":{0},\"expectedPartitions\":{1},\"nContributions\":{2},\"complete\":{3},\"key\":{4}'}'";
            return MessageFormat.format (pattern, offsetMap2Json(), expectedPartitionsToJson(), nContributions, complete, key.toJson());
        }
    }
}
