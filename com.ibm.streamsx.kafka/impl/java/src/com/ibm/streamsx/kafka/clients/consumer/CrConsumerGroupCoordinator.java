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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class represents the implementation of the consumer group MBean.
 * 
 * @author IBM Kafka toolkit maintainers
 * @see CrConsumerGroupCoordinatorMXBean
 */
public class CrConsumerGroupCoordinator extends NotificationBroadcasterSupport implements CrConsumerGroupCoordinatorMXBean {

    private final static Logger trace = Logger.getLogger(CrConsumerGroupCoordinator.class);
    private final static boolean INCLUDE_RESET_ATTEMPT_INTO_KEY = false;
    private static long SEQUENCE_NO_UNINITIALIZED = Long.MIN_VALUE;
    private final String groupId;
    private final int crIndex;
    private final Level traceLevel;
    private Map<MergeKey, CheckpointMerge> mergeMap;
    private Set<String> registeredConsumerOperators;
    //    private Gson gson = (new GsonBuilder()).enableComplexMapKeySerialization().create();
    private long sequenceNumber = SEQUENCE_NO_UNINITIALIZED;
    private AtomicBoolean rebalanceResetPending;

    /**
     * get the next sequence number for JMX notification
     * @param initialValue the initial value for sequence numbering
     * @return a sequence number
     */
    private synchronized long nextSequenceNumber (long initialValue) {
        if (sequenceNumber == SEQUENCE_NO_UNINITIALIZED) sequenceNumber = initialValue;
        return sequenceNumber++;
    }

    /**
     * constructs a new consumer group MBean
     */
    public CrConsumerGroupCoordinator (String groupId, Integer consistentRegionIndex, String traceLevel) {
        super();
        this.groupId = groupId;
        this.crIndex = consistentRegionIndex.intValue();
        this.mergeMap = new HashMap<>();
        this.registeredConsumerOperators = new HashSet<>();
        this.rebalanceResetPending = new AtomicBoolean(false);
        Level l = Level.DEBUG;
        try {
            l = Level.toLevel (traceLevel.toUpperCase());
        } catch (Exception e) {
            ;
        }
        this.traceLevel = l;
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

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#deregisterConsumerOperator(java.lang.String)
     */
    @Override
    public void deregisterConsumerOperator (String id) {
        int sz = 0;
        synchronized (registeredConsumerOperators) {
            registeredConsumerOperators.remove (id);
            sz = registeredConsumerOperators.size();
        }
        trace.info (MessageFormat.format("deregisterConsumerOperator: {0}, nRegistered consumers = {1}", id, sz));
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
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#getRegisteredConsumerOperators()
     */
    @Override
    public Set<String> getRegisteredConsumerOperators() throws IOException {
        synchronized (registeredConsumerOperators) {
            return new HashSet<String> (registeredConsumerOperators);
        }
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
     * @param nRequiredDistinctContributions the number of expected distinct contributions for merge completeness
     * @param partialResetOffsetMap the partial set of offsets being partialResetOffsetMap.keySet() typically a subset of allPartitions.
     * @param operatorName The unique name of the operator
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#mergeConsumerCheckpoint(long, int, Set, Map)
     */
    @Override
    public void mergeConsumerCheckpoint (long chkptSequenceId, int resetAttempt, int nRequiredDistinctContributions,
            Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap, String operatorName) {

        boolean mergeComplete = false;
        MergeKey mergeKey = new MergeKey (chkptSequenceId, resetAttempt);
        CheckpointMerge merge = null;
        String mergeJson = "";
        synchronized (mergeMap) {
            trace.log (traceLevel, MessageFormat.format("mergeConsumerCheckpoint() - entering: [{0}, {1}] - seqId/resetAttempt = {2}, partialResetOffsetMap = {3}, nExpectedContribs = {4}",
                    operatorName, groupId, mergeKey, partialResetOffsetMap, nRequiredDistinctContributions));
            merge = mergeMap.get(mergeKey);
            if (merge == null) {
                merge = new CheckpointMerge (mergeKey, nRequiredDistinctContributions);
                merge.setTraceLevel (traceLevel);
                mergeMap.put (mergeKey, merge);
            }
            mergeComplete = merge.addContribution (operatorName, nRequiredDistinctContributions, partialResetOffsetMap);
            if (mergeComplete)
                mergeJson = merge.toJson();
        }
        if (mergeComplete) {
            Notification notif = new Notification (CrConsumerGroupCoordinatorMXBean.MERGE_COMPLETE_NTF_TYPE, this, nextSequenceNumber (chkptSequenceId + 1l), mergeJson);
            if (trace.isEnabledFor (traceLevel)) {
                trace.debug(MessageFormat.format("mergeConsumerCheckpoint(): [{0}, {1}] - offset merge is complete. Sending merge complete notification for seqId {2}",
                        operatorName, groupId, mergeKey));
            }
            sendNotification (notif);
            if (trace.isEnabledFor (traceLevel)) {
                trace.log (traceLevel, MessageFormat.format("mergeConsumerCheckpoint(): [{0}, {1}] - JMX notification sent: {2}",
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
        trace.log (traceLevel, MessageFormat.format("getConsolidatedOffsetMap(): [{0}, {1}] - seqId/resetAttempt = {2}",
                operatorName, groupId, mergeKey));
        Map<CrConsumerGroupCoordinator.TP, Long> returnVal;
        synchronized (mergeMap) {

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
            returnVal = new HashMap<> (merge.getConsolidatedOffsetMap());
            trace.log (traceLevel, MessageFormat.format("getConsolidatedOffsetMap(): [{0}, {1}] - return = {2}",
                    operatorName, groupId, returnVal));
        }
        return returnVal;
    }




    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#cleanupMergeMap(long)
     */
    @Override
    public void cleanupMergeMap (long chkptSequenceId) throws IOException {
        int removedKeys = 0;
        synchronized (mergeMap) {
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
            trace.log (traceLevel, MessageFormat.format ("cleanupMergeMap() {0} {1} removed for checkpoint sequence {2}",
                    removedKeys, (removedKeys == 1? "merge": "merges"), chkptSequenceId));
    }

    @Override
    public boolean getAndSetRebalanceResetPending (boolean pending, String operatorName) {
        boolean previousVal = this.rebalanceResetPending.getAndSet (pending);
        if (trace.isEnabledFor (traceLevel)) 
            trace.log (traceLevel, MessageFormat.format ("getAndSetRebalanceResetPending: old state = {0}; new state = {1}", previousVal, pending));
        if (previousVal != pending) {
            trace.log (traceLevel, MessageFormat.format ("[{0}, {1}] rebalance reset pending state toggled to {2}",
                    operatorName, groupId, pending));
        }
        return previousVal;
    }

    @Override
    public void setRebalanceResetPending (boolean pending, String operatorName) {
        boolean previousVal = this.rebalanceResetPending.getAndSet (pending);
        if (trace.isEnabledFor (traceLevel)) 
            trace.log (traceLevel, MessageFormat.format ("setRebalanceResetPending: old state = {0}; new state = {1}", previousVal, pending));
        if (previousVal != pending) {
            trace.log (traceLevel, MessageFormat.format ("[{0}, {1}] rebalance reset pending state toggled to {2}",
                    operatorName, groupId, pending));
        }
    }

    @Override
    public boolean isRebalanceResetPending() {
        boolean val = this.rebalanceResetPending.get();
        if (trace.isEnabledFor (traceLevel)) 
            trace.log (traceLevel, MessageFormat.format ("getRebalanceResetPending: state = {0}", val));
        return val;
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
            this.toString = MessageFormat.format("{0,number,#}/{1}", sequenceId, resetAttempt);
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
            String pattern = "'{'\"sequenceId\":{0,number,#},\"resetAttempt\":{1,number,#},\"toString\":\"{2}\"'}'";
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
            String pattern = "'{'\"topic\":\"{0}\",\"partition\":{1,number,#},\"toString\":\"{2}\"'}'";
            return MessageFormat.format (pattern, topic, partition, toString);
        }
    }

    public static void main (String[] args) {
        // test JSON generation
        com.google.gson.Gson gson = (new com.google.gson.GsonBuilder()).enableComplexMapKeySerialization().create();
        int nExpectedContribs = 3;
        CheckpointMerge m = new CheckpointMerge (new MergeKey(1002,  1), nExpectedContribs);
        Map <TP, Long> m1 = new HashMap <>();
        m1.put (new TP ("top1", 0), 10l);
        m1.put (new TP ("top1", 1), 11l);

        Map <TP, Long> m2 = new HashMap <>();
        m2.put (new TP ("top1", 2), 12l);
        m2.put (new TP ("top2", 0), 20l);

        Map <TP, Long> m3 = new HashMap <>();
        m3.put (new TP ("top2", 1), 21l);

        m.addContribution ("opName1", nExpectedContribs, m1);
        m.addContribution ("opName2", nExpectedContribs, m2);
        m.addContribution ("opName3", nExpectedContribs, m3);
        String expected = gson.toJson (m);
        String actual = m.toJson();
        if (!actual.equals(expected)) {
            System.out.println ("Different JSONs:");
            System.out.println ("expected: " + expected);
        }
        System.out.println ("actual:   " + actual);
        System.out.println ("----- reversing expected ----");
        CheckpointMerge expected_revers = gson.fromJson (expected, CrConsumerGroupCoordinator.CheckpointMerge.class);
        System.out.println (expected_revers.getKey());
        System.out.println (expected_revers.getnExpectedContributions());
        System.out.println (expected_revers.getNumContributions());
        System.out.println (expected_revers.isComplete());
        System.out.println (expected_revers.getConsolidatedOffsetMap());
        System.out.println (expected_revers.getContributedOperatorNames());

        System.out.println ("----- reversing actual ----");
        CheckpointMerge actual_revers = gson.fromJson (actual, CrConsumerGroupCoordinator.CheckpointMerge.class);
        System.out.println (actual_revers.getKey());
        System.out.println (actual_revers.getnExpectedContributions());
        System.out.println (actual_revers.getNumContributions());
        System.out.println (actual_revers.isComplete());
        System.out.println (actual_revers.getConsolidatedOffsetMap());
        System.out.println (actual_revers.getContributedOperatorNames());
    }


    /**
     * This class represents a checkpoint merge being in progress.
     */
    public static class CheckpointMerge implements Serializable {
        private static final long serialVersionUID = 1L;
        private Map<CrConsumerGroupCoordinator.TP, Long> consolidatedOffsetMap  = new HashMap<>();
        /** Set of topic partitions for which we expect offsets by several calls of {@link CrConsumerGroupCoordinator#mergeConsumerCheckpoint(long, int, Set, Map, String)}*/
        private Set<String> contributedOperatorNames = new HashSet<>();
        private int nExpectedContributions = 0;
        private int nContributions = 0;
        private boolean complete = false;
        private final MergeKey key;
        private transient Level traceLevel = Level.DEBUG;

        /**
         * @param key
         */
        public CheckpointMerge (MergeKey key, int nExpectedContributions) {
            this.key = key;
            this.nExpectedContributions = nExpectedContributions;
        }


        /**
         * @param traceLevel the traceLevel to set
         */
        public void setTraceLevel (Level traceLevel) {
            this.traceLevel = traceLevel;
        }

        /**
         * @return the key
         */
        public MergeKey getKey() {
            return key;
        }

        public boolean addContribution (String operatorName, int nDistinctOperatorNames, Map <CrConsumerGroupCoordinator.TP, Long> partialResetOffsetMap) {
            if (this.nExpectedContributions > 0) {
                if (this.nExpectedContributions != nDistinctOperatorNames) {
                    trace.log (traceLevel, MessageFormat.format ("addContribution(): [{0}, {1}] - operator checkpoints have different expected number of contributions: {2} <--> {3}",
                            operatorName, key, this.nExpectedContributions, nDistinctOperatorNames));
                }
            }
            if (nDistinctOperatorNames > this.nExpectedContributions) {
                this.nExpectedContributions = nDistinctOperatorNames;
            }
            if (!this.contributedOperatorNames.contains (operatorName)) {
                this.contributedOperatorNames.add (operatorName);
                ++nContributions;
            }
            this.consolidatedOffsetMap.putAll (partialResetOffsetMap);

            if (this.nContributions < this.nExpectedContributions) {
                trace.log (traceLevel, MessageFormat.format ("addContribution(): [{0}, {1}] - still missing {2} offset contribution(s) for partitions",
                        operatorName, key, (this.nExpectedContributions - nContributions)));
            }
            else {
                trace.log (traceLevel, MessageFormat.format ("addContribution(): [{0}, {1}] - consolidated offset map treated complete with {2} contributions",
                        operatorName, key, nContributions));
                this.complete = true;
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
         * @return the contributedOperatorNames
         */
        public Set<String> getContributedOperatorNames() {
            return contributedOperatorNames;
        }

        /**
         * @return the nExpectedContributions
         */
        public int getnExpectedContributions() {
            return nExpectedContributions;
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
            result = prime * result + ((contributedOperatorNames == null) ? 0 : contributedOperatorNames.hashCode());
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + nContributions;
            result = prime * result + nExpectedContributions;
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
            if (contributedOperatorNames == null) {
                if (other.contributedOperatorNames != null)
                    return false;
            } else if (!contributedOperatorNames.equals(other.contributedOperatorNames))
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (nContributions != other.nContributions)
                return false;
            if (nExpectedContributions != other.nExpectedContributions)
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

        private String contributedOperatorNames2Json() {
            String j = "[";
            int i = 0;
            for (String s: contributedOperatorNames) {
                if (i++ > 0) j += ",";
                j += "\"";
                j += s;
                j += "\"";
            }
            j += "]";
            return j;
        }

        @Deprecated
        public String toJson() {
            String pattern = "'{'\"consolidatedOffsetMap\":{0},\"contributedOperatorNames\":{1},\"nExpectedContributions\":{2,number,#},\"nContributions\":{3,number,#},\"complete\":{4},\"key\":{5}'}'";
            return MessageFormat.format (pattern, offsetMap2Json(), contributedOperatorNames2Json(), nExpectedContributions, nContributions, complete, key.toJson());
        }
    }
}
