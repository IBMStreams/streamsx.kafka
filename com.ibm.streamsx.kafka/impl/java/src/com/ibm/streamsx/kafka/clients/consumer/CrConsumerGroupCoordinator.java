/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    private final static long ID_UNASSIGNED = -1l;

    private final String groupId;
    private final int crIndex;
    private long checkpointSequenceId = ID_UNASSIGNED;
    private Map<CrConsumerGroupCoordinator.TP, Long> consolidatedOffsetMap;
    private Set<CrConsumerGroupCoordinator.TP> expectedPartitions;

    /**
     * constructs a new consumer group MBean
     */
    public CrConsumerGroupCoordinator (String groupId, Integer consistentRegionIndex) {
        super();
        // TODO: call super constructor with executor argument to enable async notifications
        //        super(new Executor() {
        //            
        //            @Override
        //            public void execute (Runnable command) {
        //                // TODO Auto-generated method stub
        //                
        //            }
        //        });
        this.groupId = groupId;
        this.crIndex = consistentRegionIndex.intValue();
        this.expectedPartitions = new HashSet<>();
        this.consolidatedOffsetMap = new HashMap<>();
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
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#mergePartialCheckpoint(java.util.Set, java.util.Map)
     */
    @Override
    public void mergePartialCheckpoint (long chkptSequenceId, Set<TP> allPartitions, Map<TP, Long> partialResetOffsetMap) {
        trace.info ("mergePartialCheckpoint(): seqId = " + chkptSequenceId + ", expectedPartitions = " + allPartitions + ", partialResetOffsetMap = " + partialResetOffsetMap);
        if (this.checkpointSequenceId != chkptSequenceId) {
            // reset the data
            initializeMerge (chkptSequenceId);
        }
        if (!this.expectedPartitions.isEmpty()) {
            if (!this.expectedPartitions.equals (allPartitions)) {
                trace.info ("operator checkpoints have different allPartition sets: " + this.expectedPartitions + " <--> " + allPartitions);
            }
        }
        this.expectedPartitions.addAll (allPartitions);
        this.consolidatedOffsetMap.putAll (partialResetOffsetMap);
        trace.info ("mergePartialCheckpoint(): consolidated offset map: " + consolidatedOffsetMap + ", expected partition set: " + expectedPartitions);
        if (this.consolidatedOffsetMap.keySet().containsAll (this.expectedPartitions)) {
            trace.info("mergePartialCheckpoint() complete. Sending merge complete notification");
            //TODO: send merge complete notification, JMX clients can now fetch the merged reset offsets
            // make sure next mergePartialCheckpoint() resets the consolidated map
            this.checkpointSequenceId = ID_UNASSIGNED;
        }
    }

    /**
     * Returns the consolidated offset map.
     * @return the consolidated offset map
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#getConsolidatedOffsetMap()
     */
    @Override
    public Map<TP, Long> getConsolidatedOffsetMap() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Registers a topic partition in the Group MBean so that the MBean has 
     * the overview over all partitions that are assigned to all consumers in 
     * the Consumer group.
     * @param topic      the topic name 
     * @param partition  the partition number
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#registerAssignedPartition(java.lang.String, int)
    @Override
    public void registerAssignedPartition (String topic, int partition) {
        trace.info ("registerAssignedPartition(); topic = " + topic + ", partition = " + partition);
        CrConsumerGroupCoordinator.TP  tp = new CrConsumerGroupCoordinator.TP (topic, partition);
        if (assignedPartitions.contains (tp)) return;
        assignedPartitions.add (tp);
        try {
            byte[] serializedState = serializeControlState();
            trace.info ("size of serialized control state = " + serializedState.length);
            trace.info ("going to call persistControlState(...)");
//            java.io.IOException: javax.management.InstanceNotFoundException: com.ibm.streamsx.kafka:groupId=group1,type=consumergroup
//                    at com.ibm.streams.operator.control.AbstractPersistentControlMBean.persistControlState(AbstractPersistentControlMBean.java:58)
            persistControlState (serializedState);
        } catch (IOException e) {
            trace.error (e);
            e.printStackTrace();
        }
    }
     */


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.CrConsumerGroupCoordinatorMXBean#getAssignedPartitions()
    @Override
    public String getAssignedPartitions() {
        return assignedPartitions.toString();
    }
     */

    /**
     * clears the consolidation map and the expected partitions set.
     * @param chkptSequenceId
     */
    private void initializeMerge (long chkptSequenceId) {
        this.checkpointSequenceId = chkptSequenceId;
        this.expectedPartitions.clear();
        this.consolidatedOffsetMap.clear();
    }

    /**
     * Serialize the assignedPartitions Set to byte array 
    private byte[] serializeControlState() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream (bos)) {
            os.writeObject(assignedPartitions);
        }
        return bos.toByteArray();
    }
     */

    /**
     * @see com.ibm.streams.operator.control.PersistentControlMBean#updateControlState(byte[])
    @SuppressWarnings("unchecked")
    @Override
    public void updateControlState (byte[] controlState) {
        trace.info ("updateControlState() ... (" + controlState.length + " bytes)");
        ByteArrayInputStream bis = new ByteArrayInputStream (controlState);
        ObjectInput in;
        try {
            in = new ObjectInputStream(bis);
            this.assignedPartitions = (Set<CrConsumerGroupCoordinator.TP>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            trace.error(e);
            e.printStackTrace();
        }
    }
     */



    /**
     * This inner class represents a topic partition.
     * We do not use the TopicPartition class from the Kafka libs because this class must be available in the JCP.
     */
    public static class TP implements Serializable {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private final int partition;

        @ConstructorProperties ({"topic", "partition"})
        public TP (String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
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
            return topic + "-" + partition;
        }
    }
}
