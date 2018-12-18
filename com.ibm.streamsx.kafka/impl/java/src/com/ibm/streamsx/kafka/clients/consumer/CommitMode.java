/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

/**
 * Mode for committing offsets
 * 
 * @author The IBM Kafka toolkit team
 */
public enum CommitMode {
    TupleCount,
    Time,
    
    ConsistentRegionDrain,
    Checkpoint
}
