/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer.queuing;

import com.ibm.streams.operator.Tuple;

/**
 * 
 * @author The IBM Kafka Toolkit maintainers
 */
public interface TupleProcessedHook {
    /**
     * Called after a tuple has been sucessfully processed.
     * @param tuple The tuple that has been processed
     */
    void onTupleProduced (Tuple tuple);
    
    /**
     * Called when a tuple could not be produced on all topics.
     * @param tuple The tuple that failed.
     * @param failure the failure what went wrong with the tuple
     */
    void onTupleFailed (Tuple tuple, FailureDescription failure);
}
