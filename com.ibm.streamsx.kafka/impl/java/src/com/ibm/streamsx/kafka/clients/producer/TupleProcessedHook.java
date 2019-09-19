/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer;

import com.ibm.streams.operator.Tuple;

/**
 * Interface for a Hook that can be set to implement actions 
 * when a tuple has been successufully produced or that failed.
 * @author The IBM Kafka Toolkit maintainers
 * @since toolkit version 2.2
 */
public interface TupleProcessedHook {
    /**
     * Called after a tuple has been sucessfully processed, i.e. produced.
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
