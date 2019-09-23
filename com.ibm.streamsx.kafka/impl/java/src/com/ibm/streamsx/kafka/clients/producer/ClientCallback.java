package com.ibm.streamsx.kafka.clients.producer;

import java.util.Set;

public interface ClientCallback {

    /**
     * A tuple has sucessfully processed.
     * @param seqNumber the tuple's sequence number
     */
    void tupleProcessed (long seqNumber);

    /**
     * A tuple failed to send to at least one topic, which cannot be recovered.
     * @param seqNumber the tuple's sequence number
     * @param failedTopics the topics, which failed for the tuple
     * @param lastException the last occurred exception.
     * @param initiateRecovery When set to true, the operator is tried to recover.
     */
    void tupleFailedFinally (long seqNumber, Set<String> failedTopics, Exception lastException, boolean initiateRecovery);

    
    /**
     * A tuple failed to send to at least one topic. A recovery can be tried.
     * @param seqNumber the tuple's sequence number
     * @param exception an exception if there is one.
     */
    void tupleFailedTemporarily (long seqNumber, Exception exception);
}
