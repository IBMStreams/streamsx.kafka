/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

/**
 * This class represents the arguments of an event of type START_POLLING.
 * @author IBM Streams toolkit team.
 */
public class StartPollingEventParameters {

    private final long pollTimeoutMs;
    private final long throttlePauseMs;
    
    /**
     * @param pollTimeoutMs
     */
    public StartPollingEventParameters (long pollTimeoutMs) {
        this (pollTimeoutMs, 0);
    }
    
    /**
     * @param pollTimeoutMs
     * @param throttlePauseMs
     */
    public StartPollingEventParameters (long pollTimeoutMs, long throttlePauseMs) {
        this.pollTimeoutMs = pollTimeoutMs;
        this.throttlePauseMs = throttlePauseMs;
    }

    /**
     * @return the pollTimeoutMs
     */
    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    /**
     * @return the throttlePauseMs
     */
    public long getThrottlePauseMs() {
        return throttlePauseMs;
    }
}
