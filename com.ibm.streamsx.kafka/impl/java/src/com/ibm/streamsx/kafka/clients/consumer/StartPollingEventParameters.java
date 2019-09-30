/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
