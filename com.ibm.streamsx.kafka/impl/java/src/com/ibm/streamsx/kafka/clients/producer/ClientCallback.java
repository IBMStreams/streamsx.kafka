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
