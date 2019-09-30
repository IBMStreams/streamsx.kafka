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
