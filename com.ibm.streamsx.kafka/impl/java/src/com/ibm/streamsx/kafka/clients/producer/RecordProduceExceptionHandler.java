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

import org.apache.kafka.common.TopicPartition;

/**
 * @author The IBM Kafka toolkit team
 */
public interface RecordProduceExceptionHandler {
    /**
     * Called when an exception occurred asynchronous producing a record.
     * 
     * @param seqNo  the sequence number of the {@link RecordProduceAttempt}
     * @param tp     the failed topic partition; note that the partition may be undefined (-1)
     * @param e      the exception
     * @param nProducerGenerations the number of producer generations for the record.
     *                             This number should be the same as the number of send attempts.
     */
    void onRecordProduceException (long seqNo, TopicPartition tp, Exception e, int nProducerGenerations);
}
