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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author The IBM Kafka toolkit team
 */
public interface RecordProducedHandler {
    /**
     * called when the producer record has been produced successfully
     * @param seqNo     the sequence number of the {@link RecordProduceAttempt}
     * @param record    the produced record
     * @param metadata  the meta data of the produced record
     */
    void onRecordProduced (long seqNo, ProducerRecord<?, ?> record, RecordMetadata metadata);
}
