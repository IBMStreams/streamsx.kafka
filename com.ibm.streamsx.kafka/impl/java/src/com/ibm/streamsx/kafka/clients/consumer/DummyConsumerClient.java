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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class represents an ConsumerClient implementation that behaves passive.
 * It does not fetch messages and does not provide tuples.
 * 
 * @author The IBM Kafka toolkit maintainers
 * @since toolkit 3.0
 */
public class DummyConsumerClient extends AbstractKafkaClient implements ConsumerClient {

    private boolean processing = false;


    public DummyConsumerClient (OperatorContext operatorContext, KafkaOperatorProperties kafkaProperties) {
        super (operatorContext, kafkaProperties, true);
        operatorContext.getMetrics().getCustomMetric ("isGroupManagementActive").setValue (0L);
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        processing = true;
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.Collection, java.util.Collection, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Collection<String> topics, Collection<Integer> partitions, StartPosition startPosition) throws Exception {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.Collection, java.util.Collection, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Collection<String> topics, Collection<Integer> partitions, long timestamp) throws Exception {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithOffsets(java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public void subscribeToTopicsWithOffsets (String topic, List<Integer> partitions, List<Long> startOffsets) throws Exception {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopicsWithTimestamp(java.util.regex.Pattern, long)
     */
    @Override
    public void subscribeToTopicsWithTimestamp (Pattern pattern, long timestamp) throws Exception {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#subscribeToTopics(java.util.regex.Pattern, com.ibm.streamsx.kafka.clients.consumer.StartPosition)
     */
    @Override
    public void subscribeToTopics (Pattern pattern, StartPosition startPosition) throws Exception {
    }

    /**
     * @return false
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#isSubscribedOrAssigned()
     */
    @Override
    public boolean isSubscribedOrAssigned() {
        return false;
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#supports(com.ibm.streamsx.kafka.clients.consumer.ControlPortAction)
     */
    @Override
    public boolean supports (ControlPortAction action) {
        return false;
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#sendStartPollingEvent()
     */
    @Override
    public void sendStartPollingEvent() {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#sendStopPollingEvent()
     */
    @Override
    public void sendStopPollingEvent() throws InterruptedException {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onControlPortAction(com.ibm.streamsx.kafka.clients.consumer.ControlPortAction)
     */
    @Override
    public void onControlPortAction (ControlPortAction update) throws InterruptedException {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onDrain()
     */
    @Override
    public void onDrain() throws Exception {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpointRetire(long)
     */
    @Override
    public void onCheckpointRetire (long id) {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onCheckpoint (Checkpoint checkpoint) throws InterruptedException {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onReset(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onReset (Checkpoint checkpoint) throws InterruptedException {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onResetToInitialState()
     */
    @Override
    public void onResetToInitialState() throws InterruptedException {
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onShutdown(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public void onShutdown (long timeout, TimeUnit timeUnit) throws InterruptedException {
        processing = false;
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#isProcessing()
     */
    @Override
    public boolean isProcessing() {
        return processing;
    }

    /**
     * @return null
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#getNextRecord(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public ConsumerRecord<?, ?> getNextRecord (long timeout, TimeUnit timeUnit) throws InterruptedException {
        Thread.sleep (timeUnit.toMillis (timeout));
        return null;
    }

    /**
     * Empty implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#postSubmit(org.apache.kafka.clients.consumer.ConsumerRecord)
     */
    @Override
    public void postSubmit (ConsumerRecord<?, ?> submittedRecord) {
    }


    /**
     * The builder for the consumer client following the builder pattern.
     */
    public static class Builder implements ConsumerClientBuilder {

        private KafkaOperatorProperties kafkaProperties;
        private OperatorContext operatorContext;

        public final Builder setOperatorContext(OperatorContext c) {
            this.operatorContext = c;
            return this;
        }

        public final Builder setKafkaProperties(KafkaOperatorProperties p) {
            this.kafkaProperties = new KafkaOperatorProperties();
            this.kafkaProperties.putAll (p);
            return this;
        }

        @Override
        public ConsumerClient build() throws Exception {
            KafkaOperatorProperties p = new KafkaOperatorProperties();
            p.putAll (this.kafkaProperties);
            return new DummyConsumerClient (operatorContext, p);
        }

        @Override
        public int getImplementationMagic() {
            return DummyConsumerClient.class.getName().hashCode();
        }
    }
}
