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
package com.ibm.streamsx.kafka.test.utils;

public interface Constants {

    public static final String TOPIC_TEST = "test";
    public static final String TOPIC_OTHER1 = "other1";
    public static final String TOPIC_OTHER2 = "other2";
    public static final String TOPIC_POS = "position";
    public static final String APP_CONFIG = "kafka-test";

    public static final String KafkaProducerOp = "com.ibm.streamsx.kafka::KafkaProducer";
    public static final String KafkaConsumerOp = "com.ibm.streamsx.kafka::KafkaConsumer";

    public static final Long PRODUCER_DELAY = 5000l;

    public static final String[] STRING_DATA = {"dog", "cat", "bird", "fish", "lion", "wolf", "elephant", "zebra", "monkey", "bat"};
    public static final String PROPERTIES_FILE_PATH = "etc/brokers.properties";
}
