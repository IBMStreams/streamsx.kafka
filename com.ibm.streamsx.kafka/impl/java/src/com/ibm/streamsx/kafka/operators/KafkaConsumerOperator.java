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
package com.ibm.streamsx.kafka.operators;

import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(name = "KafkaConsumer", namespace = "com.ibm.streamsx.kafka", description=KafkaConsumerOperator.DESC)
@InputPorts({
    @InputPortSet (description = ""
            + "This port is used to specify the topics or topic partitions that the consumer should begin reading messages from. When this "
            + "port is specified, the `topic`, `partition` and `startPosition` parameters cannot be used. The operator will only begin "
            + "consuming messages once a tuple is received on this port, which specifies a partition assignment or a topic subscription.\\n"
            + "\\n"
            + "When the KafkaConsumer participates in a consistent region, only partition assignment via control port is supported. **The support "
            + "of consistent region with the control port is deprecated and may be removed in next major toolkit version.**\\n"
            + "\\n"
            + "When a *topic subscription* is specified, the operator benefits from Kafka's group management (Kafka assigns the partitions to consume). "
            + "When an assignment is specified in a control tuple, the operator self-assigns to the given partition(s). "
            + "Assignments and subscriptions via control port cannot be mixed. Note, that it is not possible to use both assignment and subscription, "
            + "it is also not possible to subscribe after a previous assignment and unassignment, and vice versa.\\n"
            + "This input port must contain a single `rstring` attribute that takes a JSON formatted string.\\n"
            + "\\n"
            + "**Adding or removing a topic subscription**\\n"
            + "\\n"
            + "To add or remove a topic subscription, the single `rstring` attribute must contain "
            + "a JSON string in the following format:\\n"
            + "\\n"
            + "    {\\n"
            + "      \\\"action\\\" : \\\"ADD\\\" or \\\"REMOVE\\\",\\n" 
            + "      \\\"topics\\\" : [\\n" 
            + "        {\\n"
            + "          \\\"topic\\\" : \\\"topic-name\\\"\\n"
            + "        },\\n" 
            + "        ...\\n" 
            + "      ]\\n"  
            + "    }\\n"
            + "\\n"
            + "The following types and convenience functions are available to aid in creating the JSON string: \\n"
            + "\\n"
            + "* `rstring createMessageAddTopic (rstring topic);`\\n"
            + "* `rstring createMessageAddTopics (list<rstring> topics);`\\n"
            + "* `rstring createMessageRemoveTopic (rstring topic)`\\n"
            + "* `rstring createMessageRemoveTopics (list<rstring> topics);`\\n"
            + "\\n"
            + "\\n"
            + "**Adding or removing a manual partition assignment**\\n"
            + "\\n"
            + "To add or remove a topic partition assignment, the single `rstring` attribute must contain "
            + "a JSON string in the following format:\\n"
            + "\\n"
            + "    {\\n"
            + "      \\\"action\\\" : \\\"ADD\\\" or \\\"REMOVE\\\",\\n" 
            + "      \\\"topicPartitionOffsets\\\" : [\\n" 
            + "        {\\n"
            + "          \\\"topic\\\" : \\\"topic-name\\\"\\n"
            + "          ,\\\"partition\\\" : <partition_number>\\n" 
            + "          ,\\\"offset\\\" : <offset_number>             <--- the offset attribute is optional \\n" 
            + "        },\\n" 
            + "        ...\\n" 
            + "      ]\\n" 	
            + "    }\\n"
            + "\\n"
            + "The `offset` element is optional. It specifies the offset of the first record to consume from "
            + "the topic partition. This works as follows: "
            + "\\n"
            + " * To seek to the beginning of a topic-partition, set the value of the offset to `-2.`\\n"
            + " * To seek to the end of a topic-partition, set the value of the offset attribute to `-1.`\\n"
            + " * To start fetching from the default position, omit the offset attribute or set the value of the offset to `-3`\\n"
            + " * Any other value will cause the operator to seek to that offset value. If that value does not exist, then the operator will use the "
            + "`auto.offset.reset` policy to determine where to begin reading messages from.\\n"
            + "\\n"
            + "The following types and convenience functions are available to aid in creating the JSON string: \\n"
            + "\\n"
            + "* `type Control.TopicPartition = rstring topic, int32 partition;`\\n"
            + "* `type Control.TopicPartitionOffset = rstring topic, int32 partition, int64 offset;`\\n"
            + "* `rstring createMessageRemoveTopicPartition (rstring topic, int32 partition);`\\n" 
            + "* `rstring createMessageAddTopicPartition (rstring topic, int32 partition, int64 offset);`\\n" 
            + "* `rstring createMessageAddTopicPartition (list<Control.TopicPartitionOffset> topicPartitionsToAdd);`\\n" 
            + "* `rstring createMessageAddTopicPartition (list<Control.TopicPartition> topicPartitionsToAdd);`\\n"
            + "* `rstring createMessageRemoveTopicPartition (rstring topic, int32 partition);`\\n" 
            + "* `rstring createMessageRemoveTopicPartition (list<Control.TopicPartition> topicPartitionsToRemove);`\\n"
            + "\\n"
            + "**Important Note:** This input port must not receive a final punctuation. Final markers are automatically "
            + "forwarded causing downstream operators close their input ports. When this input port receives a final marker, "
            + "it will stop fetching Kafka messages and stop submitting tuples.", 
            cardinality = 1, optional = true, controlPort = true)})
@OutputPorts({
    @OutputPortSet(description = "This port produces tuples based on records read from the Kafka topic(s). A tuple will be output for "
            + "each record read from the Kafka topic(s).", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Free)})
@Icons(location16 = "icons/KafkaConsumer_16.gif", location32 = "icons/KafkaConsumer_32.gif")
public class KafkaConsumerOperator extends AbstractKafkaConsumerOperator {

    public static final String DESC = ""
            + "The KafkaConsumer operator is used to consume messages from Kafka topics. "
            + "The operator can be configured to consume messages from one or more topics, "
            + "as well as consume messages from specific partitions within topics.\\n"
            + "\\n"
            + "The standard use patterns for the KafkaConsumer operator are described in the "
            + "[https://ibmstreams.github.io/streamsx.kafka/docs/user/overview/|overview] of the user documentation.\\n"
            + "\\n"
            + "# Supported Kafka Versions\\n"
            + "\\n"
            + "This version of the toolkit supports **Apache Kafka 0.10.2, 0.11, 1.0, 1.1, and all 2.x versions up to 2.7**.\\n"
            + "\\n"
            + KafkaSplDoc.CONSUMER_CONNECT_WITH_KAFKA_010
            + "\\n"
            + "# Kafka Properties\\n"
            + "\\n"
            + KafkaSplDoc.CONSUMER_WHERE_TO_FIND_PROPERTIES
            + "Properties can be "
            + "specified in a file or in an application configuration. If specifying properties "
            + "via a file, the **propertiesFile** parameter can be used. If specifying properties "
            + "in an application configuration, the name of the application configuration must be "
            + "specified using the **appConfigName** parameter.\\n"
            + "\\n"
            + "The only property that the user is required to set is the `bootstrap.servers` property, "
            + "which points to the Kafka brokers, for example `kafka-0.mydomain:9092,kafka-1.mydomain:9092,kafka-2.mydomain:9092`. "
            + "All other properties are optional.\\n"
            + "\\n"
            + "The operator sets or adjusts some properties by default to enable users to quickly get started with the operator. "
            + "The following lists which properties the operator sets by default: \\n"
            + "\\n"
            + KafkaSplDoc.CONSUMER_DEFAULT_AND_ADJUSTED_PROPERTIES
            + "\\n"
            + KafkaSplDoc.CONSUMER_AUTOMATIC_DESERIALIZATION
            + "\\n"
            + KafkaSplDoc.CONSUMER_EXPOSED_KAFKA_METRICS
            + "\\n"
            + KafkaSplDoc.CONSUMER_COMMITTING_OFFSETS
            + "\\n"
            + KafkaSplDoc.CONSUMER_KAFKA_GROUP_MANAGEMENT
            + "\\n"
            + KafkaSplDoc.CONSUMER_STATIC_GROUP_MEMBERSHIP
            + "\\n"
            + KafkaSplDoc.CONSUMER_INCREMENTAL_COOPERATIVE_REBALANCING
            + "\\n"
            + KafkaSplDoc.CONSUMER_CHECKPOINTING_CONFIG
            + "\\n"
            + KafkaSplDoc.CONSUMER_RESTART_BEHAVIOUR
            + "\\n"
            + KafkaSplDoc.CONSUMER_CONSISTENT_REGION_SUPPORT
            + "\\n"
            + "# Error Handling\\n"
            + "\\n"
            + "Many exceptions thrown by the underlying Kafka API are considered fatal. In the event that Kafka throws "
            + "an exception, the operator will restart.\\n";
}
