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
            + "This port is used to specify the topic-partition offsets that the consumer should begin reading messages from. When this "
            + "port is specified, the operator will ignore the `topic`, `partition` and `startPosition` parameters. The operator will only begin "
            + "consuming messages once a tuple is received on this port. Each tuple received on this port will cause the operator to "
            + "seek to the offsets for the specified topic-partitions. This works as follows: "
            + "\\n"
            + " * To seek to the beginning of a topic-partition, set the value of the offset to `-2.`\\n"
            + " * To seek to the end of a topic-partition, set the value of the offset attribute to `-1.`\\n"
            + " * To start fetching from the default position, omit the offset attribute or set the value of the offset to `-3`\\n"
            + " * Any other value will cause the operator to seek to that offset value. If that value does not exist, then the operator will use the "
            + "`auto.offset.reset` policy to determine where to begin reading messages from.\\n"
            + "\\n"
            + "This input port must contain a single `rstring` attribute. In order to add or remove a topic partition, the attribute must contain "
            + "a JSON string in the following format: \\n"
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
            + "The following types and convenience functions are available to aid in creating the messages: \\n"
            + "\\n"
            + "* `type Control.TopicPartition = rstring topic, int32 partition;`\\n"
            + "* `type Control.TopicPartitionOffset = rstring topic, int32 partition, int64 offset;`\\n"
            + "* `rstring addTopicPartitionMessage (rstring topic, int32 partition, int64 offset);`\\n" 
            + "* `rstring addTopicPartitionMessage (rstring topic, int32 partition);`\\n" 
            + "* `rstring addTopicPartitionMessage (list<Control.TopicPartitionOffset> topicPartitionsToAdd);`\\n" 
            + "* `rstring addTopicPartitionMessage (list<Control.TopicPartition> topicPartitionsToAdd);`\\n" 
            + "* `rstring removeTopicPartitionMessage (rstring topic, int32 partition);`\\n" 
            + "* `rstring removeTopicPartitionMessage (list<Control.TopicPartition> topicPartitionsToRemove);`\\n"
            + "\\n"
            + "**Important Note:** This input port must not receive a final punctuation. Final markers are automatically "
            + "forwarded causing downstream operators close their input ports. When this input port receives a final marker, "
            + "it will stop fetching Kafka messages and stop submitting tuples.", 
            cardinality = 1, optional = true, controlPort = true)})
@OutputPorts({
    @OutputPortSet(description = "This port produces tuples based on records read from the Kafka topic(s). A tuple will be output for "
            + "each record read from the Kafka topic(s).", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating)})
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
            + "This version of the toolkit supports **Apache Kafka v0.10.2, v0.11.x, 1.0.x, 1.1.x, v2.0.x, v2.1.x, v2.2.x**, and **v2.3.x**.\\n"
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
