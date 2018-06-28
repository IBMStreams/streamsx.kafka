/* Generated by Streams Studio: April 24, 2017 at 1:58:21 PM EDT */
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
	@InputPortSet(description = "This port is used to specify the topic-partition offsets that the consumer should begin reading messages from. When this "
			+ "port is specified, the operator will ignore the `topic`, `partition` and `startPosition` parameters. The operator will only begin "
			+ "consuming messages once a tuple is received on this port. Each tuple received on this port will cause the operator to "
			+ "seek to the offsets for the specified topic-partitions. This works as follows: "
			+ "\\n"
			+ " * To seek to the beginning of a topic-partition, set the value of the offset to `-1.`\\n"
			+ " * To seek to the end of a topic-partition, set the value of the offset attribute to `-2.`\\n"
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
			+ "          \\\"topic\\\" : \\\"topic-name\\\",\\n"
			+ "          \\\"partition\\\" : <partition_number>,\\n" 
			+ "          \\\"offset\\\" : <offset_number>\\n" 
			+ "        },\\n" 
			+ "        ...\\n" 
			+ "      ]\\n" 	
			+ "    }\\n"
			+ "\\n"
			+ "The following convenience functions are available to aid in creating the messages: \\n"
			+ "\\n"
			+ " * `rstring addTopicPartitionMessage(rstring topic, int32 partition, int64 offset);` \\n" 
			+ "\\n"
			+ " * `rstring addTopicPartitionMessage(list<tuple<rstring topic, int32 partition, int64 offset>> topicPartitionsToAdd);` \\n" 
			+ "\\n"
			+ " * `rstring removeTopicPartitionMessage(rstring topic, int32 partition);` \\n" 
			+ "\\n"  
			+ " * `rstring removeTopicPartitionMessage(list<tuple<rstring topic, int32 partition>> topicPartitionsToRemove);`", 
			cardinality = 1, optional = true)})
@OutputPorts({
        @OutputPortSet(description = "This port produces tuples based on records read from the Kafka topic(s). A tuple will be output for "
        		+ "each record read from the Kafka topic(s).", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Icons(location16 = "icons/KafkaConsumer_16.gif", location32 = "icons/KafkaConsumer_32.gif")
public class KafkaConsumerOperator extends AbstractKafkaConsumerOperator {

	public static final String DESC = 
    		"The KafkaConsumer operator is used to consume messages from Kafka topics. " //$NON-NLS-1$
    		+ "The operator can be configured to consume messages from one or more topics, " //$NON-NLS-1$
    		+ "as well as consume messages from specific partitions within topics.\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "# Supported Kafka Version\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "This version of the toolkit supports **Apache Kafka v0.10.2, v0.11.x, and v1.0.x**.\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
    		+ "# Kafka Properties\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"The operator implements Kafka's KafkaConsumer API. As a result, it supports all " //$NON-NLS-1$
    		+ "Kafka properties that are supported by the underlying API. Properties can be " //$NON-NLS-1$
    		+ "specified in a file or in an application configuration. If specifying properties " //$NON-NLS-1$
    		+ "via a file, the **propertiesFile** parameter can be used. If specifying properties " //$NON-NLS-1$
    		+ "in an application configuration, the name of the application configuration must be " //$NON-NLS-1$
    		+ "specified using the **appConfigName** parameter. \\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"The only property that the user is required to set is the `bootstrap.servers` property, " //$NON-NLS-1$
    		+ "which points to the Kafka brokers. All other properties are optional. The operator " //$NON-NLS-1$
    		+ "sets some properties by default to enable users to quickly get started with the operator. " //$NON-NLS-1$
    		+ "The following lists which properties the operator sets by default: \\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"---\\n" +  //$NON-NLS-1$
    		"| Property Name | Default Value |\\n" +  //$NON-NLS-1$
    		"|===|\\n" +  //$NON-NLS-1$
    		"| client.id | Randomly generated ID in the form: `client-<random_string>` |\\n" +  //$NON-NLS-1$
    		"|---|\\n" +  //$NON-NLS-1$
    		"| group.id | Randomly generated ID in the form: `group-<random_string>` |\\n" +  //$NON-NLS-1$
    		"|---|\\n" +  //$NON-NLS-1$
    		"| key.deserializer | See **Automatic deserialization** section below |\\n" +  //$NON-NLS-1$
    		"|---|\\n" +  //$NON-NLS-1$
    		"| value.deserializer | See **Automatic deserialization** section below |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| auto.commit.enable | `false` |\\n" +  //$NON-NLS-1$
    		"---\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"**NOTE:** Users can override any of the above properties by explicitly setting the property " //$NON-NLS-1$
    		+ "value in either a properties file or in an application configuration.\\n" //$NON-NLS-1$
    		+ "\\n" //$NON-NLS-1$
    		+ "# Automatic Deserialization\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"The operator will automatically select the appropriate deserializers for the key and message " //$NON-NLS-1$
    		+ "based on their types. The following table outlines which deserializer will be used given a " //$NON-NLS-1$
    		+ "particular type: \\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"---\\n" +  //$NON-NLS-1$
    		"| Deserializer | SPL Types |\\n" +  //$NON-NLS-1$
    		"|===|\\n" +  //$NON-NLS-1$
    		"| org.apache.kafka.common.serialization.StringDeserializer | rstring |\\n" +  //$NON-NLS-1$
    		"|---|\\n" +  //$NON-NLS-1$
    		"| org.apache.kafka.common.serialization.IntegerDeserializer | int32, uint32 |\\n" +  //$NON-NLS-1$
    		"|---|\\n" +  //$NON-NLS-1$
    		"| org.apache.kafka.common.serialization.LongDeserializer | int64, uint64 |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| org.apache.kafka.common.serialization.FloatDeserializer | float32 |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| org.apache.kafka.common.serialization.DoubleDeserializer | float64 |\\n" +  //$NON-NLS-1$
    		"|---|\\n" +  //$NON-NLS-1$
    		"| org.apache.kafka.common.serialization.ByteArrayDeserializer | blob | \\n" +  //$NON-NLS-1$
    		"---\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"These deserializers are wrapped by extensions that catch exceptions of type "
    		+ "`org.apache.kafka.common.errors.SerializationException` to allow the operator to skip "
    		+ "over malformed messages. The used extensions do not modify the actual deserialization "
    		+ "function of the given base deserializers from the above table.\\n" +
    		"\\n" +
    		"Users can override this behaviour and specify which deserializer to use by setting the " //$NON-NLS-1$
    		+ "`key.deserializer` and `value.deserializer` properties. \\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		
    		"# Committing received Kafka messages\\n"
    		+ "\\n"
    		+ "**a) The operator is not part of a consistent region**\\n"
    		+ "\\n"
    		+ "When not explicitly specified, the operator sets the consumer property `auto.commit.enable` to `false` "
    		+ "to disable auto-committing messages by the Kafka client. After tuple submission, the consumer operator commits the offsets of "
    		+ "those Kafka messages that have been submitted as tuples. The frequency in terms of "
    		+ "number of tuples can be specified with the **commitCount** parameter. This parameter has a default value of 500. "
    		+ "Offsets are committed asynchronously.\\n"
    		+ "\\n"
    		+ "It is also possible to enable automatic committing of consumed Kafka messages. To achieve this, the consumer "
    		+ "property `auto.commit.enable` must be set to true. In addition, the frequency in milliseconds can be adjusted "
    		+ "via consumer property `auto.commit.interval.ms`, which has a default value of 5000.\\n"
    		+ "\\n"
    		+ "**b) The operator is part of a consistent region**\\n"
    		+ "\\n"
    		+ "Offsets are always committed when the consistent region drains, i.e. when the region becomes a consistent state. "
    		+ "On drain, the consumer operator commits the offsets of those Kafka messages that have been submitted as tuples. "
    		+ "When the operator is in a consistent region, all auto-commit related settings via consumer properties are "
    		+ "ignored by the operator. The parameter **commitCount** is also ignored because the commit frequency is given "
    		+ "by the trigger period of the consistent region.\\n"
    		+ "In a consistent region, offsets are committed synchronously, i.e. the offsets are committed when the drain "
    		+ "processing of the operator finishes. Commit failures result in consistent region reset.\\n"
    		+ "\\n" +
    		
    		"# Kafka's Group Management\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"The operator is capable of taking advantage of Kafka's group management functionality. " //$NON-NLS-1$
    		+ "In order for the operator to use this functionality, the following requirements " //$NON-NLS-1$
    		+ "must be met\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"* The operator cannot be in a consistent region\\n" +  //$NON-NLS-1$
    		"* The **startPosition** parameter value must be `Default`, or not specified\\n" +  //$NON-NLS-1$
    		"* None of the topics specified by the **topics** parameter can specify which partition to be assigned to\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"In addition to the above, the application needs to set the `group.id` Kafka property or the `groupId` parameter in " //$NON-NLS-1$
    		+ "order to assign the KafkaConsumer to a specific group. \\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		
    		"# Consistent Region Support\\n" +  //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
    		"The `KafkaConsumer` operator can participate in a consistent region. The operator " //$NON-NLS-1$
    		+ "can be the start of a consistent region. Both operator driven and periodic checkpointing " //$NON-NLS-1$
    		+ "are supported. If using operator driven, the **triggerCount** parameter must be set to " //$NON-NLS-1$
    		+ "indicate how often the operator should initiate a consistent region. On checkpoint, the " //$NON-NLS-1$
    		+ "operator will save the last offset for each topic-partition that it is assigned to. In the " //$NON-NLS-1$
    		+ "event of a reset, the operator will seek to the saved offset for each topic-partition and " //$NON-NLS-1$
    		+ "begin consuming messages from that point." + //$NON-NLS-1$
    		"\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
    		
			"# Error Handling\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"Many exceptions thrown by the underlying Kafka API are considered fatal. In the event that Kafa throws " //$NON-NLS-1$
			+ "an exception, the operator will restart.\\n"; //$NON-NLS-1$
}
