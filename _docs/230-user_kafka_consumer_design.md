---
title: "KafkaConsumer operator design"
permalink: /docs/user/KafkaConsumerDesign/
excerpt: "Describes the design of the KafkaConsumer operator."
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

The KafkaConsumer operator is used to consume messages from Kafka topics. The operator can be configured to consume messages from one or more topics, as well as consume messages from specific partitions within topics. 

### Apache Kafka - Supported Version

These operators will only support Apache Kafka v0.10.x and newer. For older versions of Kafka, it is recommended that the Kafka operators from the **com.ibm.streamsx.messaging** toolkit be used.

### Supported SPL Types

The operator supports the following SPL types for the key and message attributes:

 * rstring
 * int32/int64
 * uint32/uint64
 * float32/float64
 * blob


### Parameters

| Parameter Name | Default | Description |
| --- | --- | --- |
| propertiesFile | | Specifies the name of the properties file containing Kafka properties. |
| appConfigName | | Specifies the name of the application configuration containing Kafka properties. |
| startPosition | Default | Specifies whether the operator should start reading from the end of the topic, or start reading all messages from the beginning of the topic. Valid options include: `Default`, `Beginning`, `End`, `Time`, and `Offset`. If not specified, the default value is `Default`. |
| startTime | | This parameter is only used when the **startPosition** parameter is set to `Time`. Then the operator will begin reading records from the earliest offset whose timestamp is greater than or equal to the timestamp specified by this parameter. If no offsets are found, then the operator will begin reading messages from what is is specified by the auto.offset.reset consumer property, which is latest as default value. The timestamp must be given as an 'int64' type in milliseconds since Unix epoch. |
| startOffset | | This parameter indicates the start offset that the operator should begin consuming messages from. In order for this parameter's values to take affect, the **startPosition** parameter must be set to `Offset`. Furthermore, the specific partition(s) that the operator should consume from must be specified via the **partition** parameter. |
| topic | | Specifies the topic or topics that the consumer should subscribe to. To assign the consumer to specific partitions, use the **partition** parameter. |
| groupId | *random generated* | The consumer group that the consumer belongs to |
| clientId | *random generated* | The client ID |
| partition | | Specifies the partitions that the consumer should be assigned to for each of the topics specified. It should be noted that using this parameter will "assign" the consumer to the specified topics, rather than "subscribe" to them. This implies that the consumer will not use Kafka's group management feature. |
| outputKeyAttributeName | "key" | Specifies the output attribute name that should contain the key. If not specified, the operator will attempt to store the message in an attribute named 'key'. |
| outputMessageAttributeName | "message" | Specifies the output attribute name that will contain the message. If not specified, the operator will attempt to store the message in an attribute named 'message'. |
| outputTopicAttributeName | "topic" | Specifies the output attribute name that should contain the topic. If not specified, the operator will attempt to store the message in an attribute named 'topic'. |
| outputPartitionAttributeName | "partition" | Specifies the output attribute name that should contain the partition number. If not specified, the operator will attempt to store the partition number in an attribute named 'partition'. The attribute must have the SPL type `int32` or `uint32`. |
| outputOffsetAttributeName | "offset" | Specifies the output attribute name that should contain the offset. If not specified, the operator will attempt to store the message in an attribute named 'offset'. The attribute must have the SPL type `int64` or `uint64`. |
| outputTimestampAttributeName | "messageTimestamp" | Specifies the output attribute name that should contain the record's timestamp. It is presented in milliseconds since Unix epoch.If not specified, the operator will attempt to store the message in an attribute named 'messageTimestamp'. The attribute must have the SPL type `int64` or `uint64`. |
| userLib | '[application_dir]/etc/libs' | Allows the user to specify paths to JAR files that should be loaded into the operators classpath. This is useful if the user wants to be able to specify their own partitioners. The value of this parameter can either point to a specific JAR file, or to a directory. In the case of a directory, the operator will load all files ending in *.jar into the classpath.  By default, this parameter will load all jar files found in <application_dir>/etc/libs. |
| triggerCount | | This parameter specifies the number of messages that will be submitted to the output port before initiating a checkpoint. The operator retrieves batches of messages from Kafka, and the consistent region is only started after all messages in the batch have been submitted. The implication of this is that more tuples maybe submitted by the operator before a consistent region is triggered. This parameter is only used if the operator is the start of a consistent region. |
| commitCount | 500 | This parameter specifies the number of tuples that will be submitted to the output port before committing their offsets. This parameter is only used when the operator is *not* part of a consistent region. When the operator participates in a consistent region, offsets are always committed when the region drains. |

### Automatic deserialization

The operator will automatically select the appropriate deserializers for the key and message based on their types. The following table outlines which deserializer will be used given a particular type: 

| Deserializer | SPL Types |
| --- | --- |
| org.apache.kafka.common.serialization.StringDeserializer | rstring |
| org.apache.kafka.common.serialization.IntegerDeserializer | int32, uint32 |
| org.apache.kafka.common.serialization.LongDeserializer | int64, uint64 |
| org.apache.kafka.common.serialization.DoubleDeserializer | float64 |
| org.apache.kafka.common.serialization.FloatDeserializer | float32 |
| org.apache.kafka.common.serialization.ByteArrayDeserializer | blob | 

All thses deserializers are extended by a corresponding wrapper that catches `SerializationException`.

Users can override this behaviour and specify which deserializer to use by setting the `key.deserializer` and `value.deserializer` properties. 

### Kafka's Group Management

The operator is capable of taking advantage of Kafka's group management functionality. In order for the operator to use this functionality, the following requirements must be met

 * The operator cannot be in a consistent region
 * The **startPosition** parameter must be `Default` or not specified
 * None of the topics specified by the **topics** parameter can specify which partition to be assigned to

In addition to the above, the application needs to set the `group.id` Kafka property in order to assign the KafkaConsumer to a specific group. 

### Consistent Region Support

The operator can be in a consistent region and can be the start of a consistent region. The operator behaves as follows during the consistent region stages: 

#### Drain
The operator stops submitting tuples and stops polling for new Kafka messages. The offsets of the submitted tuples are committed synchronously. 

#### Checkpoint
The operator will save the last offset position that the KafkaConsumer client retrieved messages from. During reset, the operator will consume records starting from this offset position.
After checkpointing, the operator will start consuming messages from the Kafka broker and buffer them internally. When the operator acquires a permit to submit tuples, it starts submitting tuples.

#### Reset
The operator will seek to the offset position saved in the checkpoint. The operator will begin consuming records starting from this position. 

#### ResetToInitialState
The first time the operator was started, the initial offset that the KafkaConsumer client would begin reading from was stored in the JCP operator. When `resetToInitialState()` is called, the operator will retrieve this initial offset from the JCP and seek to this position. The operator will begin consumer records starting from this position. 

### Input Ports

The operator has a control input port, that can be used to assign and de-assign topic partitions with offsets from which the operator is to consume tuples.
When the operator is configured with the optional input port, the parameters **topic**, **partition**, **startPosition** and related are ignored. Kafka's group
management is disabled when the control port is used because the consumer manually assigns to topic partitions.

### Output Ports

The operator has a single output port. Each individual record retrieved from each of the Kafka topics will be submitted as a tuple to this output port. The `outputKeyAttributeName`, `outputMessageAttributeName` and `outputTopicAttributeName` parameters are used to specify the attributes that will contain the record contents.

### Error Handling

#### Exception due to poll()

| Exception | Handling |
| --- | --- |
| SerializationException | The Kafka message is dropped, and a metric is increased. This exception can happen if there is a message on the queue that the Consumer is unable to deserialize with the specified deserializer. See also [KAFKA-4740](https://issues.apache.org/jira/browse/KAFKA-4740) |
| InvalidOffsetException | It is unlikely that this exception will be encountered. If this is encountered, a RuntimeException will be thrown resulting in the operator restarting. |
| WakeupException | Should not be encountered as there is no place in the code where KafkaConsumer.wakeup() is being called. If for some reason this exception is encountered, a RuntimeException will be thrown, causing the operator to restart. |
| InterruptException | A RuntimeException exception will be thrown, causing the operator to restart. |
| KafkaException | This is described as a catch-all for unrecoverable errors (both existing and future errors). As this is described as "unrecoverable", the operator will throw a RuntimeException. |
| java.lang.IllegalArgumentException | Only occurs if the timeout value set on poll() is negative. Since the timeout is controlled internally, this exception should not occur |
| java.lang.IllegalStateException | Only occurs if the operator is not subscribed to any topics or manually assigned to any partitions. The operator will perform internal checks to make sure that at least one topic is being subscribed (assigned) to. Therefore, this exception should not occur. |
