---
title: "KafkaProducer operator design"
permalink: /docs/user/KafkaProducerDesign
excerpt: "Describes the design of the KafkaProducer operator."
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

The KafkaProducer operator is used to produce messages on Kafka topics. The operator can be configured to produce messages to one or more topics.

### Apache Kafka - Supported Version

These operators will only support Apache Kafka v0.10.x. For older versions of Kafka, it is recommended that the Kafka operators from the **com.ibm.streamsx.messaging** toolkit be used. 

### Supported SPL Types

The operator supports the following SPL types for the key and message attributes:

 * rstring
 * int32/int64
 * uint32/uint64
 * float64, ~~float32~~ (see note below)
 * blob

**NOTE 1:** As of Kafka v0.10.2.0, there is no support for (de)serializing `java.Lang.Float` data types. Therefore, the `float32` SPL type cannot be supported as key and message attribute types (`float32` maps to `java.Lang.Float`). Support for (de)serialization`java.Lang.Float` will be added in v0.11.0.0 as per this issue: [KAFKA-4769](https://issues.apache.org/jira/browse/KAFKA-4769). Adding support `float32` data types should be re-evaluated once v0.11.0.0 is available. 

### Parameters

| Parameter Name | Default | Description |
| --- | --- | --- |
| propertiesFile | | Specifies the name of the properties file containing Kafka properties. |
| appConfigName | | Specifies the name of the application configuration containing Kafka properties. |
| userLib | '[application_dir]/etc/libs' | Allows the user to specify paths to JAR files that should be loaded into the operators classpath. This is useful if the user wants to be able to specify their own partitioners. The value of this parameter can either point to a specific JAR file, or to a directory. In the case of a directory, the operator will load all files ending in *.jar into the classpath.  By default, this parameter will load all jar files found in <application_dir>/etc/libs. |
| topic | | Specifies the topic(s) that the producer should send messages to. The value of this parameter will take precedence over the **topicAttrName** parameter. This parameter will also take precedence if the input tuple schema contains an attribute named `topic`.|
| messageAttribute | "message" | Specifies the attribute on the input port that contains the message payload. If not specified, the operator will look for an input attribute named "message". If this parameter is not specified and there is no input attribute named "message", the operator will throw an exception and terminate. |
| keyAttribute | "key" | Specifies the input attribute that contains the Kafka key value. If not specified, the operator will look for an input attribute named "key".
| topicAttribute | "topic" | Specifies the input attribute that contains the name of the topic that the message should be written to. If this parameter is not specified, the operator will attempt to look for an input attribute named `topic`. This parameter value is overridden if the **topic** parameter is specified.
| partitionAttribute | "partition" | Specifies the input attribute that contains the partition number that the message should be written to. If this parameter is not specified, the operator will look for an input attribute named **partition**. If the user does not indicate which partition the message should be written to, then Kafka's default partitioning strategy will be used instead (partition based on the specified partitioner or in a round-robin fashion). |

### Automatic Serialization

The operator will automatically select the appropriate serializers for the key and message based on their types. The following table outlines which serializer will be used given a particular type:

| Serializer | SPL Types |
| --- | --- |
| org.apache.kafka.common.serialization.StringSerializer | rstring |
| org.apache.kafka.common.serialization.IntegerSerializer | int32, uint32 |
| org.apache.kafka.common.serialization.LongSerializer | int64, uint64 |
| org.apache.kafka.common.serialization.DoubleSerializer | float64 |
| org.apache.kafka.common.serialization.ByteArraySerializer | blob |

Users can override this behaviour and specify which serializer to use by setting the `key.serializer` and `value.serializer` properties.

### Input Ports

The operator will have a single input port. The `messageAttrName` and `keyAttrName` parameters are used to specify the input attributes that contain the message and key values, respectively. Each tuple received by this operator will be written to the topic.

### Output Ports

The operator does not have any output ports.

### Consistent Region Strategy

The operator can be part of a consistent region, however it cannot be the start of a consistent region. The operator is capable of supporting at least once message delivery semantics.


### Error Handling

#### Synchronous exceptions due to send()

| Exception | Handling |
| --- | --- |
| **InterruptException** | Rethrow the exception, causing operator to restart |
| **SerializationException** | Rethrow the exception, causing operator to restart |
| **TimeoutException** - | Rethrow the exception, causing operator to restart |
| **KafkaException** - | Rethrow the exception, causing operator to restart |

#### Asynchronous exceptions caught in Callback

| Exception | Handling |
| --- | --- |
| **InvalidTopicException** | Fatal, non-retriable. Rethrow the exception, causing operator to restart. |
| **OffsetMetadataTooLargeException** | Fatal, non-retriable. Rethrow the exception, causing operator to restart. |
| **RecordBatchTooLargeException** | Fatal, non-retriable. Rethrow the exception, causing operator to restart. |
| **RecordTooLargeException** | Fatal, non-retriable. Rethrow the exception, causing operator to restart. |
| **UnknownServerException** | Fatal, non-retriable. Rethrow the exception, causing operator to restart. |
| **CorruptRecordException** | Retriable (if `retries` > 0). If caught in callback, rethrow the exception, causing operator to restart. |
