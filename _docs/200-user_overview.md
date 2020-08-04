---
title: "Toolkit Usage Overview"
permalink: /docs/user/overview/
excerpt: "How to use this toolkit."
last_modified_at: 2020-08-04T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# Introduction
The Kafka toolkit contains two operators, the *KafkaConsumer*, and the *KafkaProducer*.
The KafkaConsumer operator consumes messages from a Kafka topic and creates Tuples which are processed by other downstream operators of the Streams application.
It is a source operator within your Streams application.

The KafkaProducer operator creates Kafka messages from tuples and acts therefore as a sink operator within your Streams application.

For the KafkaConsumer there is a one-to-one relationship between Kafka messages and tuples. For the KafkaProducer, there can be a relation of one-to-many between tuples and Kafka messages when multiple topics are specified.
Read more about how to use these operators in the [SPL documentation](https://ibmstreams.github.io/streamsx.kafka/docs/user/SPLDoc/).

# Kafka client versions
The Kafka toolkit contains the Java Kafka clients package. Which toolkit version ships with which kafka-clients version, can be found in the following table.

| Kafka toolkit version | kafka-clients version |
| --- | --- |
| 3.0.x | 2.3.1 |
| 2.x | 2.2.1 |
| 1.9.x | 2.1.1 |
| 1.3.0 - 1.8.x | 1.0.0 |
| 1.1.0 - 1.2.x | 0.10.2.1 |
| 1.0.0 | 0.10.2 |


# Common consumer patterns and use cases

## Use of consistent region

Kafka itself has the capability of at-least-once delivery from producers to consumers. To keep this delivery semantics within Streams applications consuming messages from Kafka topics, it is recommended to consider using a consistent region within the Streams application unless used operators do not support consistent region.

## Overview

Assumptions:
* One consumer operator consumes messages from one single topic with a string message, for example a JSON message
* For a production environment, the consumer starts consuming at the default start position
* Kafka guarantees no ordering of messages accross partitions.

There are three standard patterns for Streams reading messages from Kafka.
* [**All partitions**](https://ibmstreams.github.io/streamsx.kafka/docs/user/UsecaseAllPartitions/) - A single `KafkaConsumer` invocation consumes all messages from all partitions of a topic
* [**Kafka consumer group**](https://ibmstreams.github.io/streamsx.kafka/docs/user/UsecaseConsumerGroup/) - the partitions of a topic are automatically assigned to multiple `KafkaConsumer` invocations for consumption
* [**Assigned partitions**](https://ibmstreams.github.io/streamsx.kafka/docs/user/UsecaseAssignedPartitions/) - Multiple `KafkaConsumer` invocations with each invocation assigned specific partitions.

The KafkaConsumer operator needs a configuration with
[Kafka consumer properties](https://kafka.apache.org/documentation.html#consumerconfigs). These can be specified in a property file or in an application configuration. The following examples use a property file in the etc directory of the application's toolkit. Some operator parameters, like **groupId**, and **clientId** map directly to properties. Other properties are adjusted by the operator. Which one, can be reviewed in the [SPL documentation](https://ibmstreams.github.io/streamsx.kafka/docs/user/SPLDoc/) of the operators.

**Property file example**
```
bootstrap.servers=kafka-0.mydomain:9092,kafka-1.mydomain:9092,kafka-2.mydomain:9092
# property files can also contain comments and empty lines

# a consumer group identifier can also specified via 'groupId' operator parameter
group.id=myConsumerGroup
```

# Samples

* [KafkaSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample) - most basic sample
* [KafkaAppConfigSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAppConfigSample) - use an application configuration to configure consumer and producer
* [KafkaAttrNameParamsSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAttrNameParamsSample) - specify non-default attribute names for the stream schema
* [KafkaAvroSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAvroSample) - Use of Avro formatted data with Kafka
* [KafkaBlobSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaBlobSample) - consume blob type data for key and message
* [KafkaClientIdSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaClientIdSample) - specify a unique client identifier for consumers in a parallel region
* [KafkaConsumerLoadBalanceSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerLoadBalanceSample) - Kafka consumer group for load balancing
* [KafkaConsumerGroupWithConsistentRegion](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupWithConsistentRegion) - create a consumer group with at-least-once processing in a consistent region
* [KafkaConsumerInputPortSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerInputPortSample) - control the topic partitions to consume via control input port
* [KafkaConsumerGroupInputPortSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupInputPortSample) - control the topics to subscribe for a consumer group via control port
* [KafkaCRTransactionalProducer](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaCRTransactionalProducer) - produce messages within Kafka transactions
* [KafkaFloatSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaFloatSample) - use SPL `float` type for Kafka message and key
* [KafkaIntegerSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaIntegerSample) - use SPL `int32` type for Kafka message and key
* [KafkaProducerCustomPartitioner](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaProducerCustomPartitioner) - use a custom partitioner to produce messages
* [KafkaPublishToPartitionSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaPublishToPartitionSample) - produce data to a given topic partition
* [KafkaStartOffsetSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaStartOffsetSample) - start consuming at a given offset
* [KafkaJAASConfigSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaJAASConfigSample) - configure the operators with user authentication (JAAS)
