---
title: "Toolkit Usage Overview"
permalink: /docs/user/overview/
excerpt: "How to use this toolkit."
last_modified_at: 2018-04-13T12:37:48+01:00
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

For both, the KafkaConsumer and the KafkaProducer there is a one-to-one relationship between tuples and Kafka messages.
Read more about how to use these operators in the [SPL documentaion](https://ibmstreams.github.io/streamsx.kafka/docs/user/SPLDoc/).

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

* [KafkaAppConfigSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAppConfigSample)
* [KafkaAttrNameParamsSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAttrNameParamsSample)
* [KafkaBlobSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaBlobSample)
* [KafkaClientIdSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaClientIdSample)
* [KafkaConsumerGroupWithConsistentRegion](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupWithConsistentRegion)
* [KafkaConsumerInputPortSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerInputPortSample)
* [KafkaConsumerGroupInputPortSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupInputPortSample)
* [KafkaConsumerLoadSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerLoadSample)
* [KafkaCRTransactionalProducer](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaCRTransactionalProducer)
* [KafkaFloatSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaFloatSample)
* [KafkaIntegerSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaIntegerSample)
* [KafkaProducerCustomPartitioner](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaProducerCustomPartitioner)
* [KafkaPublishToPartitionSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaPublishToPartitionSample)
* [KafkaSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample)
* [KafkaStartOffsetSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaStartOffsetSample)
