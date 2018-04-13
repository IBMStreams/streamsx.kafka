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

The Kafka toolkit contains two operators, the *KafkaConsumer*, and the *KafkaProducer*.
The KafkaConsumer operator consumes messages from a Kafka topic and creates Tuples which are processed by other downstream operators of the Streams application.
It is a source operator within your Streams application.

The KafkaProducer operator creates Kafka messages from tuples and acts therefore as a sink operator within your Streams application.

For both, the KafkaConsumer and the KafkaProducer there is a one-to-one relationship between tuples and Kafka messages.
Read more about how to use these operators in the [SPL documentaion](/streamsx.kafka/doc/spldoc/html/index.html).

### Samples

* [KafkaAppConfigSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAppConfigSample)
* [KafkaAttrNameParamsSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAttrNameParamsSample)
* [KafkaBlobSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaBlobSample)
* [KafkaConsumerLoadSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerLoadSample)
* [KafkaCRTransactionalProducer](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaCRTransactionalProducer)
* [KafkaFloatSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaFloatSample)
* [KafkaIntegerSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaIntegerSample)
* [KafkaProducerCustomPartitioner](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaProducerCustomPartitioner)
* [KafkaPublishToPartitionSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaPublishToPartitionSample)
* [KafkaSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample)
* [KafkaClientIdSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaClientIdSample)
* [KafkaConsumerInputPortSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerInputPortSample)
