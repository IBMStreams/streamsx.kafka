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
Read more about how to use these operators in the [SPL documentaion](https://ibmstreams.github.io/streamsx.kafka/docs/user/SPLDoc/).

### Samples

* [KafkaAppConfigSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAppConfigSample)
* [KafkaAttrNameParamsSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAttrNameParamsSample)
* [KafkaBlobSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaBlobSample)
* [KafkaClientIdSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaClientIdSample)
* [KafkaConsumerGroupWithConsistentRegion](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupWithConsistentRegion)
* [KafkaConsumerInputPortSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerInputPortSample)
* [KafkaConsumerLoadSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerLoadSample)
* [KafkaCRTransactionalProducer](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaCRTransactionalProducer)
* [KafkaFloatSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaFloatSample)
* [KafkaIntegerSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaIntegerSample)
* [KafkaProducerCustomPartitioner](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaProducerCustomPartitioner)
* [KafkaPublishToPartitionSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaPublishToPartitionSample)
* [KafkaSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample)
* [KafkaStartOffsetSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaStartOffsetSample)

### Common consumer patterns and use cases

#### Use of consistent region

Kafka itself has the capability of at-least-once delivery from producers to consumers. To keep this delivery semantics within Streams applications that consume messages from Kafka topics, it is recommended to consider using a consistent region within the Streams application unless used operators do not support consistent region.

#### Overview

Assumptions:
* One consumer operator consumes messages from one single topic with a string message, for example a JSON message
* For a production environment, the consumer starts consuming at the default start position
* Kafka guarantees no ordering of messages accross partitions.

There are three standard patterns for Streams reading messages from Kafka.
* **All partitions** - A single `KafkaConsumer` invocation consumes all messages from all partitions of a topic
* **Kafka consumer group** - the partitions of a topic are automatically assigned to multiple `KafkaConsumer` invocations for consumption
* **Assigned partitions** - Multiple `KafkaConsumer` invocations with each invocation assigned specific partitions.

The KafkaConsumer operator needs a configuration with 
[Kafka consumer properties](https://kafka.apache.org/10/documentation.html#newconsumerconfigs). These can be specified in a property file or in an application configuration. The following examples use a property file in the etc directory of the application's toolkit.

**Property file example**

```
bootstrap.servers=kafka-0.mydomain:9092,kafka-1.mydomain:9092,kafka-2.mydomain:9092
# property files can also contain comments and empty lines

# a consumer group identifier can also specified via 'groupId' operator parameter
group.id=myConsumerGroup
```

#### All partitions

##### Overview

A single `KafkaConsumer` operator consumes all messages from a topic regardless of the number of partitions.

##### Details

Without a partition specification, the operator will consume from all partitions of the topic. When a unique group identifier is given for the operator, the partitions of the subscribed topic are assigned by Kafka, and the operator represents a consumer group with only one member. In this case the operator will automatically be assigned new partitions, when partitions are added to the topic. On the other side, partition de-assignment and re-assignment can happen when

* Group management related timers expire
* The broker node being the group coordinator goes down
* Meta data of the subscribed topic changes, for example the number of partitions

Partition re-assignment makes the consumer replay Kafka messages beginning with last committed offsets. 

When **no group identifier is given**, the operator self-assignes to all partitions of the topic. When new partitions are added to the topic, the PE that contains the operator must be restarted to read also added partitions.

##### Pros and Cons

* **Pro:** Very simple
* **Con:** Volume is limited by a single operator reading messages from all partitions

##### Guaranteed processing

* Consistent region: Supported (periodic and operator driven)
* Checkpointing via `config checkpoint`: Not supported

When the operator is used in a consistent region, at least once processing through the Streams application is guaranteed.

Without a consistent region, tuples can get lost within the Streams application. When the consumer's PE restarts due to failure or PE relocation, the messages are consumed at least once, but  when an intermediate PE restarts, tuples can get lost.

##### Operator configuration

No assignment of partitions is configured through the **partition** operator parameter.

**Consume messages without the key**:

```
    stream <rstring json> Messages = KafkaConsumer() {
        param
            propertiesFile: getThisToolkitDir() + "/etc/consumer.properties";
            topic: "myTopic";
            outputMessageAttributeName: "json";
    }
```

**Consume keyed messages within an operator driven consistent region:**

```
    () as JCP = JobControlPlane() {}
    
    @consistent (trigger = operatorDriven)
    stream <rstring json, rstring messageKey> Messages = KafkaConsumer() {
        param
            propertiesFile: getThisToolkitDir() + "/etc/consumer.properties";
            topic: "myTopic";
            outputMessageAttributeName: "json";
            outputKeyAttributeName: "messageKey";
            triggerCount: 10000;   // make the region consistent every 10000 tuples
    }

```

**Consume keyed messages within a periodic consistent region:**

```
    () as JCP = JobControlPlane() {}
    
    @consistent (trigger = periodic, period = 60.0)
    stream <rstring message, rstring key> Messages = KafkaConsumer() {
        param
            propertiesFile: getThisToolkitDir() + "/etc/consumer.properties";
            topic: "myTopic";
    }
```

`message` and `key` are the default attribute names for the Kafka message and the key. They need not be specified.

#### Kafka consumer group

##### Overview

Multiple `KafkaConsumer` operators consume from the same topic(s) where the topic partitions are automatically distributed over the consumer operators.

* Continual processing of messages from all partitions in the event of failure
* No assumption about which partition is consumed by which consumer operator, thus no guarantee that a message with key 'K' will be processed by the same operator.
* When partitions are added to the subscribed topic, these new partitions will be automatically assigned to one of the consumers in the group.

##### Details

`N` Consumer operators within a single streams graph (using UDP or manually added to graph) have the same consumer group id (Kafka property `group.id`) accessing `M` partitions where (typically) N <= M.

Kafka will:

* automatically assign operators to partitions
* reassign partitions during a failure

If an operator or resource `Cx` fails then while the operator is restarting messages in the partition previously assigned to `Cx` will continue to be consumed by reassigning the partition to an existing operator `Cy`. When `Cx` recovers, it will re-join the group, and the partitions will be re-balanced.

More operators than partitions can be used (N > M) with N-M operators being idle until a failure occurs and they get (potentially) reassigned by the broker.

**Examples with six partitions:**

* Three operators
    * each will pick two partitions (1/3 of the messages assuming even load distribution over partitions) during normal operation
    * when one operator is stopped or down for a longer period of time, the six partitions are distributed across the two remaining operators, each with three
    * Once the failed operator restarts, the partitions will again be redistributed across the three operators
* Six operators
    * each operator will pick one partition during normal operation, processing 1/6 of message volume
    * on failure of one operator, one of the remaining five will take over the partition, one of the operators will consume two partitions, the other four one.
* Seven operators
    * six operators will pick up a partition each, one operator will be idle.
    * on failure of one operator, the idle operator takes the partition of the failed one.

Partition de-assignment and re-assignment can happen when

* Group management related timers expire, for example the heart-beet timeout `session.timeout.ms` or the poll timeout `max.poll.interval.ms`.
* The broker node being the group coordinator goes down
* Meta data of the subscribed topic changes, for example the number of partitions

Partition re-assignment makes the consumer replay Kafka messages beginning with last committed offsets. 

The **startPosition** parameter must be `Default` or not specified when the operator is not used in a consistent region. In a consistent region, all values except `Offset` can be used for the initial start position.

##### Pros and Cons

* **Pro:** High volume by having multiple operators reading messages in parallel from partitions
* **Pro:** Takeover of partitions from failed or stopped consumers by other members of the consumer group.
* **Pro:** No manual assignment of partitions, any number of operators will always correctly read all messages.
* **Con:** Keyed messages may be handled by any operator after failure and reassignment. As a workaround, the messages can be repartitioned by the message key in the Streams application with abutting parallel region.

##### Guaranteed processing

* Consistent region: Supported (periodic only)
* Checkpointing via `config checkpoint`: Not supported

When the operator is used in a consistent region, at least once processing through the Streams application is guaranteed.

Without a consistent region, tuples can get lost within the Streams application. When the consumer's PE restarts due to failure or PE relocation, the messages are consumed at least once, but  when an intermediate PE restarts, tuples can get lost.

##### Operator configuration

**parameters / consumer properties**

* No assignment of partitions is configured through the **partition** operator parameter.
* A group identifier must be specified either by the consumer property `group.id`, or by using the **groupId** parameter, which would have precedence over a bare property.
* When not in a consistent region, the **startPosition** parameter must not be specified or must have the value `Default`.

**operator placement**

Invocations of consumer operators should be exlocated from each other (separate PEs) to ensure upon failure multiple consumers are not taken out.

**Example without consistent region**

```
composite ConsumerGroup {
param
    expression <int32> $N: (int32) getSubmissionTimeValue ("consumerGroupSize", "3");
graph
    @parallel (width = $N)
    stream <rstring json, rstring messageKey> Messages = KafkaConsumer() {
        param
            propertiesFile: getThisToolkitDir() + "/etc/consumer.properties";
            topic: "myTopic";
            groupId: "myConsumerGroup";
            outputMessageAttributeName: "json";
            outputKeyAttributeName: "messageKey";
            commitCount: 1000;      // commit every 1000 messages
        config placement: partitionExlocation ("A");
    }
    
    // do partitioned processing in Streams
    // messages with same key go always into the same parallel channel
    @parallel (width = 4, partitionBy = [{port = Messages, attributes = [messageKey]}])
    stream <rstring json> Processed = StatefulLogic (Messages) {
    }
}

public composite StatefulLogic (input In; output Out) {
graph
    // In fact, this logic is stateless ...
    stream <rstring json> Out = Functor (In) {
    }
}
```

**Example with consumer group in a consistent region, group-ID specified in property file**

```
composite ConsumerGroupCR {
param
    expression <int32> $N: (int32) getSubmissionTimeValue ("consumerGroupSize", "3");
graph
    () as JCP = JobControlPlane() {}
    
    @consistent (trigger = periodic, period = 60.0, drainTimeout = 300.0, maxConsecutiveResetAttempts = 10)
    @parallel (width = $N)
    stream <rstring json, rstring messageKey> Messages = KafkaConsumer() {
        param
            propertiesFile: getThisToolkitDir() + "/etc/consumer.properties";
            topic: "myTopic";
            outputMessageAttributeName: "json";
            outputKeyAttributeName: "messageKey";
        config placement: partitionExlocation ("A");
    }
    
    // do partitioned processing in Streams
    // messages with same key go always into the same parallel channel
    @parallel (width = 4, partitionBy = [{port = Messages, attributes = [messageKey]}])
    stream <rstring json> Processed = Processing (Messages) {
    }
}

public composite Processing (input In; output Out) {
graph
    // In fact, this logic is stateless ...
    stream <rstring json> Out = Functor (In) {
    }
}
```

The `etc/consumer.properties` file must contain a line with

```
group.id=myConsumerGroup
```

additional to the other properties, like `bootstrap.servers`.


#### Assigned partitions

##### Overview
##### Details
##### Pros and Cons
##### Guaranteed processing
##### Operator configuration



