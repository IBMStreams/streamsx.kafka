---
title: "Usecase: Kafka Consumer Group"
permalink: /docs/user/UsecaseConsumerGroup/
excerpt: "How to use this toolkit."
last_modified_at: 2018-10-17T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# Overview

Multiple `KafkaConsumer` operators consume from the same topic(s) where the topic partitions are automatically distributed over the consumer operators.

* Continual processing of messages from all partitions in the event of failure
* No assumption about which partition is consumed by which consumer operator, thus no guarantee that a message with key 'K' will be processed by the same operator.
* When partitions are added to the subscribed topic, these new partitions will be automatically assigned to one of the consumers in the group.

# Details

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

# Pros and Contras

* **Pro:** High volume by having multiple operators reading messages in parallel from partitions
* **Pro:** Takeover of partitions from failed or stopped consumers by other members of the consumer group.
* **Pro:** No manual assignment of partitions, any number of operators will always correctly read all messages.
* **Con:** Keyed messages may be handled by any operator after failure and reassignment. As a workaround, the messages can be repartitioned by the message key in the Streams application with abutting parallel region.

# Guaranteed processing

* Consistent region: Supported (periodic only)
* Checkpointing via `config checkpoint`: Not supported

When the operator is used in a consistent region, at least once processing through the Streams application is guaranteed.
Without a consistent region, tuples can get lost within the Streams application when a PE restarts.

# Operator configuration

## Parameters / consumer properties

* No assignment of partitions is configured through the **partition** operator parameter.
* A group identifier must be specified either by the consumer property `group.id`, or by using the **groupId** parameter, which would have precedence over a bare property.
* When not in a consistent region, the **startPosition** parameter must not be specified or must have the value `Default`.
* When in a consistent region, the **startPosition** parameter must not be `Offset`.


## Operator placement

Invocations of consumer operators should be exlocated from each other (separate PEs) to ensure upon failure multiple consumers are not taken out.

## Consistent region

The consumer group must not have consumers outside of the consistent region.

## Multiple copies

* Create a composite containing the `KafkaConsumer` invocation
* Annotate composite invocation with `@parallel` with width N (e.g. `width=3` to handle 6 partitions).

or

* Invoke N copies of the operator.

# Examples
## Without consistent region

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

## Consumer group in a consistent region, group-ID specified in property file

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
