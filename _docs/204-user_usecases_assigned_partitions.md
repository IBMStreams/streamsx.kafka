---
title: "Usecase: Assigned Partitions"
permalink: /docs/user/UsecaseAssignedPartitions/
excerpt: "How to use this toolkit."
last_modified_at: 2020-10-07T09:10:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# Overview

Multiple `KafkaConsumer` operators reading from the same topic(s). Each operator assigned a
specific set of partitions. With keyed partitions it is guaranteed that a key is
always processed by the same sub-flow/channel.

# Details

Each operator is uniquely assigned a subset of the topic's partitions, so that all
partitions are covered by the set of operators reasoning.

# Pros and Contras

* Pro: High volume by having multiple operators reading messages in parallel from partitions.
* Pro: Keyed messages consistently processed by the same source operator and downstream
  flow (channel in UDP) so for example can build up state for single customer (key) in single channel.
* Con: Need to manually assign partitions:
    * Potential to configure and miss partitions leading to missing data
    * Only 1-1 is a simple case (one consumer is assigned to one partition), needs
      changing UDP width when partitions are added to the topic
    * Need to refactor SPL code if using UDP and not 1-1 (e.g. with 12 partitions code for
      6 operators is different to 4).
    * Changing width on UDP region might break application (missed messages).
* Con: Delay in recovering from failure governed by operator restart time

# Guaranteed processing

* Consistent region: Supported (periodic and operator driven under certain conditions)
* Checkpointing via `config checkpoint`: Supported, but ignored. The operator does not save any data.

When the operator is used in a consistent region, at least once processing through
the Streams application is guaranteed. Without a consistent region, tuples can get
lost within the Streams application when a PE restarts.

## Periodic consistent region and UDP

* region per parallel channel - reachability of consumer operators is limited to the channel.
* single region
    * reachability of consumer operators is a single region, i.e. there is downstream
      processing after the parallel region that joins the outputs.
    * `@consistent` annotation is on the composite containing parallel region.

## Operator driven consistent region and UDP

* The **triggerCount** parameter must be used to specify the number of submitted
  tuples after which the region is made consistent
* One region per parallel channel with one consumer operator per channel - reachability of
  consumer operators is limited to the channel. (single region is not supported as only a
  single source operator can drive the region, and in a UDP setting all source operators
  have the same configuraton).

With manual invocation (e.g. explicitly invoke six operators with their own unique partition(s))
then any consistent region setup can be created, subject to the limitation of a region
can only have a single operator driven source.
<https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.1/com.ibm.streams.ref.doc/doc/consistentannotation.html>

# Operator configuration

## Parameters / consumer properties

* **partition** - set to specific partition to consume
    * With manual invocation of multiple operators partitions are explicitly defined, e.g. `partition: 0, 1;`
    * With UDP `getChannel()` can be used, e.g. simple case of 1-1 channel to partition: `partition: getChannel();`
* **groupId** - can be used to identify the consumers by the server to get started with last committed offset when a Streams job is cancelled and re-submitted; crucial for the mode of partition assignment (manual/automatic), is the use of the **partition** parameter.

## Operator placement

Invocations of consumer operators should be exlocated from each other (separate PEs)
to ensure upon failure multiple consumers are not taken out.

## Multiple copies

* Create a composite containing a consumer operator invocation - it is sensible to add more
  operators, for example a parser
* Set **partition** operator parameter based upon `getChannel()`
* Annotate composite invocation with `@parallel` with width N (e.g. `width = 6` to handle 6 partitions).

or

* Invoke N copies of the operator.
* Set **partition** operator parameter for each operator so that all partitions are
  consumed once and once only by the N operators.

# Examples

## Consume a thee partition topic with 1-to-1 relation

This example has a dedicated consumer operator for each partition. The number of partitions
can be set at job submission time via a submission time parameter. In this example, every parallel
channel is a separate consisitent region with one trigger operator, so that an operator driven
consistent region can be configured.

```
public composite Assigned3Partitions {
param
    expression <int32> $nPartitions: (int32) getSubmissionTimeValue ("nPartitions", "3");
graph
    () as JCP = JobControlPlane() {}

    @consistent (trigger = operatorDriven/*periodic, period = 60.0*/)
    @parallel (width = $nPartitions)
    () as PartitionChannel = SinglePartitionConsumer() {
        param
            topic: "myTopic";
            partition: getChannel();
        config placement: partitionExlocation ("A");
    }
}

public composite SinglePartitionConsumer {
param
    expression <rstring> $topic;
    expression <int32> $partition;
graph
    stream <rstring json> Messages = KafkaConsumer() {
        param
            propertiesFile: "etc/consumer.properties";
            topic: $topic;
            partition: $partition;
            triggerCount: 1000;
            outputMessageAttributeName: "json";
    }

    // ... any other processing including the Sink for the stream
    () as X = Custom (Messages) {
    }
}
```

## Consume six partitions with 2-to-1 relation

The below example assigns a number of partitions, here 6, to a number of consumers,
each one in a parallel channel, where each consumer takes two partitions.

```
public composite Assigned6Partitions {
param
    expression <int32> $nPartitions: 6;
graph
    @parallel (width = ($nPartitions +1) /2)
    () as PartitionChannel = TwoPartitionConsumer() {
        param
            topic: "myTopic";
            partition1: getChannel() *2;
            partition2: getChannel() *2 + ((getChannel() *2 +1) > ($nPartitions -1)? 0: 1);
        config placement: partitionExlocation ("A");
    }
}

public composite TwoPartitionConsumer {
param
    expression <rstring> $topic;
    expression <int32> $partition1;
    expression <int32> $partition2;
graph
    stream <rstring json> Messages = KafkaConsumer() {
        param
            propertiesFile: "etc/consumer.properties";
            topic: $topic;
            partition: $partition1, $partition2;
            outputMessageAttributeName: "json";
    }

    // ... any other processing including the Sink for the stream
    () as X = Custom (Messages) {
    }
}
```

**Partitions per parallel channel for six partitions:**

| channel number | partition parameter |
| --- | --- |
| 0 | partition: 0, 1; |
| 1 | partition: 2, 3; |
| 2 | partition: 4, 5; |


If the number of partitions would be chosen as an **odd number**, the channel with
the highest channel number is assigned one partition. Its **partition** parameter
has twice the same number, for example, `partition: 4, 4;`, when `$nPartitions: 5;` is configured.

**Partitions per parallel channel for five partitions:**

| channel number | partition parameter |
| --- | --- |
| 0 | partition: 0, 1; |
| 1 | partition: 2, 3; |
| 2 | partition: 4, 4; |

