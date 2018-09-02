---
title: "Kafka Consumer group support in a consistent region"
permalink: /docs/user/ConsumerGroupConsistentRegion/
excerpt: "Functional description how consumer groups are supported in a consistent region"
last_modified_at: 2018-08-31T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{% include editme %}

# Support of Consumer Groups in a consistent region

In Apache Kafka, the consumer group concept is a way of achieving two things:

1. Having consumers as part of the same consumer group means providing the “competing consumers” pattern with whom the messages from topic partitions are spread across the members of the group. Each consumer receives messages from one or more partitions (“automatically” assigned to it) and the same messages won’t be received by the other consumers (assigned to different partitions). In this way, we can scale the number of the consumers up to the number of the partitions (having one consumer reading only one partition); in this case, a new consumer joining the group will be in an idle state without being assigned to any partition.

2. Having consumers as part of different consumer groups means providing the “publish/subscribe” pattern where the messages from topic partitions are sent to all the consumers across the different groups. It means that inside the same consumer group, we’ll have the rules explained above, but across different groups, the consumers will receive the same messages. It’s useful when the messages inside a topic are of interest for different applications that will process them in different ways. We want all the interested applications to receive all the same messages from the topic.

Another great advantage of consumers grouping is the rebalancing feature. When a consumer joins a group, if there are still enough partitions available (i.e. we haven’t reached the limit of one consumer per partition), a re-balancing starts and the partitions will be reassigned to the current consumers, plus the new one. In the same way, if a consumer leaves a group, the partitions will be reassigned to the remaining consumers. For Streams applications in a consistent region, this feature is not relevant, however because the number of consumers will not change at runtime of a Streams application.
The other way round, when partitions are added to topics, Kafka will automatically assign the new partitions to consumers in the group rebalancing the partitions. This scenario must be supported by the Streams operators.

A consumer operator represents one member of a group, therefore a consumer is a synonymon for a consumer operator. When we talk about partitions, Kafka topic partitions are meant.

## Assumptions and restrictions

- All members of a consumer group must be in the same consistent region; the group must not have members outside the consistent region or outside of the Streams application.

- In a consistent region, the control input port is not supported. Current function of the control port is to assign or unassign partitions to or from a consumer, which is not in the sense of consumer groups.

- The initial start point for consuming messages can be configured (the **startPosition** parameter). It can be the beginning of a partition, the end, last committed offset, or a message timestamp, but not a specific offset because an offset belongs to a partition. Partition assignment is not under control by the operator.

## Operator design

### Threads and queues

The operator uses the Kafka consumer Java API. This API is not thread-safe. That's why, the following design was chosen:

Each consumer operator has two threads.

- The **event thread** invokes the consumer API for polling for messages, committing offsets, processing partition revocation and assignment. It also processes the *StateHandler* related events of the consistent region, like drain, checkpoint, and reset. This thread is controlled over events placed by other threads into the **event queue**. Most of the time, this thread polls for messages from Kafka and enqueues them into the **message queue**. Messages are received from the Kafka broker in batches, where the maximum batch size is configurable via the `max.poll.records` consumer configuration. The capacity of the message queue is `100 \* max.poll.records`. Leaving the default value for this config, the message queue has a capacity of 50,000 messages.

- The **process thread** pulls messages from the message queue and, when a consistent region permit is acquired, it creates and submits tuples. This thread also saves the offsets of the submitted tuples for every topic and partition in a data structure called the *offset manager*.

### Checkpointed data

The operator's checkpoint contains following data

1. The set of assignable partitions of all subscribed topics. When there is more than one operator in a consumer group, each operator is assigned to a not overlapping subset of this.
2. the offset manager. The offset manager contains the mapping from \[topic, partition\] to the offset of the message fetched next for the assigned partitions. These are the start offsets for these partitions after reset.
3. a partition offset map for *all partitions of all subscribed topics* containing the offsets committed at last drain. The subset for the assigned partitions must be the same as the mappings in the offset manager.

**Note:** when one operator (A) creates a checkpoint, an other operator (B) may not yet haved committed offsets, so that operator A may have lower offsets in the map for those partitions that are assigned to operator B.

## Operator Function

### Initialization

The consumer operators, which are members of a consumer group try to create a job-scoped MBean in the JCP operator, which has an object name unique for the consumer group. It is initialized with group-ID and consistent region index. All operators do this concurrently, but only one manages to create the MBean. All operators compare their own consistent region index with the consistent region index attribute of the MBean. If the indexes differ, the rule that all members must be in the same group is violated. Operator initialization fails.

Next, each operator independently determines the initial offsets for all partitions of all topics that the operator subscribes later. These mappings from topic partitions to offsets are stored in a JCP control variable and used to seek initially after partition assignment or after a reset to initial state.

The operator maintains a *seek offset map*, which contains topic-partition-to-offset mappings for all partitions. This map is initially filled with the initial offsets (It is also filled during reset of the consistent region).

Then, the opertor subscribes to the configured topics. It has no partitions assigned yet. The client is in state SUBSCRIBED.

### Partition revocation and assignment

Partition assignment and revocation is done by Kafka. The client is notified by callbacks about these events whithin the call of the `poll` API of the KafkaConsumer - and only then. `onPartitionsRevoked` is always followed by `onPartitionsAssigned` with the newly assigned partitions.

#### Partition revocation
`onPartitionsRevoked` is ignored when the client has just subscribed, or after reset to initial state or to a checkpoint. Otherwise, a reset of the consistent region is initiated by the operator:
- A stop polling event is submitted to the event queue, which lets the operator stop polling for new messages. Letting the operator stop polling avoids that the callbacks are called again before the reset event is processed.
- A reset of the consitent region is triggered asynchronous. The state of the client changes to POLLING_CR_RESET_INITIATED.
- `onPartitionsAssigned` with the new partition assignment is always called back after `onPartitionsRevoked` within the context of the current poll.
- When it comes to the consistent region reset, the operator places a reset event into the event queue. This event is 
always processed by the event thread *after* the current `poll` has finished. Therefore also `onPartitionsAssigned` is called before the reset event can be processed.

#### Partition assignment

The cycle `onPartitionsRevoked` - `onPartitionsAssigned` can happen whenever the client polls for messages. The client performs following:

- update the *assignable partitions* from the meta data of the subscribed topics. Partitions may have been added to the topics.
- for the partitions that are *not assigned anymore* to this consumer
    - remove all messages from the message queue, so that they will not be submitted as tuples
    - remove the \[topic, partition\] to offset mappings from the offset manager
- save the newly assigned partitions for this operator
- add the new partitions to the offset manager, and get the fetch positions for each assigned partition from the Kafka broker.
- When the operator is in state SUBSCRIBED or RESET_COMPLETE, i.e. not awaiting a reset of the consistent region, seek all partitions to the offsets in the *seek offset map*. The seek position for those partitions, which are not in the map is calculated like an initial offset (what the **startPosition** parameter is).
- When the operator is in state POLLING_CR_RESET_INITIATED (awaiting a reset) the assigned partitions are not seeked. This happens during reset later. 

The operator maintains the metric **nPartitionsAssigned** showing the number of currently assigned partitions.

## drain processing

On drain, the operator stops polling for new messages, and, if the region is *not operator driven*, waits until the message queue has been emptied by the process thread. Then, the offsets from the offset manager are committed in one single synchronous server request. If this fails, for only one partition, an exception is thrown causing an operator restart with subsequent reset to the last consistent state.

The operator maintains two metrics, the drain time of last drain, and the maximum drain time, both measured in milliseconds.

## reset processing

When the operator resets to a previous state, each operator must have a *seek offset map*, which maps topic partitions to offsets for all partitions that can be potentially assigned. The way how this map is created is different for reset to initial state and reset to a checkpoint.

A reset of the consistent region can happen 
- after new partitions have been assigned (the operator has triggered the reset before during `onPartitionsRevoked`)
- without change of partition assignment, for example, when a downstream PE has caused the reset
- with subsequent partition assignment, for example, when a PE hosting a consumer operator restarted.

Each operator performs these steps on reset of the consistent region:

- the operator stops polling for new messages. This happens automatically when the reset or reset to inital event is enqueued into the event queue.
- it clears the message queue
- create the *seek offset map*
- for the currently assigned partitions, seek to the offsets in the seek offset map. For partitions that have no mapping, seek to what the **startPosition** parameter is
- update the offsets in the offset manager with the start positions for the assigned partitions
- set the client state to RESET_COMPLETE
- the operator starts polling for messages filling the message queue
- when the operator acquires a permit to submit messages, the message queue is processed submitting tuples.

### create the seek offset map during reset to initial state

The seek offset map is restored from the initial offsets gathered from the control variable in the JCP.

### create the seek offset map during reset using a checkpoint

The checkpoint of each consumer operator contains three data items:

1. the assignable partitions (of all subscribed topics) at checkpoint time
2. the offset manager, next fetch positions (offsets) for those partitions, which were assigned to the consumer at checkpoint time
3. a partition-to-offset map with committed offsets of all assignable partitions. These are those offsets, which each operator saw in the Kafka cluster at its individual checkpoint time.

The goal is to restore a seek offset map from data (2) of all consumer operators within the consumer group. Then it could be *guaranteed* that a consumer seeks to the right offset of a partition even if this partition was assigned to a different consumer at the last consistent cut.

The data portions (2), are merged by using the MBean created in the JCP. Each consumer operator sends its partition-offset map (2) together with the assignable partitions (1), and the checkpoint-ID/resetAttempt to the MBean, which creates a merge in a map with \[checkpoint-ID, resetAttempt\] as the key. When the MBean has received mappings for all partitions in the assignable partitions (1) for the combination \[checkpoint-ID, resetAttempt\], the merge is treated being complete, and a JMX notification is emitted. The JMX notification also contains the checkpoint-ID and the resetAttempt, so that the operator can wait for exactly the notification for current reset attempt of a checkpoint sequence-ID. When an operator has received the right notification, it obtains the merged offset map from the MBean and creates the seek offset map from the merged offset map.

When a consumer has not received the notification in time, the map in the MBean may be incomplete. In this case, the offset for the seek offset map is taken from the partition to offset map (3).

The timeout for receiving the JMX notification is a quarter of the reset timeout, but not more than `min (max.poll.interval.ms/2, 30000)` milliseconds.

The merges in the MBean and the received JMX notifications are removed on `retireCheckpoint`.
