---
title: "Kafka Consumer group support in a consistent region"
permalink: /docs/user/ConsumerGroupConsistentRegion/
excerpt: "Functional description how consumer groups are supported in a consistent region"
last_modified_at: 2019-03-05T12:37:48+01:00
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

- The **event thread** invokes the consumer API for polling for messages, committing offsets, processing partition revocation and assignment. It also processes the *StateHandler* related events of the consistent region, checkpoint, and reset. This thread is controlled over events placed by other threads into the **event queue**. Most of the time, the event thread polls for messages from Kafka and enqueues them into the **message queue**. Messages are received from the Kafka broker in batches, where the maximum batch size is configurable via the `max.poll.records` consumer configuration. The capacity of the message queue is `100 * max.poll.records`. Leaving the default value for this config, the message queue has a capacity of 50,000 messages.

- The **process thread** pulls messages from the message queue and, when a consistent region permit is acquired, it creates and submits tuples. This thread also saves the offsets of the submitted tuples for every topic and partition in a data structure called the *offset manager* after tuple submission.

### Shared data

When group management is used in a consistent region, the consumer client implementation `CrKafkaConsumerGroupClient` is used in the operator. This client implementation uses a job-scoped MXBean in the JobControlPlane operator, which is unique for the consumer group to which the client belongs to. When the connection to the JobControlPlane is created, every Consumer operator registers itself with its operator name, so that each member of the group can obtain the number of consumer operators in the consumer group and its names. Using this MXBean is one reason, why a consumer group must not be spread over multiple Streams jobs. This MXBean is not persisted. When the JobControlPlane operator restarts, it reconnects to all consumer operators, and then they register again, so that the registration will be restored.

For every topic partition that is assigned to any of the operators in the consumer group, a shared control variable of Long type is created for the consumer group. Such a variable contains the initial fetch offset for a particular topic partition in the consumer group. The variable is created when a consumer sees a topic partition the first time. These control variables are used on reset to initial state to ensure that the fetch position is the same on every reset to initial state.

### Checkpointed data

The operator's checkpoint contains following data

1. The operator name of the operator that is checkpointed.
2. All operator names that participate in the consumer group. These are the names of the operators registered in the MXBean.
3. the offset manager. The offset manager contains the mapping from \[topic, partition\] to the offset of the message fetched next for the assigned partitions. These are the start offsets for these partitions after reset.

## Operator Function

### Initialization

The consumer operators, which are members of a consumer group try to create a job-scoped MXBean in the JCP operator, which has an object name unique for the consumer group. It is initialized with group-ID and consistent region index. All operators do this concurrently, but only one manages to create the MXBean. All operators compare their own consistent region index with the consistent region index attribute of the MBean. If the indexes differ, the rule is violated that all group members must belong to the same consistent region. The operator initialization will fail in this case.

Then, the opertor subscribes to the configured topics or pattern. It has no partitions assigned yet. The client is in state SUBSCRIBED.

### Partition revocation and assignment

Partition assignment and revocation is done by Kafka. The client is notified by callbacks about these events whithin the call of the `poll` API of the KafkaConsumer - and only then. `onPartitionsRevoked` is always followed by `onPartitionsAssigned` with the newly assigned partitions. `onPartitionsRevoked` is always called *before* partitions are unassigned. When partitions are revoked, the Kafka consumer (part of the kafka client) forgets its assignment and all current fetch positions of the partitions being revoked.

#### Partition revocation

`onPartitionsRevoked` is ignored when the client has just subscribed, or after reset to initial state or to a checkpoint, and if the client has not yet fetched messages. Otherwise, a reset of the consistent region is initiated by the operator:
- The message queue is cleared, so that tuple submission stops
- A stop polling event is asynchronously submitted to the event queue, which lets the operator stop polling for new messages. Stop polling avoids that the callbacks are called again before the reset event is processed.
- The consistent region reset is triggered in *force mode*. *Force mode* resets the consecutive reset counter.
- The state of the client changes to CR_RESET_PENDING.
- When it comes to the consistent region reset, the operator places a reset event into the event queue. This event is
always processed by the event thread *after* the current `poll` has finished. Therefore also `onPartitionsAssigned` is called before the reset event can be processed.

#### Partition assignment

The cycle `onPartitionsRevoked` - `onPartitionsAssigned` can happen whenever the client polls for messages. The client performs following:

- for the partitions that are *not assigned anymore* to this consumer
    - remove the \[topic, partition\] to offset mappings from the offset manager
- save the newly assigned partitions for this operator
- add the new topic partitions to the offset manager
- seek the assigned partitions to
  - an offset from a *merged* checkpoint
  - an initial offset from a control variable, or
  - an initial offset determined from the **startPosition** parameter, which is then saved in a contol variable
- When the operator is in state CR_RESET_PENDING (awaiting a reset) the assigned partitions are not seeked. This happens during reset later.

The operator maintains following metrics when partitions are assigned:
- **nPartitionsAssigned** showing the number of currently assigned partitions
- **nConsumedTopics** showing the number of topics consumed by this operator in the group

## Drain processing

On drain, the operator stops polling for new messages, and, if the region is *not operator driven*, drains the entire content of the message queue into a temporary buffer. Then, the offsets from the offset manager are committed in one single synchronous server request. If this fails, for only one partition, an exception is thrown causing an operator restart with subsequent reset to the last consistent state.

The operator maintains two metrics, the drain time of last drain, and the maximum drain time, both measured in milliseconds.

## Reset processing

A reset of the consistent region can happen
- after new partitions have been assigned (a consumer operator within the group has triggered the reset during its `onPartitionsRevoked`)
- without change of partition assignment, for example, when a downstream PE has caused the reset
- with subsequent partition assignment, for example, when a PE hosting a consumer operator restarted.

When the operator is reset without restart, it seeks the assigined partitions to initial offsets or offsets from a merged checkpoint. A partition assignment *can* happen afterwards. When the operator is reset after relaunch of its PE, a partition assignement will definitely happen. It can be an empty partition assignment, however. The partition assignment will consist of partition revocation followed by a partition assignment. The partition assignment can also change when partitions are assigned at the time of reset. After reset processing the consumer client goes into state `RESET_COMPLETE` and starts polling with throttled speed.

### Reset processing to inital state

Polling for new Kafka messages is stopped, the drain buffer and the message queues are cleared.
When the operator has assigned partitions, a *seek offset map* is created from the initial offsets stored in the control variables. This map maps the currently assigned partitions to the initial offsets.

The assigned partitions are seeked to the initial offsets, and the offset manager is updated and refreshed with the fetch positions from the broker. After reset processing the consumer client goes into state `RESET_COMPLETE` and starts polling with throttled speed.

### Reset processing to a previous state using a checkpoint

When the operator resets to a previous state, each operator must have offsets of its currently assigned partitions and those partitons, which can be assigned later. The currently assigned partitions can be different from tha assigned partitions at checkpoint time. That's why the checkpoints of the consumer operators within a consumer group must be merged, so that all consumer operators have the same *merged checkpoint*.

The checkpoint of each operator contains

1. Its own operator name
2. The operator names of all consumer operators at the time of checkpoint
3. Its offset manager at the time of the checkpoint. The offset manager can be understood of a map from topic partition to fetch offset.

The offsets (3) are merged by using the MXBean in the JobControlPlane.
Each consumer operator sends its partition-offset map (3) together with its operator name (1), the number of distinct consumer operators *N* (from (2)), and the checkpoint sequence-ID to the MXBean. When the MXBean has received *N* contributions with *N* different operator names for the chackpoint sequence-ID, the merge is treated being complete, and a JMX notification with the merged offset map is emitted. The JMX notification also contains the checkpoint-ID, so that the operator can wait for exactly the notification for current checkpoint sequence-ID. When an operator has received the right notification, it creates the seek offset map from the merged offset map received with the JMX notification.

The timeout for receiving the JMX notification is `minimum (CR_resetTimeout/2, max.poll.interval.ms/2, 15000 ms)`. The merges in the MXBean and the received JMX notifications in the consumer client are removed on `retireCheckpoint`.

After this checkpoint merge happened, polling for new Kafka messages is stopped, the drain buffer and the message queues are cleared.

The assigned partitions are seeked to the offsets from merged checkpoint, and the offset manager is updated and refreshed with the fetch positions from the broker. After reset processing the consumer client goes into state `RESET_COMPLETE` and starts polling with throttled speed.

### Partition assignment after reset.

The `onPartitionsRevoked` callback is ignored when a reset happened before. The partition assignment within the `onPartitionsAssigned` callback is done as described above.

