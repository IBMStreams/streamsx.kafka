---
title: "Consuming big messages from Kafka"
permalink: /docs/user/ConsumingBigMessages/
excerpt: "How to configure the KafkaConsumer for consuming big messages."
last_modified_at: 2019-03-30T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# Introduction and Challenges

The variety in size, which messages transferred over Kafka can have, is in the range of some bytes up to some megabytes, for example X-ray images in a hospital.

## Assumptions and preconditions

This article is written for the Kafka toolkit version 1.9.1. For older toolkit versions, some parts of this article do not apply.
We assume that the Kafka broker is capable to handle big messages. Out of the box, the Kafka brokers can handle messages up to 1MB (in practice, a little bit less than 1MB) with the default configuration settings, though Kafka is optimized for small messages of about 1K in size. The configuration settings for the broker and topics for bigger messages are not in scope of this article.

## What you should know about the messages you want to consume

You should know following:
- What is the absolute maximum message size including the key?
- Are the messages produced with compression? If yes, what is the average compression ratio roughly? Note, that messages are decompressed in the consumer after they have been fetched.
- What is the average uncompressed messages size and what is the bandwidth of the size?

In this article, we assume that the messages are de-serialized into byte arrays (SPL blobs). When de-serializing data into Strings, we must keep in mind, that Strings in Java are always UTF-16 coded, so that *N* bytes raw serialized data can be expanded to a String of *2N* bytes.

## Downstream processing

Big Kafka messages are most likely modeled as `blob` type attributes in SPL. Transferring big tuples from PE to PE or from Java operators to C++ operators involves always additional serialization and de-serialization of the tuples limiting the tuple rate in the Streams runtime. When the consumer operator is fetching messages from Kafka faster than they can be processed downstreams as tuples, the consumer operator will begin to queue the fetched messages in an operator-internal message queue. This queueing is consuming memory.

The challenge here is to avoid that the consumer operator goes out of memory heap space when messages are queued in the operator.

# Heap memory in the Java Virtual Machine

The Java Virtual Machine has - simplified - two memory limits, the *maximum* memory, and the *total* memory. The *maximum* memory is the amount of memory, that can be allocated from the operating system. It can be set with `-XmxNN`, for example, `-Xmx2G`. The *total* memory is the memory that is currently allocated. The *free* memory is the differencs between total memory and used memory. The minimum allocatable memory is therefore `max - total + free`.

These memory limits can be obtained from the Java Runtime.

# Relevant Kafka consumer configs

**fetch.max.bytes**

The maximum amount of (potentially compressed) data the server should return for a fetch request. The default value is 52,428,800 (50MB) and may be sufficient.

**max.partition.fetch.bytes**

The maximum amount of (potentially compressed) data per-partition the server will return. The default value of 1,048,576 (1MB) might be too small and should be at least the maximum message size plus some overhead (1KB).

**max.poll.records**

The maximum number of messages returned by a single fetch request. The default value is 500. When the majority of messages is large, this config value can be reduced.

# Fetching and enquing messages

The consumer within the Kafka library is a nearly a blackbox. We can only assume, how it works, and what memory it requires. The consumer fetches a batch of messages wich is limited to `fetch.max.bytes` in size. These raw bytes must be stored in a buffer, which must be allocated. Next, the content of this buffer must be converted into a container structure of messages. Here, decompression can happen. When the received messages are not compressed, the additional memory consumption for the messages will not be more than the size of the buffer assuming they are deserialized into byte arrays (blobs). Final enqueuing into the operator-internal queue moves adds references to the queue. The memory for the messages is allocated from the heap space. The starting point for the minimum allocatable memory *before* fetching messages is `2.1 * fetch.max.bytes`.

When this amount of memory is not available, the consumer pauses all assigned partitions until memory is available. (Note: Fetching can also be paused when memory is available, but the queue  is full.)

When the size of all fetched messages, multiplied with 2.1, is higher than the last minimum allocatable memory, the minimum allocatable memory for low memory detection is increased to `2.1 * Sz`, where *Sz* is the size of the data (keys and values) that has been enqueued.

Even though, the oparator can throw **OutOfMemoryError**. When messages are compressed, the deserialized batch can be larger than the current minimum free memory estimate, or the memory can be used up by another Java operator in the same PE shortly after the check.

# Custom Metrics

Whenever fetching must be paused because of *memory limitations*, the metric `nLowMemoryPause` is incremented. When the queue is full, and fetching is paused, the metric `nQueueFullPause` is incremented. The metric `nPendingMessages` displays the number of messages in the operator internal message queue.

# Tweeks for the memory monitor

As default, the operator monitors the used and available memory before *each* fetch, i.e. before each `consumer.poll(Duration)` incocation. This is a safe setting, which impacts throughput at high message rate with small messages. To avoid this, a queue fill threshold for the memory monitor can be set. You can specify a factor *N* (an integer number), so that the operator internal queue must be filled by `N * max.poll.records` before memory is monitored. Specify this setting as Java property `kafka.memcheck.threshold.factor`.

Note, that the minimum queue fill for memory monitoring can also be controlled by the consumer config `max.poll.records`, which should be high for high message rate at small messages.

The initial minimum free (allocatable) memory is `2.1 * fetch.max.bytes`. This value can be adjusted with Java property `kafka.prefetch.min.free.mb` when the default value is too small.

**Example:**
```
    stream <MessageType.BlobMessage> ReceivedMsgs = KafkaConsumer() {
      param
        groupId: "group1";
        topic: "topic";
        propertiesFile: "etc/consumer.properties";
        vmArg: "-Dkafka.prefetch.min.free.mb=250",  // 250 MB initial instead of 2.1*fetch.max.bytes
          "-Dkafka.memcheck.threshold.factor=4",
          "-Xmx1G";
}
```

# Tips to deal with OutOfMemoryError in the consumer operator

A typical OutOfMemoryError during fetching has the `KafkaConsumer.poll` method on its stack trace:

```
 java.lang.OutOfMemoryError: Java heap space
 org.apache.kafka.common.utils.Utils.toArray(Utils.java:262)
 org.apache.kafka.common.utils.Utils.toArray(Utils.java:225)
 org.apache.kafka.clients.consumer.internals.Fetcher.parseRecord(Fetcher.java:1026)
 org.apache.kafka.clients.consumer.internals.Fetcher.access$3300(Fetcher.java:110)
 org.apache.kafka.clients.consumer.internals.Fetcher$PartitionRecords.fetchRecords(Fetcher.java:1247)
 org.apache.kafka.clients.consumer.internals.Fetcher$PartitionRecords.access$1400(Fetcher.java:1096)
 org.apache.kafka.clients.consumer.internals.Fetcher.fetchRecords(Fetcher.java:544)
 org.apache.kafka.clients.consumer.internals.Fetcher.fetchedRecords(Fetcher.java:505)
 org.apache.kafka.clients.consumer.KafkaConsumer.pollForFetches(KafkaConsumer.java:1256)
 org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1188)
 org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1164)
...
```

* Increase the memory for the PE by using the `-Xmx` Java argument and / or isolate operators
* When messages can be large, and messages are compressed, decrease `max.poll.records` so that the number of (compressed) messages per batch, and the decompressed memory footprint of the messages is smaller.
* Finally, try to design the application in a way that it keeps up with the speed, in which messages are published.






