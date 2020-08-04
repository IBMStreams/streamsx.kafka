---
title: "Getting Started with Kafka Operators"
permalink: /docs/user/GettingStarted/
excerpt: "Getting Started Guide for IBM Streams Kafka Toolkit operators"
last_modified_at: 2019-09-17T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

---
layout: docs
title: Getting Started with Kafka Operators
description:  Getting Started Guide for IBM Streams Kafka Toolkit operators
weight: 10
published: true
---

## Introduction

The IBM Streams Kafka Toolkit is designed to get you connected to your messaging servers as quickly as possible. Kafka is an ideal messaging server for stream computing. This guide will get you sending and receiving messages in no time, and will highlight some of the best practices. We will also cover how to get the Kafka operators running in a consistent region.



## Skill Level

Readers of this guide are expected to have a basic understanding of Kafka and IBM Streams terminology. To get up to speed on Kafka basics, run through their great [Quick Start guide](http://kafka.apache.org/documentation.html#quickstart).

If you are new to Streams, [follow the Quick Start for an overview](/streamsx.documentation/docs/spl/quick-start/qs-0/).


## Requirements
Prior to using Kafka operators, the following software must be installed and configured:

* **IBM Streams** - A <a target="_blank" href="/streamsx.documentation//docs/latest/qse-intro/">Quick Start Edition</a> is available for free. This guide assumes that you have a Streams domain and instance up and running.

* **Kafka Toolkit** - You can download it from the IBM Streams GitHub Kafka Toolkit Repository <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/releases">Release Page</a>.

* **Kafka Brokers** - This guide assumes you are using Kafka 0.10.2 or above. To quickly get a Kafka server up and running, follow <a target="_blank" href="http://kafka.apache.org/documentation.html#quickstart">these directions</a>.

## Information to Collect
Once you have your Kafka server (or servers) set up, you will need their hostnames and listener ports. You can find them in your configuration file for each server (default is `<Kafka-Install>/config/server.properties`):

~~~~~~
# The port the socket server listens on
port=9092

# Hostname the broker will bind to. If not set, the server will bind to all interfaces
host.name=myhost.mycompany.com
~~~~~~

## Steps - Send and Receive Messages
2. **Configure the SPL compiler to find the Kafka toolkit directory. Use one of the following methods.**
   * *Set the STREAMS_SPLPATH environment variable to the root directory of the toolkit (with : as a separator)*

        `export STREAMS_SPLPATH=\<kafka-toolkit-location\>/com.ibm.streamsx.kafka:$STREAMS_SPLPATH`

   * *Specify the -t or --spl-path command parameter when you run the sc command.*

     `sc -t \<kafka-toolkit-location\>/com.ibm.streamsx.kafka -M MyMain`

   * *If  Streams Studio is used to compile and run SPL application, add Kafka toolkit to toolkit locations in Streams Explorer by following [these directions](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.studio.doc/doc/tusing-working-with-toolkits-adding-toolkit-locations.html).*


2. **Create an SPL application and add a toolkit dependency on the Kafka toolkit in your application.** You can do this by [editing the application dependency](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.studio.doc/doc/tcreating-spl-toolkit-app-elements-edit-toolkit-information-dependencies.html) in Streams Studio, or by creating/editing the info.xml for the application and adding the dependency directly (you can also just start with the <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample">KafkaSample</a> to skip this and the following step).

    Sample info.xml from the <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample">KafkaSample</a>:

   <pre><code>&lt;?xml version=&quot;1.0&quot; encoding=&quot;UTF-8&quot;?&gt;
&lt;info:toolkitInfoModel xmlns:common=&quot;http://www.ibm.com/xmlns/prod/streams/spl/common&quot; xmlns:info=&quot;http://www.ibm.com/xmlns/prod/streams/spl/toolkitInfo&quot;&gt;
  &lt;info:identity&gt;
    &lt;info:name&gt;KafkaSample&lt;/info:name&gt;
    &lt;info:description&gt;Sample application showing a Streams Kafka Producer and Consumer&lt;/info:description&gt;
    &lt;info:version&gt;1.0.0&lt;/info:version&gt;
    &lt;info:requiredProductVersion&gt;4.0.0.0&lt;/info:requiredProductVersion&gt;
  &lt;/info:identity&gt;
  <b style="color:blue">&lt;info:dependencies&gt;
        &lt;info:toolkit&gt;
          &lt;common:name&gt;com.ibm.streamsx.kafka&lt;/common:name&gt;
          &lt;common:version&gt;[3.0.0,6.0.0)&lt;/common:version&gt;
        &lt;/info:toolkit&gt;
  &lt;/info:dependencies&gt;</b>
&lt;/info:toolkitInfoModel&gt;
</code></pre>

3. **Add the Kafka operator use directives to your application.** If Streams Studio is used, this directive is automatically added when dragging and dropping a Kafka operator onto SPL application in the graphical editor (if you start with a sample from the Kafka toolkit, this step is already done for you).

  `use com.ibm.streamsx.kafka::*;`

  or

  `use com.ibm.streamsx.kafka::KafkaProducer;`

  `use com.ibm.streamsx.kafka::KafkaConsumer;`

5. **Configure the Kafka Producer to send messages to a Kafka Broker.** You must:
    * **Create a producer.properties file and place it in the `etc` directory of your application.** This ensures that it will be included in the .sab application bundle (important for cloud and HA deployment). The following is a sample producer.properties file. See <a target="_blank" href="https://kafka.apache.org/23/documentation.html#producerconfigs">here</a> for more producer configuration details.
        <pre><code>bootstrap.servers=broker.host.1:9092,broker.host.2:9092,broker.host.3:9092</code></pre>
    * **Specify the location of the producer.properties file in the KafkaProducer operator using the propertiesFile parameter.** You can specify either an absolute or a relative file path, where the path is relative to the application directory:

        `propertiesFile : etc/producer.properties;`
    * **Specify the Kafka topic to send messages to.** This can be done via the rstring topic attribute in the incoming tuple or you can specify this using the topic parameter in the KafkaProducer (see the highlighted code in the beacon operator below).


    Here is the sample beacon and KafkaProducer code from the <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample">KafkaSample</a>:

    <pre><code>//create some messages and send to KafkaProducer
    stream&lt;<b style="color:blue">rstring topic, rstring key, rstring message</b>&gt; OutputStream = Beacon() {
        param
            initDelay : 5.0;
            period : 0.2;
        output
            OutputStream: <b style="color:blue">topic = $topic
                        , message = &quot;Reality is merely an illusion, albeit a very persistent one.&quot;
                        , key = &quot;Einstein&quot;</b>;
    }


    () as KafkaSinkOp = KafkaProducer(OutputStream) {
        param
            <b style="color:blue">propertiesFile : &quot;etc/producer.properties&quot;</b>;
    }
    </code></pre>




    <div class="alert alert-success" role="alert"><b>Notice: </b>We don't specify the topic as a parameter, but instead as a part of the incoming tuple. This means that each incoming tuple can be directed towards a different topic.</div>

6. **Configure the Kafka Consumer to receive messages from the Kafka Broker.** You must:
    * **Create a consumer.properties file and place it in the `etc` directory of your application.** Here is a sample consumer.properties file (for more details on Kafka Consumer configs, see <a target="_blank" href="https://kafka.apache.org/23/documentation.html#consumerconfigs">here</a>:
        <pre><code>bootstrap.servers=broker.host.1:9092,broker.host.2:9092,broker.host.3:9092</code></pre>
    * **Specify the location of the consumer.properties file in the KafkaConsumer operator using the propertiesFile parameter:**

        `propertiesFile : etc/consumer.properties`
    * **Specify the Kafka topic (or topics) to subscribe to receive messages from.** Do this using the rstring topic parameter in the KafkaConsumer. You can subscribe to multiple topics by using a comma separated list:

        `topic: "topic1" , "topic2" , "topic3";`

  Here is the KafkaConsumer operator from the <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaSample">KafkaSample</a>:

  <pre class="source-code"><code>stream&lt;rstring key, rstring message&gt; KafkaStream = KafkaConsumer()
    {
        param
            <b style="color:blue">propertiesFile : &quot;etc/consumer.properties&quot;;
            topic : $topic;</b>
    }</code></pre>

   <div class="alert alert-success" role="alert"><b>Notice: </b>Perhaps you miss the group identifier for the consumer. When not specified, the KafkaConsumer operator generates a unique identifier for this consumer.</div>

## Consistent Regions

Kafka operators support consistent regions, which are sections of your operator graph where tuple processing is guaranteed. The KafkaProducer can participate in a consistent region (it cannot be the start), and can guarantee at-least-once tuple processing. No special configuration is required to use the KafkaProducer in a consistent region, so this section will only focus on the KafkaConsumer.

The KafkaConsumer supports at-least-once tuple processing and starts a consistent region (since it is a source).
  For general questions on consistent region, read this <a target="_blank" href="https://developer.ibm.com/streamsdev/2015/02/20/processing-tuples-least-infosphere-streams-consistent-regions/">overview</a> and these <a target="_blank" href="https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/consistentregions.html">docs</a>.

To start a consistent region with a KafkaConsumer, you must:

* **Place an `@consistent` annotation above the operator**

* **Specify triggerCount parameter for operatorDriven trigger** - The trigger count gives you control over the approximate number of messages between checkpointing. If you are using a periodic trigger for your consistent region, you do not need to specify this.
Here is the KafkaConsumer from the <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupWithConsistentRegion">KafkaConsumerGroupWithConsistentRegion</a> sample:
<pre class="source-code"><code>    //Read in from a kafka server and start consistent region
    <b style="color:blue">@consistent (trigger=periodic, period=60.0 /*seconds*/)</b>
    stream <rstring message, int32 partition, rstring key> ConsumedMsgs = KafkaConsumer()
    {
        param
            propertiesFile: &quot;etc/consumer.properties&quot; ;
            topic: $topic ;
            groupId: &quot;myGroupId&quot; ;
    }
</code></pre>


## Parallel Consuming
Consuming in parallel lets you take advantage of the scalability of both Kafka and Streams. Kafka allows for parallel consumption of multi-topic partitions via <a href="http://kafka.apache.org/intro#intro_consumers" target="_blank">consumer groups</a>. Any consumer with the same group identifier `group.id=<consumer-group-name>` or operator parameter `groupId: <consumer-group-name>` will be a part of the same consumer group and will share the partitions of a topic. If you would like to directly control which partitions your consumer is reading from, you can do that by specifying the `partition` parameter in the KafkaConsumer operator.

<div class="alert alert-info" role="alert"><b>Best Practice: </b>For a multi-partition topic, you can have as many consumers as you have partitions (if you have more consumers than partitions, they will just sit idly).</div>

The easiest way to consume from a single topic in parallel is to:

* **Use @parallel with a width equal to the number of partitions in your topic:**

    `@parallel(width = $numTopicPartitions)`

Here is a simple example of using three consumers to read from a 3-partition topic using <a href="https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/udpoverview.html" target="_blank">User Defined Parallelism</a>:

<pre class="source-code"><code>    <b style="color:blue">@parallel(width = 3)</b>
    stream&lt;rstring message, rstring key&gt; KafkaConsumerOut = KafkaConsumer()
    {
        param
            propertiesFile : &quot;etc/consumer.properties&quot; ;
            topic : $topic ;
            groupId : "myGroupId" ;
    }
</code></pre>

If you would like to consume in parallel within a consistent region, check out this <a target="_blank" href="https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaConsumerGroupWithConsistentRegion">KafkaConsumerGroupWithConsistentRegion sample</a>.

You find more information about the common consumer patterns for parallel processing  in the
<a target="_blank" href="https://ibmstreams.github.io/streamsx.kafka/docs/user/overview/"> user documentation</a> of the streamsx.kafka toolkit for the
<a target="_blank" href="https://ibmstreams.github.io/streamsx.kafka/docs/user/UsecaseConsumerGroup/"> consumer group</a> with dynamic partition assignment, and for
<a target="_blank" href="https://ibmstreams.github.io/streamsx.kafka/docs/user/UsecaseAssignedPartitions/"> user defined partition assignment</a>.

## Connecting to IBM Event Streams

<a href="https://cloud.ibm.com/catalog/services/event-streams#about">IBM Event Streams</a> is IBM’s Kafka as a service offering. You can use the [Message Hub toolkit](https://github.com/IBMStreams/streamsx.messagehub) to connect to it. The Message Hub toolkit is based on the Kafka toolkit but simplifies connection to the service.

<p>See this article on how to <a href="https://www.ibm.com/cloud/blog/get-started-streaming-analytics-message-hub" target="_blank">connect to Event Streams</a>.</p>

## Additional Resources
* <a target="_blank" href="https://ibmstreams.github.io/streamsx.kafka/">Streams Kafka toolkit project page</a>

* <a target="_blank" href="https://ibmstreams.github.io/streamsx.kafka/doc/spldoc/html/index.html">Streams Kafka Toolkit SPLDoc</a>

* <a target="_blank" href="http://kafka.apache.org/documentation.html">Kafka Documentation website</a>
