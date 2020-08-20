# KafkaConsumer sample with dynamic start position

This sample demonstrates how to implement a start position for the KafkaConsumer that can be configured at submission time. This allows to decide at submission time, from where to start reading messages:

- at the beginning of the Kafka log
- at the end of the Kafka log
- at a record that matches a timestamp
- or at Kafka's default position, which is usually after the last committed record.

To achieve this, the application defines these submission time parameters (defaults in brackets):

- **start.position** ("Default") The sample supports "Beginning", "End", "Default", and "Time".
- **start.timestamp.millis** (0) The start timestamp when "Time" is used as the value for **start.position**. This timestamp must be given as milliseconds since Epoch.

To create a consumer group, the submission time parameter **num.kafka.consumers** can be set to a value greater than 1.
The producer operator will generate 50 tuples/sec after an initial delay of 10 seconds. Received messages are written together with its meta data topic, partition, offset, and timestamp to the stdout/stderr trace file.


### Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like:

```
bootstrap.servers=mybroker1:9092,mybroker2:9092,mybroker3:9092
```

### Launch from command line

1. Build the sample with `make`
2. Make sure you created a topic with name **test** when the Kafka server does not automatically create topics
3. In Streams Quickstart Edition, submit the application bundle with

    streamtool submitjob output/com.ibm.streamsx.kafka.sample.KafkaConsumerVariableStartPositionSample/com.ibm.streamsx.kafka.sample.KafkaConsumerVariableStartPositionSample.sab \
    -P start.position=Beginning -P start.timestamp.millis=234

    streamtool submitjob output/com.ibm.streamsx.kafka.sample.KafkaConsumerVariableStartPositionSample/com.ibm.streamsx.kafka.sample.KafkaConsumerVariableStartPositionSample.sab \
    -P start.position=Time -P start.timestamp.millis=<timestamp>

**Hint:** In a Linux console, you get the current timestamp in milliseconds since Epoch with `echo $(($(date +%s%N)/1000000))`, so you could use the submission timeparameters

    -P start.position=Time -P start.timestamp.millis=$(($(date +%s%N)/1000000-120000))

to start consuming records with timestamps two minutes (120000 milliseconds) before now.
