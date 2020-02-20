# KafkaAvroSample

This sample uses a AVRO blob for message and rstring for the key.
The Kafka operators will automatically use the correct serializer and deserializer by examining the key and message types. 

## Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```

### Build the sample application

Before building with `make`, you need to set the environment variable `STREAMSX_AVRO_TOOLKIT` containing the toolkit location of the **com.ibm.streamsx.avro** toolkit.

## Utilized Toolkits

 - com.ibm.streamsx.kafka
 - [com.ibm.streamsx.avro](https://github.com/IBMStreams/streamsx.avro)
