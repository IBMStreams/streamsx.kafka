# KafkaFloatSample

This sample uses a float for the key and message. The Kafka operators will automatically use the correct serializer and deserializer by examining the key and message types. 


### Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```
