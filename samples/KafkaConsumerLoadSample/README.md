# KafkaConsumer Load Balancing Sample

This sample demonstrates how to configure KafkaConsumers in a UDP region to load balance the messages being consumed. The key here is that the 'etc/consumer.properties' file specifies the `group.id` property, which is used by all of the consumers. This means that each consumer will be placed in the same group, resulting in Kafka sending each message to only one consumer. 

The producer operator will generate 10 tuples/sec. Each consumer should be receiving and submitting approximately 3 tuples/sec.


### Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```

