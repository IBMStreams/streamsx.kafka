# KafkaConsumer Client ID Sample

 * This sample demonstrates how to use the 'clientId' parameter. A common use case is being able to specify a different clientId for each consumer is when running in a parallel region. 

This sample is nearly identical to the KafkaConsumeLoadBalance sample, but with the addition of the 'clientId' parameter. 


### Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```

