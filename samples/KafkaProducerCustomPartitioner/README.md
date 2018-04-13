# KafkaProducerCustomPartitioner

This sample demonstrates how to use a custom partitioner with the KafkaProducer operator. In order for the KafkaProducer to use a custom partitioner, the following steps must be taken: 

 1. The JAR file containing the custom partitioner must be specified via the **userLib** property.
 2. The `partitioner.class` property must be set in the KafkaProducer's property file (`producer.properties` in this sample)

This sample uses the JAR file created in the **CustomPartitionerJavaProj** sample.


### Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193

