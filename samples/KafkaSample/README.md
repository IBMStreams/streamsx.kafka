# KafkaSample

This is a very basic sample demonstrating how to use the **KafkaConsumer** and **KafkaProducer** operators. This samples uses a properties file to load the `bootstrap.servers` property.

### Setup

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```
