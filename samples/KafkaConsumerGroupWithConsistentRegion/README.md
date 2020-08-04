# KafkaConsumerGroupWithConsistentRegion

This sample demonstrates how to use the KafkaConsumer operator in a consumer group within a Consistent Region.
In this consiguration, the Streams application benefits from both Kafka Consumer groups and the Consistent Region
features.
When in a consumer group with multiple consumers, the *group of consumers* replays the Kafka messages on reset of
the region. After reset, a consumer operator may replay tuples that a different consumer operator has generated 
before the reset happened. The application design must reflect this property of the consumer group.

To make this sample work, these preconditions must be met:
* The Streams instance or domain must be configured with a checkpoint repository.
* The version of the Kafka broker must be 0.11 or higher.


### Setup

Make sure that either the properties
```
instance.checkpointRepository
instance.checkpointRepositoryConfiguration
```
or
```
domain.checkpointRepository
domain.checkpointRepositoryConfiguration
```
have valid values. For example, if you use a local redis server, you can set the properties to following values:
```
instance.checkpointRepository=redis
instance.checkpointRepositoryConfiguration={ "replicas" : 1, "shards" : 1, "replicaGroups" : [ { "servers" : ["localhost:6379"], "description" : "localhost" } ] }
```
Use the command `streamtool getproperty -a | grep checkpoint` and `streamtool getdomainproperty -a | grep checkpoint` to see the current values.

In the Kafka broker, a *partitioned* topic with name `test` must be created. If a different topic name is used, the name must be configured as `$topic` composite parameter 
in the main composite. The Kafka message producer in this example distributes the messages round-robin to the partitions. The number of partitions must be configured for
the producer `nPartitions` composite parameter. 
To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to.
Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```

Compile the sample with `make` or `gradle` and submit the job with
`streamtool submitjob ./output/com.ibm.streamsx.kafka.sample.KafkaConsumerGroupWithConsistentRegion/com.ibm.streamsx.kafka.sample.KafkaConsumerGroupWithConsistentRegion.sab`.
Don't forget to rebuild the application when you change Kafka properties in one of the property files for the consumer or producer because they go 
into the application's bundle file.
