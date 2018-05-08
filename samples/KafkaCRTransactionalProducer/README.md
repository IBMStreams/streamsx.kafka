# KafkaCRTransactionalProducer

This sample demonstrates how to use the KafkaProducer operator in a consitent region with Transactional policy.
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

To run this sample, replace `<your_brokers_here>` in both the `etc/consumer.properties` and `etc/producer.properties` files with the Kafka brokers that you wish to connect to. Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```
