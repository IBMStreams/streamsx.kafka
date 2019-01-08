# KafkaConsumer with input port

This sample demonstrates how to use the KafkaConsumer operator with the optional input port.
The KafkaConsumer operator is configured with operator driven checkpointing.

To make this sample work, these preconditions must be met:
* The Streams instance or domain must be configured with a checkpoint repository.


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

In the Kafka broker the topics `t1` and `t2` must be created with at least two partitions each. 
To run this sample, replace `<your_brokers_here>` in the `etc/consumer.properties` file with the Kafka brokers that you wish to connect to.
Here is an example of what this may look like: 

```
bootstrap.servers=mybroker1:9191,mybroker2:9192,mybroker3:9193
```

Compile the sample with `make` or `gradle` and submit the job with
`streamtool submitjob ./output/com.ibm.streamsx.kafka.sample.KafkaConsumerInputPortSample/com.ibm.streamsx.kafka.sample.KafkaConsumerInputPortSample.sab`.
Don't forget to rebuild the application when you change Kafka properties in the property file for the consumer because they go into the application's bundle file.
