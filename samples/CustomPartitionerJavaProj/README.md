# CustomPartitioner Java Project

This is a Java project that demonstrates creating a custom Kafka partitioner. The **KafkaProducer** operator can add the JAR file to the operator's classpath via the **userLib** parameter. Users can then specify the partitioner to use via the `partitioner.class` Kafka property. 

### Build

Run the following command to build the project: 
```
make
```
or if you are in the cloned or forked git repository

```
../../gradlew build
```

### Sample

The **KafkaProducerCustomPartitioner** sample demonstrates how to load the JAR file onto the classpath and configure the Kafka property. 
