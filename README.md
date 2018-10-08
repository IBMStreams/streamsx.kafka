# Kafka Toolkit

Welcome to the Kafka Toolkit. This toolkit enables SPL applications to integrate with Apache Kafka. 

This version of the toolkit currently supports: Apache Kafka 0.10.2, 0.11, 1.0, 1.1, and 2.0. For the older
Kafka versions 0.9 and 0.10.0, please use the Kafka operators from the [com.ibm.streamsx.messaging](https://github.com/IBMStreams/streamsx.messaging) toolkit.

## Event Stream cloud service (Message Hub)

For building applications that connect to the IBM Cloud Event Stream service, it is recommended that the [**com.ibm.streamsx.messagehub**](https://github.com/IBMStreams/streamsx.messagehub) toolkit be used.
This toolkit provides functionality on top of the Kafka toolkit to simplify the configuration needed to connect to the cloud service. 


## Migrating from com.ibm.streamsx.messaging

To migrate applications using the old Kafka operators in the `com.ibm.streamsx.messaging` toolkit, refer to the [Migration Document](https://github.com/IBMStreams/streamsx.kafka/wiki/Migration-Document-(Messaging-Toolkit-to-Kafka-Toolkit)).


## Documentation 
- [Toolkit overview](https://developer.ibm.com/streamsdev/docs/introducing-kafka-toolkit/)
- [Improve performance when consuming from Kafka](https://developer.ibm.com/streamsdev/docs/improving-application-throughput-consuming-kafka/)
- [Other kafka related content on Streamsdev](https://developer.ibm.com/streamsdev/tag/kafka/)
- Toolkit documentation can be found here: [SPLDoc](https://ibmstreams.github.io/streamsx.kafka/) 


## Build the toolkit

```
cd com.ibm.streamsx.kafka
../gradlew build
```

## Build SPLDoc

```
./gradlew spldoc
```

**NOTE:** SPLDocs will be generated in the `docs/spldoc` directory.


## Release
```
./gradlew release
```

**NOTE:** The release will be available in the `build/release/output` folder. 


## Test

```
cd tests/KafkaTests
./setup.sh -b <list_of_bootstrap_servers>
../../gradlew test
```

**NOTE 1:** `setup.sh` will add a domain-level app config called "kafka-tests", as well as create a properties file containing the `bootstrap.servers` property.

**NOTE 2:** Tests will run using the local domain specified by the STREAMS_DOMAIN_ID env var. All tests run in Distributed mode.

### Following topics are required for the tests

| topic name | number of partitions |
| --- | --- |
| test | 3 |
| other1 | 1 |
| other2 | 1 |
| position | 1 |

Create them by hand before or make sure that they are automatically created with the required number of partitions.

**Hint:** The test case `kafkaStartPositionTest` requires that the *position* topic is re-created before every test run. 

## Samples

Each sample contains a build.gradle file and a makefile. The samples can be built/compiled by running `../../gradlew build` or `make` from the sample directory. 
