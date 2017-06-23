# Kafka Toolkit

Welcome to the Kafka Toolkit. This toolkit enables SPL applications to integrate with Apache Kafka. 

This version of the toolkit currently supports: <mark>Apache Kafka v0.10.x</mark>


## Migrating from com.ibm.streamsx.messaging

To migrate applications using the old Kafka operators in the `com.ibm.streamsx.messaging` toolkit, refer to the [Migration Document](https://github.com/IBMStreams/streamsx.kafka/wiki/Migration-Document-(Messaging-Toolkit-to-Kafka-Toolkit)).


## Documentation 

Toolkit documentation can be found here: [SPLDoc](https://ibmstreams.github.io/streamsx.kafka/) (COMING SOON!)


## Build

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
cd test/KafkaTests
./setup.sh -b <list_of_bootstrap_servers>
../../gradlew test
```

**NOTE 1:** `setup.sh` will add an instance-level app config called "kafka-tests", as well as create a properties file containing the `bootstrap.servers` property

**NOTE 2:** Tests will run using the local domain specified by the STREAMS_DOMAIN_ID env var. All tests run in Distributed mode.


## Samples

Each sample contains a build.gradle file. The samples can be built/compiled by running `gradle build` from the sample directory. 
