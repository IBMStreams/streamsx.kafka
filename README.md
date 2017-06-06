# Kafka Toolkit

## Build

 1. Navigate to `com.ibm.streamsx.kafka` directory
 2. Run `../gradlew build`

## Test

 1. Navigate to `tests/KafkaTests`
 2. Run `setup.sh -b <list_of_bootstrap_servers>` to add the app config to your domain and create a properties file
 3. Run `../../gradlew test` to run the tests

 **NOTE:** Tests will run using the local domain specified by the STREAMS_DOMAIN_ID env var. All tests run in Distributed mode.
