---
title: "Migrating an application from the streamsx.messaging to the streamsx.kafka toolkit"
permalink: /docs/user/migration_from_messaging/
excerpt: "How to migrate from messaging toolkit to the Kafka toolkit."
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

Developers with applications using the Kafka operators from `com.ibm.streamsx.messaging` toolkit are encouraged
to migrate to the new `com.ibm.streamsx.kafka` toolkit. Use the following guidelines to migrate applications to the new toolkit. 

### Toolkit Changes

| (Previous) Messaging Toolkit | (New) Kafka Toolkit | Additional Information |
| --- | --- | --- | 
| Old namespace: `com.ibm.streamsx.messaging.kafka` | New namespace: `com.ibm.streamsx.kafka` | Applications need to replace the `com.ibm.streamsx.messaging.kafka` namespace with `com.ibm.streamsx.kafka`. |

### KafkaConsumer Changes

| Old Operator | New Operator | Additional Information |
| --- | --- | --- | 
| **appConfigPropertyName** parameter | Removed in `KafkaConsumer` | The `KafkaConsumer` contains a **appConfigName** parameter that allows users to specify the name of the application configuration containing the Kafka properties. Each property in the app config is loaded as a Kafka property. Users no longer need to specify which properties should be loaded from the app config. |
| **consumerPollTimeout** parameter | Removed in `KafkaConsumer` | The value for the poll timeout is hard-coded into the operator. Providing the option to change this value does not currently provide any advantages to the end-user. | 
| **jaasFile** parameter | Removed in `KafkaConsumer` | Kafka v0.10 introduced a new property called `sasl.jaas.config` where the JAAS configuration can be stored. The introduction of this property eliminates the need for a separate JAAS file. More information on this new parameter can be found here: [KIP-85](https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients)|
| **jaasFilePropName** parameter | Removed in `KafkaConsumer` | See the above comments regarding **appConfigPropertyName** and **jaasFile** parameters. The JAAS configuration can specified in the application configuration by setting a property called `sasl.jaas.config`. |
| **kafkaProperty** parameter | Removed in `KafkaConsumer` | The `KafkaConsumer` supports loading properties via property files (**propertiesFile** param) or application configurations (**appConfigName** param). |
| **keyAttribute** parameter | Renamed to **outputKeyAttributeName** | Applications should replace the **keyAttribute** parameter with the **outputKeyAttributeName** parameter. |
| **messageAttribute** parameter | Renamed to **outputMessageAttributeName** | Application should replace the **messageAttribute** parameter with the **outputMessageAttributeName** parameter. |

### KafkaProducer Changes

| Old Operator | New Operator | Additional Information |
| --- | --- | --- | 
| **appConfigPropertyName** parameter | Removed in `KafkaProducer` | The `KafkaProducer` contains a **appConfigName** parameter that allows users to specify the name of the application configuration containing the Kafka properties. Each property in the app config is loaded as a Kafka property. Users no longer need to specify which properties should be loaded from the app config. |
| **consumerPollTimeout** parameter | Removed in `KafkaProducer` | The value for the poll timeout is hard-coded into the operator. Providing the option to change this value does not currently provide any advantages to the end-user. | 
| **jaasFile** parameter | Removed in `KafkaProducer` | Kafka v0.10 introduced a new property called `sasl.jaas.config` where the JAAS configuration can be stored. The introduction of this property eliminates the need for a separate JAAS file. More information on this new parameter can be found here: [KIP-85](https://cwiki.apache.org/confluence/display/KAFKA/KIP-85%3A+Dynamic+JAAS+configuration+for+Kafka+clients)|
| **jaasFilePropName** parameter | Removed in `KafkaProducer` | See the above comments regarding **appConfigPropertyName** and **jaasFile** parameters. The JAAS configuration can specified in the application configuration by setting a property called `sasl.jaas.config`. |
| **kafkaProperty** parameter | Removed in `KafkaProducer` | The `KafkaProducer` supports loading properties via property files (**propertiesFile** param) or application configurations (**appConfigName** param). |
