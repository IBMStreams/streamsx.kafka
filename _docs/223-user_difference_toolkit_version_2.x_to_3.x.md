---
title: "Difference between Kafka toolkit v2.x and v3.x"
permalink: /docs/user/kafka_toolkit_2_vs_3/
excerpt: "Difference between Kafka toolkit v2.x and v3.x"
last_modified_at: 2020-08-04T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

## Introduction
The toolkit version 3 has four potentially breaking changes:

1. The minimum required Streams version is 4.3. For older Streams versions, please use toolkit version 2.x.
2. When the **KafkaConsumer** is used with input port *and* one of the parameters **topic**, **pattern**, **partition**, or **startPosition** is used, the SPL compiler raises an error. Remove these parameters when the operator is configured with an input port.
3. The **guaranteeOrdering** parameter of the **KafkaProducer** operator is incompatible with Kafka version less than 0.11.  When guaranteed record order within a partition is required with older Kafka servers, users must set the producer config `max.in.flight.requests.per.connection=1` instead of using the **guaranteeOrdering** parameter.
4. The default `isoloation.level` of the **KafkaConsumer** is now `read_committed`. This breaks applications consuming from Kafka with a version older than 0.11. Users consuming from Kafka older than 0.11 must use the `isolation.level=read_uncommitted` consumer configuration.


For compatible changes, please visit the [CHANGELOG](https://github.com/IBMStreams/streamsx.kafka/blob/develop/com.ibm.streamsx.kafka/CHANGELOG.md).
