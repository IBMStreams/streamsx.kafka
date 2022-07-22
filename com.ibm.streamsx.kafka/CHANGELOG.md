# Changes
==========
## v3.2.5
Upgrade slf4j-reload4j version 1.7.30 to 1.7.36

## v3.2.4
* [228](https://github.com/IBMStreams/streamsx.kafka/issues/230) create a new release to fix log4j issue 
* slf4j-log4j12 upgraded to version: '1.7.36'

## v3.2.3
* [228](https://github.com/IBMStreams/streamsx.kafka/issues/230) create a new release to fix log4j issue

## v3.2.2
* [228](https://github.com/IBMStreams/streamsx.kafka/issues/228) KafkaConsumer: support submission time parameter for multiple topics

## v3.2.1
* added operator parameter *krb5Debug*
* [#227](https://github.com/IBMStreams/streamsx.kafka/issues/227) Expansion of {applicationDir} in vmArg does not work

## v3.2.0
* [#226](https://github.com/IBMStreams/streamsx.kafka/issues/226) Makefiles of sample application prepared for CP4D Streams build service, supports build with [VS Code](https://marketplace.visualstudio.com/items?itemName=IBM.ibm-streams)
* [#220](https://github.com/IBMStreams/streamsx.kafka/issues/220) SPL doc: Add hints, what to do when the toolkit is used with Kafka version 0.10
* [#218](https://github.com/IBMStreams/streamsx.kafka/issues/218) support incremental cooperative rebalancing
* [#217](https://github.com/IBMStreams/streamsx.kafka/issues/217) upgrade kafka-clients from 2.3.1 to 2.5.1

## v3.1.3
* [#223](https://github.com/IBMStreams/streamsx.kafka/issues/223) I18n: update message translation

## v3.1.2
* [#224](https://github.com/IBMStreams/streamsx.kafka/issues/224) Application directory cannot be used as file location for files specified via system property and vmArg
* [#222](https://github.com/IBMStreams/streamsx.kafka/issues/222) KafkaConsumer: Reset of CR may fail when partitions are revoked before the reset actually happens

## v3.2.0
* [#220](https://github.com/IBMStreams/streamsx.kafka/issues/220) SPL doc: Add hints, what to do when the toolkit is used with Kafka version 0.10
* [#218](https://github.com/IBMStreams/streamsx.kafka/issues/218) support incremental cooperative rebalancing
* [#217](https://github.com/IBMStreams/streamsx.kafka/issues/217) upgrade kafka-clients from 2.3.1 to 2.5.1, for more information, see also the [release page](https://github.com/IBMStreams/streamsx.kafka/releases/tag/v3.2.0)

## v3.1.1
* [#216](https://github.com/IBMStreams/streamsx.kafka/issues/216) Toolkit includes a vulnerable version of log4j.
* [#214](https://github.com/IBMStreams/streamsx.kafka/issues/214) Add sample with KafkaConsumer and start position as submission time value

## v3.1.0:
* [#213](https://github.com/IBMStreams/streamsx.kafka/issues/213) KafkaConsumer: dynamic startPosition parameter

## v3.0.4:
* [#203](https://github.com/IBMStreams/streamsx.kafka/issues/203) KafkaConsumer: assign output attributes via index rather than attribute name
* [#206](https://github.com/IBMStreams/streamsx.kafka/issues/206) Make main composites of samples public.
  This allows using the samples with _streamsx_ Python package.
* [#208](https://github.com/IBMStreams/streamsx.kafka/issues/208) KafkaProducer: message or key attribute with underline causes error at context checker.
  All previous versions back to 1.0.0 are affected by this issue.
* New sample: [KafkaAvroSample](https://github.com/IBMStreams/streamsx.kafka/tree/develop/samples/KafkaAvroSample)

## v3.0.3:
* [#198](https://github.com/IBMStreams/streamsx.kafka/issues/198) - The "nConsecutiveRuntimeExc" variable never reaches 50 when exceptions occur

## v3.0.2
* [#200](https://github.com/IBMStreams/streamsx.kafka/issues/200) - I18n update

## v3.0.1
* [#196](https://github.com/IBMStreams/streamsx.kafka/issues/196) - KafkaProducer: Consistent region reset can trigger addtional reset

## v3.0.0
### Changes and enhancements

* The included Kafka client has been upgraded from version 2.2.1 to 2.3.1.
* The schema of the output port of the `KafkaProducer` operator supports optional types for the error description.
* The optional input port of the `KafkaConsumer` operator can be used to change the *topic subscription*, not only the *partition assignment*.
* The **guaranteeOrdering** parameter now enables the idempotent producer when set to `true`, which allows a higher throughput by allowing more
  in-flight requests per connection (requires Kafka server version 0.11 or higher).
* The `KafkaConsumer` operator now enables and benefits from group management when the user does not specify a group identifier.
* Checkpoint reset of the `KafkaConsumer` is optimized in consistent region when the consumer is the only group member.
* The `KafkaConsumer` operator can be configured as a static consumer group member (requires Kafka server version 2.3 or higher).
  See also the *Static Consumer Group Membership* chapter in the KafkaConsumer's documentation.
* The `KafkaConsumer` operator now uses `read_committed` as the default `isolation.level` configuration unless the user has specified a different value.
  In `read_committed` mode, the consumer will read only those transactional messages which have been successfully committed.
  Messages of aborted transactions are now skipped. The consumer will continue to read non-transactional messages as before.
  This new default setting is incompatible with Kafka 0.10.2.

### Deprecated features

The use of the input control port has been deprecated when the `KafkaConsumer` is used in a consistent region.

### Incompatible changes

* The toolkit requires at minimum Streams version 4.3.
* The **guaranteeOrdering** parameter of the `KafkaProducer` operator is incompatible with Kafka version 0.10.x when used with value `true`.
  The work-around for Kafka 0.10.x is given in the parameter description.
* When the `KafkaConsumer` operator is configured with input port, the **topic**, **pattern**, **partition**, and **startPosition**
  parameters used to be ignored in previous versions. Now an SPL compiler failure is raised when one of these parameters is used
  together with the input port.
* The default `isolation.level` configuration of the `KafkaConsumer` operator is incompatible with Kafka broker version 0.10.x.
  When connecting with Kafka 0.10.x, `isolation.level=read_uncommitted` must be used for the consumer configuration.

## v2.2.1
* [#179](https://github.com/IBMStreams/streamsx.kafka/issues/179) - KafkaProducer: Lost output tuples on FinalMarker reception

## v2.2.0
* The `KafkaProducer` operator supports an optional output port, configurable via the new **outputErrorsOnly** operator parameter
* Exception handling of the `KafkaProducer` operator in autonomous region changed. The operator does not abort its PE anymore; it recovers internally instead.
* New custom metrics for the `KafkaProducer` operator: `nFailedTuples`, `nPendingTuples`, and `nQueueFullPause`

## v2.1.0
### Changes and enhancements
* This toolkit version has been tested also with Kafka 2.3
* [#169](https://github.com/IBMStreams/streamsx.kafka/issues/169) new optional operator parameter **sslDebug**. For debugging SSL issues see also the [toolkit documentation](https://ibmstreams.github.io/streamsx.kafka/docs/user/debugging_ssl_issues/)
* [#167](https://github.com/IBMStreams/streamsx.kafka/issues/167) changed default values for following consumer and producer configurations:

  - `client.dns.lookup = use_all_dns_ips`
  - `reconnect.backoff.max.ms = 10000` (Kafka's default is 1000)
  - `reconnect.backoff.ms = 250` (Kafka's default is 50)
  - `retry.backoff.ms = 500` (Kafka's default is 100)

* Changed exception handling for the KafkaProducer when not used in a consistent region: https://github.com/IBMStreams/streamsx.kafka/issues/163#issuecomment-505402607

### Bug fixes
* [#163](https://github.com/IBMStreams/streamsx.kafka/issues/163) KafkaProducer's exception handling makes the operator lose tuples when in CR
* [#164](https://github.com/IBMStreams/streamsx.kafka/issues/164) on reset() the KafkaProducerOperator should instantiate a new producer instance
* [#166](https://github.com/IBMStreams/streamsx.kafka/issues/166) Resource leak in KafkaProducer when reset to initial state in a CR

## v2.0.1
* [#171](https://github.com/IBMStreams/streamsx.kafka/issues/171) Resetting from checkpoint will fail when sequence id is >1000

## v2.0.0
### Changes and enhancements

* The included Kafka client has been upgraded from version 2.1.1 to 2.2.1, [#160](https://github.com/IBMStreams/streamsx.kafka/issues/160)
* Support for Kafka broker 2.2 has been added, [#161](https://github.com/IBMStreams/streamsx.kafka/issues/161)
- The toolkit has enhancements for the **KafkaConsumer** when it is used in an autonomous region (i.e. not part of a consistent region):
    - The KafkaConsumer operator can now participate in a consumer group with **startPosition** parameter values `Beginning`, `End`, and `Time`, [#94](https://github.com/IBMStreams/streamsx.kafka/issues/94)
    - After re-launch of the PE, the KafkaConsumer operator does not overwrite the initial fetch offset to what the **startPosition** parameter is, i.e. after PE re-launch the consumer starts consuming at last committed offset, [#107](https://github.com/IBMStreams/streamsx.kafka/issues/107)

The new **startPosition** handling requires that the application always includes a **JobControlPlane** operator when **startPosition** is different from `Default`.

### Incompatible changes

The behavior of the KafkaConsumer operator changes when
1. the operator is *not* used in a consistent region, and
1. the **startPosition** parameter is used with `Beginning`, `End`, `Time`, or` Offset`.

In all other cases the behavior of the KafkaConsumer is unchanged. Details of the changed behavior, including sample code that breaks, can be found in the [Toolkit documentation on Github](https://ibmstreams.github.io/streamsx.kafka/docs/user/kafka_toolkit_1_vs_2/).

## v1.9.5
* [#171](https://github.com/IBMStreams/streamsx.kafka/issues/171) Resetting from checkpoint will fail when sequence id is >1000

## Older releases
Please consult the release notes for the release you are interested in.
