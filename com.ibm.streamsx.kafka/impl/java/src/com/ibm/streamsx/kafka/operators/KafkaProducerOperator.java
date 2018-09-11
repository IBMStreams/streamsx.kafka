/* Generated by Streams Studio: April 6, 2017 at 3:30:27 PM EDT */
package com.ibm.streamsx.kafka.operators;

import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(name = "KafkaProducer", namespace = "com.ibm.streamsx.kafka", description = KafkaProducerOperator.DESC)
@InputPorts({ @InputPortSet(description = "This port consumes tuples to be written to the Kafka topic(s). Each tuple received on "
		+ "this port will be written to the Kafka topic(s).", cardinality = 1, optional = false) })
@Icons(location16 = "icons/KafkaProducer_16.gif", location32 = "icons/KafkaProducer_32.gif")
public class KafkaProducerOperator extends AbstractKafkaProducerOperator {

	public static final String DESC = 
			"The KafkaProducer operator is used to produce messages on Kafka " //$NON-NLS-1$
			+ "topics. The operator can be configured to produce messages to " //$NON-NLS-1$
			+ "one or more topics.\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "# Supported Kafka Version\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "This version of the toolkit supports **Apache Kafka v0.10.2, v0.11.x, and v1.0.x**.\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "# Kafka Properties\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "The operator implements Kafka's KafkaProducer API. As a result, " //$NON-NLS-1$
			+ "it supports all Kafka properties that are supported by the " //$NON-NLS-1$
			+ "underlying API. Properties can be specified in a file or in an " //$NON-NLS-1$
			+ "application configuration. If specifying properties via a file, " //$NON-NLS-1$
			+ "the **propertiesFile** parameter can be used. If specifying properties " //$NON-NLS-1$
			+ "in an application configuration, the name of the application configuration " //$NON-NLS-1$
			+ "can be specified using the **appConfigName** parameter.\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"The only property that the user is required to set is the `bootstrap.servers` " //$NON-NLS-1$
			+ "property, which points to the Kafka brokers. All other properties are optional. " //$NON-NLS-1$
			+ "The operator sets some properties by default to enable users to quickly get " //$NON-NLS-1$
			+ "started with the operator. The following lists which properties the operator " //$NON-NLS-1$
			+ "sets by default: \\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"---\\n" +  //$NON-NLS-1$
			"| Property name | Default Value |\\n" +  //$NON-NLS-1$
			"|===|\\n" +  //$NON-NLS-1$
			"| client.id | Generated ID in the form: `C-J<JobId>-<operator name>` |\\n" +  //$NON-NLS-1$
			"|---|\\n" +  //$NON-NLS-1$
			"| key.serializer | See **Automatic Serialization** section below |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| value.serializer | See **Automatic Serialization** section below |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| enable.idempotence | `true` only when in consistent region and **consistentRegionPolicy** parameter is set to `Transactional`. |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| transactional.id | Randomly generated ID in the form: `tid-<random_string>` only when in consistent region and **consistentRegionPolicy** parameter is set to `Transactional`. |\\n" +  //$NON-NLS-1$
			"---\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"**NOTE:** Users can override any of the above properties by explicitly setting " //$NON-NLS-1$
			+ "the property value in either a properties file or in an application configuration. \\n" +  //$NON-NLS-1$
			"\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "# Kafka Properties via Application Configuration\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"Users can specify Kafka properties using Streams' application configurations. Information " //$NON-NLS-1$
			+ "on configuring application configurations can be found here: " //$NON-NLS-1$
			+ "[https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.1/com.ibm.streams.admin.doc/doc/" //$NON-NLS-1$
			+ "creating-secure-app-configs.html|Creating application configuration objects to securely " //$NON-NLS-1$
			+ "store data]. Each property set in the application configuration " //$NON-NLS-1$
			+ "will be loaded as a Kafka property. For example, to specify the bootstrap servers that " //$NON-NLS-1$
			+ "the operator should connect to, an app config property named `bootstrap.servers` should " //$NON-NLS-1$
			+ "be created.\\n" //$NON-NLS-1$
			+ "\\n" +  //$NON-NLS-1$
			"# Automatic Serialization\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"The operator will automatically select the appropriate serializers for the key " //$NON-NLS-1$
			+ "and message based on their types. The following table outlines which " //$NON-NLS-1$
			+ "deserializer will be used given a particular type: \\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"---\\n" +  //$NON-NLS-1$
			"| Serializer | SPL Types |\\n" +  //$NON-NLS-1$
			"|===|\\n" +  //$NON-NLS-1$
			"| org.apache.kafka.common.serialization.StringSerializer | rstring |\\n" +  //$NON-NLS-1$
			"|---|\\n" +  //$NON-NLS-1$
			"| org.apache.kafka.common.serialization.IntegerSerializer | int32, uint32 |\\n" +  //$NON-NLS-1$
			"|---|\\n" +  //$NON-NLS-1$
			"| org.apache.kafka.common.serialization.LongSerializer | int64, uint64 |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| org.apache.kafka.common.serialization.FloatSerializer | float32 |\\n" +  //$NON-NLS-1$
            "|---|\\n" +  //$NON-NLS-1$
            "| org.apache.kafka.common.serialization.DoubleSerializer | float64 |\\n" +  //$NON-NLS-1$
			"|---|\\n" +  //$NON-NLS-1$
			"| org.apache.kafka.common.serialization.ByteArraySerializer | blob |\\n" +  //$NON-NLS-1$
			"---\\n" +  //$NON-NLS-1$
			"\\n" //$NON-NLS-1$
			+ "\\n" +			 //$NON-NLS-1$
			"# Consistent Region Strategy\\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			"The `KafkaProducer` operator can participate in a consistent region. The operator " //$NON-NLS-1$
			+ "cannot be the start of a consistent region. The operator supports *at least once* " //$NON-NLS-1$
			+ "(default behavior) and *exactly once* delivery semantics. The delivery semantics " //$NON-NLS-1$
			+ "can be controlled by the **" + CONSISTENT_REGION_POLICY_PARAM_NAME + "** parameter. " //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "# *At least once* delivery\\n" //$NON-NLS-1$
			+ "If the operator crashes or is reset while in a consistent " //$NON-NLS-1$
			+ "region, the operator will write all tuples replayed. This ensures that every " //$NON-NLS-1$
			+ "tuple sent to the operator will be written to the topic(s). However, *at least once* " //$NON-NLS-1$
			+ "semantics implies that duplicate messages may be written to the topic(s). \\n" //$NON-NLS-1$
			+ "# *Exactly once* delivery\\n" //$NON-NLS-1$
			
			+ "Messages are always inserted into a topic within the context of a transaction. " //$NON-NLS-1$
			+ "Transactions are committed when the operator checkpoints. If the operator crashes " //$NON-NLS-1$
			+ "or is reset while in a consistent region, the operator will abort the ongoing " //$NON-NLS-1$
			+ "transaction and write all tuples replayed within a new transaction. External "
			+ "consumers configured with `isolation.level=read_committed` will not " //$NON-NLS-1$
            + "read the duplicates from the aborted transactions. Consumers that use a "
            + "different isolation level will read duplicate messages similar to *at least once* delivery.\\n"
            + "\\n"
            + "To ensure that replayed tuples are not committed multiple times when the consistent region "
            + "fails between commit and the point when the entire region has reached a consistent state, the "
            + "operator writes the current checkpoint sequence number as commit-ID together with "
            + "its unique *transactional ID* into the control topic **__streams_control_topic** "
            + "as part of the committed transaction. The checkpoint sequence number is part of "
            + "the operator's checkpointed state and represents the last successfully committed ID. \\n"
            + "\\n"
            + "When the consistent region fails after commit, the last successfully committed "
            + "ID is reset to the previous value and tuples are replayed. When the "
            + "operator checkpoints next time, it detects the "
            + "difference between the committed ID within the control topic and the "
            + "restored ID from the checkpoint data and aborts the Kafka transaction rather "
            + "than committing the replayed data again."
			+ "\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "**NOTE 1:** Transactions in Kafka have an inactivity timeout with default value of 60 seconds. " //$NON-NLS-1$
			+ "If the consistent region triggers less frequently and you expect a low message rate, " //$NON-NLS-1$
			+ "consider to to increase the timeout by setting the producer property `transaction.timeout.ms` " //$NON-NLS-1$
			+ "to a higher value, for example 120000 (milliseconds). " //$NON-NLS-1$
			+ "The maximum value of this property is limited by the server property " //$NON-NLS-1$
			+ "`transaction.max.timeout.ms`, which has a default value of 900000.\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "**NOTE 2:** In Kafka environments that have disabled automatic topic creation "
			+ "(broker config `auto.create.topics.enable=false`), the control topic with the name "
			+ "**__streams_control_topic** must be created manually before **consistentRegionPolicy** can "
			+ "be used with `Transactional`.\\n"
			+ "\\n"
			+ "**NOTE 3:** For *exactly once* delivery semantics, the Kafka broker must have version 0.11 or higher " //$NON-NLS-1$
			+ "because older brokers do not support transactions." //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "# Error Handling\\n" //$NON-NLS-1$
			+ "\\n" //$NON-NLS-1$
			+ "Many exceptions thrown by the underlying Kafka API are considered fatal. In the event " //$NON-NLS-1$
			+ "that Kafka throws an exception, the operator will restart. Some exceptions can be " //$NON-NLS-1$
			+ "retried, such as those that occur due to network error. Users are encouraged " //$NON-NLS-1$
			+ "to set the KafkaProducer `retries` property to a value greater than 0 to enable the producer's " //$NON-NLS-1$
			+ "retry mechanism. \\n" +  //$NON-NLS-1$
			"\\n" +  //$NON-NLS-1$
			""; //$NON-NLS-1$
}
