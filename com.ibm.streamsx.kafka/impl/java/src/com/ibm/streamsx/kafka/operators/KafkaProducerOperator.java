/*
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.streamsx.kafka.operators;

import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(name = "KafkaProducer", namespace = "com.ibm.streamsx.kafka", description = KafkaProducerOperator.DESC)
@InputPorts({
    @InputPortSet(description = "This port consumes tuples to be written to the Kafka topic(s). Each tuple "
            + "received on this port will be written to the Kafka topic(s).",
            cardinality = 1, optional = false)})

@OutputPorts({
    @OutputPortSet(description = "This port is an optional output port. Dependent on the "
            + "**" + AbstractKafkaProducerOperator.OUTPUT_ERRORS_ONLY_PARM_NAME + "** parameter, "
            + "the output stream includes only tuples for input tuples that failed to get published on one or all "
            + "of the specified topics, or it contains tuples corresponding to *all* input tuples, successfully produced "
            + "ones and failed tuples.\\n"
            + "\\n"
            + "The output port is asynchronous to the input port of the operator. The sequence of the submitted tuples "
            + "may also differ from the sequence of the input tuples. Window punctuations from the input stream are not forwarded.\\n"
            + "\\n"
            + "The schema of the output port must consist of one optional attribute of tuple type with the same schema "
            + "as the input port and one optional attribute of type `rstring`, `ustring`, `optional<rstring>`, or `optional<ustring>`, "
            + "that takes a JSON formatted description "
            + "of the occured error, or remains *empty* for successfully produced tuples. Emptiness of the attribute means that the attribute contains a "
            + "string with zero length when declared as `rstring` or `ustring`, and an empty optional (optional without a value) when declared as optional. "
            + "Both attributes can have any names and can be declared in any order in the schema.\\n"
            + "\\n"
            + "**Example for declaring the output stream as error output:**\\n"
            + "\\n"
            + "    stream <Inp failedTuple, rstring failure> Errors = KafkaProducer (Data as Inp) {\\n"
            + "        ...\\n"
            + "    }\\n"
            + ""
            + "**Example of the failure description**, which would go into the *failure* attribute above:\\n"
            + "\\n"
            + "    {\\n"
            + "        \\\"failedTopics\\\":[\\\"topic1\\\"],\\n"
            + "        \\\"lastExceptionType\\\":\\\"org.apache.kafka.common.errors.TopicAuthorizationException\\\",\\n"
            + "        \\\"lastFailure\\\":\\\"Not authorized to access topics: [topic1]\\\"\\n"
            + "    }\\n"
            + ""
            + "Please note that the generated JSON does not contain line breaks as in the example above, where the JSON has "
            + "been broken into multiple lines to better show its structure.\\n"
            + "\\n"
            + "**Example for declaring the output stream for both successfully produced input tuples and failures:**\\n"
            + "\\n"
            + "    stream <Inp inTuple, optional<rstring> failure> ProduceStatus = KafkaProducer (Data as Inp) {\\n"
            + "        param\\n"
            + "            outputErrorsOnly: false;\\n"
            + "        ...\\n"
            + "    }\\n"
            + "",
            cardinality = 1, optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Free)})

@Icons(location16 = "icons/KafkaProducer_16.gif", location32 = "icons/KafkaProducer_32.gif")
public class KafkaProducerOperator extends AbstractKafkaProducerOperator {

    public static final String DESC = ""
            + "The KafkaProducer operator is used to produce messages on Kafka "
            + "topics. The operator can be configured to produce messages to "
            + "one or more topics.\\n"
            + "\\n"
            + "# Supported Kafka Versions\\n"
            + "\\n"
            + "This version of the toolkit supports **Apache Kafka 0.10.2, 0.11, 1.0, 1.1, and all 2.x versions up to 2.5**.\\n"
            + "\\n"
            + "# Kafka Properties\\n"
            + "\\n"
            + KafkaSplDoc.PRODUCER_WHERE_TO_FIND_PROPERTIES
            + "Properties can be specified in a file or in an "
            + "application configuration. If specifying properties via a file, "
            + "the **propertiesFile** parameter can be used. If specifying properties "
            + "in an application configuration, the name of the application configuration "
            + "can be specified using the **appConfigName** parameter.\\n"
            + "\\n"
            + "The only property that the user is required to set is the `bootstrap.servers` "
            + "property, which points to the Kafka brokers, for example `kafka-0.mydomain:9092,kafka-1.mydomain:9092,kafka-2.mydomain:9092`. "
            + "All other properties are optional.\\n"
            + "\\n"
            + "The operator sets some properties by default to enable users to quickly get "
            + "started with the operator. The following lists which properties the operator "
            + "sets by default: \\n"
            + "\\n"
            + KafkaSplDoc.PRODUCER_DEFAULT_AND_ADJUSTED_PROPERTIES
            + "\\n"
            + "\\n"
            + "# Kafka Properties via Application Configuration\\n"
            + "\\n"
            + "Users can specify Kafka properties using Streams' application configurations. Information "
            + "on configuring application configurations can be found here: "
            + "[https://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.1/com.ibm.streams.admin.doc/doc/"
            + "creating-secure-app-configs.html|Creating application configuration objects to securely "
            + "store data]. Each property set in the application configuration "
            + "will be loaded as a Kafka property. For example, to specify the bootstrap servers that "
            + "the operator should connect to, an app config property named `bootstrap.servers` should "
            + "be created.\\n"
            + "\\n"
            + KafkaSplDoc.PRODUCER_AUTOMATIC_SERIALIZATION
            + "\\n"
            + KafkaSplDoc.PRODUCER_EXPOSED_KAFKA_METRICS
            + "\\n"
            + KafkaSplDoc.PRODUCER_CHECKPOINTING_CONFIG
            + "\\n"
            + KafkaSplDoc.PRODUCER_CONSISTENT_REGION_SUPPORT
            + "\\n"
            + "# Error Handling\\n"
            + "\\n"
            + "Many exceptions thrown by the underlying Kafka API are considered fatal. In the event "
            + "that Kafka throws a **retriable exception**, the operator behaves different when used in consistent region or not. "
            + "When used in a consistent region, the operator initiates a reset of the consistent region. The reset processing will instantiate a new "
            + "Kafka producer within the operator.\\n"
            + "\\n"
            + "When the operator is not used within a consistent region, the operator tries to recover internally "
            + "by instantiating a new Kafka producer within the operator and resending all producer records, which are not yet acknowledged. "
            + "Records that fail two producer generations are considered being finally failed. The corresponding tuple is counted in the "
            + "custom metric `nFailedTuples`, and, if the operator is configured with an output port, an output tuple is submitted.\\n"
            + "\\n"
            + "In the event that Kafka throws a **non-retriable exception**, the tuple that caused the exception is counted in the "
            + "custom metric `nFailedTuples`, and, if the operator is configured with an output port, an output tuple is submitted.\\n"
            + "\\n"
            + "Some exceptions can be "
            + "retried by Kafka itself, such as those that occur due to network error. Therefore, it is not recommended "
            + "to set the KafkaProducer `retries` property to 0 to disable the producer's "
            + "retry mechanism.";
}
