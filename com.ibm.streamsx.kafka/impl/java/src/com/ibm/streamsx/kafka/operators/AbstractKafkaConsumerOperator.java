package com.ibm.streamsx.kafka.operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.icu.text.MessageFormat;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.clients.consumer.ConsistentRegionAssignmentMode;
import com.ibm.streamsx.kafka.clients.consumer.ConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.CrKafkaConsumerGroupClient;
import com.ibm.streamsx.kafka.clients.consumer.CrKafkaStaticAssignConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.NonCrKafkaConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.StartPosition;
import com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate;
import com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdateAction;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public abstract class AbstractKafkaConsumerOperator extends AbstractKafkaOperator {	

    private static final Logger logger = Logger.getLogger(AbstractKafkaConsumerOperator.class);
    private static final long DEFAULT_CONSUMER_TIMEOUT = 100l;
    private static final long SHUTDOWN_TIMEOUT = 5l;
    private static final TimeUnit SHUTDOWN_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
    private static final StartPosition DEFAULT_START_POSITION = StartPosition.Default;
    private static final String DEFAULT_OUTPUT_MESSAGE_ATTR_NAME = "message"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_KEY_ATTR_NAME = "key"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_TOPIC_ATTR_NAME = "topic"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_TIMESTAMP_ATTR_NAME = "messageTimestamp"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_OFFSET_ATTR_NAME = "offset"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_PARTITION_ATTR_NAME = "partition"; //$NON-NLS-1$
    // parameter names
    public static final String OUTPUT_KEY_ATTRIBUTE_NAME_PARAM = "outputKeyAttributeName"; //$NON-NLS-1$
    public static final String OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM = "outputMessageAttributeName"; //$NON-NLS-1$
    public static final String OUTPUT_TOPIC_ATTRIBUTE_NAME_PARAM = "outputTopicAttributeName"; //$NON-NLS-1$
    public static final String OUTPUT_TIMESTAMP_ATTRIBUTE_NAME_PARAM = "outputTimestampAttributeName"; //$NON-NLS-1$
    public static final String OUTPUT_OFFSET_ATTRIBUTE_NAME_PARAM = "outputOffsetAttributeName"; //$NON-NLS-1$
    public static final String OUTPUT_PARTITION_ATTRIBUTE_NAME_PARAM = "outputPartitionAttributeName"; //$NON-NLS-1$
    public static final String CR_ASSIGNMENT_MODE_PARAM = "consistentRegionAssignmentMode"; //$NON-NLS-1$
    public static final String TOPIC_PARAM = "topic"; //$NON-NLS-1$
    public static final String PARTITION_PARAM = "partition"; //$NON-NLS-1$
    public static final String START_POSITION_PARAM = "startPosition"; //$NON-NLS-1$
    public static final String START_TIME_PARAM = "startTime"; //$NON-NLS-1$
    public static final String TRIGGER_COUNT_PARAM = "triggerCount"; //$NON-NLS-1$
    public static final String COMMIT_COUNT_PARAM = "commitCount"; //$NON-NLS-1$
    public static final String START_OFFSET_PARAM = "startOffset"; //$NON-NLS-1$

    private static final int DEFAULT_COMMIT_COUNT = 500;

    private Thread processThread;
    private ConsumerClient consumer;
    private AtomicBoolean shutdown;
    private Gson gson;

    /* Parameters */
    private String outputKeyAttrName = DEFAULT_OUTPUT_KEY_ATTR_NAME;
    private String outputMessageAttrName = DEFAULT_OUTPUT_MESSAGE_ATTR_NAME;
    private String outputTopicAttrName = DEFAULT_OUTPUT_TOPIC_ATTR_NAME;
    private String outputMessageTimestampAttrName = DEFAULT_OUTPUT_TIMESTAMP_ATTR_NAME;
    private String outputOffsetAttrName = DEFAULT_OUTPUT_OFFSET_ATTR_NAME;
    private String outputPartitionAttrName = DEFAULT_OUTPUT_PARTITION_ATTR_NAME;
    private List<String> topics;
    private List<Integer> partitions;
    private List<Long> startOffsets;
    private StartPosition startPosition = DEFAULT_START_POSITION;
    private int triggerCount;
    private int commitCount = DEFAULT_COMMIT_COUNT;
    private String groupId = null;
    private Long startTime = -1l;
    private ConsistentRegionAssignmentMode consistentRegionAssignmentMode = ConsistentRegionAssignmentMode.Static; 

    private long consumerPollTimeout = DEFAULT_CONSUMER_TIMEOUT;
    private CountDownLatch resettingLatch;
    private boolean hasOutputTopic;
    private boolean hasOutputKey;
    private boolean hasOutputOffset;
    private boolean hasOutputPartition;
    private boolean hasOutputTimetamp;

    // The number of messages in which the value was malformed and could not be deserialized
    private Metric nMalformedMessages;
    long maxDrainMillis = 0l;

    // Initialize the metrics
    @CustomMetric (kind = Metric.Kind.COUNTER, name = "nDroppedMalformedMessages", description = "Number of dropped malformed messages")
    public void setnMalformedMessages (Metric nMalformedMessages) {
        this.nMalformedMessages = nMalformedMessages;
    }
    
    @CustomMetric (kind = Metric.Kind.COUNTER, name = "nPartitionRebalances", description = "Number of partition rebalances within the consumer group")
    public void setnPartitionRebalances (Metric nPartitionRebalances) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.GAUGE, description = "Number of pending messages to be submitted as tuples.")
    public void setnPendingMessages(Metric nPendingMessages) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.COUNTER, description = "Number times message polling was paused due to low memory.")
    public void setnLowMemoryPause(Metric nLowMemoryPause) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.COUNTER, description = "Number times message polling was paused due to full queue.")
    public void setnQueueFullPause(Metric nQueueFullPause) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.GAUGE, description = "Number of Kafka topic partitions assigned to the consumer.")
    public void setnAssignedPartitions(Metric nAssignedPartitions) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @Parameter(optional = true, name = CR_ASSIGNMENT_MODE_PARAM,
            description = "Specifies how the operator assigns topic partitions when in a consistent region."
                    + "\\n"
                    + "* If `Static` is specified, the operator assigns itself to the partitions specified in "
                    + "**partition** parameter or assigns itself to all partitions of the specified topics. "
                    + "The consumer will not be managed by Kafka. Group management is disabled. "
                    + "This mode guarantees that the operator "
                    + "replays the same tuples after reset of the consistent region that it has submitted "
                    + "before. The partition assignment of an operator does not change after region reset.\\n"
                    + "\\n"
                    + "* If `GroupCoordinated` is specified, the operator will participate in a consumer group. "
                    + "In this case, Kafka decides which topic partitions are assigned to the operator for consumption. "
                    + "This implies that the partition assignment of an individual operator can change during "
                    + "consistent region reset. After reset, the operator can replay tuples that have been "
                    + "submitted by a different operator in the same consumer group before the reset happened. "
                    + "All operators in the consumer group together replay the same set of tuples, however.\\n"
                    + "\\n"
                    + "Using the `GroupCoordinated` parameter value, a **group ID** must be specified, which "
                    + "must be shared by all operators that belong to the consumer group. The group ID "
                    + "can be specified as operator parameter **groupId** or as consumer property `group.id` "
                    + "in a property file or app option. The operator will fail at initialization time when "
                    + "it detects that the default random group ID is used.\\n"
                    + "\\n"
                    + "The `GroupCoordinated` parameter value is incompatible with the control input port and with "
                    + "the **partition** parameter. The **startPosition** value `Offset` cannot be used as "
                    + "it requires the **partition** parameter.\\n"
                    + "\\n"
                    + "The default value is `Static` for backward compatibility.\\n"
                    + "\\n"
                    + "The parameter is ignored when the operator is not part of a consistent region.")
    public void setConsistentRegionAssignmentMode (String consistentRegionAssignmentMode) {
        /*public void setConsistentRegionAssignmentMode (ConsistentRegionAssignmentMode consistentRegionAssignmentMode) {*/
        try {
            this.consistentRegionAssignmentMode = ConsistentRegionAssignmentMode.valueOf (consistentRegionAssignmentMode);
        }
        catch (Exception e) {
            throw new RuntimeException (Messages.getString ("INVALID_PARAMETER_VALUE",
                    consistentRegionAssignmentMode, CR_ASSIGNMENT_MODE_PARAM, Arrays.toString (ConsistentRegionAssignmentMode.values())));
        }
    }

    @Parameter(optional = true, name=OUTPUT_TIMESTAMP_ATTRIBUTE_NAME_PARAM,
            description="Specifies the output attribute name that should contain the record's timestamp. "
                    + "It is presented in milliseconds since Unix epoch."
                    + "If not specified, the operator will attempt to store the message in an "
                    + "attribute named 'messageTimestamp'. The attribute must have the SPL type 'int64' or 'uint64'.")
    public void setOutputMessageTimestampAttrName(String outputMessageTimestampAttrName) {
        this.outputMessageTimestampAttrName = outputMessageTimestampAttrName;
    }

    @Parameter(optional = true, name=OUTPUT_OFFSET_ATTRIBUTE_NAME_PARAM,
            description="Specifies the output attribute name that should contain the offset. If not "
                    + "specified, the operator will attempt to store the message in an attribute "
                    + "named 'offset'. The attribute must have the SPL type 'int64' or 'uint64'.")
    public void setOutputOffsetAttrName(String outputOffsetAttrName) {
        this.outputOffsetAttrName = outputOffsetAttrName;
    }

    @Parameter(optional = true, name=OUTPUT_PARTITION_ATTRIBUTE_NAME_PARAM,
            description="Specifies the output attribute name that should contain the partition number. If not "
                    + "specified, the operator will attempt to store the partition number in an "
                    + "attribute named 'partition'. The attribute must have the SPL type 'int32' or 'uint32'.")
    public void setOutputPartitionAttrName(String outputPartitionAttrName) {
        this.outputPartitionAttrName = outputPartitionAttrName;
    }

    @Parameter(optional = true, name="startOffset",
            description="This parameter indicates the start offset that the operator should begin consuming "
                    + "messages from. In order for this parameter's values to take affect, the **startPosition** "
                    + "parameter must be set to `Offset`. Furthermore, the specific partition(s) that the operator "
                    + "should consume from must be specified via the **partition** parameter.\\n"
                    + "\\n"
                    + "If multiple partitions are specified via the **partition** parameter, then the same number of "
                    + "offset values must be specified. There is a one-to-one mapping between the position of the "
                    + "partition from the **partition** parameter and the position of the offset from the **startOffset** "
                    + "parameter. For example, if the **partition** parameter has the values `0, 1`, and the **startOffset** "
                    + "parameter has the values `100l, 200l`, then the operator will begin consuming messages from "
                    + "partition 0 at offset 100 and will consume messages from partition 1 at offset 200.\\n"
                    + "\\n"
                    + "A limitation with using this parameter is that **only one single topic** can be specified "
                    + "via the **topic** parameter. ")
    public void setStartOffsets(long[] startOffsets) {
        this.startOffsets = Longs.asList(startOffsets);
    }

    @Parameter(optional = true, name="startTime",
            description="This parameter is only used when the **startPosition** parameter is set to `Time`. "
                    + "Then the operator will begin "
                    + "reading records from the earliest offset whose timestamp is greater than or "
                    + "equal to the timestamp specified by this parameter. If no offsets are found, then "
                    + "the operator will begin reading messages from what is is specified by the "
                    + "`auto.offset.reset` consumer property, which is `latest` as default value. The timestamp "
                    + "must be given as an 'int64' type in milliseconds since Unix epoch.")
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    @Parameter(optional = true, name="groupId",
            description="Specifies the group ID that should be used "
                    + "when connecting to the Kafka cluster. The value "
                    + "specified by this parameter will override the `group.id` "
                    + "Kafka property if specified. If this parameter is not "
                    + "specified and the `group.id` Kafka property is not "
                    + "specified, the operator will use a random group ID.")
    public void setGroupId (String groupId) {
        this.groupId = groupId;
    }

    @Parameter(optional = true, name="startPosition", 
            description="Specifies where the operator should start "
                    + "reading from topics. Valid options include: `Beginning`, `End`, `Default`, `Time`, and `Offset`.\\n"
                    + "* `Beginning`: The consumer starts reading from the beginning of the data in the Kafka topics. "
                    + "It starts with smallest available offset."
                    + "\\n"
                    + "* `End`: The consumer starts reading at the end of the topic. It consumes only newly inserted data."
                    + "\\n"
                    + "* `Default`: Kafka decides where to start reading. "
                    + "When the consumer has a group ID that is already known to the Kafka broker, the consumer starts reading "
                    + "the topic partitions from where it left off (after last committed offset). When the consumer has an "
                    + "unknown group ID, consumption starts at the position defined by the consumer "
                    + "config `auto.offset.reset`, which defaults to `latest`. Consumer offsets are retained for the "
                    + "time specified by the broker config `offsets.retention.minutes`, which defaults to 1440 (24 hours). "
                    + "When this time expires, the Consumer won't be able to resume after last committed offset, and the "
                    + "value of consumer property `auto.offset.reset` applies (default `latest`). "
                    + "**Note:** If you do not specify a group ID in Kafka consumer properties or via **groupId** parameter, "
                    + "the operator uses a random generated group ID, which makes the operator start consuming at the last position "
                    + "or what is specified in the `auto.offset.reset` consumer property."
                    + "\\n"
                    + "* `Time`: The consumer starts consuming messages with at a given timestamp. More precisely, "
                    + "when a time is specified, the consumer starts at the earliest offset whose timestamp is greater "
                    + "than or equal to the given timestamp. If no consumer offset is found for a given timestamp, "
                    + "the consumer starts consuming from what is configured by consumer config `auto.offset.reset`, "
                    + "which defaults to `latest`. "
                    + "The timestamp where to start consuming must be given as **startTime** parameter in milliseconds since Unix epoch."
                    + "\\n"
                    + "* `Offset`: The consumer starts consuming at a specific offset. The offsets must be specified "
                    + "for all topic partitions by using the **startOffset** parameter. This implies that the **partition** parameter "
                    + "must be specified and that Kafka's group management feature cannot be used as the operator "
                    + "assigns itself statically to the given partition(s). In consistent region, the value `Offset` "
                    + "is therefore incompatible with the `GroupCoordinated` value of the **consistentRegionAssignmentMode** parameter. "
                    + "When `Offset` is used as the start position **only one single topic** can be specified via the "
                    + "**topic** parameter.\\n"
                    + "\\n"
                    + "\\n"
                    + "If this parameter is not specified, the start position is `Default`.")
    public void setStartPosition(StartPosition startPosition) {
        this.startPosition = startPosition;
    }

    @Parameter(optional = true, name=PARTITION_PARAM,
            description="Specifies the partitions that the consumer should be "
                    + "assigned to for each of the topics specified. When you specify "
                    + "multiple topics, the consumer reads from the given partitions of "
                    + "all given topics.\\n"
                    + "For example, if the **topic** parameter has the values "
                    + "`\\\"topic1\\\", \\\"topic2\\\"`, and the **partition** parameter "
                    + "has the values `0, 1`, then the consumer will assign to "
                    + "`{topic1, partition 0}`, `{topic1, partition 1}`, "
                    + "`{topic2, partition 0}`, and `{topic2, partition 1}`.\\n"
                    + "\\n"
                    + "**Notes:**\\n"
                    + "* Partiton numbers in Kafka are zero-based. For example, a topic "
                    + "with three partitions has the partition numbers 0, 1, and 2.\\n"
                    + "* When using this parameter, the consumer will *assign* the "
                    + "consumer to the specified topics partitions, rather than *subscribe* "
                    + "to the topics. This implies that the consumer will not use Kafka's "
                    + "group management feature.")
    public void setPartitions(int[] partitions) {
        this.partitions = Ints.asList(partitions);
    }

    @Parameter(optional = true, name=TOPIC_PARAM,
            description="Specifies the topic or topics that the consumer should "
                    + "subscribe to. To assign the consumer to specific partitions, "
                    + "use the **partitions** parameter. To specify multiple topics "
                    + "from which the operator should consume, separate the the "
                    + "topic names by comma, for example `topic: \\\"topic1\\\", \\\"topic2\\\";`.")
    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    @Parameter(optional = true, name=OUTPUT_KEY_ATTRIBUTE_NAME_PARAM,
            description="Specifies the output attribute name that should contain "
                    + "the key. If not specified, the operator will attempt to "
                    + "store the message in an attribute named 'key'.")
    public void setOutputKeyAttrName(String outputKeyAttrName) {
        this.outputKeyAttrName = outputKeyAttrName;
    }

    @Parameter(optional = true, name=OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM,
            description="Specifies the output attribute name that will contain the "
                    + "message. If not specified, the operator will attempt to store "
                    + "the message in an attribute named 'message'.")
    public void setOutputMessageAttrName(String outputMessageAttrName) {
        this.outputMessageAttrName = outputMessageAttrName;
    }

    @Parameter(optional = true, name=OUTPUT_TOPIC_ATTRIBUTE_NAME_PARAM,
            description="Specifies the output attribute name that should contain the topic. "
                    + "If not specified, the operator will attempt to store the message in "
                    + "an attribute named 'topic'.")
    public void setOutputTopicAttrName(String outputTopicAttrName) {
        this.outputTopicAttrName = outputTopicAttrName;
    }

    @Parameter(optional = true, name=TRIGGER_COUNT_PARAM, 
            description="This parameter specifies the number of tuples that will be "
                    + "submitted to the output port before triggering a consistent region. "
                    + "This parameter is only used if the operator is the start of an "
                    + "*operator driven* consistent region and is ignored otherwise.")
    public void setTriggerCount(int triggerCount) {
        this.triggerCount = triggerCount;
    }

    @Parameter(optional = true, name = COMMIT_COUNT_PARAM, description = 
            "This parameter specifies the number of tuples that will be submitted to "
                    + "the output port before committing their offsets. This parameter is optional "
                    + "and has a default value of " + DEFAULT_COMMIT_COUNT + ". "
                    + "This parameter is only used when the "
                    + "operator is not part of a consistent region. When the operator participates in a "
                    + "consistent region, offsets are always committed when the region drains.")
    public void setCommitCount (int commitCount) {
        this.commitCount = commitCount;
    }

    @ContextCheck(compile = true)
    public static void checkStartOffsetRequiresPartition(OperatorContextChecker checker) {
        // parameters startOffset and partition must have the same size - can be checked only at runtime.
        // This implies that partition parameter is required when startOffset is specified - can be checked at compile time.
        OperatorContext operatorContext = checker.getOperatorContext();
        Set<String> parameterNames = operatorContext.getParameterNames();
        if (parameterNames.contains(START_OFFSET_PARAM) && !parameterNames.contains(PARTITION_PARAM)) {
            checker.setInvalidContext(Messages.getString("PARAM_X_REQUIRED_WHEN_PARAM_Y_USED", PARTITION_PARAM, START_OFFSET_PARAM), new Object[0]); //$NON-NLS-1$
        }
    }

    @ContextCheck(compile = true)
    public static void checkTriggerCommitCount(OperatorContextChecker checker) {
        OperatorContext operatorContext = checker.getOperatorContext();
        ConsistentRegionContext crContext = operatorContext.getOptionalContext(ConsistentRegionContext.class);
        Set<String> parameterNames = operatorContext.getParameterNames();
        if (crContext != null) {
            if (parameterNames.contains(COMMIT_COUNT_PARAM)) {
                System.err.println (Messages.getString ("PARAM_IGNORED_IN_CONSITENT_REGION", COMMIT_COUNT_PARAM));
            }
            if (crContext.isStartOfRegion() && crContext.isTriggerOperator()) {
                if (!parameterNames.contains(TRIGGER_COUNT_PARAM)) {
                    checker.setInvalidContext(Messages.getString("TRIGGER_PARAM_MISSING"), new Object[0]); //$NON-NLS-1$
                }
            }
        }
        else {
            // not in a CR ...
            if (parameterNames.contains(TRIGGER_COUNT_PARAM)) {
                System.err.println (Messages.getString ("PARAM_IGNORED_NOT_IN_CONSITENT_REGION", TRIGGER_COUNT_PARAM));
            }
        }
    }

    @ContextCheck(compile = true)
    public static void checkCrAssignmentModeWhenNotInCr (OperatorContextChecker checker) {
        OperatorContext operatorContext = checker.getOperatorContext();
        ConsistentRegionContext crContext = operatorContext.getOptionalContext(ConsistentRegionContext.class);
        Set<String> parameterNames = operatorContext.getParameterNames();
        if (crContext == null) {
            // not in a CR
            if (parameterNames.contains(CR_ASSIGNMENT_MODE_PARAM)) {
                System.err.println (Messages.getString ("PARAM_IGNORED_NOT_IN_CONSITENT_REGION", CR_ASSIGNMENT_MODE_PARAM));
            }
        }
    }

    @ContextCheck(compile = true)
    public static void checkInputPort(OperatorContextChecker checker) {
        List<StreamingInput<Tuple>> inputPorts = checker.getOperatorContext().getStreamingInputs();
        Set<String> paramNames = checker.getOperatorContext().getParameterNames();
        if(inputPorts.size() > 0) {
            /*
             * optional input port is present, thus need to ignore the following parameters:
             *  * topic
             *  * partition
             *  * startPosition
             */
            if(paramNames.contains(TOPIC_PARAM) 
                    || paramNames.contains(PARTITION_PARAM) 
                    || paramNames.contains(START_POSITION_PARAM)) {
                System.err.println(Messages.getString("PARAMS_IGNORED_WITH_INPUT_PORT")); //$NON-NLS-1$
            }

            StreamingInput<Tuple> inputPort = inputPorts.get(0);
            checker.checkAttributeType(inputPort.getStreamSchema().getAttribute(0), MetaType.RSTRING);
        }
    }

    @ContextCheck(compile = true)
    public static void checkForTopicOrInputPort(OperatorContextChecker checker) {
        List<StreamingInput<Tuple>> inputPorts = checker.getOperatorContext().getStreamingInputs();
        if(inputPorts.size() == 0 && !checker.getOperatorContext().getParameterNames().contains(TOPIC_PARAM)) {
            checker.setInvalidContext(Messages.getString("TOPIC_OR_INPUT_PORT"), new Object[0]); //$NON-NLS-1$
        }
    }

    @ContextCheck(compile = false, runtime = true)
    public static void checkParams(OperatorContextChecker checker) {
        StreamSchema streamSchema = checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema();
        Set<String> paramNames = checker.getOperatorContext().getParameterNames();

        String messageAttrName = paramNames.contains(OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM) ? 
                checker.getOperatorContext().getParameterValues(OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM).get(0) //$NON-NLS-1$
                : DEFAULT_OUTPUT_MESSAGE_ATTR_NAME;

                // set invalid context if message attribute name does not exist
                Attribute messageAttr = streamSchema.getAttribute(messageAttrName);
                if (messageAttr == null) {
                    checker.setInvalidContext(Messages.getString("OUTPUT_MESSAGE_ATTRIBUTE_MISSING"), new Object[0]); //$NON-NLS-1$
                } else {
                    // validate the attribute type
                    checker.checkAttributeType(messageAttr, SUPPORTED_ATTR_TYPES);
                }

                // check that user-specified key attr name exists
                Attribute keyAttr;
                if (paramNames.contains(OUTPUT_KEY_ATTRIBUTE_NAME_PARAM)) {
                    String keyAttrName = checker.getOperatorContext().getParameterValues(OUTPUT_KEY_ATTRIBUTE_NAME_PARAM).get(0);
                    keyAttr = streamSchema.getAttribute(keyAttrName);
                    if (keyAttr == null) {
                        checker.setInvalidContext(Messages.getString("OUTPUT_ATTRIBUTE_NOT_FOUND", keyAttrName), new Object[0]); //$NON-NLS-1$
                    }
                } else {
                    keyAttr = streamSchema.getAttribute(DEFAULT_OUTPUT_KEY_ATTR_NAME);
                }

                // validate the attribute type
                if (keyAttr != null)
                    checker.checkAttributeType(keyAttr, SUPPORTED_ATTR_TYPES);

                // check that the user-specified topic attr name exists
                checkUserSpecifiedAttributeNameExists(checker, OUTPUT_TOPIC_ATTRIBUTE_NAME_PARAM);

                // check that the user-specified timestamp attr name exists
                checkUserSpecifiedAttributeNameExists(checker, OUTPUT_TIMESTAMP_ATTRIBUTE_NAME_PARAM);

                // check that the user-specified offset attr name exists
                checkUserSpecifiedAttributeNameExists(checker, OUTPUT_OFFSET_ATTRIBUTE_NAME_PARAM);

                // check that the user-specified partition attr name exists
                checkUserSpecifiedAttributeNameExists(checker, OUTPUT_PARTITION_ATTRIBUTE_NAME_PARAM);


                if(paramNames.contains(START_POSITION_PARAM)) {
                    String startPositionValue = checker.getOperatorContext().getParameterValues(START_POSITION_PARAM).get(0);
                    if(startPositionValue.equals(StartPosition.Time.name())) {
                        // check that the startTime param exists if the startPosition param is set to 'Time'
                        if(!paramNames.contains(START_TIME_PARAM)) {
                            checker.setInvalidContext(Messages.getString("START_TIME_PARAM_NOT_FOUND"), new Object[0]); //$NON-NLS-1$
                        }
                    } else if(startPositionValue.equals(StartPosition.Offset.name())) {
                        // check that the startOffset param exists if the startPosition param is set to 'Offset
                        if(!paramNames.contains(START_OFFSET_PARAM)) {
                            checker.setInvalidContext(Messages.getString("START_OFFSET_PARAM_NOT_FOUND"), new Object[0]); //$NON-NLS-1$
                            return;
                        }

                        int numPartitionValues = checker.getOperatorContext().getParameterValues(PARTITION_PARAM).size();
                        int numStartOffsetValues = checker.getOperatorContext().getParameterValues(START_OFFSET_PARAM).size();
                        if(numPartitionValues != numStartOffsetValues) {
                            checker.setInvalidContext(Messages.getString("PARTITION_SIZE_NOT_EQUAL_TO_OFFSET_SIZE"), new Object[0]); //$NON-NLS-1$
                            return;
                        }

                        int numTopicValues = checker.getOperatorContext().getParameterValues(TOPIC_PARAM).size();
                        if(numTopicValues > 1) {
                            checker.setInvalidContext(Messages.getString("ONLY_ONE_TOPIC_WHEN_USING_STARTOFFSET_PARAM"), new Object[0]); //$NON-NLS-1$
                        }
                    }
                }
                checkTriggerCountValue (checker);
                checkCrPartitionAssignmentMode (checker);
    }

    private static void checkUserSpecifiedAttributeNameExists(OperatorContextChecker checker, String paramNameToCheck) {
        StreamSchema streamSchema = checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema();
        Set<String> paramNames = checker.getOperatorContext().getParameterNames();

        Attribute attr = null;
        if (paramNames.contains(paramNameToCheck)) {
            String topicAttrName = checker.getOperatorContext().getParameterValues(paramNameToCheck).get(0);
            attr = streamSchema.getAttribute(topicAttrName);
            if(attr == null) {
                checker.setInvalidContext(Messages.getString("OUTPUT_ATTRIBUTE_NOT_FOUND", attr), //$NON-NLS-1$
                        new Object[0]);
            }
        }
    }

    private static void checkCrPartitionAssignmentMode (OperatorContextChecker checker) {
        OperatorContext operatorContext = checker.getOperatorContext();
        ConsistentRegionContext crContext = operatorContext.getOptionalContext(ConsistentRegionContext.class);
        List<StreamingInput<Tuple>> inputPorts = operatorContext.getStreamingInputs();
        Set<String> parameterNames = operatorContext.getParameterNames();
        if (crContext != null) {
            if (parameterNames.contains(CR_ASSIGNMENT_MODE_PARAM)) {
                // get parameter value 
                final String crAssignModeParamVal = operatorContext.getParameterValues (CR_ASSIGNMENT_MODE_PARAM).get(0);
                if (crAssignModeParamVal.equals(ConsistentRegionAssignmentMode.GroupCoordinated.name())) {
                    // incompatible with startPosition: Offset - would assign partitions
                    if (parameterNames.contains(START_POSITION_PARAM)) {
                        final String startPositionVal = operatorContext.getParameterValues (START_POSITION_PARAM).get(0);
                        if (startPositionVal.equals (StartPosition.Offset.name())) {
                            checker.setInvalidContext (Messages.getString ("PARAM_VAL_INCOMPATIBLE_WITH_OTHER_PARAM_VAL",
                                    START_POSITION_PARAM, StartPosition.Offset,
                                    CR_ASSIGNMENT_MODE_PARAM, crAssignModeParamVal), new Object[0]);
                        }
                    }
                    // incompatible with 'partition' param - manual partition assignment incompatible with Group Management 
                    if (parameterNames.contains(PARTITION_PARAM)) {
                        checker.setInvalidContext (Messages.getString ("PARAM_INCOMPATIBLE_WITH_OTHER_PARAM_VAL",
                                PARTITION_PARAM, CR_ASSIGNMENT_MODE_PARAM, crAssignModeParamVal), new Object[0]);
                    }
                    // incompatible with input port
                    if(inputPorts.size() > 0) {
                        checker.setInvalidContext (Messages.getString ("PARAM_VAL_INCOMPATIBLE_WITH_INPUT_PORT",
                                CR_ASSIGNMENT_MODE_PARAM, crAssignModeParamVal), new Object[0]);
                    }
                }
            }
        }
    }


    private static void checkTriggerCountValue (OperatorContextChecker checker) {
        ConsistentRegionContext crContext = checker.getOperatorContext()
                .getOptionalContext(ConsistentRegionContext.class);
        if (crContext != null) {
            if (crContext.isStartOfRegion() && crContext.isTriggerOperator()) {
                // here we have checked (compile time) that the TRIGGER_COUNT_PARAM parameter exists...
                int triggerCount = Integer.valueOf(checker.getOperatorContext().getParameterValues(TRIGGER_COUNT_PARAM).get(0));
                if (triggerCount <= 0) {
                    checker.setInvalidContext(Messages.getString("INVALID_PARAMETER_VALUE_GT", TRIGGER_COUNT_PARAM, "" + triggerCount, "0"), //$NON-NLS-1$
                            new Object[0]);
                }
            }
        }
    }


    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        // Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                + context.getPE().getJobId());
        shutdown = new AtomicBoolean(false);
        gson = new Gson();

        StreamSchema outputSchema = context.getStreamingOutputs().get(0).getStreamSchema();
        hasOutputKey = outputSchema.getAttribute(outputKeyAttrName) != null;
        hasOutputTopic = outputSchema.getAttribute(outputTopicAttrName) != null;
        hasOutputTimetamp = outputSchema.getAttribute(outputMessageTimestampAttrName) != null;
        hasOutputPartition = outputSchema.getAttribute(outputPartitionAttrName) != null;
        hasOutputOffset = outputSchema.getAttribute(outputOffsetAttrName) != null;


        Class<?> keyClass = hasOutputKey ? getAttributeType(context.getStreamingOutputs().get(0), outputKeyAttrName)
                : String.class; // default to String.class for key type
        Class<?> valueClass = getAttributeType(context.getStreamingOutputs().get(0), outputMessageAttrName);
        KafkaOperatorProperties kafkaProperties = getKafkaProperties();

        // set the group ID property if the groupId parameter is specified
        if(groupId != null && !groupId.isEmpty()) {
            kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        crContext = context.getOptionalContext (ConsistentRegionContext.class);

        if (crContext == null) {
            NonCrKafkaConsumerClient.Builder builder = new NonCrKafkaConsumerClient.Builder();
            builder.setOperatorContext(context)
            .setKafkaProperties(kafkaProperties)
            .setKeyClass(keyClass)
            .setValueClass(valueClass)
            .setPollTimeout(this.consumerPollTimeout)
            .setCommitCount(commitCount);
            consumer = builder.build();
        } 
        else switch (this.consistentRegionAssignmentMode) {
        case GroupCoordinated: {
            CrKafkaConsumerGroupClient.Builder builder = new CrKafkaConsumerGroupClient.Builder();
            builder.setOperatorContext(context)
            .setKafkaProperties(kafkaProperties)
            .setKeyClass (keyClass)
            .setValueClass (valueClass)
            .setPollTimeout (this.consumerPollTimeout)
            .setTriggerCount (this.triggerCount)
            .setInitialStartPosition (this.startPosition)
            .setInitialStartTimestamp (this.startTime);
            consumer = builder.build();
            break;
        }
        case Static: {
            CrKafkaStaticAssignConsumerClient.Builder builder = new CrKafkaStaticAssignConsumerClient.Builder();
            builder.setOperatorContext(context)
            .setKafkaProperties(kafkaProperties)
            .setKeyClass(keyClass)
            .setValueClass(valueClass)
            .setPollTimeout(this.consumerPollTimeout)
            .setTriggerCount(this.triggerCount);
            consumer = builder.build();
            break;
        }
        default:
            throw new NotImplementedException (this.consistentRegionAssignmentMode + " not implemented");
        }
        try {
            consumer.startConsumer();
        }
        catch (KafkaClientInitializationException e) {

            e.printStackTrace();
            logger.error(e.getLocalizedMessage(), e);
            logger.error("root cause: " + e.getRootCause());
            throw e;
        }

        // input port not used, so topic must be defined
        if(context.getStreamingInputs().size() == 0) {
            if (topics != null) {
                final boolean registerAsInput = true;
                registerForDataGovernance(context, topics, registerAsInput);

                if(startPosition == StartPosition.Time) {
                    consumer.subscribeToTopicsWithTimestamp(topics, partitions, startTime);
                } else if(startPosition == StartPosition.Offset) {
                    consumer.subscribeToTopicsWithOffsets(topics.get(0), partitions, startOffsets);
                } else {
                    consumer.subscribeToTopics(topics, partitions, startPosition);
                }
            }	
        }

        if (crContext != null && context.getPE().getRelaunchCount() > 0) {
            resettingLatch = new CountDownLatch(1);
        }

        processThread = getOperatorContext().getThreadFactory().newThread(new Runnable() {

            @Override
            public void run() {
                try {
                    // initiates start polling if assigned or subscribed by sending an event
                    produceTuples();
                } catch (Exception e) {
                    Logger.getLogger(this.getClass()).error("Operator error", e); //$NON-NLS-1$
                    // Propagate all exceptions to the runtime to make the PE fail and possibly restart.
                    // Otherwise this thread terminates leaving the PE in a healthy state without being healthy.
                    throw new RuntimeException (e.getLocalizedMessage(), e);
                }
            }
        });

        processThread.setDaemon(false);
    }

    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() //$NON-NLS-1$ //$NON-NLS-2$
                + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$
        // start the thread that produces the tuples out of the message queue. The thread runs the produceTuples() method.
        if(processThread != null)
            processThread.start();
    }

    private void produceTuples() throws Exception {

        if (crContext != null && resettingLatch != null) {
            logger.debug("Operator is in the middle of resetting. No tuples will be submitted until reset completes."); //$NON-NLS-1$
            try {
                resettingLatch.await();
            } catch (InterruptedException e) {
                // shutdown occurred in the middle of CR reset, finish gracefully
                return;
            }
        }

        if(consumer.isSubscribedOrAssigned()) {
            consumer.sendStartPollingEvent();
        }
        /*
         * Shutdown implementation:
         * On shutdown, all threads get interrupted and throw InterruptedException, which must be caught and handled.
         * 
         * When we catch InterruptedException, the method is immediately left by return statement. When we only leave
         * the while-loop, we run into `consumer.sendStopPollingEvent();`, which contains a wait, that another thread processes
         * the event. This will most likely not happen because this thread also has been interrupted and finished working.
         */
        while (!shutdown.get()) {
            if (crContext != null) {
                try {
                    //logger.trace("Acquiring consistent region permit..."); //$NON-NLS-1$
                    crContext.acquirePermit();
                } catch (InterruptedException e) {
                    // shutdown occured waiting for permit, finish gracefully
                    logger.debug (Messages.getString("ERROR_ACQUIRING_PERMIT", e.getLocalizedMessage())); //$NON-NLS-1$
                    return;
                }
            }
            try {
                // Any exceptions except InterruptedException thrown here are propagated to the caller
                ConsumerRecord<?, ?> record = consumer.getNextRecord (1000, TimeUnit.MILLISECONDS);
                if (record != null) {
                    submitRecord(record);
                    consumer.postSubmit(record);
                }
            }
            catch (InterruptedException ie) {
                logger.debug("Queue processing thread interrupted", ie);
                return;
            }
            finally {
                if (crContext != null) {
                    if (logger.isDebugEnabled()) logger.debug ("Releasing consistent region permit..."); //$NON-NLS-1$
                    crContext.releasePermit();
                }
            }
        }
        try {
            consumer.sendStopPollingEvent();
        }
        catch (InterruptedException ie) {
            // interrupted during shutdown
            return;
        }
    }

    private void submitRecord(ConsumerRecord<?, ?> record) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("Preparing to submit record: " + record.topic() + "-" + record.partition() + "[" + record.offset() + "]"); //$NON-NLS-1$
        }
        // issue #65 (https://github.com/IBMStreams/streamsx.kafka/issues/65):
        // in case of deserialization errors we return 'null', otherwise a vaild object.
        // In these cases we drop the record and increment the metric 'nMalformedMessages'.
        if (record.value() == null) {
            logger.warn("dropping message with malformed value from topic = "
                    + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
            nMalformedMessages.increment();
            return;
        }
        final StreamingOutput<OutputTuple> out = getOutput(0);
        OutputTuple tuple = out.newTuple();
        setTuple(tuple, outputMessageAttrName, record.value());

        if (hasOutputKey) {
            // if record.key() is null, we have no evidence that this happend really by a malformed key.
            // It can also be an unkeyed message. So, dropping the message seems not appropriate in this case.
            // 
            // key = null would be mapped to
            // * empty rstring
            // * 0 for Integer, or float64
            // 
            // in the key attribute of the outgoing tuple. 
            //            if (record.key() == null) {
            //                logger.warn("dropping message with malformed key from topic = "
            //                        + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
            //                nMalformedMessages.increment();
            //                return;
            //            }
            setTuple(tuple, outputKeyAttrName, record.key());
        }

        if (hasOutputTopic) {
            tuple.setString(outputTopicAttrName, record.topic());
        }

        if(hasOutputOffset) {
            tuple.setLong(outputOffsetAttrName, record.offset());
        }

        if(hasOutputPartition) {
            tuple.setInt(outputPartitionAttrName, record.partition());
        }

        if(hasOutputTimetamp) {
            tuple.setLong(outputMessageTimestampAttrName, record.timestamp());
        }            
        out.submit(tuple);
    }

    private void setTuple(OutputTuple tuple, String attrName, Object attrValue) throws Exception {
        if(attrValue == null)
            return; // do nothing

        if (attrValue instanceof String || attrValue instanceof RString)
            tuple.setString(attrName, (String) attrValue);
        else if (attrValue instanceof Integer)
            tuple.setInt(attrName, (Integer) attrValue);
        else if (attrValue instanceof Double)
            tuple.setDouble(attrName, (Double) attrValue);
        else if (attrValue instanceof Float)
            tuple.setFloat(attrName, (Float) attrValue);
        else if (attrValue instanceof Long)
            tuple.setLong(attrName, (Long) attrValue);
        else if (attrValue instanceof Byte)
            tuple.setByte(attrName, (Byte) attrValue);
        else if (attrValue instanceof byte[])
            tuple.setBlob(attrName, ValueFactory.newBlob((byte[]) attrValue));
        else
            throw new Exception(Messages.getString("UNSUPPORTED_TYPE_EXCEPTION", (attrValue.getClass().getTypeName()), attrName)); //$NON-NLS-1$
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {

        boolean interrupted = false;
        try {
            String jsonString = tuple.getString(0);
            JsonObject jsonObj = gson.fromJson(jsonString, JsonObject.class);

            TopicPartitionUpdateAction action = null;
            if(jsonObj.has("action")) { //$NON-NLS-1$
                action = TopicPartitionUpdateAction.valueOf(jsonObj.get("action").getAsString().toUpperCase()); //$NON-NLS-1$
            } else {
                logger.error(Messages.getString("INVALID_JSON_MISSING_KEY", "action", jsonString)); //$NON-NLS-1$ //$NON-NLS-2$
                return;
            }

            Map<TopicPartition, Long> topicPartitionOffsetMap = null;
            if(jsonObj.has("topicPartitionOffsets")) { //$NON-NLS-1$
                topicPartitionOffsetMap = new HashMap<TopicPartition, Long>();
                JsonArray arr = jsonObj.get("topicPartitionOffsets").getAsJsonArray(); //$NON-NLS-1$
                Iterator<JsonElement> it = arr.iterator();
                while(it.hasNext()) {
                    JsonObject tpo = it.next().getAsJsonObject();
                    if(!tpo.has("topic")) { //$NON-NLS-1$
                        logger.error(Messages.getString("INVALID_JSON_MISSING_KEY", "topic", jsonString)); //$NON-NLS-1$ //$NON-NLS-2$
                        return;
                    }

                    if(!tpo.has("partition")) { //$NON-NLS-1$
                        logger.error(Messages.getString("INVALID_JSON_MISSING_KEY", "partition", jsonString)); //$NON-NLS-1$ //$NON-NLS-2$
                        return;
                    }


                    if(action == TopicPartitionUpdateAction.ADD && !tpo.has("offset")) { //$NON-NLS-1$
                        logger.error(Messages.getString("INVALID_JSON_MISSING_KEY", "offset", jsonString)); //$NON-NLS-1$ //$NON-NLS-2$
                        return;
                    }

                    String topic = tpo.get("topic").getAsString(); //$NON-NLS-1$
                    int partition = tpo.get("partition").getAsInt(); //$NON-NLS-1$
                    long offset = tpo.has("offset") ? tpo.get("offset").getAsLong() : 0l; //$NON-NLS-1$ //$NON-NLS-2$

                    topicPartitionOffsetMap.put(new TopicPartition(topic, partition), offset);
                }
            }

            //        	consumer.sendStopPollingEvent();
            consumer.sendUpdateTopicAssignmentEvent(new TopicPartitionUpdate(action, topicPartitionOffsetMap));
        } catch (InterruptedException e) {
            // interrupted during shutdown
            interrupted = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (!interrupted && consumer.isSubscribedOrAssigned()) {
                consumer.sendStartPollingEvent();
            }
        }
    }

    /**
     * Shutdown this operator, which will interrupt the thread executing the
     * <code>produceTuples()</code> method.
     * 
     * @throws Exception
     *             Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        shutdown.set(true);
        consumer.onShutdown (SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT_TIMEUNIT);
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() //$NON-NLS-1$ //$NON-NLS-2$
                + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$

        // Must call super.shutdown()
        super.shutdown();
    }

    @Override
    public void drain() throws Exception {
        logger.info(">>> DRAIN"); //$NON-NLS-1$
        long before = System.currentTimeMillis();
        consumer.onDrain();
        // When a checkpoint is to be created, the operator must stop sending tuples by pulling messages out of the messageQueue.
        // This is achieved via acquiring a permit. In the background, more messages are pushed into the queue by a receive thread
        // incrementing the read offset.
        // For every tuple that is submitted, its next offset is stored in a data structure (offset manager).
        // On checkpoint, the offset manager is saved. On reset of the CR, the consumer starts reading at these previously saved offsets,
        // reading the messages since last checkpoint again.
        long after = System.currentTimeMillis();
        final long duration = after - before;
        getOperatorContext().getMetrics().getCustomMetric(ConsumerClient.DRAIN_TIME_MILLIS_METRIC_NAME).setValue(duration);
        if (duration > maxDrainMillis) {
            getOperatorContext().getMetrics().getCustomMetric(ConsumerClient.DRAIN_TIME_MILLIS_MAX_METRIC_NAME).setValue(duration);
            maxDrainMillis = duration;
        }
        logger.info(">>> DRAIN took " + duration + " ms");
    }

    /**
     * @see com.ibm.streamsx.kafka.operators.AbstractKafkaOperator#retireCheckpoint(long)
     */
    @Override
    public void retireCheckpoint (long id) throws Exception {
        logger.debug(">>> RETIRE CHECKPOINT (ckpt id=" + id + ")");
        consumer.onCheckpointRetire (id);
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.info(">>> CHECKPOINT (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        consumer.onCheckpoint (checkpoint);
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        final int attempt = crContext.getResetAttempt();
        final long sequenceId = checkpoint.getSequenceId();
        logger.info(MessageFormat.format(">>> RESET (ckpt id/attempt={0}/{1})", sequenceId, attempt));
        final long before = System.currentTimeMillis();
        try {
            consumer.onReset (checkpoint);
        }
        catch (InterruptedException e) {
            logger.info("RESET interrupted)");
            return;
        }
        finally {
            // latch will be null if the reset was caused
            // by another operator
            if (resettingLatch != null) resettingLatch.countDown();
            final long after = System.currentTimeMillis();
            final long duration = after - before;
            logger.info(MessageFormat.format(">>> RESET took {0} ms (ckpt id/attempt={1}/{2})", duration, sequenceId, attempt));
        }
    }

    @Override
    public void resetToInitialState() throws Exception {
        final int attempt = crContext.getResetAttempt();
        logger.info(MessageFormat.format(">>> RESET TO INIT (attempt={0})", attempt));
        final long before = System.currentTimeMillis();
        consumer.onResetToInitialState();

        // latch will be null if the reset was caused
        // by another operator
        if (resettingLatch != null) resettingLatch.countDown();
        final long after = System.currentTimeMillis();
        final long duration = after - before;
        logger.info(MessageFormat.format(">>> RESET TO INIT took {0} ms (attempt={1})", duration, attempt));
    }
}
