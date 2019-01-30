package com.ibm.streamsx.kafka.operators;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

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
import com.ibm.streamsx.kafka.Features;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.TopicPartitionUpdateParseException;
import com.ibm.streamsx.kafka.clients.consumer.CommitMode;
import com.ibm.streamsx.kafka.clients.consumer.ConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.CrKafkaConsumerGroupClient;
import com.ibm.streamsx.kafka.clients.consumer.CrKafkaStaticAssignConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.NonCrKafkaConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.NonCrKafkaConsumerGroupClient;
import com.ibm.streamsx.kafka.clients.consumer.StartPosition;
import com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate;
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
    public static final String TOPIC_PARAM = "topic"; //$NON-NLS-1$
    public static final String PARTITION_PARAM = "partition"; //$NON-NLS-1$
    public static final String START_POSITION_PARAM = "startPosition"; //$NON-NLS-1$
    public static final String START_TIME_PARAM = "startTime"; //$NON-NLS-1$
    public static final String TRIGGER_COUNT_PARAM = "triggerCount"; //$NON-NLS-1$
    public static final String COMMIT_COUNT_PARAM = "commitCount"; //$NON-NLS-1$
    public static final String COMMIT_PERIOD_PARAM = "commitPeriod"; //$NON-NLS-1$
    public static final String START_OFFSET_PARAM = "startOffset"; //$NON-NLS-1$

    private static final double DEFAULT_COMMIT_PERIOD = 5.0;

    private Thread processThread;
    private ConsumerClient consumer;
    private AtomicBoolean shutdown;

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
    private int commitCount = 0;
    private double commitPeriod = DEFAULT_COMMIT_PERIOD;
    private CommitMode commitMode = CommitMode.Time;
    private String groupId = null;
    private boolean groupIdSpecified = false;
    private Long startTime = -1l;

    private long consumerPollTimeout = DEFAULT_CONSUMER_TIMEOUT;
    private CountDownLatch resettingLatch;
    private boolean hasOutputTopic;
    private boolean hasOutputKey;
    private boolean hasOutputOffset;
    private boolean hasOutputPartition;
    private boolean hasOutputTimetamp;

    // The number of messages in which the value was malformed and could not be deserialized
    private Metric nMalformedMessages;
    private Metric isGroupManagementActive;
    long maxDrainMillis = 0l;

    // Initialize the metrics
    @CustomMetric (kind = Metric.Kind.GAUGE, name = "isGroupManagementActive", description = "Shows the Kafka group management state of the operator. "
            + "When the metric shows 1, group management is active. When the metric is 0, group management is not in place.")
    public void setIsGroupManagementActive (Metric isGroupManagementActive) {
        this.isGroupManagementActive = isGroupManagementActive;
    }

    @CustomMetric (kind = Metric.Kind.COUNTER, name = "nDroppedMalformedMessages", description = "Number of dropped malformed messages")
    public void setnMalformedMessages (Metric nMalformedMessages) {
        this.nMalformedMessages = nMalformedMessages;
    }

    @CustomMetric (kind = Metric.Kind.GAUGE, description = "Number of pending messages to be submitted as tuples.")
    public void setnPendingMessages(Metric nPendingMessages) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.COUNTER, description = "Number times message fetching was paused due to low memory.")
    public void setnLowMemoryPause(Metric nLowMemoryPause) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.COUNTER, description = "Number times message fetching was paused due to full queue.")
    public void setnQueueFullPause(Metric nQueueFullPause) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
    }

    @CustomMetric (kind = Metric.Kind.GAUGE, description = "Number of topic partitions assigned to the consumer.")
    public void setnAssignedPartitions(Metric nAssignedPartitions) {
        // No need to do anything here. The annotation injects the metric into the operator context, from where it can be retrieved.
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
        if (startOffsets != null) {
            this.startOffsets = new ArrayList<>(startOffsets.length);
            for (long o: startOffsets) this.startOffsets.add (o);
        }
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
                    + "specified, the operator will use a generated group ID, "
                    + "and the group management feature is not active.")
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
                    + "value of consumer property `auto.offset.reset` applies (default `latest`)."
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
                    + "assigns itself statically to the given partition(s). "
                    + "When `Offset` is used as the start position **only one single topic** can be specified via the "
                    + "**topic** parameter, and the operator cannot participate in a consumer group.\\n"
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
        if (partitions != null) {
            this.partitions = new ArrayList<>(partitions.length);
            for (int p: partitions) this.partitions.add (p);
        }

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

    @Parameter(optional = true, name = COMMIT_PERIOD_PARAM, description = 
            "This parameter specifies the period of time in seconds, after which the offsets of submitted tuples are committed. "
                    + "This parameter is optional "
                    + "and has a default value of " + DEFAULT_COMMIT_PERIOD + ". Its minimum value is "
                    + "`0.1`, smaller values are pinned to 0.1 seconds. This parameter cannot be used when the **"
                    + COMMIT_COUNT_PARAM + "** parameter is used.\\n"
                    + "\\n" 
                    + "This parameter is only used when the "
                    + "operator is not part of a consistent region. When the operator participates in a "
                    + "consistent region, offsets are always committed when the region drains.")
    public void setCommitPeriod (double period) {
        if (period < 0.1) {
            logger.warn ("The commitPeriod has been pinned to 0.1");
            this.commitPeriod = 0.1;
        }
        else this.commitPeriod = period;
    }

    @Parameter(optional = true, name = COMMIT_COUNT_PARAM, description = 
            "This parameter specifies the number of tuples that will be submitted to "
                    + "the output port before committing their offsets. Valid values are greater than zero. "
                    + "This parameter is optional and conflicts with the **" + COMMIT_PERIOD_PARAM + "** parameter.\\n"
                    + "\\n"
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
            if (parameterNames.contains(COMMIT_PERIOD_PARAM)) {
                System.err.println (Messages.getString ("PARAM_IGNORED_IN_CONSITENT_REGION", COMMIT_PERIOD_PARAM));
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
            if (parameterNames.contains(COMMIT_COUNT_PARAM) && parameterNames.contains(COMMIT_PERIOD_PARAM)) {
                checker.setInvalidContext (Messages.getString ("PARAMETERS_EXCLUDE_EACH_OTHER", COMMIT_COUNT_PARAM, COMMIT_PERIOD_PARAM), new Object[0]); //$NON-NLS-1$
            }
        }
    }

    //    @ContextCheck (compile = true)
    public static void warnStartPositionParamRequiresJCP (OperatorContextChecker checker) {
        if (!(Features.ENABLE_NOCR_CONSUMER_GRP_WITH_STARTPOSITION || Features.ENABLE_NOCR_NO_CONSUMER_SEEK_AFTER_RESTART)) {
            return;
        }
        OperatorContext opCtx = checker.getOperatorContext();
        Set<String> paramNames = opCtx.getParameterNames();
        List<StreamingInput<Tuple>> inputPorts = opCtx.getStreamingInputs();

        if (opCtx.getOptionalContext (ConsistentRegionContext.class) == null
                && paramNames.contains (START_POSITION_PARAM)
                && inputPorts.size() == 0) {
            System.err.println (Messages.getString ("WARN_ENSURE_JCP_ADDED_STARTPOS_NOT_DEFAULT", opCtx.getKind()));
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

    // check that startPosition: Offset|Time is specified together with startOffset|startTime parameter
    @ContextCheck(compile = false, runtime = true)
    public static void checkStartPositionAdditionalParameters (OperatorContextChecker checker) {
        Set<String> paramNames = checker.getOperatorContext().getParameterNames();
        if (paramNames.contains (START_POSITION_PARAM)) {
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
                }
            }
        }
    }

    @ContextCheck(compile = false, runtime = true)
    public static void checkParams(OperatorContextChecker checker) {
        StreamSchema streamSchema = checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema();
        Set<String> paramNames = checker.getOperatorContext().getParameterNames();

        String messageAttrName = paramNames.contains(OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM)? checker.getOperatorContext().getParameterValues(OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM).get(0): DEFAULT_OUTPUT_MESSAGE_ATTR_NAME;

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
        logger.info ("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                + context.getPE().getJobId());
        shutdown = new AtomicBoolean(false);

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
        if (groupId != null && !groupId.isEmpty()) {
            kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        final boolean hasInputPorts = context.getStreamingInputs().size() > 0;
        final String gid = kafkaProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        this.groupIdSpecified = gid != null && !gid.isEmpty();
        logger.debug ("group-ID specified: " + this.groupIdSpecified);
        crContext = context.getOptionalContext (ConsistentRegionContext.class);
        boolean groupManagementEnabled;
    
        if (crContext == null && !Features.ENABLE_NOCR_CONSUMER_GRP_WITH_STARTPOSITION)
            groupManagementEnabled = this.groupIdSpecified && !hasInputPorts && (this.partitions == null || this.partitions.isEmpty()) && startPosition == StartPosition.Default;
        else 
            groupManagementEnabled = this.groupIdSpecified && !hasInputPorts && (this.partitions == null || this.partitions.isEmpty());
        if (this.groupIdSpecified && !groupManagementEnabled) {
            if (hasInputPorts) {
                logger.warn (MessageFormat.format ("The group.id ''{0}'' is specified. The ''{1}'' operator "
                        + "will NOT participate in a consumer group because the operator is configured with an input port.",
                        gid, context.getName()));
            }
            if (this.partitions != null && !this.partitions.isEmpty()) {
                logger.warn (MessageFormat.format ("The group.id ''{0}'' is specified. The ''{1}'' operator "
                        + "will NOT participate in a consumer group because partitions to consume are specified.",
                        gid, context.getName()));
            }
            if (startPosition != StartPosition.Default && !Features.ENABLE_NOCR_CONSUMER_GRP_WITH_STARTPOSITION && crContext == null) {
                logger.warn (MessageFormat.format ("The group.id ''{0}'' is specified. The ''{1}'' operator "
                        + "will NOT participate in a consumer group because a startPosition != Default is configured.",
                        gid, context.getName()));
            }
        }
        if (crContext != null) {
            commitMode = CommitMode.ConsistentRegionDrain;
        }
        else {
            final Set <String> parameterNames = context.getParameterNames();
            commitMode = parameterNames.contains (COMMIT_COUNT_PARAM)? CommitMode.TupleCount: CommitMode.Time;
        }
        this.isGroupManagementActive.setValue (groupManagementEnabled? 1: 0);
        if (crContext == null) {
            if (groupManagementEnabled) {
                NonCrKafkaConsumerGroupClient.Builder builder = new NonCrKafkaConsumerGroupClient.Builder();
                builder.setOperatorContext(context)
                .setKafkaProperties(kafkaProperties)
                .setKeyClass(keyClass)
                .setValueClass(valueClass)
                .setNumTopics (this.topics == null? 0: this.topics.size())
                .setPollTimeout(this.consumerPollTimeout)
                .setInitialStartPosition (this.startPosition)
                .setCommitMode (commitMode)
                .setCommitPeriod (commitPeriod)
                .setCommitCount(commitCount);
                consumer = builder.build();
            }
            else {
                NonCrKafkaConsumerClient.Builder builder = new NonCrKafkaConsumerClient.Builder();
                builder.setOperatorContext(context)
                .setKafkaProperties(kafkaProperties)
                .setKeyClass(keyClass)
                .setValueClass(valueClass)
                .setPollTimeout(this.consumerPollTimeout)
                .setInitialStartPosition (this.startPosition)
                .setCommitMode (commitMode)
                .setCommitPeriod (commitPeriod)
                .setCommitCount(commitCount);
                consumer = builder.build();
            }
        } 
        else {
            if (groupManagementEnabled) {
                CrKafkaConsumerGroupClient.Builder builder = new CrKafkaConsumerGroupClient.Builder();
                builder.setOperatorContext(context)
                .setKafkaProperties(kafkaProperties)
                .setKeyClass (keyClass)
                .setValueClass (valueClass)
                .setPollTimeout (this.consumerPollTimeout)
                .setNumTopics (this.topics == null? 0: this.topics.size())
                .setTriggerCount (this.triggerCount)
                .setInitialStartPosition (this.startPosition)
                .setInitialStartTimestamp (this.startTime);
                consumer = builder.build();
            }
            else {
                CrKafkaStaticAssignConsumerClient.Builder builder = new CrKafkaStaticAssignConsumerClient.Builder();
                builder.setOperatorContext(context)
                .setKafkaProperties(kafkaProperties)
                .setKeyClass(keyClass)
                .setValueClass(valueClass)
                .setPollTimeout(this.consumerPollTimeout)
                .setTriggerCount(this.triggerCount);
                consumer = builder.build();
            }
        }
        logger.info (MessageFormat.format ("consumer client {0} created", consumer.getClass().getName()));
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
        if (!hasInputPorts) {
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
        logger.info ("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() //$NON-NLS-1$ //$NON-NLS-2$
                + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$
        // start the thread that produces the tuples out of the message queue. The thread runs the produceTuples() method.
        if (processThread != null) {
            processThread.start();
        }
    }

    private void produceTuples() throws Exception {

        if (crContext != null && resettingLatch != null) {
            logger.debug ("Defer tuple submission until reset finishes. Waiting ..."); //$NON-NLS-1$
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
                // Make timeout for 'getNextRecord' not too high as it influences the granularity of time based offset commit
                ConsumerRecord<?, ?> record = consumer.getNextRecord (100, TimeUnit.MILLISECONDS);
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
            TopicPartitionUpdate updt = TopicPartitionUpdate.fromJSON (tuple.getString(0));
            consumer.onTopicAssignmentUpdate (updt);
        } catch (TopicPartitionUpdateParseException e) {
            logger.error("Could not process control tuple. Parsing JSON '" + e.getJson() + "' failed.");
            logger.error (e.getMessage(), e);
        } catch (InterruptedException e) {
            // interrupted during shutdown
            interrupted = true;
        } catch (Exception e) {
            logger.error("Could not process control tuple: '" + tuple + "'");
            logger.error(e.getMessage(), e);
        } finally {
            if (!interrupted && consumer.isSubscribedOrAssigned()) {
                logger.info ("sendStartPollingEvent ...");
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
        final OperatorContext context = getOperatorContext();
        logger.info ("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() //$NON-NLS-1$ //$NON-NLS-2$
                + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$
        shutdown.set(true);
        consumer.onShutdown (SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT_TIMEUNIT);

        // Must call super.shutdown()
        super.shutdown();
    }

    @Override
    public void drain() throws Exception {
        logger.debug (">>> DRAIN"); //$NON-NLS-1$
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
        logger.debug (">>> DRAIN took " + duration + " ms");
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
        logger.debug (">>> CHECKPOINT (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        consumer.onCheckpoint (checkpoint);
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        final int attempt = crContext == null? -1: crContext.getResetAttempt();
        final long sequenceId = checkpoint.getSequenceId();
        logger.debug (MessageFormat.format(">>> RESET (ckpt id/attempt={0}/{1})", sequenceId, (crContext == null? "-": "" + attempt)));
        final long before = System.currentTimeMillis();
        try {
            consumer.sendStopPollingEvent();
            consumer.onReset (checkpoint);
        }
        catch (InterruptedException e) {
            logger.debug ("RESET interrupted)");
            return;
        }
        finally {
            // latch will be null if the reset was caused
            // by another operator
            if (resettingLatch != null) resettingLatch.countDown();
            final long after = System.currentTimeMillis();
            final long duration = after - before;
            logger.debug (MessageFormat.format(">>> RESET took {0} ms (ckpt id/attempt={1}/{2})", duration, sequenceId, attempt));
        }
    }

    @Override
    public void resetToInitialState() throws Exception {
        final int attempt = crContext.getResetAttempt();
        logger.debug (MessageFormat.format(">>> RESET TO INIT (attempt={0})", attempt));
        final long before = System.currentTimeMillis();
        consumer.sendStopPollingEvent();
        consumer.onResetToInitialState();

        // latch will be null if the reset was caused
        // by another operator
        if (resettingLatch != null) resettingLatch.countDown();
        final long after = System.currentTimeMillis();
        final long duration = after - before;
        logger.debug (MessageFormat.format(">>> RESET TO INIT took {0} ms (attempt={1})", duration, attempt));
    }
}
