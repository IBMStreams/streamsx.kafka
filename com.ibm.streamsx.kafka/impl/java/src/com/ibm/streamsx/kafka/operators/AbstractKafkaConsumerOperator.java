package com.ibm.streamsx.kafka.operators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.ibm.streamsx.kafka.clients.consumer.KafkaConsumerClient;
import com.ibm.streamsx.kafka.clients.consumer.StartPosition;
import com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdate;
import com.ibm.streamsx.kafka.clients.consumer.TopicPartitionUpdateAction;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public abstract class AbstractKafkaConsumerOperator extends AbstractKafkaOperator {	
	
    private static final Logger logger = Logger.getLogger(AbstractKafkaConsumerOperator.class);
    private static final Long DEFAULT_CONSUMER_TIMEOUT = 100l;
    private static final Long SHUTDOWN_TIMEOUT = 5l;
    private static final TimeUnit SHUTDOWN_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
    private static final StartPosition DEFAULT_START_POSITION = StartPosition.Default;
    private static final String DEFAULT_OUTPUT_MESSAGE_ATTR_NAME = "message"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_KEY_ATTR_NAME = "key"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_TOPIC_ATTR_NAME = "topic"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_TIMESTAMP_ATTR_NAME = "messageTimestamp"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_OFFSET_ATTR_NAME = "offset"; //$NON-NLS-1$
    private static final String DEFAULT_OUTPUT_PARTITION_ATTR_NAME = "partition"; //$NON-NLS-1$
    private static final String OUTPUT_KEY_ATTRIBUTE_NAME_PARAM = "outputKeyAttributeName"; //$NON-NLS-1$
    private static final String OUTPUT_MESSAGE_ATTRIBUTE_NAME_PARAM = "outputMessageAttributeName"; //$NON-NLS-1$
    private static final String OUTPUT_TOPIC_ATTRIBUTE_NAME_PARAM = "outputTopicAttributeName"; //$NON-NLS-1$
    private static final String OUTPUT_TIMESTAMP_ATTRIBUTE_NAME_PARAM = "outputTimestampAttributeName"; //$NON-NLS-1$
    private static final String OUTPUT_OFFSET_ATTRIBUTE_NAME_PARAM = "outputOffsetAttributeName"; //$NON-NLS-1$
    private static final String OUTPUT_PARTITION_ATTRIBUTE_NAME_PARAM = "outputPartitionAttributeName"; //$NON-NLS-1$
    private static final String TOPIC_PARAM = "topic"; //$NON-NLS-1$
    private static final String PARTITION_PARAM = "partition"; //$NON-NLS-1$
    private static final String START_POSITION_PARAM = "startPosition"; //$NON-NLS-1$
    private static final String START_TIME_PARAM = "startTime"; //$NON-NLS-1$
    private static final String TRIGGER_COUNT_PARAM = "triggerCount"; //$NON-NLS-1$
    private static final String START_OFFSET_PARAM = "startOffset"; //$NON-NLS-1$
    
    private Thread processThread;
    private KafkaConsumerClient consumer;
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
    private String groupId = null;
    private Long startTime;

    private Long consumerPollTimeout = DEFAULT_CONSUMER_TIMEOUT;
    private CountDownLatch resettingLatch;
    private boolean hasOutputTopic;
    private boolean hasOutputKey;
	private boolean hasOutputOffset;
	private boolean hasOutputPartition;
	private boolean hasOutputTimetamp;

    // The number of messages in which the value was malformed and could not be deserialized
    private Metric nMalformedMessages;

    // Initialize the metrics
    @CustomMetric (kind = Metric.Kind.COUNTER, name = "nDroppedMalformedMessages", description = "Number of dropped malformed messages")
    public void setnMalformedMessages (Metric nMalformedMessages) {
        this.nMalformedMessages = nMalformedMessages;
    }


    @Parameter(optional = true, name=OUTPUT_TIMESTAMP_ATTRIBUTE_NAME_PARAM,
    		description="Specifies the output attribute name that should contain the record's timestamp. "
    				+ "If not specified, the operator will attempt to store the message in an "
    				+ "attribute named 'messageTimestamp'.")
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
    				+ "should consume from must be specified via the `partition` parameter.\\n"
    				+ "\\n"
    				+ "If multiple partitions are specified via the `partition` parameter, then the same number of "
    				+ "offset values must be specified. There is a one-to-one mapping between the position of the "
    				+ "partition from the `partition` parameter and the position of the offset from the `startOffset` "
    				+ "parameter. For example, if the `partition` parameter has the values '0, 1', and the `startOffset` "
    				+ "parameter has the values '100, 200', then the operator will begin consuming messages from "
    				+ "partition 0 at offset 100 and will consume messages from partition 1 at offset 200.\\n"
    				+ "\\n"
    				+ "A limitation with using this parameter is that only a single topic can be specified. ")
    public void setStartOffsets(long[] startOffsets) {
    	this.startOffsets = Longs.asList(startOffsets);
    }
    
    @Parameter(optional = true, name="startTime",
    		description="This parameter is only used when the **startPosition** parameter is set to `Time`. "
    				+ "When the **startPosition** parameter is to `Time`, the operator will begin "
    				+ "reading records from the earliest offset whose timestamp is greater than or "
    				+ "equal to the timestamp specified by this parameter. If no offsets are found, then "
    				+ "the operator will begin reading messages from the end of the topic(s).")
    public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

    @Parameter(optional = true, name="groupId",
            description="Specifies the group ID that should be used "
                    + "when connecting to the Kafka cluster. The value "
                    + "specified by this parameter will override the `group.id` "
                    + "Kafka property if specified. If this parameter is not "
                    + "specified and he the `group.id` Kafka property is not "
                    + "specified, the operator will use a random group ID.")
    public void setGroupId (String groupId) {
        this.groupId = groupId;
    }

    @Parameter(optional = true, name="startPosition", 
    		description="Specifies whether the operator should start "
    				+ "reading from the end of the topic, the beginning of "
    				+ "the topic or from a specific timestamp. Valid "
    				+ "options include: `Beginning`, `End`, `Time`. If reading "
    				+ "from a specific timestamp (i.e. setting the parameter "
    				+ "value to `Time`), then the **startTime** parameter "
    				+ "must also be defined. If this parameter is not specified, "
    				+ "the default value is `End`.")
    public void setStartPosition(StartPosition startPosition) {
        this.startPosition = startPosition;
    }

    @Parameter(optional = true, name="partition",
    		description="Specifies the partitions that the consumer should be "
    				+ "assigned to for each of the topics specified. It should "
    				+ "be noted that using this parameter will *assign* the "
    				+ "consumer to the specified topics, rather than *subscribe* "
    				+ "to them. This implies that the consumer will not use Kafka's "
    				+ "group management feature.")
    public void setPartitions(int[] partitions) {
    	this.partitions = Ints.asList(partitions);
	}
    
    @Parameter(optional = true, name="topic",
    		description="Specifies the topic or topics that the consumer should "
    				+ "subscribe to. To assign the consumer to specific partitions, "
    				+ "use the **partitions** parameter.")
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
        	if(startPositionValue.equals("Time")) { //$NON-NLS-1$
                // check that the startTime param exists if the startPosition param is set to 'Time'
        		if(!paramNames.contains(START_TIME_PARAM)) {
        			checker.setInvalidContext(Messages.getString("START_TIME_PARAM_NOT_FOUND"), new Object[0]); //$NON-NLS-1$
        		}
        	} else if(startPositionValue.equals("Offset")) { //$NON-NLS-1$
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
    
    @ContextCheck(compile = true)
    public static void checkTriggerCount(OperatorContextChecker checker) {
        ConsistentRegionContext crContext = checker.getOperatorContext()
                .getOptionalContext(ConsistentRegionContext.class);
        if (crContext != null) {
            if (crContext.isStartOfRegion() && crContext.isTriggerOperator()) {
                if (!checker.getOperatorContext().getParameterNames().contains(TRIGGER_COUNT_PARAM)) {
                    checker.setInvalidContext(Messages.getString("TRIGGER_PARAM_MISSING"), new Object[0]); //$NON-NLS-1$
                }
            }
        }
    }

    @ContextCheck(compile = false, runtime = true)
    public static void checkTriggerCountValue (OperatorContextChecker checker) {
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
    	if(inputPorts.size() == 0 && !checker.getOperatorContext().getParameterNames().contains("topic")) { //$NON-NLS-1$
    		checker.setInvalidContext(Messages.getString("TOPIC_OR_INPUT_PORT"), new Object[0]); //$NON-NLS-1$
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
        logger.debug("kafkaProperties: " + kafkaProperties); //$NON-NLS-1$

        // set the group ID property if the groupId parameter is specified
        if(groupId != null && !groupId.isEmpty()) {
            kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        consumer = new KafkaConsumerClient.KafkaConsumerClientBuilder()
        			.setKafkaProperties(kafkaProperties)
        			.setKeyClass(keyClass)
        			.setValueClass(valueClass)
        			.setOperatorContext(context)
        			.build();
        
        // If an exception occurred during init, throw it!
        if(consumer.getInitializationException() != null) {
        	Exception e = consumer.getInitializationException();
            e.printStackTrace();
            logger.error(e.getLocalizedMessage(), e);
            throw e;      	
        }
        
        // input port not use, so topic must be defined
        if(context.getStreamingInputs().size() == 0) {
            if (topics != null) {
                registerForDataGovernance(context, topics);
                
                if(startPosition == StartPosition.Time) {
                	consumer.subscribeToTopicsWithTimestamp(topics, partitions, startTime);
                } else if(startPosition == StartPosition.Offset) {
                	consumer.subscribeToTopicsWithOffsets(topics, partitions, startOffsets);
                } else {
                	consumer.subscribeToTopics(topics, partitions, startPosition);
                }
            }	
        }
        
        crContext = context.getOptionalContext(ConsistentRegionContext.class);
        if (crContext != null && context.getPE().getRelaunchCount() > 0) {
            resettingLatch = new CountDownLatch(1);
        }

        processThread = getOperatorContext().getThreadFactory().newThread(new Runnable() {

            @Override
            public void run() {
                try {
                    produceTuples();
                } catch (Exception e) {
                    Logger.getLogger(this.getClass()).error("Operator error", e); //$NON-NLS-1$
                    // Propagate all exceptions to the runtime to make the PE fail and possibly restart.
                    // Otherwise this thread terminates leaving the PE in a healthy state without being healthy.
                    throw new RuntimeException (e);
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

        if(processThread != null)
        	processThread.start();
    }

    private void produceTuples() throws Exception {

        int nTuplesForOpDrivenCR = 0;
        if (crContext != null && resettingLatch != null) {
            logger.debug("Operator is in the middle of resetting. No tuples will be submitted until reset completes."); //$NON-NLS-1$
            try {
                resettingLatch.await();
            } catch (InterruptedException e) {
                // shutdown occurred in the middle of CR reset, finish gracefully
                return;
            }
        }

        if(consumer.isAssignedToTopics()) {
        	consumer.sendStartPollingEvent(consumerPollTimeout);
        }
        while (!shutdown.get()) {
            if (crContext != null) {
                try {
                    //logger.trace("Acquiring consistent region permit..."); //$NON-NLS-1$
                    crContext.acquirePermit();
                } catch (InterruptedException e) {
                    logger.error(Messages.getString("ERROR_ACQUIRING_PERMIT", e.getLocalizedMessage())); //$NON-NLS-1$
                    // shutdown occured waiting for permit, finish gracefully
                    return;
                }
            }
            try {
                // Any exceptions thrown here are propagated to the caller
                //logger.trace("Polling for messages, timeout=" + consumerPollTimeout); //$NON-NLS-1$
                ConsumerRecord<?, ?> record = consumer.getNextRecord();
                if(record != null) {
                	submitRecord(record);

                    if (crContext != null) {
                        // save offset for *next* record for {topic, partition} 
                        consumer.getOffsetManager().savePosition(record.topic(), record.partition(), record.offset()+1l);
                        if (crContext.isTriggerOperator() && ++nTuplesForOpDrivenCR >= triggerCount) {
                            logger.debug("Making region consistent..."); //$NON-NLS-1$
                            // makeConsistent blocks until all operators in the CR have drained and checkpointed
                            boolean isSuccess = crContext.makeConsistent();
                            nTuplesForOpDrivenCR = 0;
                            logger.debug("Completed call to makeConsistent: isSuccess=" + isSuccess); //$NON-NLS-1$
                        }
                    }
                }
            } finally {
                if (crContext != null) {
                    if (logger.isDebugEnabled()) logger.debug ("Releasing consistent region permit..."); //$NON-NLS-1$
                    crContext.releasePermit();
                }
            }
        }
        consumer.sendStopPollingEvent();
    }

    private void submitRecord(ConsumerRecord<?, ?> record) throws Exception {
        if (logger.isTraceEnabled())
    	     logger.trace("Preparing to submit record: " + record); //$NON-NLS-1$
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
        if (logger.isDebugEnabled()) logger.debug("Submitting tuple: " + tuple); //$NON-NLS-1$
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
        	
        	consumer.sendStopPollingEvent();
        	consumer.sendUpdateTopicAssignmentEvent(new TopicPartitionUpdate(action, topicPartitionOffsetMap));	
    	} catch (Exception e) {
    		logger.error(e.getMessage(), e);
    	} finally {
        	consumer.sendStartPollingEvent(consumerPollTimeout);
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
        consumer.sendShutdownEvent(SHUTDOWN_TIMEOUT, SHUTDOWN_TIMEOUT_TIMEUNIT);
//        if (processThread != null && processThread.isAlive()) {
//            processThread.interrupt();
//        }
//        processThread.join(5000);
//        processThread = null;
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() //$NON-NLS-1$ //$NON-NLS-2$
                + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$

        // Must call super.shutdown()
        super.shutdown();
    }

    @Override
    public void drain() throws Exception {
        logger.debug(">>> DRAIN"); //$NON-NLS-1$
        // When a checkpoint is to be created, the operator must stop sending tuples by pulling messages out of the messageQueue.
        // This is achieved via acquiring a permit. In the background, more messages are pushed into the queue by a receive thread
        // incrementing the read offset.
        // For every tuple that is submitted, its next offset is stored in a data structure (offset manager).
        // On checkpoint, the offset manager is saved. On reset of the CR, the consumer starts reading at these previously saved offsets,
        // reading the messages since last checkpoint again.
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.debug(">>> CHECKPOINT (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        consumer.sendCheckpointEvent(checkpoint); // blocks until checkpoint completes
        consumer.sendStartPollingEvent(consumerPollTimeout); // checkpoint is done, resume polling for records
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        logger.debug(">>> RESET (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        consumer.sendResetEvent(checkpoint); // blocks until reset completes
        consumer.sendStartPollingEvent(consumerPollTimeout); // done resetting,start polling for records

        // latch will be null if the reset was caused
        // by another operator
        if(resettingLatch != null)
        	resettingLatch.countDown();
    }

    @Override
    public void resetToInitialState() throws Exception {
        logger.debug(">>> RESET TO INIT..."); //$NON-NLS-1$
        consumer.sendResetToInitEvent(); // blocks until resetToInit completes
        consumer.sendStartPollingEvent(consumerPollTimeout); // done resettings, start polling for records

        // latch will be null if the reset was caused
        // by another operator
        if(resettingLatch != null)
        	resettingLatch.countDown();
    }
}
