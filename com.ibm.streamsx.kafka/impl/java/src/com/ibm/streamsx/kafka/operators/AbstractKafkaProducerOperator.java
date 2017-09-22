package com.ibm.streamsx.kafka.operators;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.DefaultAttribute;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.PerformanceLevel;
import com.ibm.streamsx.kafka.clients.producer.AtLeastOnceKafkaProducerClient;
import com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public abstract class AbstractKafkaProducerOperator extends AbstractKafkaOperator {
    private static final String DEFAULT_MESSAGE_ATTR_NAME = "message"; //$NON-NLS-1$
    private static final String DEFAULT_KEY_ATTR_NAME = "key"; //$NON-NLS-1$
    private static final String DEFAULT_TOPIC_ATTR_NAME = "topic"; //$NON-NLS-1$
    private static final String DEFAULT_PARTITION_ATTR_NAME = "partition"; //$NON-NLS-1$
    private static final String DEFAULT_TIMESTAMP_ATTR_NAME = "messageTimestamp"; //$NON-NLS-1$
    
    private static final String MESSAGEATTR_PARAM_NAME = "messageAttribute"; //$NON-NLS-1$
    private static final String KEYATTR_PARAM_NAME = "keyAttribute"; //$NON-NLS-1$
    private static final String TOPIC_PARAM_NAME = "topic"; //$NON-NLS-1$
    private static final String TOPICATTR_PARAM_NAME = "topicAttribute"; //$NON-NLS-1$
    private static final String PARTITIONATTR_PARAM_NAME = "partitionAttribute"; //$NON-NLS-1$
    private static final String TIMESTAMPATTR_PARAM_NAME = "timestampAttribute"; //$NON-NLS-1$
    
    private static final Logger logger = Logger.getLogger(KafkaProducerOperator.class);

    /* Parameters */
    protected TupleAttribute<Tuple, ?> keyAttr;
    protected TupleAttribute<Tuple, ?> messageAttr;
    protected TupleAttribute<Tuple, String> topicAttr;
    protected TupleAttribute<Tuple, Integer> partitionAttr;
    protected TupleAttribute<Tuple, Long> timestampAttr;
    protected List<String> topics;

    private KafkaProducerClient producer;
    private AtomicBoolean isResetting;
    private String keyAttributeName = null;
    private String partitionAttributeName = null;
    private String timestampAttributeName = null;

    @Parameter(optional = true, name=KEYATTR_PARAM_NAME, 
    		description="Specifies the input attribute that contains "
    				+ "the Kafka key value. If not specified, the operator "
    				+ "will look for an input attribute named *key*.")
    public void setKeyAttr(TupleAttribute<Tuple, ?> keyAttr) {
		this.keyAttr = keyAttr;
	}

    @Parameter(optional = true, name=TIMESTAMPATTR_PARAM_NAME,
    		description="Specifies the attribute on the input port that "
    				+ "contains the timestamp for the message. If not specified, the"
    				+ "operator will look for an input attribute named *messageTimestamp*. "
    				+ "If this parameter is not specified and there is no input "
    				+ "attribute named *messageTimestamp*, the operator will use the timestamp "
    				+ "provided by the underlying Kafka API.")
    public void setTimestampAttr(TupleAttribute<Tuple, Long> timestampAttr) {
		this.timestampAttr = timestampAttr;
	}
    
    @DefaultAttribute(DEFAULT_MESSAGE_ATTR_NAME)
    @Parameter(optional = true, name=MESSAGEATTR_PARAM_NAME, 
    		description="Specifies the attribute on the input port that "
    				+ "contains the message payload. If not specified, the "
    				+ "operator will look for an input attribute named *message*. "
    				+ "If this parameter is not specified and there is no input "
    				+ "attribute named *message*, the operator will throw an "
    				+ "exception and terminate.")
    public void setMessageAttr(TupleAttribute<Tuple, ?> messageAttr) {
		this.messageAttr = messageAttr;
	}

    @Parameter(optional = true, name=TOPIC_PARAM_NAME,
    		description="Specifies the topic(s) that the producer should send "
    				+ "messages to. The value of this parameter will take precedence "
    				+ "over the **topicAttrName** parameter. This parameter will also "
    				+ "take precedence if the input tuple schema contains an attribute "
    				+ "named topic.")
    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    @Parameter(optional = true, name=TOPICATTR_PARAM_NAME,
    		description="Specifies the input attribute that contains the name of "
    				+ "the topic that the message should be written to. If this "
    				+ "parameter is not specified, the operator will "
    				+ "look for an input attribute named *topic*. This parameter "
    				+ "value is overridden if the **topic** parameter is specified.")
    public void setTopicAttr(TupleAttribute<Tuple, String> topicAttr) {
		this.topicAttr = topicAttr;
	}

    @Parameter(optional = true, name=PARTITIONATTR_PARAM_NAME,
    		description="Specifies the input attribute that contains the partition "
    				+ "number that the message should be written to. If this parameter "
    				+ "is not specified, the operator will look for an input attribute "
    				+ "named **partition**. If the user does not indicate which partition "
    				+ "the message should be written to, then Kafka's default partitioning "
    				+ "strategy will be used instead (partition based on the specified "
    				+ "partitioner or in a round-robin fashion).")
    public void setPartitionAttr(TupleAttribute<Tuple, Integer> partitionAttr) {
    	this.partitionAttr = partitionAttr;
    }
    
    /*
     * Retrieving the value of a TupleAttribute parameter via OperatorContext.getParameterValues()
     * returns a string in the form "InputPortName.AttributeName". However, this ends up being the
     * C++ equivalent String, which looks like: "iport$0.get_myAttr()". 
     * 
     * This methods will return "myAttr", which is the name of the attribute that the parameter is
     * referring to.  
     */
    private static String parseFQAttributeName(String attrString) {
    	return attrString.split("_")[1].replace("()", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    /*
     * If the `partitionAttribute` is not defined, then the operator will look
     * for an input attribute called "partition". Here, we need to check that this
     * input attribute is of type "int32". 
     */
    @ContextCheck(compile = true)
    public static void checkPartitionAttributeType(OperatorContextChecker checker) {
    	if(!checker.getOperatorContext().getParameterNames().contains(PARTITIONATTR_PARAM_NAME)) {
    		StreamSchema schema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
    		Attribute partition = schema.getAttribute("partition"); //$NON-NLS-1$
    		if(partition != null) {
    			if(!checker.checkAttributeType(partition, MetaType.INT32)) {
    				checker.setInvalidContext(Messages.getString("PARTITION_ATTRIBUTE_NOT_INT32"), new Object[0]); //$NON-NLS-1$
    			}
    		}
    	}
    }
    
    @ContextCheck(runtime = true, compile = false)
    public static void checkAttributes(OperatorContextChecker checker) {
        StreamSchema streamSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();

        /*
         * The message attribute must either be defined via the 'messageAttr' parameter, 
         * or the input schema must contain an attribute named "message". Otherwise, 
         * a context error is returned.
         */
        Attribute msgAttr;
        List<String> messageAttrParamValues = checker.getOperatorContext().getParameterValues(MESSAGEATTR_PARAM_NAME);
        if(messageAttrParamValues != null && !messageAttrParamValues.isEmpty()) {
        	msgAttr = streamSchema.getAttribute(parseFQAttributeName(messageAttrParamValues.get(0)));
        } else {
        	// the 'messageAttr' parameter is not specified, so check if input schema contains an attribute named "message"
        	msgAttr = streamSchema.getAttribute(DEFAULT_MESSAGE_ATTR_NAME);        	        	
        }
        
        if(msgAttr != null) {
            // validate the message attribute type
            checker.checkAttributeType(msgAttr, SUPPORTED_ATTR_TYPES);
        } else {
        	// the operator does not specify a message attribute, so set an invalid context
        	checker.setInvalidContext(Messages.getString("MESSAGE_ATTRIBUTE_NOT_FOUND"), new Object[0]); //$NON-NLS-1$
        }
 
        /*
         * A key attribute can either be specified via the 'keyAttr' parameter,
         * or the input schema can contain an attribute named "key". If neither is true, 
         * then a 'null' key will be used when writing records to Kafka (i.e. do not 
         * set an invalid context) 
         */
        List<String> keyParamValues = checker.getOperatorContext().getParameterValues(KEYATTR_PARAM_NAME);
        Attribute keyAttr = (keyParamValues != null && !keyParamValues.isEmpty()) ? 
        		streamSchema.getAttribute(parseFQAttributeName(keyParamValues.get(0))) :
        			streamSchema.getAttribute(DEFAULT_KEY_ATTR_NAME);

        // validate the key attribute type
        if (keyAttr != null)
            checker.checkAttributeType(keyAttr, SUPPORTED_ATTR_TYPES);
        

        /*
         * For topics, one of the following must be true: 
         *  * the 'topic' parameter is specified that lists topics to write to
         *  * the 'topicAttr' parameter is specified that points to an input attribute containing the topic to write to
         *  * neither of the above parameters are specified but the input schema contains an attribute named "topic"
         *  
         * An invalid context is set if none of the above conditions are true
         */
        if(!checker.getOperatorContext().getParameterNames().contains(TOPIC_PARAM_NAME)) { 
        	// 'topic' param not specified, check for 'topicAttr' param
        	if(!checker.getOperatorContext().getParameterNames().contains(TOPICATTR_PARAM_NAME)) {
        		// 'topicAttr' param also not specified, check for input attribute named "topic"
        		Attribute topicAttribute = streamSchema.getAttribute(DEFAULT_TOPIC_ATTR_NAME);
        		if(topicAttribute == null) {
        			// "topic" input attribute does not exist...set invalid context
        			checker.setInvalidContext(Messages.getString("TOPIC_NOT_SPECIFIED"), new Object[0]); //$NON-NLS-1$
        		}
        	}
        }
    }

    @ContextCheck(compile = true)
	public static void checkConsistentRegion(OperatorContextChecker checker) {

    	// check that the operator is not the start of the consistent region
		OperatorContext opContext = checker.getOperatorContext();
		ConsistentRegionContext crContext = opContext.getOptionalContext(ConsistentRegionContext.class);
		if (crContext != null) {
			if (crContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("PRODUCER_NOT_START_OF_CONSISTENT_REGION"), new Object[0]); ////$NON-NLS-1$ 
			}
		}
	}
    
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * 
     * @param context
     *            OperatorContext for this operator.
     * @throws Exception
     *             Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
        logger.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                + context.getPE().getJobId());

        StreamSchema inputSchema = context.getStreamingInputs().get(0).getStreamSchema();

        // check for key attribute and get type
        Attribute keyAttribute = null;
        if(keyAttr != null && keyAttr.getAttribute() != null) {
        	keyAttribute = keyAttr.getAttribute();
    	} else {
    		keyAttribute = inputSchema.getAttribute(DEFAULT_KEY_ATTR_NAME);
    	}

        if(keyAttribute != null) {
        	keyType = keyAttribute.getType().getObjectType();
        	keyAttributeName = keyAttribute.getName();
        }
        
        // check for partition attribute
        Attribute partitionAttribute = null;
        if(partitionAttr != null && partitionAttr.getAttribute() != null) {
        	partitionAttribute = partitionAttr.getAttribute();
        } else {
        	partitionAttribute = inputSchema.getAttribute(DEFAULT_PARTITION_ATTR_NAME);
        }
        partitionAttributeName = partitionAttribute != null ? partitionAttribute.getName() : null;
        
        // check for timestamp attribute
        Attribute timestampAttribute = null;
        if(timestampAttr != null && timestampAttr.getAttribute() != null) {
        	timestampAttribute = timestampAttr.getAttribute();
        } else {
        	timestampAttribute = inputSchema.getAttribute(DEFAULT_TIMESTAMP_ATTR_NAME);
        }
        timestampAttributeName = timestampAttribute != null ? timestampAttribute.getName() : null;
        
        // get message type
        messageType = messageAttr.getAttribute().getType().getObjectType();
        
        initProducer();

        registerForDataGovernance(context, topics);

        crContext = context.getOptionalContext(ConsistentRegionContext.class);
        if (crContext != null) {
            isResetting = new AtomicBoolean(context.getPE().getRelaunchCount() > 0);
        }

        logger.info(">>> Operator initialized! <<<"); //$NON-NLS-1$
    }

    private void initProducer() throws Exception {
        // configure producer
        KafkaOperatorProperties props = getKafkaProperties();
        logger.info("Creating AtLeastOnce producer"); //$NON-NLS-1$
        if(crContext == null) {
            producer = new KafkaProducerClient(getOperatorContext(), keyType, messageType, props);
        } else {
        	producer = new AtLeastOnceKafkaProducerClient(getOperatorContext(), keyType, messageType, props);
        }
    }

    /**
     * Notification that initialization is complete and all input and output
     * ports are connected and ready to receive and submit tuples.
     * 
     * @throws Exception
     *             Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() //$NON-NLS-1$ //$NON-NLS-2$
                + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
        if (crContext != null && isResetting.get()) {
            logger.debug("Operator is in the middle of resetting...skipping tuple processing!"); //$NON-NLS-1$
            return;
        }

        List<String> topicList = getTopics(tuple);
        Object key = keyAttributeName != null ? toJavaPrimitveObject(keyType, tuple.getObject(keyAttributeName)) : null;
        Object value = toJavaPrimitveObject(messageType, messageAttr.getValue(tuple));
        Integer partition = (partitionAttributeName != null) ? tuple.getInt(partitionAttributeName) : null;
        Long timestamp = (timestampAttributeName) != null ? tuple.getLong(timestampAttributeName) : null;
        
        // send message to all topics
        for (String topic : topicList)
            producer.processTuple(new ProducerRecord(topic, partition, timestamp, key, value));
    }
    
    private List<String> getTopics(Tuple tuple) {
    	List<String> topicList;
    	
    	if(this.topics != null && !this.topics.isEmpty()) {
    		topicList = this.topics;
    	} else if(topicAttr != null) {
    		topicList = Arrays.asList(topicAttr.getValue(tuple));
    	} else {
    		// the context checker guarantees that this will be here
    		// if the above 2 conditions are false
    		topicList = Arrays.asList(tuple.getString(DEFAULT_TOPIC_ATTR_NAME));
    	}
    	
    	return topicList;
    }
    
    /**
     * Shutdown this operator, which will interrupt the thread executing the
     * <code>produceTuples()</code> method.
     * 
     * @throws Exception
     *             Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " //$NON-NLS-1$ //$NON-NLS-2$
                + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId()); //$NON-NLS-1$

        producer.flush();
        producer.close();

        // Must call super.shutdown()
        super.shutdown();
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void drain() throws Exception {
        logger.debug(">>> DRAIN"); //$NON-NLS-1$
        // flush all records from buffer...
        // if any messages fail to
        // be acknowledged, an exception
        // will be thrown and the
        // region will be reset
        producer.drain();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        logger.debug(">>> CHECKPOINT (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        producer.checkpoint(checkpoint);
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        logger.debug(">>> RESET (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$

        /*
         * Close the producer and initialize a new once. Calling close() will
         * flush out all remaining messages and then shutdown the producer.
         */
        producer.close();
        producer = null;
        initProducer();

        logger.debug("Initiating reset..."); //$NON-NLS-1$
        producer.reset(checkpoint);

        // reset complete
        isResetting.set(false);
        logger.debug("Reset complete!"); //$NON-NLS-1$
    }

    @Override
    public void resetToInitialState() throws Exception {
        logger.debug(">>> RESET TO INIT..."); //$NON-NLS-1$

        initProducer();
        producer.resetToInitialState();
        isResetting.set(false);
    }

    @Override
    public void retireCheckpoint(long id) throws Exception {
        logger.debug(">>> RETIRE CHECKPOINT: " + id); //$NON-NLS-1$
    }

    /*
     * FOR DEBUGGING!!
     */
    @SuppressWarnings("unused")
    private void printExecutionTime(String methodName, StopWatch sw) {
        logger.log(PerformanceLevel.PERF,
                String.format("%s time: %d ms", methodName, sw.getTime(TimeUnit.MILLISECONDS))); //$NON-NLS-1$
    }
}
