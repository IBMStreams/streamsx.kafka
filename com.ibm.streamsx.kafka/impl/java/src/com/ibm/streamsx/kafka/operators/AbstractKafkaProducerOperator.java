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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.DefaultAttribute;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.PerformanceLevel;
import com.ibm.streamsx.kafka.clients.producer.ConsistentRegionPolicy;
import com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient;
import com.ibm.streamsx.kafka.clients.producer.TrackingProducerClient;
import com.ibm.streamsx.kafka.clients.producer.TransactionalCrProducerClient;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public abstract class AbstractKafkaProducerOperator extends AbstractKafkaOperator {
    protected static final String GUARANTEE_ORDERING_PARAM_NAME = "guaranteeOrdering";
    protected static final String DEFAULT_MESSAGE_ATTR_NAME = "message"; //$NON-NLS-1$
    protected static final String DEFAULT_KEY_ATTR_NAME = "key"; //$NON-NLS-1$
    protected static final String DEFAULT_TOPIC_ATTR_NAME = "topic"; //$NON-NLS-1$
    protected static final String DEFAULT_PARTITION_ATTR_NAME = "partition"; //$NON-NLS-1$
    protected static final String DEFAULT_TIMESTAMP_ATTR_NAME = "messageTimestamp"; //$NON-NLS-1$

    protected static final String MESSAGEATTR_PARAM_NAME = "messageAttribute"; //$NON-NLS-1$
    protected static final String KEYATTR_PARAM_NAME = "keyAttribute"; //$NON-NLS-1$
    protected static final String TOPIC_PARAM_NAME = "topic"; //$NON-NLS-1$
    protected static final String TOPICATTR_PARAM_NAME = "topicAttribute"; //$NON-NLS-1$
    protected static final String PARTITIONATTR_PARAM_NAME = "partitionAttribute"; //$NON-NLS-1$
    protected static final String TIMESTAMPATTR_PARAM_NAME = "timestampAttribute"; //$NON-NLS-1$
    protected static final String CONSISTENT_REGION_POLICY_PARAM_NAME = "consistentRegionPolicy";
    protected static final String FLUSH_PARAM_NAME = "flush";
    protected static final String OUTPUT_ERRORS_ONLY_PARM_NAME = "outputErrorsOnly";

    protected static final int I_PORT_MAX_PENDING_TUPLES = 5000;
    protected static final int O_PORT_DEFAULT_QUEUE_CAPACITY = 5000;
    protected static final long O_PORT_QUEUE_OFFER_TIMEOUT_MS = 15000;
    protected static final boolean O_PORT_SUBMIT_ONLY_ERRORS = true;

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
    // AtLeastOnce as default in order to support also Kafka 0.10 out of the box in Consistent Region.
    private ConsistentRegionPolicy consistentRegionPolicy = ConsistentRegionPolicy.NonTransactional;
    private boolean guaranteeOrdering = false;
    private boolean outputErrorsOnly = O_PORT_SUBMIT_ONLY_ERRORS;
    private int flush = 0;
    private OutputPortSubmitter errorPortSubmitter = null;

    @Parameter (optional = true, name = FLUSH_PARAM_NAME,
            description = "Specifies the number of tuples, after which the producer is flushed. When not specified, "
                    + "or when the parameter value is not positive, "
                    + "the flush interval is adaptively  calculated to avoid queing times significantly over five seconds.\\n"
                    + "\\n"
                    + "Flushing the producer makes all buffered records immediately available to send to the server "
                    + "(even if `linger.ms` is greater than 0) and blocks on the completion of the requests "
                    + "associated with the buffered records. When a small value is specified, the batching of tuples to server "
                    + "requests and compression (if used) may get inefficient.\\n"
                    + "\\n"
                    + "Under normal circumstances, this parameter should be used only when the adaptive flush control gives not "
                    + "the desired results, for example when the custom metrics **buffer-available-bytes** goes very small and "
                    + "**record-queue-time-max** or **record-queue-time-avg** gets too high.")
    public void setFlush (int value) {
        this.flush = value;
    }

    @Parameter(optional = true, name=CONSISTENT_REGION_POLICY_PARAM_NAME,
            description="Specifies the policy to use when in a consistent region.\\n"
                    + "\\n"
                    + "When `NonTransactional` "
                    + "is specified, the operator guarantees that every tuple is written to the "
                    + "topic(s) at least once. When the consistent region resets, duplicates will most "
                    + "likely appear in the output topic(s). For consumers of the output topics, "
                    + "messages appears as they are produced.\\n"
                    + "\\n"
                    + " When `Transactional` is specified, the operator will write "
                    + "tuples to the topic(s) within the context of a transaction. Transactions are commited "
                    + "when the operator checkpoints. This implies that downstream Kafka consumers may not see the messages "
                    + "until operator checkpoints.\\n"
                    + "Transactional delivery minimizes (though not eliminates) duplicate messages for consumers of "
                    + "the output topics when they are configured with the consumer property `isolation.level=read_committed`. "
                    + "Consumers that read with the default isolation level `read_uncommitted` see all messages as "
                    + "they are produced. For these consumers, there is no difference between transactional and "
                    + "non-transactional message delivery.\\n"
                    + "\\n"
                    + "For backward compatibility, the parameter value `AtLeastOnce` can also be specified, but is "
                    + "deprecated and can be removed in a future version. `AtLeastOnce` is equivalent to `NonTransactional`.\\n"
                    + "\\n"
                    + "This parameter is ignored if the operator is not part of a consistent region. "
                    + "The default value is `NonTransactional`. **NOTE**: Kafka brokers older than version v0.11 "
                    + "do not support transactions.")
    public void setConsistentRegionPolicy(ConsistentRegionPolicy consistentRegionPolicy) {
        this.consistentRegionPolicy = consistentRegionPolicy;
    }

    @Parameter(optional = true, name = GUARANTEE_ORDERING_PARAM_NAME,
            description = "If set to true, the operator guarantees that the order of records within "
                    + "a topic partition is the same as the order of processed tuples when it comes "
                    + "to retries. This implies that the operator sets the "
                    + "`max.in.flight.requests.per.connection` producer property automatically to 1 "
                    + "if retries are enabled, i.e. when the `retries` property is unequal 0, what "
                    + "is the operator default value.\\n"
                    + "\\n"
                    + "If unset, the default value of this parameter is `false`, which means that the "
                    + "order can change due to retries. Please be aware that setting " 
                    + GUARANTEE_ORDERING_PARAM_NAME
                    + " to `true` degrades the producer throughput as only one PRODUCE request per topic partition "
                    + "is active at any time.")
    public void setGuaranteeOrdering (boolean guaranteeOrdering) {
        this.guaranteeOrdering = guaranteeOrdering;
    }

    @Parameter(optional = true, name = OUTPUT_ERRORS_ONLY_PARM_NAME,
            description = "If set to `true`, the operator submits tuples to the optional output port only "
                    + "for the tuples that failed to produce completely. "
                    + "If set to `false`, the operator submits also tuples for the successfully produced input tuples.\\n"
                    + "\\n"
                    + "If unset, the default value of this parameter is " + O_PORT_SUBMIT_ONLY_ERRORS + ". "
                    + "This parameter is ignored when the operator is not configured with an output port.")
    public void setOutputErrorsOnly (boolean errsOnly) {
        this.outputErrorsOnly = errsOnly;
    }

    @Parameter(optional = true, name=KEYATTR_PARAM_NAME, 
            description="Specifies the input attribute that contains "
                    + "the Kafka key value. If not specified, the operator "
                    + "will look for an input attribute named *key*.")
    public void setKeyAttr(TupleAttribute<Tuple, ?> keyAttr) {
        this.keyAttr = keyAttr;
    }

    @Parameter(optional = true, name=TIMESTAMPATTR_PARAM_NAME,
            description="Specifies the attribute on the input port that "
                    + "contains the timestamp for the message. If not specified, the "
                    + "operator will look for an input attribute named *messageTimestamp*. "
                    + "If this parameter is not specified and there is no input "
                    + "attribute named *messageTimestamp*, the operator will use the timestamp "
                    + "provided by Kafka (broker config `log.message.timestamp.type=\\\\[CreateTime|LogAppendTime\\\\]`).")
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
                    + "over the **" + TOPICATTR_PARAM_NAME + "** parameter. This parameter will also "
                    + "take precedence if the input tuple schema contains an attribute "
                    + "named *topic*.")
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

    @CustomMetric (kind = Metric.Kind.COUNTER, name = "producerGeneration", description = "The producer generation. When a new producer is created, a new generation is created.")
    public void setnMalformedMessages (Metric producerGeneration) { }

    @CustomMetric (kind = Metric.Kind.GAUGE, name = "nPendingTuples", description = "Number of input tuples not yet produced (acknowledged from Kafka)")
    public void setnPendingTuples (Metric nPendingTuples) { }

    @CustomMetric (kind = Metric.Kind.COUNTER, name = "nQueueFullPause", description = "Number times the input tuple processing was paused due to full tuple queue of pending tuples.")
    public void setnQueueFullPause (Metric nQueueFullPause) { }

    @CustomMetric (kind = Metric.Kind.COUNTER, name = "nFailedTuples", description = "Number of tuples that could not be produced for all topics")
    public void setnFailedTuples (Metric nFailedTuples) { }

    /**
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

    /**
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


    @ContextCheck(compile = true)
    public static void checkErrorPortSchema (OperatorContextChecker checker) {
        final OperatorContext opCtx = checker.getOperatorContext();
        final int nOPorts = opCtx.getNumberOfStreamingOutputs();
        if (nOPorts == 0) return;

        StreamSchema inPortSchema = opCtx.getStreamingInputs().get(0).getStreamSchema();
        StreamSchema outSchema = opCtx.getStreamingOutputs().get(0).getStreamSchema();
        if (outSchema.getAttributeCount() > 2) {
            checker.setInvalidContext (Messages.getString("PRODUCER_INVALID_OPORT_SCHEMA", opCtx.getKind()), new Object[0]); //$NON-NLS-1$
        }
        // check attribute types
        int nTupleAttrs = 0;
        int nStringAttrs = 0;
        for (String outAttrName: outSchema.getAttributeNames()) {
            Attribute attr = outSchema.getAttribute (outAttrName);
            MetaType metaType = attr.getType().getMetaType();
            switch (metaType) {
            case TUPLE:
                ++nTupleAttrs;
                TupleType tupleType = (TupleType) attr.getType();
                StreamSchema tupleSchema = tupleType.getTupleSchema();
                if (!tupleSchema.equals (inPortSchema)) {
                    checker.setInvalidContext (Messages.getString("PRODUCER_INVALID_OPORT_SCHEMA", opCtx.getKind()), new Object[0]); //$NON-NLS-1$
                }
                break;
            case RSTRING:
            case USTRING:
                ++nStringAttrs;
                break;
            default:
                checker.setInvalidContext (Messages.getString("PRODUCER_INVALID_OPORT_SCHEMA", opCtx.getKind()), new Object[0]); //$NON-NLS-1$
            }
        }
        if (nTupleAttrs > 1 || nStringAttrs > 1)
            checker.setInvalidContext (Messages.getString("PRODUCER_INVALID_OPORT_SCHEMA", opCtx.getKind()), new Object[0]); //$NON-NLS-1$
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
        Attribute keyAttr = (keyParamValues != null && !keyParamValues.isEmpty())? streamSchema.getAttribute(parseFQAttributeName(keyParamValues.get(0))): streamSchema.getAttribute(DEFAULT_KEY_ATTR_NAME);

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
                Set<MetaType> allowedTopicMTypes = new HashSet<>(Arrays.asList(
                        MetaType.RSTRING, MetaType.BSTRING, MetaType.USTRING
                        ));
                if (!allowedTopicMTypes.contains (topicAttribute.getType().getMetaType())) {
                    checker.setInvalidContext(Messages.getString("TOPIC_ATTRIBUTE_NOT_STRING"), new Object[0]);
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
                checker.setInvalidContext(Messages.getString("OPERATOR_NOT_START_OF_CONSISTENT_REGION", opContext.getKind()), new Object[0]); ////$NON-NLS-1$ 
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

        crContext = context.getOptionalContext(ConsistentRegionContext.class);
        // isResetting can always be false when not in consistent region.
        // When not in consistent region, reset happens _before_ allPortsReady(), so that tuple processing 
        // is not conflicting with RESET processing, for which this flag is used.
        isResetting = new AtomicBoolean (crContext != null && context.getPE().getRelaunchCount() > 0);
        if (getOperatorContext().getNumberOfStreamingOutputs() > 0) {
            this.errorPortSubmitter = new OutputPortSubmitter (context, 
                    O_PORT_DEFAULT_QUEUE_CAPACITY, 
                    O_PORT_QUEUE_OFFER_TIMEOUT_MS,
                    outputErrorsOnly);
        }
        initProducer();
        final boolean registerAsInput = false;
        registerForDataGovernance(context, topics, registerAsInput);

        logger.debug(">>> Operator initialized <<<"); //$NON-NLS-1$
    }

    private void initProducer() throws Exception {
        // configure producer
        KafkaOperatorProperties props = getKafkaProperties();
        TrackingProducerClient pClient;
        if(crContext == null) {
            pClient = new TrackingProducerClient (getOperatorContext(), keyType, messageType, guaranteeOrdering, props);
        } else {
            switch(consistentRegionPolicy) {
            case AtLeastOnce:
            case NonTransactional:
                pClient = new TrackingProducerClient (getOperatorContext(), keyType, messageType, guaranteeOrdering, props);
                break;
            case Transactional:
                pClient = new TransactionalCrProducerClient(getOperatorContext(), keyType, messageType, guaranteeOrdering, props);
                break;
            default:
                throw new RuntimeException("Unrecognized ConsistentRegionPolicy: " + consistentRegionPolicy);
            }
        }
        // when we want a hook for produced or failed tuples, we must set a TupleProcessedHook implementation.
        pClient.setTupleProcessedHook (this.errorPortSubmitter);
        pClient.setMaxPendingTuples (I_PORT_MAX_PENDING_TUPLES);
        pClient.setMaxProducerGenerations (2);  // retry tuples only once
        producer = pClient;
        producer.setFlushAfter (flush);
        logger.info ("producer client " + producer.getThisClassName() + " created");
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
        if (this.errorPortSubmitter != null) this.errorPortSubmitter.start();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
        if (isResetting.get()) {
            logger.debug("Operator is in the middle of resetting...skipping tuple processing!"); //$NON-NLS-1$
            return;
        }

        List<String> topicList = getTopics(tuple);
        Object key = keyAttributeName != null ? toJavaPrimitveObject(keyType, tuple.getObject(keyAttributeName)) : null;
        Object value = toJavaPrimitveObject(messageType, messageAttr.getValue(tuple));
        Integer partition = (partitionAttributeName != null) ? tuple.getInt(partitionAttributeName) : null;
        Long timestamp = (timestampAttributeName) != null ? tuple.getLong(timestampAttributeName) : null;

        // send message to all topics
        if (topicList.size() == 1) {
            producer.processRecord (new ProducerRecord (topicList.get(0), partition, timestamp, key, value), tuple);
        }
        else {
            List<ProducerRecord<?, ?>> records = new ArrayList<> (topicList.size());
            for (String topic : topicList) records.add (new ProducerRecord (topic, partition, timestamp, key, value));
            producer.processRecords (records, tuple);
        }
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
        producer.close (KafkaProducerClient.CLOSE_TIMEOUT_MS);
        if (this.errorPortSubmitter != null) this.errorPortSubmitter.stop();

        // Must call super.shutdown()
        super.shutdown();
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
        // flush also the hook as it queues tuples
        if (this.errorPortSubmitter != null) {
            this.errorPortSubmitter.flush();
        }
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        if (crContext == null) return;  // ignore 'config checkpoint'
        logger.debug(">>> CHECKPOINT (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        producer.checkpoint(checkpoint);
    }

    @Override
    public void reset(Checkpoint checkpoint) throws Exception {
        if (crContext == null) return;  // ignore 'config checkpoint'
        logger.debug (">>> RESET (ckpt id=" + checkpoint.getSequenceId() + ")"); //$NON-NLS-1$ //$NON-NLS-2$
        logger.debug("Initiating reset..."); //$NON-NLS-1$
        producer.tryCancelOutstandingSendRequests (/*mayInterruptIfRunning = */true);
        producer.reset (checkpoint);
        producer.close (0L);
        if (errorPortSubmitter != null) {
            errorPortSubmitter.reset();
        }
        producer = null;
        initProducer();
        isResetting.set(false);
        logger.debug ("Reset complete"); //$NON-NLS-1$
    }

    @Override
    public void resetToInitialState() throws Exception {
        if (crContext == null) return;  // ignore 'config checkpoint'
        logger.debug (">>> RESET TO INIT..."); //$NON-NLS-1$
        producer.tryCancelOutstandingSendRequests (/*mayInterruptIfRunning = */true);
        producer.reset (null);
        producer.close(0L);
        if (errorPortSubmitter != null) {
            errorPortSubmitter.reset();
        }
        producer = null;
        initProducer();
        isResetting.set(false);
        logger.debug ("Reset to init complete"); //$NON-NLS-1$
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
