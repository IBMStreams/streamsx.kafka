package com.ibm.streamsx.kafka.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.kafka.test.utils.Constants;
import com.ibm.streamsx.kafka.test.utils.Delay;
import com.ibm.streamsx.kafka.test.utils.KafkaSPLStreamsUtils;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.tester.Condition;
import com.ibm.streamsx.topology.tester.Tester;

/*
 * This is simple green thread test to verify
 * the operators are functioning.
 * 
 * This test requires the following: 
 *  - topic "test" be created on the Kafka server
 *  - appConfig "kafka-test" be created on the domain
 */
public class KafkaOperatorsAttrNameParamsTest extends AbstractKafkaTest {

    private static final String TEST_NAME = "KafkaOperatorsAttrNameParamsTest";
    private static final String PROD_KEY_ATTR_NAME = "myProdKey";
    private static final String PROD_MSG_ATTR_NAME = "myProdMsg";
    private static final String PROD_TOPIC_ATTR_NAME = "myProdTopic";
    private static final String PROD_PARTITION_ATTR_NAME = "myPartitionNum";
    private static final String CONS_KEY_ATTR_NAME = "myConsKey";
    private static final String CONS_MSG_ATTR_NAME = "myConsMsg";
    private static final String CONS_TOPIC_ATTR_NAME = "myConsTopic";	

    private static final Integer PARTITION_NUM = 0;
    private static final Integer KEY = 100;
    private static final String MSG = "myMsg";

    public KafkaOperatorsAttrNameParamsTest() throws Exception {
        super(TEST_NAME);
    }

    @Test
    public void kafkaAttrNameParamsTest() throws Exception {
        Topology topo = getTopology();

        StreamSchema producerSchema = com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<int32 " + PROD_KEY_ATTR_NAME + ", rstring " + PROD_MSG_ATTR_NAME + ", rstring " + PROD_TOPIC_ATTR_NAME + ", int32 " + PROD_PARTITION_ATTR_NAME + ">");

        // create the producer (produces tuples after a short delay)
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("propertiesFile", Constants.PROPERTIES_FILE_PATH);
        producerProps.put("messageAttribute", producerSchema.getAttribute(PROD_MSG_ATTR_NAME));
        producerProps.put("keyAttribute", producerSchema.getAttribute(PROD_KEY_ATTR_NAME));
        producerProps.put("topicAttribute", producerSchema.getAttribute(PROD_TOPIC_ATTR_NAME));
        producerProps.put("partitionAttribute", producerSchema.getAttribute(PROD_PARTITION_ATTR_NAME));
        TStream<String> srcStream = topo.strings(MSG).modify(new Delay<>(5000));
        SPL.invokeSink(Constants.KafkaProducerOp, 
                SPLStreams.convertStream(srcStream, new ProducerConverter(), producerSchema), 
                producerProps);

        // create the consumer
        StreamSchema consumerSchema = com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<int32 " + CONS_KEY_ATTR_NAME + ", rstring " + CONS_MSG_ATTR_NAME + ", rstring " + CONS_TOPIC_ATTR_NAME + ">");

        Map<String, Object> consumerProps = new HashMap<String, Object>();
        consumerProps.put("propertiesFile", Constants.PROPERTIES_FILE_PATH);
        consumerProps.put("outputMessageAttributeName", CONS_MSG_ATTR_NAME);
        consumerProps.put("outputKeyAttributeName", CONS_KEY_ATTR_NAME);
        consumerProps.put("outputTopicAttributeName", CONS_TOPIC_ATTR_NAME);
        consumerProps.put("topic", Constants.TOPIC_TEST);
        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, consumerProps, consumerSchema);
        SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> {
            return t.getString(CONS_TOPIC_ATTR_NAME) + ":" + t.getInt(CONS_KEY_ATTR_NAME) + ":" + t.getString(CONS_MSG_ATTR_NAME);
        }));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        Condition<List<String>> condition = KafkaSPLStreamsUtils.stringContentsUnordered(tester, msgStream, Constants.TOPIC_TEST + ":" + KEY + ":" + MSG);
        tester.complete(context, new HashMap<>(), condition, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue(condition.getResult().size() > 0);
        Assert.assertTrue(condition.getResult().toString(), condition.valid());		
    }

    private static class ProducerConverter implements BiFunction<String, OutputTuple, OutputTuple> {
        private static final long serialVersionUID = 1L;

        @Override
        public OutputTuple apply(String msg, OutputTuple outTuple) {
            outTuple.setInt(PROD_KEY_ATTR_NAME, KEY);
            outTuple.setString(PROD_MSG_ATTR_NAME, msg);
            outTuple.setString(PROD_TOPIC_ATTR_NAME, Constants.TOPIC_TEST);
            outTuple.setInt(PROD_PARTITION_ATTR_NAME, PARTITION_NUM);

            return outTuple;
        }		
    }
}
