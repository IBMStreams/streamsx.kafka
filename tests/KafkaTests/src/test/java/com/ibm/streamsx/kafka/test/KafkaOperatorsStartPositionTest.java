package com.ibm.streamsx.kafka.test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.streamsx.kafka.test.utils.Constants;
import com.ibm.streamsx.kafka.test.utils.KafkaSPLStreamsUtils;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
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
public class KafkaOperatorsStartPositionTest extends AbstractKafkaTest {

    private static final String TEST_NAME = "KafkaOperatorsStartPositionTest";

    public enum StartPosition {
        Beginning;
    }

    public KafkaOperatorsStartPositionTest() throws Exception {
        super(TEST_NAME);
    }

    @Test
    public void kafkaStartPositionTest() throws Exception {
        Topology producerTopo = createTopology("producerTopo");

        // create the producer (produces tuples after a short delay)
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("topic", Constants.TOPIC_POS);
        producerProps.put("propertiesFile", Constants.PROPERTIES_FILE_PATH);

        TStream<String> stringSrcStream = producerTopo.strings(Constants.STRING_DATA);
        SPL.invokeSink(Constants.KafkaProducerOp, 
                KafkaSPLStreamsUtils.convertStreamToKafkaTuple(stringSrcStream), 
                producerProps);

        // Launch the producer and allow it to finish writing
        // the data to the queue. Consumer should run AFTER 
        // producer is finished.
        @SuppressWarnings("unchecked")
        Future<BigInteger> future = (Future<BigInteger>)StreamsContextFactory.getStreamsContext(Type.STANDALONE).submit(producerTopo);
        future.get();
        Thread.sleep(TimeUnit.SECONDS.toMillis(50));
        if(!future.isDone()) {
            future.cancel(true);
        }

        // create the consumer
        Topology topo = getTopology();

        Map<String, Object> consumerParams = new HashMap<>();
        consumerParams.put("topic", Constants.TOPIC_POS);
        consumerParams.put("propertiesFile", Constants.PROPERTIES_FILE_PATH);
        consumerParams.put("startPosition", StartPosition.Beginning);

        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, consumerParams, KafkaSPLStreamsUtils.STRING_SCHEMA);
        SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> t.getString("message")));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        Condition<List<String>> condition = KafkaSPLStreamsUtils.stringContentsUnordered(tester, msgStream, Constants.STRING_DATA);
        tester.complete(context, new HashMap<>(), condition, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue(condition.getResult().size() > 0);
        Assert.assertTrue(condition.getResult().toString(), condition.valid());		
    }
}
