package com.ibm.streamsx.kafka.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.streamsx.kafka.test.utils.Constants;
import com.ibm.streamsx.kafka.test.utils.Delay;
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
public class KafkaOperatorsMultipleTopics extends AbstractKafkaTest {

	private static final String TEST_NAME = "KafkaOperatorsGreenThread";
	
	public KafkaOperatorsMultipleTopics() throws Exception {
		super(TEST_NAME);
	}

	@Test
	public void kafkaMultipleTopicsTest() throws Exception {
		Topology topo = getTopology();
		
		// create the producer (produces tuples after a short delay)
		TStream<String> stringSrcStream = topo.strings(Constants.STRING_DATA).modify(new Delay<>(Constants.PRODUCER_DELAY));
		SPL.invokeSink(Constants.KafkaProducerOp, 
				KafkaSPLStreamsUtils.convertStreamToKafkaTuple(stringSrcStream), 
				getKafkaParams());

		// create the consumer
		SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getKafkaParams(), KafkaSPLStreamsUtils.STRING_SCHEMA);
		SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> t.getString("message")));
		
		// test the output of the consumer
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
		Tester tester = topo.getTester();
		String[] expectedArr = KafkaSPLStreamsUtils.duplicateArrayEntries(Constants.STRING_DATA, 3);
		Condition<List<String>> condition = KafkaSPLStreamsUtils.stringContentsUnordered(tester, msgStream, expectedArr);
		tester.complete(context, condition, 30, TimeUnit.SECONDS);

		// check the results
		Assert.assertTrue(condition.getResult().size() > 0);
		Assert.assertTrue(condition.getResult().toString(), condition.valid());		
	}
	
	private Map<String, Object> getKafkaParams() {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("topic", Arrays.asList(Constants.TOPIC_TEST, Constants.TOPIC_OTHER1, Constants.TOPIC_OTHER2).toArray(new String[0]));
		params.put("appConfigName", Constants.APP_CONFIG);
		
		return params;
	}
}
	