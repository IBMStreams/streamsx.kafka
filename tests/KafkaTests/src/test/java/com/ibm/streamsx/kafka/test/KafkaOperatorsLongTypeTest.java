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
public class KafkaOperatorsLongTypeTest extends AbstractKafkaTest {

    private static final String TEST_NAME = "KafkaOperatorsLongTypeTest";
    private static final String[] DATA = {"10", "20", "30", "40", "50"};

    public KafkaOperatorsLongTypeTest() throws Exception {
        super(TEST_NAME);
    }

    @Test
    public void kafkaLongTypeTest() throws Exception {
        Topology topo = getTopology();
        StreamSchema schema = KafkaSPLStreamsUtils.LONG_SCHEMA;
        // create the producer (produces tuples after a short delay)
        TStream<Long> srcStream = topo.strings(DATA).transform(s -> Long.valueOf(s)).modify(new Delay<>(5000));
        SPLStream splSrcStream = SPLStreams.convertStream(srcStream, new Converter(), schema);
        SPL.invokeSink(Constants.KafkaProducerOp, 
                splSrcStream, 
                getKafkaParams());

        // create the consumer
        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getKafkaParams(), schema);
        SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> String.valueOf(t.getLong("message"))));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        Condition<List<String>> condition = KafkaSPLStreamsUtils.stringContentsUnordered(tester, msgStream, DATA);
        tester.complete(context, new HashMap<>(), condition, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue(condition.getResult().size() > 0);
        Assert.assertTrue(condition.getResult().toString(), condition.valid());
    }

    private Map<String, Object> getKafkaParams() {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("topic", Constants.TOPIC_TEST);
        params.put("appConfigName", Constants.APP_CONFIG);

        return params;
    }

    private static class Converter implements BiFunction<Long, OutputTuple, OutputTuple> {
        private static final long serialVersionUID = 1L;
        private long counter = 0;

        @Override
        public OutputTuple apply(Long val, OutputTuple outTuple) {
            outTuple.setLong("key", counter++);
            outTuple.setLong("message", val.longValue());
            return outTuple;
        }
    }
}
