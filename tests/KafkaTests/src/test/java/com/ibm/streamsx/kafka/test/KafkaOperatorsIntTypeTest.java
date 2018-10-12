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
public class KafkaOperatorsIntTypeTest extends AbstractKafkaTest {

    private static final String TEST_NAME = "KafkaOperatorsIntTypeTest";
    private static final String[] DATA = {"10", "20", "30", "40", "50"};

    public KafkaOperatorsIntTypeTest() throws Exception {
        super(TEST_NAME);
    }

    @Test
    public void kafkaIntTypeTest() throws Exception {
        Topology topo = getTopology();


        StreamSchema schema = KafkaSPLStreamsUtils.INT_SCHEMA;


        // create the producer (produces tuples after a short delay)
        TStream<Integer> srcStream = topo.strings(DATA).transform(s -> Integer.valueOf(s)).modify(new Delay<>(5000));
        SPLStream splSrcStream = SPLStreams.convertStream(srcStream, new Converter(), schema);
        SPL.invokeSink(Constants.KafkaProducerOp, 
                splSrcStream, 
                getKafkaParams());

        // create the consumer
        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getKafkaParams(), schema);
        SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> String.valueOf(t.getInt("message"))));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        Condition<List<String>> stringContentsUnordered = tester.stringContentsUnordered (msgStream.toStringStream(), DATA);
        HashMap<String, Object> config = new HashMap<>();
//      config.put (ContextProperties.KEEP_ARTIFACTS, new Boolean (true));
//      config.put (ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);

        tester.complete(context, config, stringContentsUnordered, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue (stringContentsUnordered.valid());
        Assert.assertTrue (stringContentsUnordered.getResult().size() == DATA.length);
    }

    private Map<String, Object> getKafkaParams() {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("topic", Constants.TOPIC_TEST);
        params.put("appConfigName", Constants.APP_CONFIG);

        return params;
    }

    private static class Converter implements BiFunction<Integer, OutputTuple, OutputTuple> {
        private static final long serialVersionUID = 1L;
        private int counter = 0;

        @Override
        public OutputTuple apply(Integer val, OutputTuple outTuple) {
            outTuple.setInt("key", counter++);
            outTuple.setInt("message", val.intValue());
            return outTuple;
        }
    }
}
