package com.ibm.streamsx.kafka.test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.kafka.test.utils.Constants;
import com.ibm.streamsx.kafka.test.utils.Delay;
import com.ibm.streamsx.kafka.test.utils.Message;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Supplier;
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
public class KafkaOperatorsTopicPartitionTest extends AbstractKafkaTest {

    private static final StreamSchema SCHEMA = com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<int32 key, rstring message>");

    public KafkaOperatorsTopicPartitionTest() throws Exception {
        super();
    }

    @Test
    public void kafkaTopicPartitionTest() throws Exception {
        Topology topo = getTopology();
        topo.addFileDependency("etc/custom_partitioner.properties", "etc");
        topo.addFileDependency("etc/custompartitioner.jar", "etc");

        // create producer
        TStream<Message<Integer, String>> src = topo.limitedSource(new MySupplier(), 9).modify(new Delay<>(Constants.PRODUCER_DELAY));
        SPLStream outStream = SPLStreams.convertStream(src, new MessageConverter(), SCHEMA);
        SPL.invokeSink(Constants.KafkaProducerOp, outStream, getKafkaProducerParams());		

        // create the consumers
        SPLStream msgStream1 = createConsumer(topo, 0);
        SPLStream msgStream2 = createConsumer(topo, 1);
        SPLStream msgStream3 = createConsumer(topo, 2);

        Set<TStream<Tuple>> s = new HashSet<>();
        s.add(msgStream2);
        s.add(msgStream3);
        TStream<String> unionStream = msgStream1.union(s).transform(t -> t.getString("message"));
        SPLStream msgStream = SPLStreams.stringToSPLStream(unionStream);

        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();

        String[] expectedArr = {"A0", "B1", "C2", "A3", "B4", "C5", "A6", "B7", "C8"};
        Condition<List<String>> stringContentsUnordered = tester.stringContentsUnordered (msgStream.toStringStream(), expectedArr);
        HashMap<String, Object> config = new HashMap<>();
        //        config.put (ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);
        //        config.put(ContextProperties.KEEP_ARTIFACTS,  new Boolean(true));

        tester.complete(context, config, stringContentsUnordered, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue (stringContentsUnordered.valid());
        Assert.assertTrue (stringContentsUnordered.getResult().size() == expectedArr.length);
    }

    private SPLStream createConsumer(Topology topo, int consumerNum) throws Exception {
        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getKafkaConsumerParams(consumerNum), SCHEMA);
        //SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> t.getString("message")));

        return consumerStream;
    }

    private Map<String, Object> getKafkaProducerParams() {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("topic", Constants.TOPIC_TEST);
        params.put("appConfigName", Constants.APP_CONFIG);
        params.put("userLib", "etc/custompartitioner.jar");
        params.put("propertiesFile", "etc/custom_partitioner.properties");

        return params;
    }

    private Map<String, Object> getKafkaConsumerParams(int partitionNum) {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("topic", Constants.TOPIC_TEST);
        params.put("partition", partitionNum);
        params.put("appConfigName", Constants.APP_CONFIG);

        return params;
    }

    private static class MySupplier implements Supplier<Message<Integer, String>> {
        private static final long serialVersionUID = 1L;
        private static final int NUM_PARTITIONS = 3;
        private static final String[] PREFIX = {"A", "B", "C"};

        private int counter = 0;

        @Override
        public Message<Integer, String> get() {
            int key = counter % NUM_PARTITIONS;
            String message = PREFIX[key] + counter;
            counter++;

            return new Message<Integer, String>(key, message);
        }

    }

    private static class MessageConverter implements BiFunction<Message<Integer, String>, OutputTuple, OutputTuple> {
        private static final long serialVersionUID = 1L;

        @Override
        public OutputTuple apply(Message<Integer, String> msg, OutputTuple outTuple) {
            outTuple.setInt("key", msg.getKey());
            outTuple.setString("message", msg.getValue());

            return outTuple;
        }
    }
}
