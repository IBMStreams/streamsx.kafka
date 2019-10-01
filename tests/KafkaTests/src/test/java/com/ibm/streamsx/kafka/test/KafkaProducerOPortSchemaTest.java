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
package com.ibm.streamsx.kafka.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.kafka.test.utils.Constants;
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

/**
 * Test the optional output port of the KafkaProducer with different SPL schemas.
 * As we have problems with automatic FinalMarker forwarding, we
 * 1. throttle the input data to 1/s to give the tuples a chance to get processed
 * 2. test only for at least tuples as the FinalMarker can go into the tester prior to the last tuple(s).
 * 
 * Test requirements:
 *  - appConfig "kafka-test" be created on the domain
 *  - topic "test" to be created
 */
public class KafkaProducerOPortSchemaTest extends AbstractKafkaTest {

    public static final long NUM_TUPLES = 10;
    private static final StreamSchema PRODUCER_IN_SCHEMA = com.ibm.streams.operator.Type.Factory.getStreamSchema("tuple<int32 key, rstring message>");

    /**
     * Supplies the input data for the producer.
     */
    private static class MySupplier implements Supplier<Message<Integer, String>> {
        private static final long serialVersionUID = 1L;
        private int counter = 0;

        @Override
        public Message<Integer, String> get() {
            int key = ++counter;
            String message = "message " + key;
            return new Message<Integer, String>(key, message);
        }
    }

    /**
     * Applies a {@link Message} object to an output tuple
     */
    private static class MessageConverter implements BiFunction<Message<Integer, String>, OutputTuple, OutputTuple> {
        private static final long serialVersionUID = 1L;

        @Override
        public OutputTuple apply (Message<Integer, String> msg, OutputTuple outTuple) {
            outTuple.setInt ("key", msg.getKey());
            outTuple.setString ("message", msg.getValue());
            return outTuple;
        }
    }

    public KafkaProducerOPortSchemaTest() throws Exception {
        super();
    }

    @Test
    public void kafkaProducerOPortRstringTest() throws Exception {
        final StreamSchema oSchema = com.ibm.streams.operator.Type.Factory.getStreamSchema(
                "tuple<tuple<int32 key, rstring message> inTuple, rstring failureDescription>"
                );
        doTestWithSPLSchema (oSchema);
    }

    @Test
    public void kafkaProducerOPortUstringTest() throws Exception {
        final StreamSchema oSchema = com.ibm.streams.operator.Type.Factory.getStreamSchema(
                "tuple<tuple<int32 key, rstring message> inTuple, ustring failureDescription>"
                );
        doTestWithSPLSchema (oSchema);
    }

    @Test
    public void kafkaProducerOPortOptionalRstringTest() throws Exception {
        final StreamSchema oSchema = com.ibm.streams.operator.Type.Factory.getStreamSchema(
                "tuple<tuple<int32 key, rstring message> inTuple, optional<rstring> failureDescription>"
                );
        doTestWithSPLSchema (oSchema);
    }

    @Test
    public void kafkaProducerOPortOptionalUstringTest() throws Exception {
        final StreamSchema oSchema = com.ibm.streams.operator.Type.Factory.getStreamSchema(
                "tuple<tuple<int32 key, rstring message> inTuple, optional<ustring> failureDescription>"
                );
        doTestWithSPLSchema (oSchema);
    }


    protected void doTestWithSPLSchema (StreamSchema splOutSchema) throws Exception {
        Topology topo = getTopology();

        // data generator
        TStream<Message<Integer, String>> src = topo.limitedSource (new MySupplier(), NUM_TUPLES);
        SPLStream outStream = SPLStreams.convertStream (src, new MessageConverter(), PRODUCER_IN_SCHEMA).throttle (1L, TimeUnit.SECONDS);
        // create producer
        SPLStream statusStream = SPL.invokeOperator ("Producer", Constants.KafkaProducerOp, outStream, splOutSchema, getKafkaProducerParams());
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext (Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        final long minNumExpectedTuples = NUM_TUPLES-3L;
        Condition<Long> numStatusTuples = tester.atLeastTupleCount (statusStream, minNumExpectedTuples);
        HashMap<String, Object> config = new HashMap<>();
        // config.put (ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);
        // config.put(ContextProperties.KEEP_ARTIFACTS,  new Boolean(true));
        tester.complete (context, config, numStatusTuples, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue (numStatusTuples.valid());
        Assert.assertTrue (numStatusTuples.getResult() >= minNumExpectedTuples);
    }


    private Map<String, Object> getKafkaProducerParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", Constants.TOPIC_TEST);
        params.put("appConfigName", Constants.APP_CONFIG);
        params.put("outputErrorsOnly", false);
        return params;
    }
}
