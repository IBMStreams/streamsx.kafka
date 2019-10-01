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

    public enum StartPosition {
        Beginning;
    }

    public KafkaOperatorsStartPositionTest() throws Exception {
        super();
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
        Thread.sleep(3000L);
        if(!future.isDone()) {
            future.cancel(true);
        }

        // create the consumer
        Topology topo = getTopology();
        topo.addJobControlPlane();
        Map<String, Object> consumerParams = new HashMap<>();
        consumerParams.put("topic", Constants.TOPIC_POS);
        consumerParams.put("propertiesFile", Constants.PROPERTIES_FILE_PATH);
        consumerParams.put("startPosition", StartPosition.Beginning);

        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, consumerParams, KafkaSPLStreamsUtils.STRING_SCHEMA);
        SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> t.getString("message")));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        Condition<List<String>> stringContentsUnordered = tester.stringContentsUnordered (msgStream.toStringStream(), Constants.STRING_DATA);
        HashMap<String, Object> config = new HashMap<>();
        //        config.put (ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);
        //        config.put(ContextProperties.KEEP_ARTIFACTS,  new Boolean(true));

        tester.complete(context, config, stringContentsUnordered, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue (stringContentsUnordered.valid());
        Assert.assertTrue (stringContentsUnordered.getResult().size() == Constants.STRING_DATA.length);
    }
}
