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
public class KafkaConsumerFanInTest extends AbstractKafkaTest {

    public KafkaConsumerFanInTest() throws Exception {
        super();
    }

    @Test
    public void kafkaFanInTest() throws Exception {
        Topology topo = getTopology();

        // create the producer (produces tuples after a short delay)
        TStream<String> stringSrcStream = topo.strings(Constants.STRING_DATA).modify(new Delay<>(5000));
        SPL.invokeSink(Constants.KafkaProducerOp, 
                KafkaSPLStreamsUtils.convertStreamToKafkaTuple(stringSrcStream), 
                getKafkaParams());

        // create the consumer
        SPLStream consumerStream1 = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getConsumerParams(1), KafkaSPLStreamsUtils.STRING_SCHEMA);
        SPLStream consumerStream2 = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getConsumerParams(2), KafkaSPLStreamsUtils.STRING_SCHEMA);
        SPLStream unionStream = KafkaSPLStreamsUtils.union(Arrays.asList(consumerStream1, consumerStream2), KafkaSPLStreamsUtils.STRING_SCHEMA);
        SPLStream msgStream = SPLStreams.stringToSPLStream(unionStream.convert(t -> t.getString("message")));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();

        // both consumers consume the same data, so each result is duplicated
        String[] expectedArr = KafkaSPLStreamsUtils.duplicateArrayEntries(Constants.STRING_DATA, 2);
        Condition<List<String>> stringContentsUnordered = tester.stringContentsUnordered (msgStream.toStringStream(), expectedArr);
        HashMap<String, Object> config = new HashMap<>();
        //        config.put (ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);
        //        config.put(ContextProperties.KEEP_ARTIFACTS,  new Boolean(true));
        tester.complete(context, config, stringContentsUnordered, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue (stringContentsUnordered.valid());
        Assert.assertTrue (stringContentsUnordered.getResult().size() == Constants.STRING_DATA.length *2);
    }

    private Map<String, Object> getConsumerParams(int consumerNum) {
        Map<String, Object> params = new HashMap<String, Object>(getKafkaParams());
        params.put("clientId", "test-client-" + consumerNum);
        return params;
    }

    private Map<String, Object> getKafkaParams() {
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("topic", Constants.TOPIC_TEST);
        params.put("appConfigName", Constants.APP_CONFIG);

        return params;
    }
}
