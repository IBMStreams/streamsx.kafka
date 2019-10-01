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

/**
 * Test requirements:
 *  - appConfig "kafka-test" be created on the domain
 *  - topics other1 and other2 are created
 */
public class KafkaConsumerPatternSubscribeTest extends AbstractKafkaTest {

    public KafkaConsumerPatternSubscribeTest() throws Exception {
        super();
    }

    @Test
    public void kafkaPatternSubscribeTest() throws Exception {
        Topology topo = getTopology();

        // create the producer (produces tuples after a short delay)
        TStream<String> stringSrcStream = topo.strings (Constants.STRING_DATA).modify (new Delay<>(Constants.PRODUCER_DELAY));
        SPL.invokeSink (Constants.KafkaProducerOp,
                KafkaSPLStreamsUtils.convertStreamToKafkaTuple(stringSrcStream),
                getProducerKafkaParams());

        // create the consumer
        SPLStream consumerStream = SPL.invokeSource(topo, Constants.KafkaConsumerOp, getConsumerKafkaParams(), KafkaSPLStreamsUtils.STRING_SCHEMA);
        SPLStream msgStream = SPLStreams.stringToSPLStream(consumerStream.convert(t -> t.getString("message")));

        // test the output of the consumer
        StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
        Tester tester = topo.getTester();
        String[] expectedArr = KafkaSPLStreamsUtils.duplicateArrayEntries(Constants.STRING_DATA, 2);
        Condition<List<String>> stringContentsUnordered = tester.stringContentsUnordered (msgStream.toStringStream(), expectedArr);
        HashMap<String, Object> config = new HashMap<>();
        //      config.put (ContextProperties.KEEP_ARTIFACTS, new Boolean (true));
        //      config.put (ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);

        tester.complete(context, config, stringContentsUnordered, 60, TimeUnit.SECONDS);

        // check the results
        Assert.assertTrue (stringContentsUnordered.valid());
        Assert.assertTrue (stringContentsUnordered.getResult().size() == Constants.STRING_DATA.length * 2);
    }

    private Map<String, Object> getProducerKafkaParams() {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("topic", Arrays.asList (Constants.TOPIC_OTHER1, Constants.TOPIC_OTHER2).toArray(new String[0]));
        params.put("appConfigName", Constants.APP_CONFIG);

        return params;
    }

    private Map<String, Object> getConsumerKafkaParams() {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("pattern", "other[12]{1}");
        params.put("groupId", "" + (int) (Math.random() * 10000000.0));
        params.put("appConfigName", Constants.APP_CONFIG);

        return params;
    }
}

