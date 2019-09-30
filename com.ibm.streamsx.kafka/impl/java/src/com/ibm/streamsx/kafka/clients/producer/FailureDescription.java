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
package com.ibm.streamsx.kafka.clients.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.errors.NotLeaderForPartitionException;

/**
 * The failure description, which is serialized to JSON for the optional output port of the producer operator.
 * 
 * @author The IBM Kafka toolkit maintainers
 * @since v2.2
 */
public class FailureDescription {
    private final List<String> failedTopics;
    private final String lastExceptionType;
    private final String lastFailure;

    /**
     * Creates a new FailureDescrition
     */
    public FailureDescription (Collection<String> failedTopics, Exception e) {
        if (failedTopics == null) {
            this.failedTopics = Collections.emptyList();
        } else {
            this.failedTopics = new ArrayList<> (failedTopics);
        }
        if (e != null) {
            this.lastExceptionType = e.getClass().getName();
            final String msg = e.getMessage();
            this.lastFailure = msg != null? msg: "";
        } else {
            this.lastExceptionType = "";
            this.lastFailure = "";
        }
    }

    public List<String> getFailedTopics() {
        return failedTopics;
    }

    public String getLastExceptionType() {
        return lastExceptionType;
    }

    public String getLastFailure() {
        return lastFailure;
    }

    /**
     * create JSON from a {@link FailureDescription} instance.
     * @param args
     */
    public static void main (String[] args) {
        Collection <String> t = new ArrayList<> (2);
        t.add("topic1");
        t.add("topic2");
        FailureDescription d = new FailureDescription(t, new NotLeaderForPartitionException("not leader for partition: boker.domain"));
        com.google.gson.Gson gson = (new com.google.gson.GsonBuilder()).enableComplexMapKeySerialization().create();
        String json = gson.toJson(d);
        System.out.println (json);
    }
}
