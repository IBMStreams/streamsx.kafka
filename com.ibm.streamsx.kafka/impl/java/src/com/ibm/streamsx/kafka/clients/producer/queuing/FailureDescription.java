/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer.queuing;

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
