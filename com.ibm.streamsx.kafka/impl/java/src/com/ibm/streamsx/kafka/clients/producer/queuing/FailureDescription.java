/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer.queuing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.errors.NotLeaderForPartitionException;

/**
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
        this.failedTopics = new ArrayList<> (failedTopics);
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
    
    // create JSON from such an instance
    public static void main (String[] args) {
        Collection <String> t = new ArrayList<> (2);
        t.add("topic1");
        t.add("topic2");
        FailureDescription d = new FailureDescription(t, new NotLeaderForPartitionException("a.b.c"));
        com.google.gson.Gson gson = (new com.google.gson.GsonBuilder()).enableComplexMapKeySerialization().create();
        String json = gson.toJson(d);
        System.out.println (json);
    }
}
