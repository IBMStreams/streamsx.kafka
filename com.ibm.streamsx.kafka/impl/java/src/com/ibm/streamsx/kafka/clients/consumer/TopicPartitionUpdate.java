package com.ibm.streamsx.kafka.clients.consumer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.kafka.TopicPartitionUpdateParseException;
import com.ibm.streamsx.kafka.i18n.Messages;

public class TopicPartitionUpdate {
    private final static Gson gson = new Gson();

    private final TopicPartitionUpdateAction action;
    private final Map<TopicPartition, Long /* offset */> topicPartitionOffsetMap;

    public TopicPartitionUpdate(TopicPartitionUpdateAction action, Map<TopicPartition, Long> topicPartitionOffsetMap) {
        this.action = action;
        this.topicPartitionOffsetMap = topicPartitionOffsetMap;
    }

    public TopicPartitionUpdateAction getAction() {
        return action;
    }

    public Map<TopicPartition, Long> getTopicPartitionOffsetMap() {
        return topicPartitionOffsetMap;
    }

    @Override
    public String toString() {
        return "TopicPartitionUpdate [action=" + action + ", topicPartitionOffsetMap=" + topicPartitionOffsetMap + "]";
    }

    /**
     * Creates an Topic partition update from a JSON formatted String
     * @param json The JSON string
     * @return a TopicPartitionUpdate object
     * @throws TopicPartitionUpdateParseException parsing JSON failed
     */
    public static TopicPartitionUpdate fromJSON (String json) throws TopicPartitionUpdateParseException {
        JsonObject jsonObj = null;
        try {
            jsonObj = gson.fromJson (json, JsonObject.class);
        }
        catch (Exception e) {
            TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (e.getMessage(), e);
            exc.setJson (json);
            throw exc;
        }

        if (jsonObj == null) {
            TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "action", json==null? "null": json));
            exc.setJson(json);
            throw exc;
        }
        TopicPartitionUpdateAction action = null;
        if (jsonObj.has ("action")) { //$NON-NLS-1$
            try {
                action = TopicPartitionUpdateAction.valueOf (jsonObj.get ("action").getAsString().toUpperCase()); //$NON-NLS-1$
            }
            catch (Exception e) {
                TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (e.getMessage(), e);
                exc.setJson (json);
                throw exc;
            }
        } else {
            TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "action", json));
            exc.setJson (json);
            throw exc;
        }

        Map <TopicPartition, Long> topicPartitionOffsetMap = new HashMap<>();
        if (jsonObj.has ("topicPartitionOffsets")) { //$NON-NLS-1$
            JsonArray arr = jsonObj.get ("topicPartitionOffsets").getAsJsonArray(); //$NON-NLS-1$
            Iterator<JsonElement> it = arr.iterator();
            while (it.hasNext()) {
                JsonObject tpo = it.next().getAsJsonObject();
                if(!tpo.has ("topic")) { //$NON-NLS-1$
                    TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "topic", json));
                    exc.setJson (json);
                    throw exc;
                }

                if(!tpo.has("partition")) { //$NON-NLS-1$
                    TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "partition", json));
                    exc.setJson (json);
                    throw exc;
                }
                try {
                    String topic = tpo.get ("topic").getAsString(); //$NON-NLS-1$
                    int partition = tpo.get ("partition").getAsInt(); //$NON-NLS-1$
                    long offset = tpo.has ("offset")? tpo.get ("offset").getAsLong(): OffsetConstants.NO_SEEK; //$NON-NLS-1$ //$NON-NLS-2$
                    topicPartitionOffsetMap.put (new TopicPartition (topic, partition), new Long (offset));
                }
                catch (Exception e) {
                    // Handle Number format errors
                    TopicPartitionUpdateParseException exc = new TopicPartitionUpdateParseException (e.getMessage(), e);
                    exc.setJson (json);
                    throw exc;
                }
            }
        }
        return new TopicPartitionUpdate (action, topicPartitionOffsetMap);
    }
}
