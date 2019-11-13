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
package com.ibm.streamsx.kafka.clients.consumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.kafka.ControlportJsonParseException;
import com.ibm.streamsx.kafka.i18n.Messages;

public class ControlPortAction {

    private final static Logger trace = Logger.getLogger (ControlPortAction.class);
    
    private final static Gson gson = new Gson();
    // actions allowed in JSON:
    private static enum JsonAction {ADD, REMOVE};
    private final ControlPortActionType action;
    private final Map<TopicPartition, Long /* offset */> topicPartitionOffsetMap;
    private final Set<String> topics;
    private final String json;

    private ControlPortAction(String json, ControlPortActionType action, Map<TopicPartition, Long> topicPartitionOffsetMap) {
        if (!(action == ControlPortActionType.ADD_ASSIGNMENT || action == ControlPortActionType.REMOVE_ASSIGNMENT)) {
            throw new IllegalArgumentException ("invalid action: " + action);
        }
        this.action = action;
        this.topicPartitionOffsetMap = topicPartitionOffsetMap;
        this.topics = null;
        this.json = json;
    }

    private ControlPortAction(String json, ControlPortActionType action, Set<String> topics) {
        if (!(action == ControlPortActionType.ADD_SUBSCRIPTION || action == ControlPortActionType.REMOVE_SUBSCRIPTION)) {
            throw new IllegalArgumentException ("invalid action: " + action);
        }
        this.action = action;
        this.topicPartitionOffsetMap = null;
        this.topics = topics;
        this.json = json;
    }

    private ControlPortAction (String json) {
        this.action = ControlPortActionType.NONE;
        this.topicPartitionOffsetMap = null;
        this.topics = null;
        this.json = json;
    }

    public ControlPortActionType getActionType() {
        return action;
    }

    public Map<TopicPartition, Long> getTopicPartitionOffsetMap() {
        return topicPartitionOffsetMap;
    }

    /**
     * @return the topics
     */
    public Set<String> getTopics() {
        return topics;
    }

    public String getJson() {
        return json;
    }

    @Override
    public String toString() {
        switch (action) {
        case ADD_ASSIGNMENT:
        case REMOVE_ASSIGNMENT:
            return " [action=" + action + ", topicPartitionOffsetMap=" + topicPartitionOffsetMap + "]";
        case ADD_SUBSCRIPTION:
        case REMOVE_SUBSCRIPTION:
            return " [action=" + action + ", topics=" + topics + "]";
        default:
            return " [action=" + action + ", topicPartitionOffsetMap=" + topicPartitionOffsetMap + ", topic=" + topics + "]";
        }
    }

    /**
     * Creates a ControlPortAction from a JSON formatted String
     * @param json The JSON string
     * @return a ControlPortAction object
     * @throws ControlportJsonParseException parsing JSON failed
     */
    public static ControlPortAction fromJSON (String json) throws ControlportJsonParseException {
        JsonObject jsonObj = null;
        try {
            jsonObj = gson.fromJson (json, JsonObject.class);
        }
        catch (Exception e) {
            ControlportJsonParseException exc = new ControlportJsonParseException (e.getMessage(), e);
            exc.setJson (json);
            throw exc;
        }

        if (jsonObj == null) {
            ControlportJsonParseException exc = new ControlportJsonParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "action", json==null? "null": json));
            exc.setJson(json);
            throw exc;
        }
        final String jason = jsonObj.toString();
        ControlPortActionType a = null;
        JsonAction action = null;
        if (jsonObj.has ("action")) { //$NON-NLS-1$
            try {
                action = JsonAction.valueOf (jsonObj.get ("action").getAsString().toUpperCase()); //$NON-NLS-1$
            }
            catch (Exception e) {
                ControlportJsonParseException exc = new ControlportJsonParseException (e.getMessage(), e);
                exc.setJson (json);
                throw exc;
            }
        } else {
            ControlportJsonParseException exc = new ControlportJsonParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "action", json));
            exc.setJson (json);
            throw exc;
        }

        if (jsonObj.has ("topicPartitionOffsets") && jsonObj.has ("topics")) { //$NON-NLS-1$
            final ControlportJsonParseException exc = new ControlportJsonParseException (Messages.getString ("INVALID_JSON", json));
            exc.setJson (json);
            throw exc;
        }
        if (!jsonObj.has ("topicPartitionOffsets") && !jsonObj.has ("topics")) { //$NON-NLS-1$
            trace.warn ("expected \"topicPartitionOffsets\" or \"topics\" element in JSON: " + jason);
        }

        Map <TopicPartition, Long> topicPartitionOffsetMap = new HashMap<>();
        Set <String> topics = new HashSet<>();
        if (jsonObj.has ("topicPartitionOffsets")) { //$NON-NLS-1$
            a = action == JsonAction.ADD? ControlPortActionType.ADD_ASSIGNMENT: ControlPortActionType.REMOVE_ASSIGNMENT;
            JsonArray arr = jsonObj.get ("topicPartitionOffsets").getAsJsonArray(); //$NON-NLS-1$
            Iterator<JsonElement> it = arr.iterator();
            while (it.hasNext()) {
                JsonObject tpo = it.next().getAsJsonObject();
                if(!tpo.has ("topic")) { //$NON-NLS-1$
                    ControlportJsonParseException exc = new ControlportJsonParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "topic", json));
                    exc.setJson (json);
                    throw exc;
                }

                if(!tpo.has("partition")) { //$NON-NLS-1$
                    ControlportJsonParseException exc = new ControlportJsonParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "partition", json));
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
                    ControlportJsonParseException exc = new ControlportJsonParseException (e.getMessage(), e);
                    exc.setJson (json);
                    throw exc;
                }
            }
            return new ControlPortAction (jason, a, topicPartitionOffsetMap);
        }
        if (jsonObj.has ("topics")) {
            a = action == JsonAction.ADD? ControlPortActionType.ADD_SUBSCRIPTION: ControlPortActionType.REMOVE_SUBSCRIPTION;
            JsonArray arr = jsonObj.get ("topics").getAsJsonArray(); //$NON-NLS-1$
            Iterator<JsonElement> it = arr.iterator();
            while (it.hasNext()) {
                JsonObject tpc = it.next().getAsJsonObject();
                if(!tpc.has ("topic")) { //$NON-NLS-1$
                    ControlportJsonParseException exc = new ControlportJsonParseException (Messages.getString("INVALID_JSON_MISSING_KEY", "topic", json));
                    exc.setJson (json);
                    throw exc;
                }
                try {
                    String topic = tpc.get ("topic").getAsString(); //$NON-NLS-1$
                    topics.add (topic);
                }
                catch (Exception e) {
                    // Handle Number format errors
                    ControlportJsonParseException exc = new ControlportJsonParseException (e.getMessage(), e);
                    exc.setJson (json);
                    throw exc;
                }
            }
            return new ControlPortAction (jason, a, topics);
        }
        return new ControlPortAction (jason);
    }
}
