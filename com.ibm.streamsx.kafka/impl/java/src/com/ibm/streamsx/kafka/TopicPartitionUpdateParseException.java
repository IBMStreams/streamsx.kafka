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
package com.ibm.streamsx.kafka;

/**
 * @author IBM Kafka toolkit maintainers
 */
public class TopicPartitionUpdateParseException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    private String json = null;

    /**
     * @return the json
     */
    public String getJson() {
        return json;
    }

    /**
     * @param json the json to set
     */
    public void setJson (String json) {
        this.json = json;
    }

    /**
     * 
     */
    public TopicPartitionUpdateParseException() {
        super();
    }

    /**
     * @param message
     */
    public TopicPartitionUpdateParseException (String message) {
        super (message);
    }

    /**
     * @param cause
     */
    public TopicPartitionUpdateParseException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public TopicPartitionUpdateParseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public TopicPartitionUpdateParseException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
