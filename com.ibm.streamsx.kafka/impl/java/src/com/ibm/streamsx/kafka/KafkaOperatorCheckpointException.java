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
 * RuntimeException that is thrown when checkpoint in a Consistent Region failed.
 * @author IBM Kafka toolkit team
 */
public class KafkaOperatorCheckpointException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new KafkaOperatorCheckpointException
     */
    public KafkaOperatorCheckpointException() {
        super();
    }

    /**
     * Constructs a new KafkaOperatorCheckpointException
     * @param message the exception message
     */
    public KafkaOperatorCheckpointException (String message) {
        super (message);
    }

    /**
     * Constructs a new KafkaOperatorCheckpointException
     * @param cause the cause of the exception
     */
    public KafkaOperatorCheckpointException (Throwable cause) {
        super (cause);
    }

    /**
     * Constructs a new KafkaOperatorCheckpointException
     * @param message the exception message
     * @param cause the cause of the exception
     */
    public KafkaOperatorCheckpointException (String message, Throwable cause) {
        super (message, cause);
    }

    /**
     * Constructs a new KafkaOperatorCheckpointException
     * @param message the exception message
     * @param cause the cause of the exception
     * @param enableSuppression
     * @param writableStackTrace
     */
    public KafkaOperatorCheckpointException (String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super (message, cause, enableSuppression, writableStackTrace);
    }

}
