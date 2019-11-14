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
 * This exception indicates that an operator is not registered in the MXBean
 * @author IBM Kafka toolkit maintainers
 */
public class KafkaOperatorNotRegisteredException extends KafkaOperatorCheckpointException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new KafkaOperatorNotRegisteredException
     */
    public KafkaOperatorNotRegisteredException() {
        super();
    }

    /**
     * Constructs a new KafkaOperatorNotRegisteredException
     * @param message - the detail message.
     */
    public KafkaOperatorNotRegisteredException(String message) {
        super(message);
    }

    /**
     * Constructs a new KafkaOperatorNotRegisteredException
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public KafkaOperatorNotRegisteredException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new KafkaOperatorNotRegisteredException
     * @param message - the detail message.
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public KafkaOperatorNotRegisteredException(String message, Throwable cause) {
        super (message, cause);
    }

    /**
     * Constructs a new KafkaOperatorNotRegisteredException
     * @param message - the detail message.
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     * @param enableSuppression
     * @param writableStackTrace
     */
    public KafkaOperatorNotRegisteredException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super (message, cause, enableSuppression, writableStackTrace);
    }
}
