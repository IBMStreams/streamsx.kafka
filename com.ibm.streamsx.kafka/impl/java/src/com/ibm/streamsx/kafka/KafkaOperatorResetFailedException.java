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
 * RuntimeException that is thrown when reset of the operator within a Consistent Region failed.
 * @author IBM Kafka toolkit team
 */
public class KafkaOperatorResetFailedException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new KafkaOperatorResetFailedException
     */
    public KafkaOperatorResetFailedException() {
        super();
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param message - the detail message.
     */
    public KafkaOperatorResetFailedException (String message) {
        super (message);
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public KafkaOperatorResetFailedException (Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param message - the detail message.
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public KafkaOperatorResetFailedException (String message, Throwable cause) {
        super (message, cause);
    }

    /**
     * Constructs a new KafkaOperatorResetFailedException
     * @param message - the detail message.
     * @param cause - the cause. (A null value is permitted, and indicates that the cause is nonexistent or unknown.)
     * @param enableSuppression - whether or not suppression is enabled or disabled
     * @param writableStackTrace - whether or not the stack trace should be writable
     */
    public KafkaOperatorResetFailedException (String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super (message, cause, enableSuppression, writableStackTrace);
    }
}
