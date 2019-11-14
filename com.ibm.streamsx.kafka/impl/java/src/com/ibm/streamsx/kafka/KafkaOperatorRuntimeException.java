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
 * This Exception is a RuntimeException, which is an unchecked exception.
 * @author The IBM Kafka toolkit team
 */
public class KafkaOperatorRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public KafkaOperatorRuntimeException() {
        this ("", null);
    }

    /**
     * @param message
     */
    public KafkaOperatorRuntimeException(String message) {
        super(message, null);
    }

    /**
     * @param cause
     */
    public KafkaOperatorRuntimeException(Throwable cause) {
        this("", cause);
    }

    /**
     * @param message
     * @param cause
     */
    public KafkaOperatorRuntimeException(String message, Throwable cause) {
        this(message, cause, false, true);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public KafkaOperatorRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * gets the root cause of the exception
     * 
     * @return the root cause or `null` if there is none.
     */
    public Throwable getRootCause() {
        Throwable rootCause = null;
        Throwable cause = getCause();
        while (cause != null) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }
}
