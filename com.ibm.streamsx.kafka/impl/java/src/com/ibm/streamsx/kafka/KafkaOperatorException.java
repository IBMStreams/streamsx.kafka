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
 * @author IBM Kafka toolkit team
 */
public class KafkaOperatorException extends Exception {

    private static final long serialVersionUID = 1449871652960826013L;

    /**
     * Constructs a new KafkaOperatorException
     */
    public KafkaOperatorException() {
    }

    /**
     * @param message
     */
    public KafkaOperatorException (String message) {
        super(message);
    }

    /**
     * @param rootCause
     */
    public KafkaOperatorException (Throwable rootCause) {
        super(rootCause);
    }

    /**
     * @param message
     * @param rootCause
     */
    public KafkaOperatorException (String message, Throwable rootCause) {
        super(message, rootCause);
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
