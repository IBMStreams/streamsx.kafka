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
 * This exception indicates that an action triggered via control port, is not 
 * supported in current operator configuration and state.
 * 
 * This Exception inherits from <tt>java.lang.RuntimeException</tt>.
 * 
 * @author IBM Kafka toolkit maintainers
 * @since 24.03.2021, future version
 */
public class UnsupportedControlPortActionException extends KafkaOperatorRuntimeException {

    private static final long serialVersionUID = 1L;

    public UnsupportedControlPortActionException() {
    }

    /**
     * @param message
     */
    public UnsupportedControlPortActionException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public UnsupportedControlPortActionException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public UnsupportedControlPortActionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     * @param cause
     * @param enableSuppression
     * @param writableStackTrace
     */
    public UnsupportedControlPortActionException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
