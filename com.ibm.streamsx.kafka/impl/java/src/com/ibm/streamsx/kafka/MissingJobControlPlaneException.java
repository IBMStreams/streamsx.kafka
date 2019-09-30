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
 * This exception is thrown when the JobControlPlane cannot be connected because it 
 * is most likely missing in the operator graph.
 * @author The IBM Kafka toolkit maintainers
 */
public class MissingJobControlPlaneException extends KafkaOperatorException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new MissingJobControlPlaneException
     */
    public MissingJobControlPlaneException() {
        super();
    }

    /**
     * Creates a new MissingJobControlPlaneException with message.
     * @param message The exception message
     */
    public MissingJobControlPlaneException(String message) {
        super (message);
    }

    /**
     * Creates a new MissingJobControlPlaneException with root cause.
     * 
     * @param rootCause  The root cause of the exception
     */
    public MissingJobControlPlaneException(Throwable rootCause) {
        super (rootCause);
    }

    /**
     * Creates a new MissingJobControlPlaneException with message and root cause.
     * 
     * @param message    The exception message
     * @param rootCause  The root cause of the exception
     */
    public MissingJobControlPlaneException(String message, Throwable rootCause) {
        super (message, rootCause);
    }
}
