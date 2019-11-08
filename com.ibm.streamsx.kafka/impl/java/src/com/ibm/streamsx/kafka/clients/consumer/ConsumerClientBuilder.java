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

/**
 * Builds a consumer client implementation.
 * @author The IBM Kafka toolkit maintainers
 * @since 3.0
 */
public interface ConsumerClientBuilder {
    /**
     * Builds a Consumer client.
     * @return the consumer client
     */
    ConsumerClient build() throws Exception;

    /**
     * Returns the implementation magic number of the built clients.
     * @return a hash number of the implementation of the runtime class.
     */
    public int getImplementationMagic();
}
