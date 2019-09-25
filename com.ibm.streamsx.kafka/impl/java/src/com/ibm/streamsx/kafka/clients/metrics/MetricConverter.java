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
package com.ibm.streamsx.kafka.clients.metrics;

/**
 * The MetricConverter converts the value of a Kafka metric into the value range of operator custom metrics.
 * Kafka metric values are typically doubles, often in the range 0..1, whereas operator metrics 
 * are positive integers in the long range.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public interface MetricConverter {

    public long convert (Object metricValue);
}
