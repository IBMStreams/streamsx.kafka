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

import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.ibm.streamsx.kafka.KafkaMetricException;

/**
 * @author The IBM Kafka toolkit maintainers 
 */
public interface MetricsProvider {
    /**
     * Gets the metrics.
     * @return the metrics.
     */
    public Map <MetricName,? extends Metric> getMetrics();
    /**
     * Creates a metric name for the custom metric of an operator from a Kafka metric name.
     * 
     * @param metricName
     * @return the name of the operator metric
     * @throws KafkaMetricException A name cannot be built.
     */
    public String createCustomMetricName (MetricName metricName) throws KafkaMetricException;
}
