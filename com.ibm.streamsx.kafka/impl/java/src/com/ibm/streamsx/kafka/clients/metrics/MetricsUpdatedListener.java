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
 * This listener is called back, after all custom metrics have been updated.
 * 
 * @author The IBM Kafka toolkit team
 * 
 * @see MetricsFetcher#registerUpdateListener(MetricsUpdatedListener)
 */
public interface MetricsUpdatedListener {
    /**
     * called back after all registered instances of {@link CustomMetricUpdateListener} have been called.
     */
    public void afterCustomMetricsUpdated();
    /**
     * called back before all registered instances of {@link CustomMetricUpdateListener} have been called.
     */
    public void beforeCustomMetricsUpdated();
}
