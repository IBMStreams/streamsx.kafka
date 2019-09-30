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

import org.apache.log4j.Logger;

/**
 * This converter multiplies the Kafka metric value with a factor and rounds it to a long value.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public class MultiplyConverter implements MetricConverter {

    private static final Logger trace = Logger.getLogger (MultiplyConverter.class);
    private String metricName;
    private double factor;
    
    /**
     * @param metricName  the metric name - for error tracing only
     * @param factor      the multiplier
     */
    public MultiplyConverter (String metricName, double factor) {
        this.metricName = metricName;
        this.factor = factor;
    }

    /**
     * @param factor the multiplier
     */
    public MultiplyConverter (double factor) {
        this ("", factor);
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.metrics.MetricConverter#convert(java.lang.Object)
     */
    @Override
    public long convert (Object metricValue) {
        if (metricValue == null) return 0l;
        if (metricValue instanceof java.lang.Number) {
            long l = Math.round (factor * ((java.lang.Number)metricValue).doubleValue());
            // Math.round() rounds to Long.MIN_VALUE for negative infinity, we want 0 in this case: 
            return l == Long.MIN_VALUE? 0l: l;
        }
        trace.error ("convert (" + metricName + "): class: " + metricValue.getClass().getCanonicalName() + ", no conversion for value = " + metricValue);
        return 0l;
    }
}
