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

import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.ibm.streamsx.kafka.clients.metrics.MetricConverter;
import com.ibm.streamsx.kafka.clients.metrics.MetricFilter;

/**
 * Filter that handles some special dynamic consumer metric names, like '<topic-partition>.records-lag'
 * @author The IBM Kafka toolkit maintainers
 */
public class DynamicMetricNameFilter extends MetricFilter {

    private static final String TOPIC_PARTITION_RECORDS_LAG_SUFFIX = ".records-lag";
    
    public DynamicMetricNameFilter() {
        super();
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.metrics.MetricFilter#apply(org.apache.kafka.common.Metric)
     */
    @Override
    public boolean apply (Metric m) {
        final MetricName mName = m.metricName();
        Map<String, MetricConverter> names = filters.get (mName.group());
        if (names == null)
            return false;
        String name = mName.name();
        if (name.endsWith (TOPIC_PARTITION_RECORDS_LAG_SUFFIX))
            return true;
//        return names.containsKey (name);
        return super.apply (m);
    }
}
