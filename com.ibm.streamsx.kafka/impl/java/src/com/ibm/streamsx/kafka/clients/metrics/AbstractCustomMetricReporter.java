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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.metrics.OperatorMetrics;
//import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streamsx.kafka.KafkaMetricException;
import com.ibm.streamsx.kafka.SystemProperties;

/**
 * This class exposes Kafka metrics as Operator Custom metrics.
 * 
 * @author The IBM Kafka toolkit maintainers
 */
public abstract class AbstractCustomMetricReporter implements MetricsReporter {

    /** value for a metric that is not applicable (any more) */
    private static final long METRIC_NOT_APPLICABLE = -1l;
    private static final Logger trace = Logger.getLogger (AbstractCustomMetricReporter.class);
    protected static final Level DEBUG_LEVEL = SystemProperties.getDebugLevelMetricsOverride();
    private OperatorContext operatorCtx = PERuntime.getCurrentContext();

    public AbstractCustomMetricReporter() {
    }

    /**
     * Gets the filter for this metric reporter.
     * The filters decides, which Kafka metrics get exposed as operator metrics, and which are suppressed.
     * 
     * @return The MetricFilter of this reporter
     */
    public abstract MetricFilter getFilter();

    
    @Override
    public void configure (Map<String, ?> configs) {
    }

    /**
     * This is called when the reporter is first registered to initially register all existing metrics
     * @param metrics All currently existing metrics
     */
    @Override
    public void init (List<KafkaMetric> metrics) {
        for (KafkaMetric m: metrics) {
            if (getFilter().apply (m)) {
                tryCreateCustomMetric (m);
            }
            else {
                trace.log (DEBUG_LEVEL, "Kafka metric NOT exposed as operator metric: " + m.metricName());
            }
        }
    }

    /**
     * This is called whenever a metric is updated or added
     * @param metric
     */
    @Override
    public void metricChange (KafkaMetric metric) {
        synchronized (this) {
            if (getFilter().apply (metric)) {
                tryCreateCustomMetric (metric);
            }
            else {
                trace.log (DEBUG_LEVEL, "Kafka metric NOT exposed as custom metric: " + metric.metricName());
            }
        }
    }

    /**
     * Creates the name of an operator custom metric from Metric name within the Kafka library.
     * This method must be overwritten by subclasses.
     * @param metricName  Kafka's metric name
     * @return            the metric name of the operator metric
     * @throws KafkaMetricException
     */
    public abstract String createCustomMetricName (MetricName metricName) throws KafkaMetricException;

    /**
     * creates a custom metric if it is not yet present.
     * The custom metric name is created using {@link #createCustomMetricName(MetricName)}.
     * @param metric The name of the Kafka metric
     */
    private void tryCreateCustomMetric (final Metric metric) {
        final MetricName metricName = metric.metricName();
        try {
            final String customMetricName = createCustomMetricName (metricName);
            OperatorMetrics operatorMetrics = operatorCtx.getMetrics();
            if (operatorMetrics.getCustomMetrics().containsKey (customMetricName)) {
                trace.info ("Custom metric already created: " + customMetricName);
                operatorMetrics.getCustomMetric(customMetricName).setValue(getFilter().getConverter(metric).convert(metric.metricValue()));
                return;
            }
            operatorMetrics.createCustomMetric (customMetricName, metricName.description(), com.ibm.streams.operator.metrics.Metric.Kind.GAUGE);
            operatorMetrics.getCustomMetric(customMetricName).setValue(getFilter().getConverter(metric).convert(metric.metricValue()));
            trace.info ("custom metric created: " + customMetricName + " (" + metricName.description() + ")");
        } catch (KafkaMetricException e) {
            trace.warn ("Cannot create custom metric from Kafka metric '" + metricName + "': " + e.getLocalizedMessage());
        }
    }


    /**
     * This is called whenever a metric is removed.
     * It sets the value of the corresponding operator metric,
     * if there is one, to {@value #METRIC_NOT_APPLICABLE} as custom metrics cannot be deleted from operators.
     * 
     * @param metric The metric that is removed from Kafka
     */
    @Override
    public void metricRemoval (KafkaMetric metric) {
        trace.log (DEBUG_LEVEL, "metricRemoval(): " + metric.metricName());
        synchronized (this) {
            if (getFilter().apply (metric)) {
                final MetricName metricName = metric.metricName();
                try {
                    final String customMetricName = createCustomMetricName (metricName);
                    OperatorMetrics operatorMetrics = operatorCtx.getMetrics();
                    if (operatorMetrics.getCustomMetrics().containsKey (customMetricName)) {
                        operatorMetrics.getCustomMetric(customMetricName).setValue (METRIC_NOT_APPLICABLE);
                    }
                } catch (KafkaMetricException e) {
                    trace.warn ("Cannot derive custom metric from Kafka metric '" + metricName + "': " + e.getLocalizedMessage());
                }
            }
            else {
                trace.log (DEBUG_LEVEL, "Kafka metric NOT exposed as custom metric: " + metric.metricName());
            }
        }
    }

    /**
     * Called when the metrics repository is closed.
     */
    @Override
    public void close() {
        trace.debug ("close()");
    }
}
