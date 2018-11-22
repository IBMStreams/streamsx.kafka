/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer;

import java.util.Map;

import org.apache.kafka.common.MetricName;

import com.ibm.streamsx.kafka.KafkaMetricException;
import com.ibm.streamsx.kafka.clients.metrics.AbstractCustomMetricReporter;
import com.ibm.streamsx.kafka.clients.metrics.MetricFilter;
import com.ibm.streamsx.kafka.clients.metrics.MultiplyConverter;
import com.ibm.streamsx.kafka.clients.metrics.RoundingConverter;

/**
 * This class implements the Reporter for producer custom metrics.
 * It can be configured as producer property 'metric.reporters'.
 *
 * @author The IBM Kafka toolkit Maintainers
 */
public class ProducerMetricsReporter extends AbstractCustomMetricReporter {

    private final static String PRODUCER_METRICS_GROUP = "producer-metrics";
    private final static String PRODUCER_TOPIC_METRICS_GROUP = "producer-topic-metrics";
    private static MetricFilter filter = new MetricFilter();
/*
    metric groups:
    
    group=app-info,
    group=kafka-metrics-count,
    group=producer-metrics,
    group=producer-node-metrics,
    group=producer-topic-metrics,
  
    metrics in the group "producer-metrics"

    name=bufferpool-wait-time-total       description=The total time an appender waits for space allocation.
    name=bufferpool-wait-ratio            description=The fraction of time an appender waits for space allocation.
    name=waiting-threads                  description=The number of user threads blocked waiting for buffer memory to enqueue their records
    name=buffer-total-bytes               description=The maximum amount of buffer memory the client can use (whether or not it is currently used).
    name=buffer-available-bytes           description=The total amount of buffer memory that is not being used (either unallocated or in the free list).
    name=buffer-exhausted-total           description=The total number of record sends that are dropped due to buffer exhaustion
    name=buffer-exhausted-rate            description=The average per-second number of record sends that are dropped due to buffer exhaustion
    name=produce-throttle-time-avg        description=The average time in ms a request was throttled by a broker
    name=produce-throttle-time-max        description=The maximum time in ms a request was throttled by a broker
    name=connection-close-total           description=The total number of connections closed
    name=connection-close-rate            description=The number of connections closed per second
    name=connection-creation-total        description=The total number of new connections established
    name=connection-creation-rate         description=The number of new connections established per second
    name=successful-authentication-total  description=The total number of connections with successful authentication
    name=successful-authentication-rate   description=The number of connections with successful authentication per second
    name=failed-authentication-total      description=The total number of connections with failed authentication
    name=failed-authentication-rate       description=The number of connections with failed authentication per second
    name=network-io-total                 description=The total number of network operations (reads or writes) on all connections
    name=network-io-rate                  description=The number of network operations (reads or writes) on all connections per second
    name=outgoing-byte-total              description=The total number of outgoing bytes sent to all servers
    name=outgoing-byte-rate               description=The number of outgoing bytes sent to all servers per second
    name=request-total                    description=The total number of requests sent
    name=request-rate                     description=The number of requests sent per second
    name=request-size-avg                 description=The average size of requests sent.
    name=request-size-max                 description=The maximum size of any request sent.
    name=incoming-byte-total              description=The total number of bytes read off all sockets
    name=incoming-byte-rate               description=The number of bytes read off all sockets per second
    name=response-total                   description=The total number of responses received
    name=response-rate                    description=The number of responses received per second
    name=select-total                     description=The total number of times the I/O layer checked for new I/O to perform
    name=select-rate                      description=The number of times the I/O layer checked for new I/O to perform per second
    name=io-wait-time-ns-avg              description=The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.
    name=io-waittime-total                description=The total time the I/O thread spent waiting
    name=io-wait-ratio                    description=The fraction of time the I/O thread spent waiting
    name=io-time-ns-avg                   description=The average length of time for I/O per select call in nanoseconds.
    name=iotime-total                     description=The total time the I/O thread spent doing I/O
    name=io-ratio                         description=The fraction of time the I/O thread spent doing I/O
    name=connection-count                 description=The current number of active connections.
    name=batch-size-avg                   description=The average number of bytes sent per partition per-request.
    name=batch-size-max                   description=The max number of bytes sent per partition per-request.
    name=compression-rate-avg             description=The average compression rate of record batches.
    name=record-queue-time-avg            description=The average time in ms record batches spent in the send buffer.
    name=record-queue-time-max            description=The maximum time in ms record batches spent in the send buffer.
    name=request-latency-avg              description=The average request latency in ms
    name=request-latency-max              description=The maximum request latency in ms
    name=record-send-total                description=The total number of records sent.
    name=record-send-rate                 description=The average number of records sent per second.
    name=records-per-request-avg          description=The average number of records per request.
    name=record-retry-total               description=The total number of retried record sends
    name=record-retry-rate                description=The average per-second number of retried record sends
    name=record-error-total               description=The total number of record sends that resulted in errors
    name=record-error-rate                description=The average per-second number of record sends that resulted in errors
    name=record-size-max                  description=The maximum record size
    name=record-size-avg                  description=The average record size
    name=requests-in-flight               description=The current number of in-flight requests awaiting a response.
    name=metadata-age                     description=The age in seconds of the current producer metadata being used.
    name=batch-split-total                description=The total number of batch splits
    name=batch-split-rate                 description=The average number of batch splits per second

    metrics in the group "producer-topic-metrics":

    name=record-send-total    description=The total number of records sent for a topic.
    name=record-send-rate     description=The average number of records sent per second for a topic.
    name=byte-total           description=The total number of bytes sent for a topic.
    name=byte-rate            description=The average number of bytes sent per second for a topic.
    name=compression-rate     description=The average compression rate of record batches for a topic.
    name=record-retry-total   description=The total number of retried record sends for a topic
    name=record-retry-rate    description=The average per-second number of retried record sends for a topic
    name=record-error-total   description=The total number of record sends that resulted in errors for a topic
    name=record-error-rate    description=The average per-second number of record sends that resulted in errors for a topic
    name=record-send-total    description=The total number of records sent for a topic.
    name=record-send-rate     description=The average number of records sent per second for a topic.
    name=byte-total           description=The total number of bytes sent for a topic.
    name=byte-rate            description=The average number of bytes sent per second for a topic.
    name=compression-rate     description=The average compression rate of record batches for a topic.
    name=record-retry-total   description=The total number of retried record sends for a topic
    name=record-retry-rate    description=The average per-second number of retried record sends for a topic
    name=record-error-total   description=The total number of record sends that resulted in errors for a topic
    name=record-error-rate    description=The average per-second number of record sends that resulted in errors for a topic

    metrics in the group "producer-node-metrics":

    name=outgoing-byte-total                      description=The total number of outgoing bytes
    name=outgoing-byte-rate                       description=The number of outgoing bytes per second
    name=request-total                            description=The total number of requests sent
    name=request-rate                             description=The number of requests sent per second
    name=request-size-avg                         description=The average size of requests sent.
    name=request-size-max                         description=The maximum size of any request sent.
    name=incoming-byte-total                      description=The total number of incoming bytes
    name=incoming-byte-rate                       description=The number of incoming bytes per second
    name=response-total                           description=The total number of responses received
    name=response-rate                            description=The number of responses received per second
    name=request-latency-avg                      description=
    name=request-latency-max                      description=
*/
    static {
        filter
        .add (PRODUCER_METRICS_GROUP, "connection-count", new RoundingConverter("connection-count"))
        .add (PRODUCER_METRICS_GROUP, "buffer-available-bytes", new RoundingConverter("buffer-available-bytes"))
        .add (PRODUCER_METRICS_GROUP, "request-rate", new RoundingConverter("request-rate"))
        .add (PRODUCER_METRICS_GROUP, "request-size-avg", new RoundingConverter("request-size-avg"))
        .add (PRODUCER_METRICS_GROUP, "request-latency-avg", new RoundingConverter("request-latency-avg"))
        .add (PRODUCER_METRICS_GROUP, "request-latency-max", new RoundingConverter("request-latency-max"))
        .add (PRODUCER_METRICS_GROUP, "requests-in-flight", new RoundingConverter("requests-in-flight"))
        .add (PRODUCER_METRICS_GROUP, "record-queue-time-avg", new RoundingConverter("record-queue-time-avg"))
        .add (PRODUCER_METRICS_GROUP, "record-queue-time-max", new RoundingConverter("record-queue-time-max"))
        .add (PRODUCER_METRICS_GROUP, "record-send-rate", new RoundingConverter("record-send-rate"))
        .add (PRODUCER_METRICS_GROUP, "records-per-request-avg", new RoundingConverter("records-per-request-avg"))
        .add (PRODUCER_METRICS_GROUP, "record-retry-total", new RoundingConverter("record-retry-total"))
        .add (PRODUCER_METRICS_GROUP, "batch-size-avg", new RoundingConverter("batch-size-avg"))
        .add (PRODUCER_METRICS_GROUP, "compression-rate-avg", new MultiplyConverter ("compression-rate-avg", 100.0))
        .add (PRODUCER_METRICS_GROUP, "outgoing-byte-rate", new RoundingConverter ("outgoing-byte-rate"))
        .add (PRODUCER_METRICS_GROUP, "bufferpool-wait-time-total", new RoundingConverter ("bufferpool-wait-time-total"))
        .add (PRODUCER_METRICS_GROUP, "io-wait-ratio", new MultiplyConverter ("io-wait-ratio", 100.0))
        .add (PRODUCER_TOPIC_METRICS_GROUP, "record-send-total", new RoundingConverter ("record-send-total"))
        .add (PRODUCER_TOPIC_METRICS_GROUP, "record-retry-total", new RoundingConverter ("record-retry-total"))
        .add (PRODUCER_TOPIC_METRICS_GROUP, "record-error-total", new RoundingConverter ("record-error-total"))
        .add (PRODUCER_TOPIC_METRICS_GROUP, "compression-rate", new MultiplyConverter ("compression-rate", 100.0))
        ;
    }


    public static MetricFilter getMetricsFilter() {
        return filter;
    }


    /**
     * Constructor must be public and argument-less as this class is instantiated by Kafka.
     */
    public ProducerMetricsReporter() {
        super();
    }

    
    /**
     * @return the filter
     */
    @Override
    public MetricFilter getFilter() {
        return filter;
    }
    
    /**
     * Creates a name for a operator custom metric. The name is topic + ":" + metricName.name() 
     * for topic metrics or only the kafka metric name for producer metrics.
     * 
     * @param metricName
     * @return The name of a the corresponding operator custom metric
     * @throws KafkaMetricException
     */
    @Override
    public String createCustomMetricName (MetricName metricName) throws KafkaMetricException {
        return createOperatorMetricName (metricName);
    }
    
    /**
     * Creates a name for a operator custom metric. The name is topic + ":" + metricName.name()
     * for topic metrics or only the kafka metric name for producer metrics.
     * 
     * @param metricName
     * @return The name of a the corresponding operator custom metric
     * @throws KafkaMetricException
     */
    public static String createOperatorMetricName (MetricName metricName) throws KafkaMetricException {
        final String group = metricName.group();
        String customMetricName = metricName.name();
        if (group.equals (PRODUCER_TOPIC_METRICS_GROUP)) {
            final Map <String, String> tags = metricName.tags();
            if (tags == null) throw new KafkaMetricException ("no tags for metric: " + group + " - " + customMetricName);
            final String topic = tags.get("topic");
            if (topic == null) throw new KafkaMetricException ("no tags 'topic' for metric: " + group + " - " + customMetricName);
            return topic + ":" + customMetricName;
        }
        else if (group.equals (PRODUCER_METRICS_GROUP)) {
            return customMetricName;
        }
        else throw new KafkaMetricException ("createCustomMetricName(): no implementation for metric group: " + group);
    }
}
