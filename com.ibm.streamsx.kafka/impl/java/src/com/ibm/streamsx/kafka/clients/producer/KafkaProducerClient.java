package com.ibm.streamsx.kafka.clients.producer;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaMetricException;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.clients.metrics.MetricsFetcher;
import com.ibm.streamsx.kafka.clients.metrics.MetricsProvider;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public class KafkaProducerClient extends AbstractKafkaClient {

    private static final Logger logger = Logger.getLogger(KafkaProducerClient.class);
    private static final int CLOSE_TIMEOUT = 5;
    private static final TimeUnit CLOSE_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;

    protected KafkaProducer<?, ?> producer;
    protected ProducerCallback callback;
    protected Exception sendException;
    protected KafkaOperatorProperties kafkaProperties;
    protected Class<?> keyClass;
    protected Class<?> valueClass;
    protected OperatorContext operatorContext;
    private MetricsFetcher metricsFetcher;
    protected final boolean guaranteeOrdering;

    public <K, V> KafkaProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            boolean guaranteeRecordOrder,
            KafkaOperatorProperties kafkaProperties) throws Exception {
        super (operatorContext, kafkaProperties, false);
        this.kafkaProperties = kafkaProperties;
        this.operatorContext = operatorContext;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.guaranteeOrdering = guaranteeRecordOrder;

        configureProperties();
        createProducer();
    }

    protected void createProducer() {
        producer = new KafkaProducer<>(this.kafkaProperties);
        callback = new ProducerCallback(this);
        if (metricsFetcher == null) {
            metricsFetcher = new MetricsFetcher (getOperatorContext(), new MetricsProvider() {
                @Override
                public Map<MetricName, ? extends Metric> getMetrics() {
                    return producer.metrics();
                }

                @Override
                public String createCustomMetricName (MetricName metricName)  throws KafkaMetricException {
                    return ProducerMetricsReporter.createOperatorMetricName(metricName);
                }
            }, ProducerMetricsReporter.getMetricsFilter(), METRICS_REPORT_INTERVAL);
        }
    }

    protected void configureProperties() throws Exception {
        if (!this.kafkaProperties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            if (keyClass != null) {
                this.kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializer(keyClass));	
            } else {
                // Kafka requires a key serializer to be specified, even if no
                // key is going to be used. Setting the StringSerializer class.  
                this.kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializer(String.class));
            }
        }
        // acks influences data integrity and has a good default value: '1' - no change
        if (kafkaProperties.containsKey (ProducerConfig.ACKS_CONFIG)) {
            final String acks = kafkaProperties.getProperty (ProducerConfig.ACKS_CONFIG);
            if (acks.equals ("0")) {
                logger.warn ("Producer property " + ProducerConfig.ACKS_CONFIG + " is set to '0'. "
                        + "This value is not recommended at all. If set to zero then the producer "
                        + "will not wait for any acknowledgment from the server at all. "
                        + "The record will be immediately added to the socket buffer and considered sent. "
                        + "No guarantee can be made that the server has received the record in this case, "
                        + "and the retries configuration will not take effect (as the client won't "
                        + "generally know of any failures).");
            }
        }
        // compression.type
        if (!kafkaProperties.containsKey (ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
            this.kafkaProperties.put (ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        }
        // linger.ms
        if (!kafkaProperties.containsKey (ProducerConfig.LINGER_MS_CONFIG)) {
            this.kafkaProperties.put (ProducerConfig.LINGER_MS_CONFIG, "100");
        }
        // retries
        if (!kafkaProperties.containsKey (ProducerConfig.RETRIES_CONFIG)) {
            this.kafkaProperties.put (ProducerConfig.RETRIES_CONFIG, "10");
        }
        // max.in.flight.requests.per.connection
        // when record order is to be kept and retries are enabled, max.in.flight.requests.per.connection must be 1
        final long retries = Long.parseLong (this.kafkaProperties.getProperty (ProducerConfig.RETRIES_CONFIG).trim());
        final String maxInFlightRequestsPerConWhenUnset = guaranteeOrdering && retries > 0l? "1": "10";
        if (!kafkaProperties.containsKey (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            this.kafkaProperties.put (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConWhenUnset);
        }
        else {
            final long maxInFlightRequests = Long.parseLong (this.kafkaProperties.getProperty (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).trim());
            if (guaranteeOrdering && maxInFlightRequests > 1l && retries > 0l) {
                this.kafkaProperties.put (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
                logger.warn("producer config '" + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "' has been turned to '1' for guaranteed retention of record order per topic partition.");
            }
        }
        // batch.size
        if (!kafkaProperties.containsKey (ProducerConfig.BATCH_SIZE_CONFIG)) {
            this.kafkaProperties.put (ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        }

        // add our metric reporter
        if (kafkaProperties.containsKey (ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG)) {
            String propVal = kafkaProperties.getProperty (ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG);
            this.kafkaProperties.put (ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, 
                    propVal + "," + ProducerMetricsReporter.class.getCanonicalName());
        }
        else {
            this.kafkaProperties.put (ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, ProducerMetricsReporter.class.getCanonicalName());
        }

        if (!kafkaProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            this.kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializer(valueClass));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<RecordMetadata> send(ProducerRecord record) throws Exception {
        if (sendException != null) {
            logger.error(Messages.getString("PREVIOUS_BATCH_FAILED_TO_SEND", sendException.getLocalizedMessage()), //$NON-NLS-1$
                    sendException);
            throw sendException;
        }
        return producer.send(record, callback);
    }

    /**
     * Makes all buffered records immediately available to send and blocks until completion of the associated requests.
     * The post-conditioin is, that all Futures are in done state.
     * 
     * @throws InterruptedException. If flush is interrupted, an InterruptedException is thrown.
     */
    public synchronized void flush() {
        logger.trace("Flusing..."); //$NON-NLS-1$
        producer.flush();
    }

    public void close() {
        logger.trace("Closing..."); //$NON-NLS-1$
        producer.close(CLOSE_TIMEOUT, CLOSE_TIMEOUT_TIMEUNIT);
    }

    public void setSendException(Exception sendException) {
        this.sendException = sendException;
    }

    @SuppressWarnings("rawtypes")
    public boolean processTuple(ProducerRecord producerRecord) throws Exception {
        send(producerRecord);
        return true;
    }

    /**
     * Tries to cancel all send requests that are not yet done. 
     * The base class has an empty implementation as it does not maintain the futures of send request.
     * @param mayInterruptIfRunning - true if the thread executing this task send request should be interrupted;
     *                              otherwise, in-progress tasks are allowed to complete
     */
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }

    public void drain() throws Exception {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }

    public void checkpoint(Checkpoint checkpoint) throws Exception {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }

    public void reset(Checkpoint checkpoint) throws Exception {
        // no implementation because this class is instantiated only when operator is not in a Consistent Region
    }
}
