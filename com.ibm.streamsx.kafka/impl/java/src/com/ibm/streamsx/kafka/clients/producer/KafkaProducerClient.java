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
package com.ibm.streamsx.kafka.clients.producer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streamsx.kafka.KafkaMetricException;
import com.ibm.streamsx.kafka.MsgFormatter;
import com.ibm.streamsx.kafka.clients.AbstractKafkaClient;
import com.ibm.streamsx.kafka.clients.metrics.CustomMetricUpdateListener;
import com.ibm.streamsx.kafka.clients.metrics.MetricsFetcher;
import com.ibm.streamsx.kafka.clients.metrics.MetricsProvider;
import com.ibm.streamsx.kafka.clients.metrics.MetricsUpdatedListener;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public abstract class KafkaProducerClient extends AbstractKafkaClient {

    private static final Logger logger = Logger.getLogger(KafkaProducerClient.class);
    public static final int CLOSE_TIMEOUT_MS = 5000;

    protected KafkaProducer<?, ?> producer;
    protected KafkaOperatorProperties kafkaProperties;
    private Class<?> keyClass;
    private Class<?> valueClass;
    private MetricsFetcher metricsFetcher;
    protected final boolean guaranteeOrdering;

    private MetricsMonitor metricsMonitor = new MetricsMonitor();

    // members for queue time monitoring 
    private final static long METRICS_CHECK_INTERVAL = 50;
    private final static long TARGET_MAX_QUEUE_TIME_MS = 5000;
    private final static long INITIAL_Q_TIME_THRESHOLD_MS = TARGET_MAX_QUEUE_TIME_MS / 10l;
    private Object flushLock = new Object();
    private AtomicReference<MetricName> bufferAvailMName = new AtomicReference<>();
    private AtomicReference<MetricName> outGoingByteRateMName = new AtomicReference<>();
    private AtomicReference<MetricName> recordQueueTimeMaxMName = new AtomicReference<>();
    private int flushAfter = 0;
    private boolean closed = false;
    private long nRecords = 0l;
    private long bufferUseThreshold = -1;
    private double expSmoothedFlushDurationMs = 0.0;
    private final long bufferSize;
    private final long maxBufSizeThresh;
    private boolean compressionEnabled;
    private final com.ibm.streams.operator.metrics.Metric producerGenerationMetric;

    /** monitors operator metrics by logging them on update **/
    private class MetricsMonitor implements MetricsUpdatedListener {

        private long recordQueueTimeMax = 0l;
        private long outgoingByteRate = 0l;
        private long recordsPerRequestAvg = 0l;
        private long batchSizeAvg = 0l;
        private long bufferAvailBytes = 0l;
        private long recordQueueTimeAvg = 0l;
        private long bufferPoolWaitTimeTotalNanos = 0l;
        private long requestRate = 0l;

        private long lastTraceMs = System.currentTimeMillis();

        public void setRecordQueueTimeMax (long v) { this.recordQueueTimeMax = v; }
        public void setOutgoingByteRate (long v) { this.outgoingByteRate  = v; }
        public void setRecordsPerRequestAvg (long v) { this.recordsPerRequestAvg = v; }
        public void setBatchSizeAvg (long v) { this.batchSizeAvg = v; }
        public void setBufferAvailBytes (long v) { this.bufferAvailBytes = v; }
        public void setRecordQueueTimeAvg (long v) { this.recordQueueTimeAvg = v; }
        public void setBufferPoolWaitTimeTotalNanos (long v) { this.bufferPoolWaitTimeTotalNanos = v; }
        public void setRequestRate (long v) { this.requestRate = v; }

        /**
         * @see com.ibm.streamsx.kafka.clients.metrics.MetricsUpdatedListener#afterCustomMetricsUpdated()
         */
        @Override
        public void afterCustomMetricsUpdated() {
            final long now = System.currentTimeMillis();
            if (logger.isEnabledFor (DEBUG_LEVEL_METRICS)) {
                logger.log (DEBUG_LEVEL_METRICS, MsgFormatter.format ("QTimeMax= {0,number,#} ,QTimeAvg= {1,number,#} ,oByteRate= {2,number,#} ,reqRate= {3,number,#} ,recsPerReqAvg= {4,number,#} ,batchSzAvg= {5,number,#} ,bufAvail= {6,number,#} ,bufPoolWaitTimeTotalNanos= {7,number,#}",
                        recordQueueTimeMax,
                        recordQueueTimeAvg,
                        outgoingByteRate,
                        requestRate,
                        recordsPerRequestAvg,
                        batchSizeAvg,
                        bufferAvailBytes,
                        bufferPoolWaitTimeTotalNanos));
                lastTraceMs = now;
            }
            else {
                // trace every 10 minutes at INFO severity
                if (now - lastTraceMs >= 600_000L) {
                    logger.info (MsgFormatter.format ("QTimeMax= {0,number,#} ,QTimeAvg= {1,number,#} ,oByteRate= {2,number,#} ,reqRate= {3,number,#} ,recsPerReqAvg= {4,number,#} ,batchSzAvg= {5,number,#} ,bufAvail= {6,number,#} ,bufPoolWaitTimeTotalNanos= {7,number,#}",
                            recordQueueTimeMax,
                            recordQueueTimeAvg,
                            outgoingByteRate,
                            requestRate,
                            recordsPerRequestAvg,
                            batchSizeAvg,
                            bufferAvailBytes,
                            bufferPoolWaitTimeTotalNanos));
                    lastTraceMs = now;
                }
            }
        }

        @Override
        public void beforeCustomMetricsUpdated() { }
    }

    public <K, V> KafkaProducerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            boolean guaranteeRecordOrder,
            KafkaOperatorProperties kafkaProperties) throws Exception {
        super (operatorContext, kafkaProperties, false);
        this.kafkaProperties = kafkaProperties;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.guaranteeOrdering = guaranteeRecordOrder;

        configureProperties();
        bufferSize = this.kafkaProperties.getBufferMemory();
        maxBufSizeThresh = 100 * bufferSize / 90;
        compressionEnabled = !this.kafkaProperties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none").trim().equalsIgnoreCase("none");
        producerGenerationMetric = operatorContext.getMetrics().getCustomMetric("producerGeneration");
        createProducer();
    }

    /**
     * Returns true, when {@link #close(long)} has been called.
     * The true state can be returned before close() has finished.
     * @return true, when the producer client has been closed, false otherwise.
     */
    public final boolean isClosed() {
        return closed;
    }

    protected final synchronized void createProducer() {
        producer = new KafkaProducer<>(this.kafkaProperties);
        producerGenerationMetric.increment();
        if (metricsFetcher == null) {
            metricsFetcher = new MetricsFetcher (getOperatorContext(), new MetricsProvider() {
                @Override
                public Map<MetricName, ? extends Metric> getMetrics() {
                    synchronized (KafkaProducerClient.this) {
                        return producer.metrics();
                    }
                }
                @Override
                public String createCustomMetricName (MetricName metricName)  throws KafkaMetricException {
                    return ProducerMetricsReporter.createOperatorMetricName(metricName);
                }
            }, ProducerMetricsReporter.getMetricsFilter(), AbstractKafkaClient.METRICS_REPORT_INTERVAL);
            metricsFetcher.registerUpdateListener (this.metricsMonitor);

            metricsFetcher.registerUpdateListener("record-queue-time-max", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setRecordQueueTimeMax (value);
                    recordQueueTimeMaxMName.compareAndSet (null, kafkaMetricName);
                }
            });
            metricsFetcher.registerUpdateListener("outgoing-byte-rate", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setOutgoingByteRate (value);
                    outGoingByteRateMName.compareAndSet (null, kafkaMetricName);
                }
            });
            metricsFetcher.registerUpdateListener("records-per-request-avg", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setRecordsPerRequestAvg (value);
                }
            });
            metricsFetcher.registerUpdateListener("batch-size-avg", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setBatchSizeAvg (value);
                }
            });
            metricsFetcher.registerUpdateListener("buffer-available-bytes", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setBufferAvailBytes (value);
                    bufferAvailMName.compareAndSet (null, kafkaMetricName);
                }
            });
            metricsFetcher.registerUpdateListener ("record-queue-time-avg", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setRecordQueueTimeAvg (value);
                }
            });
            metricsFetcher.registerUpdateListener ("bufferpool-wait-time-total", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setBufferPoolWaitTimeTotalNanos (value);
                }
            });
            metricsFetcher.registerUpdateListener ("request-rate", new CustomMetricUpdateListener() {
                @Override
                public void customMetricUpdated (final String customMetricName, final MetricName kafkaMetricName, final long value) {
                    metricsMonitor.setRequestRate (value);
                }
            });
        }
    }

    /**
     * @param value the number of tuples, after which the producer is flushed. Values <= 0 have no effect.
     */
    public void setFlushAfter (int value) {
        if (value > 0) this.flushAfter = value;
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
        // linger.ms
        if (!kafkaProperties.containsKey (ProducerConfig.LINGER_MS_CONFIG)) {
            this.kafkaProperties.put (ProducerConfig.LINGER_MS_CONFIG, "100");
        }
        // max.in.flight.requests.per.connection
        // when record order is to be kept and retries are enabled, max.in.flight.requests.per.connection must be 1
        final long retries = kafkaProperties.containsKey (ProducerConfig.RETRIES_CONFIG)? Long.parseLong (this.kafkaProperties.getProperty (ProducerConfig.RETRIES_CONFIG).trim()): Integer.MAX_VALUE;
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
        this.kafkaProperties.put (ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "10000");
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
    /**
     * Sends a producer record with callback
     * @param record the producer record
     * @param cb     the callback for the send.
     * @return the Future
     * @throws Exception
     */
    protected Future<RecordMetadata> send (ProducerRecord record, Callback cb) throws Exception {
        synchronized (flushLock) {
            if (flushAfter > 0) {
                // non-adaptive flush 
                if (nRecords >= flushAfter) {
                    long before = 0l;
                    if (logger.isEnabledFor (DEBUG_LEVEL)) {
                        logger.log(DEBUG_LEVEL, "flushing the producer ...");
                        before = System.currentTimeMillis();
                    }
                    producer.flush();
                    if (logger.isEnabledFor (DEBUG_LEVEL)) {
                        final double weightHistory = 0.5;   // must be between 0 and 1 for exponential smoothing
                        final long dur = System.currentTimeMillis() - before;
                        expSmoothedFlushDurationMs = weightHistory * expSmoothedFlushDurationMs + (1.0 - weightHistory) * dur;
                        logger.log (DEBUG_LEVEL, MsgFormatter.format ("producer flush after {0,number,#} records took {1,number,#} ms; smoothed flushtime = {2,number,#.#}", nRecords, dur, expSmoothedFlushDurationMs));
                    }
                    nRecords = 0l;
                }
                ++nRecords;
            }
            else {
                // adaptive flush 
                if (++nRecords >= METRICS_CHECK_INTERVAL && 
                        bufferAvailMName.get() != null && 
                        outGoingByteRateMName.get() != null && 
                        recordQueueTimeMaxMName.get() != null &&
                        metricsFetcher.getCurrentValue (recordQueueTimeMaxMName.get()) > INITIAL_Q_TIME_THRESHOLD_MS) {

                    if (bufferUseThreshold == -1) {
                        // estimate initial threshold
                        long outGoingByteRate = metricsFetcher.getCurrentValue (outGoingByteRateMName.get());
                        // assume outgoing rate not below 5000 byte/s
                        if (outGoingByteRate < 5000) outGoingByteRate = 5000;
                        bufferUseThreshold = TARGET_MAX_QUEUE_TIME_MS * outGoingByteRate / 1000;
                        if (bufferUseThreshold > maxBufSizeThresh)
                            bufferUseThreshold = maxBufSizeThresh;
                        if (logger.isEnabledFor (DEBUG_LEVEL)) {
                            logger.log (DEBUG_LEVEL, MsgFormatter.format ("producer flush threshold initialized with {0,number,#}", bufferUseThreshold));
                        }
                    }
                    long bufferUsed = bufferSize - metricsFetcher.getCurrentValue (bufferAvailMName.get());
                    if (bufferUsed >= bufferUseThreshold) {
                        long before = System.currentTimeMillis();
                        producer.flush();
                        final long dur = System.currentTimeMillis() - before;
                        final double weightHistory = 0.5;   // must be between 0 and 1 for exponential smoothing
                        expSmoothedFlushDurationMs = weightHistory * expSmoothedFlushDurationMs + (1.0 - weightHistory) * dur;
                        if (logger.isEnabledFor (DEBUG_LEVEL)) {
                            logger.log (DEBUG_LEVEL, MsgFormatter.format ("producer flush after {0,number,#} records took {1,number,#} ms; smoothed flushtime = {2,number,#.#}", nRecords, dur, expSmoothedFlushDurationMs));
                        }
                        nRecords = 0;
                        // time spent for flush() is approximately the maximum queue time for the last appended record.
                        // difference of maximum queue time to flush time is used to adjust the threshold
                        double deltaTMillis = 1.0 * (double) TARGET_MAX_QUEUE_TIME_MS - expSmoothedFlushDurationMs * (compressionEnabled? 1.5: 1.0);
                        double outGoingByteRatePerSecond = metricsFetcher.getCurrentValue (outGoingByteRateMName.get());
                        final long oldThreshold = bufferUseThreshold;
                        // integrate only the half to avoid overshooting or instability of threshold
                        bufferUseThreshold += (long) (0.5 * deltaTMillis * outGoingByteRatePerSecond /1000.0);
                        // limit the threshold to [1024 ... 90 % of buffer size]
                        if (bufferUseThreshold > maxBufSizeThresh) 
                            bufferUseThreshold = maxBufSizeThresh;
                        else if (bufferUseThreshold < 1024)
                            bufferUseThreshold = 1024;
                        if (logger.isEnabledFor (DEBUG_LEVEL)) {
                            logger.log (DEBUG_LEVEL, MsgFormatter.format ("producer flush threshold adjusted from {0,number,#} to {1,number,#}", oldThreshold, bufferUseThreshold));
                        }
                    }
                }
            }
        }
        return producer.send (record, cb);
    }

    /**
     * Makes all buffered records immediately available to send and blocks until completion of the associated requests.
     * The post-condition is, that all Futures are in done state.
     * 
     * @throws InterruptedException. If flush is interrupted, an InterruptedException is thrown.
     */
    public void flush() {
        if (logger.isEnabledFor (DEBUG_LEVEL))
            logger.log (DEBUG_LEVEL, "Flushing ..."); //$NON-NLS-1$
        synchronized (flushLock) {
            producer.flush();
        }
    }

    /**
     * closes the KafkaProducer, so that it releases all resources and stops the metrics fetcher.
     * @param timeoutMillis
     */
    public void close (long timeoutMillis) {
        if (logger.isEnabledFor (DEBUG_LEVEL))
            logger.log (DEBUG_LEVEL, "Closing ..."); //$NON-NLS-1$
        closed = true;
        this.metricsFetcher.stop();
        producer.close (Duration.ofMillis (timeoutMillis));
    }


    /**
     * Processes a Producer record.
     * 
     * @param producerRecord  the producer record
     * @param associatedTuple a reference to the the associated Tuple from which the producer record was created.
     * @throws Exception
     */
    public abstract void processRecord (ProducerRecord<?, ?> producerRecord, Tuple associatedTuple) throws Exception;

    /**
     * processes multiple producer records associated with a single tuple.
     * @param records  the list of records
     * @param associatedTuple reference to the the associated Tuple from which the producer records were created.
     * @throws Exception
     */
    public abstract void processRecords (List<ProducerRecord<?, ?>> records, Tuple associatedTuple) throws Exception;

    /**
     * Tries to cancel all send requests that are not yet done. 
     * The base class has an empty implementation as it does not maintain the futures of send request.
     * @param mayInterruptIfRunning - true if the thread executing this task send request should be interrupted;
     *                              otherwise, in-progress tasks are allowed to complete
     */
    public abstract void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning);

    public abstract void drain() throws Exception; 

    public abstract void checkpoint (Checkpoint checkpoint) throws Exception;

    public abstract void reset (Checkpoint checkpoint) throws Exception;
}
