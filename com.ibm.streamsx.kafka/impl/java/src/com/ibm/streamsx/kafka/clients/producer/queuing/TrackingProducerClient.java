package com.ibm.streamsx.kafka.clients.producer.queuing;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This producer client queues and tracks incoming tuples, i.e. calls to {@link #send(org.apache.kafka.clients.producer.ProducerRecord, Tuple)},
 * until they are acknowledged via their associated callback. The {@link #processRecord(ProducerRecord, Tuple)} and {@link #processRecords(List, Tuple)} 
 * calls may block when the incoming queue exceeds a limit. On Exception received in the callback,
 * the queued producer record is retried with a new generation of a KafkaProducer instance.
 * 
 * @author IBM Kafka toolkit team
 * @since toolkit version 2.2
 */
public class TrackingProducerClient extends KafkaProducerClient implements ClientCallback {

    private static class RecoveryEvent {}
    private static final Logger trace = Logger.getLogger (TrackingProducerClient.class);
    // tuple processing is blocked when this number of tuple is pending
    private static final int MAX_NUM_PENDING_TUPLES = 5000;
    private static final long PRODUCER_RECOVERY_BACKOFF_MILLIS = 5000L;
    private static final int MAX_PRODUCER_GENERATIONS_FOR_SEND = 3;

    private int producerGeneration = 0;
    private AtomicBoolean recoveryInProgress = new AtomicBoolean (false);
    private Map <Long, TupleProcessing> pendingTuples = new HashMap<>();
    private Object pendingTuplesMonitor = new Object();
    private Object recoveryPendingMonitor = new Object();
    private BlockingQueue <RecoveryEvent> recoveryQueue = new LinkedBlockingQueue <RecoveryEvent>();
    private Thread recoveryThread;
    private Metric nPendingTuples;
    private Metric nFailedTuples;
    private Metric nQueueFullPause;
    private TupleProcessedHook tupleProcessedHook = null;

    /**
     * @param operatorContext
     * @param keyClass
     * @param valueClass
     * @param guaranteeRecordOrder
     * @param kafkaProperties
     * @throws Exception
     */
    public <K, V> TrackingProducerClient (OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            boolean guaranteeRecordOrder, KafkaOperatorProperties kafkaProperties) throws Exception {
        super (operatorContext, keyClass, valueClass, guaranteeRecordOrder, kafkaProperties);
        trace.info ("constructing " + getThisClassName());
        this.recoveryThread = operatorContext.getThreadFactory().newThread (new Runnable() {
            @Override
            public void run() {
                trace.info ("Recovery thread started.");
                try {
                    while (!recoveryThread.isInterrupted()) {
                        awaitRecoveryEventAndRecover();
                    }
                } catch (InterruptedException e) {
                    trace.info ("Recovery thread interrupted.");
                }
                finally {
                    trace.info ("Recovery thread finished.");
                }
            }
        });
        try {
            this.nPendingTuples = operatorContext.getMetrics().createCustomMetric ("nPendingTuples", "Number of tuples not yet produced", Metric.Kind.GAUGE);
        } catch (IllegalArgumentException metricExists) {
            trace.info ("custom metric exists: " + metricExists.getMessage());
        }
        try {
            this.nQueueFullPause = operatorContext.getMetrics().createCustomMetric ("nQueueFullPause", "Number times tuple processing was paused due to full tuple queue of pending tuples.", Metric.Kind.COUNTER);
        } catch (IllegalArgumentException metricExists) {
            trace.info ("custom metric exists: " + metricExists.getMessage());
        }
        try {
            this.nFailedTuples = operatorContext.getMetrics().createCustomMetric ("nFailedTuples", "Number of tuples that could not be fully produced", Metric.Kind.COUNTER);
        } catch (IllegalArgumentException metricExists) {
            trace.info ("custom metric exists: " + metricExists.getMessage());
        }
        this.recoveryThread.start();
    }


    /**
     * Sets a hook that is called whenever a tuple failed or got produced successfully, i.e. to all topics.
     * @param hook the Hook to set
     */
    public void setTupleProcessedHook (TupleProcessedHook hook) {
        this.tupleProcessedHook = hook;
    }


    /**
     * Called by the recovery thread.
     * @throws InterruptedException
     */
    private void awaitRecoveryEventAndRecover() throws InterruptedException {
        // wait until an event is available in the queue; throws InterruptedException
        @SuppressWarnings("unused")
        RecoveryEvent re = recoveryQueue.take();
        recoveryInProgress.set (true);
        synchronized (recoveryPendingMonitor) {
            do {
                // with the new producer generation, re-send all associated records of all tuples
                List <Long> pendingSeqNumbers;
                synchronized (pendingTuples) {
                    pendingSeqNumbers = new LinkedList<> (pendingTuples.keySet());
                    Collections.sort (pendingSeqNumbers);
                    for (Long tupleSeqNo: pendingSeqNumbers) {
                        TupleProcessing pt = pendingTuples.get (tupleSeqNo);
                        if (pt == null) continue;
                        pt.incrementProducerGenerationCancelTasks();
                    }
                }
                trace.info ("closing the producer ...");
                producer.close (Duration.ofMillis (0L));
                // We must not assume that all threads in the producer are terminated after close(0) returns,
                // so that from now on no callbacks are fired any more.
                trace.info ("sleeping the recovery backoff time (ms): " + PRODUCER_RECOVERY_BACKOFF_MILLIS);
                Thread.sleep (PRODUCER_RECOVERY_BACKOFF_MILLIS);
                recoveryQueue.clear();
                ++producerGeneration;
                trace.info ("producer generation incremented to " + producerGeneration);
                createProducer();
                // re-sent. Note, that it may happen that records and pending tuples get finished
                // also during recovery being in progress - or they get finished later at any time.
                trace.info("re-sending associated producer records of " + pendingSeqNumbers.size() + " tuples ...");
                for (Long tupleSeqNo: pendingSeqNumbers) {
                    TupleProcessing pt = null;
                    synchronized (pendingTuples) {
                        pt = pendingTuples.get (tupleSeqNo);
                        if (pt == null) continue;
                    }
                    int nFail = 0;
                    if (trace.isEnabledFor(DEBUG_LEVEL))
                        trace.log (DEBUG_LEVEL, "re-processing tuple # " + tupleSeqNo);
                    for (RecordProduceAttempt pr: pt.getPendingRecords()) {
                        if (trace.isEnabledFor(DEBUG_LEVEL))
                            trace.log (DEBUG_LEVEL, "re-sending record # " + pr.getProducerRecordSeqNumber() + " @tuple # " + tupleSeqNo + " for topic " + pr.getTopic());
                        try {
                            Future<RecordMetadata> future = send (pr.getRecord(), pr.getCallback());
                            pr.setFuture (future);
                        } catch (Exception e) {
                            ++nFail;
                            pt.addFailedTopic (pr.getTopic());
                            pt.setException (e);
                            trace.error ("record failed to send to topic '" + pr.getTopic() + "' during recovery: " + e.getMessage());
                        }
                    }
                    if (nFail > 0) {
                        // some responses for a tuple can occur asynchronous.
                        // We ignore them, especially the potentially failed topics if the responses include an exception.
                        nFailedTuples.increment();
                        synchronized (pendingTuples) {
                            pendingTuples.remove (pt.getSeqNumber());
                        }
                        if (this.tupleProcessedHook != null) {
                            this.tupleProcessedHook.onTupleFailed (pt.getTuple(), pt.getFailure());
                        }
                    }
                }
            } while (recoveryQueue.peek() != null);
            recoveryInProgress.set (false);
        }   // synchronized (recoveryInProgressMonitor)
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#processRecord(ProducerRecord, Tuple)
     */
    @Override
    public void processRecord (ProducerRecord<?, ?> producerRecord, Tuple associatedTuple) throws Exception {
        // association record to tuple is 1-to-1
        TupleProcessing pt = new TupleProcessing (associatedTuple, producerRecord, producerGeneration, MAX_PRODUCER_GENERATIONS_FOR_SEND, this);
        try {
            waitForPermitAndSendRecords (pt);
        } catch (InterruptedException ie) {
            trace.log (DEBUG_LEVEL, "processRecord() interrupted.");
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#processRecords(List, Tuple)
     */
    @Override
    public void processRecords (List<ProducerRecord<?, ?>> records, Tuple associatedTuple) throws Exception {
        // association record to tuple is N-to-1
        TupleProcessing pt = new TupleProcessing (associatedTuple, records, producerGeneration, MAX_PRODUCER_GENERATIONS_FOR_SEND, this);
        try {
            waitForPermitAndSendRecords (pt);
        } catch (InterruptedException ie) {
            trace.log (DEBUG_LEVEL, "processRecords() interrupted.");
        }
    }

    private void waitForPermitAndSendRecords (TupleProcessing pt) throws InterruptedException {
        final long tupleSeqNo = pt.getSeqNumber();
        if (trace.isEnabledFor(DEBUG_LEVEL))
            trace.log (DEBUG_LEVEL, "processing tuple # " + tupleSeqNo);
        synchronized (pendingTuplesMonitor) {
            int n = 0;
            while (pendingTuples.size() >= MAX_NUM_PENDING_TUPLES) {
                if (n++ == 0) nQueueFullPause.increment();
                pendingTuplesMonitor.wait (10000L);
            }
        }
        synchronized (recoveryPendingMonitor) {
            // add pending tuple to the "queue". Must synchronize as the removal is done in a producer's callback thread
            if (trace.isEnabledFor(DEBUG_LEVEL))
                trace.log (DEBUG_LEVEL, "queuing tuple # " + tupleSeqNo);

            synchronized (pendingTuples) {
                pendingTuples.put (tupleSeqNo, pt);
                nPendingTuples.setValue (pendingTuples.size());
            }
            if (trace.isEnabledFor(DEBUG_LEVEL))
                trace.log (DEBUG_LEVEL, "queued tuple # " + tupleSeqNo);
            for (RecordProduceAttempt pr: pt.getPendingRecords()) {
                if (trace.isEnabledFor(DEBUG_LEVEL))
                    trace.log (DEBUG_LEVEL, "sending record # " + pr.getProducerRecordSeqNumber() + " @tuple # " + tupleSeqNo);
                try {
                    Future <RecordMetadata> future = send (pr.getRecord(), pr.getCallback());
                    pr.setFuture (future);
                    if (trace.isEnabledFor(DEBUG_LEVEL))
                        trace.log (DEBUG_LEVEL, "sent: record # " + pr.getProducerRecordSeqNumber() + " @tuple # " + tupleSeqNo);
                }
                catch (Exception e) {
                    trace.warn ("Failed sending record " + pr.getProducerRecordSeqNumber() + ": " + e.getMessage());
                    initiateRecovery();
                }
            }
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.ClientCallback#tupleProcessed(long)
     */
    @Override
    public void tupleProcessed (long seqNumber) {
        if (trace.isEnabledFor(DEBUG_LEVEL))
            trace.log (DEBUG_LEVEL, "tuple # " + seqNumber + " processed - de-queueing");
        TupleProcessing tp = null;
        int nPendingTp;
        synchronized (pendingTuples) {
            tp = pendingTuples.remove (seqNumber);
            nPendingTp = pendingTuples.size();
        }
        nPendingTuples.setValue (nPendingTp);
        if (tp == null) {
            // tuple already de-queued
            return;
        }
        if (this.tupleProcessedHook != null) {
            this.tupleProcessedHook.onTupleProduced (tp.getTuple());
        }
        // notify tuple processing
        synchronized (pendingTuplesMonitor) {
            pendingTuplesMonitor.notifyAll();
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.ClientCallback#tupleFailedFinally(long, java.lang.Exception)
     */
    @Override
    public void tupleFailedFinally (long seqNumber, Set<String> failedTopics, Exception lastException) {
        if (trace.isEnabledFor(DEBUG_LEVEL))
            trace.log (DEBUG_LEVEL, "tuple # " + seqNumber + " failed - de-queueing");
        TupleProcessing tp = null;
        int nPendingTp;
        synchronized (pendingTuples) {
            tp = pendingTuples.remove (seqNumber);
            nPendingTp = pendingTuples.size();
        }
        nPendingTuples.setValue (nPendingTp);
        if (tp == null) {
            // tuple already de-queued
            return;
        }
        nFailedTuples.increment();
        if (this.tupleProcessedHook != null) {
            this.tupleProcessedHook.onTupleFailed (tp.getTuple(), tp.getFailure());
        }
        synchronized (pendingTuplesMonitor) {
            pendingTuplesMonitor.notifyAll();
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.ClientCallback#tupleFailedTemporarily(long, java.lang.Exception)
     */
    @Override
    public void tupleFailedTemporarily (long seqNumber, Exception exception) {
        initiateRecovery();
    }

    private void initiateRecovery() {
        trace.info ("sending producer recovery event.");
        recoveryQueue.offer (new RecoveryEvent());
    }
}
