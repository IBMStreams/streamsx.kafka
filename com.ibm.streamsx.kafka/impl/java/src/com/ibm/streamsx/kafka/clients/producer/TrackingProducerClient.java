package com.ibm.streamsx.kafka.clients.producer;

import java.io.IOException;
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
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This producer client queues and tracks incoming tuples, i.e. calls to {@link #send(org.apache.kafka.clients.producer.ProducerRecord, Tuple)},
 * until they are acknowledged via their associated callback. The {@link #processRecord(ProducerRecord, Tuple)} and {@link #processRecords(List, Tuple)} 
 * calls may block when the incoming queue exceeds a limit. On Exception received in the callback,
 * the queued producer record is retried with a new generation of a KafkaProducer instance when not in consistent region. When in
 * consistent region, the region is reset.
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
    private int maxPendingTuples = MAX_NUM_PENDING_TUPLES;
    private int maxProducerGenerations = MAX_PRODUCER_GENERATIONS_FOR_SEND;
    private AtomicBoolean recoveryInProgress = new AtomicBoolean (false);
    private Map <Long, TupleProcessing> pendingTuples = new HashMap<>();
    private Object pendingTuplesMonitor = new Object();
    private Object recoveryPendingMonitor = new Object();
    private BlockingQueue <RecoveryEvent> recoveryQueue = new LinkedBlockingQueue <RecoveryEvent>();
    private Thread recoveryThread;
    private Metric nPendingTuples;
    private Metric nFailedTuples;
    private Metric nQueueFullPause;
    private final ConsistentRegionContext crContext;
    private AtomicBoolean resetInitiatedOnce = new AtomicBoolean (false);
    private final boolean inConsistentRegion;
    private TupleProcessedHook tupleProcessedHook = null;
    private final ErrorCategorizer errorCategorizer;

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
        this.crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        this.inConsistentRegion = this.crContext != null;
        if (inConsistentRegion) {
            // we are in consistent region mode
            // when in consistent region, ALL exceptions are recovered by resetting the region when there is no error port.
            // when there is an error output port, the retriable exceptions lead to reset of the region.
            // The non-retriable exceptions are reported o the error output port
            if (operatorContext.getNumberOfStreamingOutputs() == 0) {
                this.errorCategorizer = new RecoverAllErrors();
            } else {
                this.errorCategorizer = new RecoverRetriable();
            }
        }
        else {
            // when not in CR, the producer is re-created (client recovered) on retriable exceptions only.
            // non-retriable Kafka exceptions are treated as final failure - they get reported to
            // error output port immediately when present
            this.errorCategorizer = new RecoverRetriable();
            this.recoveryThread = operatorContext.getThreadFactory().newThread (new Runnable() {
                @Override
                public void run() {
                    trace.info ("Recovery thread started.");
                    try {
                        while (!(isClosed() || recoveryThread.isInterrupted())) {
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
            this.recoveryThread.start();
        }
        this.nPendingTuples = operatorContext.getMetrics().getCustomMetric ("nPendingTuples");
        this.nQueueFullPause = operatorContext.getMetrics().getCustomMetric ("nQueueFullPause");
        this.nFailedTuples = operatorContext.getMetrics().getCustomMetric ("nFailedTuples");
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#close(long)
     */
    @Override
    public void close(long timeoutMillis) {
        super.close (timeoutMillis);
        if (recoveryThread != null) recoveryThread.interrupt();
    }


    /**
     * Returns the number of allowed pending tuples.
     * If not explicitly set, the value is {@value #MAX_NUM_PENDING_TUPLES}. 
     * @return the maxPendingTuples
     * @see #setMaxPendingTuples(int)
     */
    public int getMaxPendingTuples() {
        return maxPendingTuples;
    }


    /**
     * Sets the maximum number of pending tuples before {@link #processRecord(ProducerRecord, Tuple)} 
     * or {@link #processRecords(List, Tuple)} blocks.
     * @param n the maximum number of pending tuples to set.
     * If not explicitly set, the value is {@value #MAX_NUM_PENDING_TUPLES}. 
     * @see #getMaxPendingTuples()
     */
    public void setMaxPendingTuples (int n) {
        this.maxPendingTuples = n;
    }


    /**
     * @return the maxProducerGenerations
     * @see #setMaxProducerGenerations(int)
     */
    public int getMaxProducerGenerations() {
        return maxProducerGenerations;
    }


    /**
     * Sets the maximum number of producer generations for a producer record.
     * When set to one, each record is sent with only one producer generation, and not re-sent
     * with a recovered producer. 
     * @param n the maxProducerGenerations to set, values < 1 are bound to 1
     * If not explicitly set, the value is {@value #MAX_PRODUCER_GENERATIONS_FOR_SEND}.
     * @see #getMaxProducerGenerations() 
     */
    public void setMaxProducerGenerations (int n) {
        this.maxProducerGenerations = n < 1? 1: n;
    }


    /**
     * Sets a hook that is called whenever a tuple failed or got produced successfully, i.e. produced to all topics.
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
        if (resetInitiatedOnce.get()) {
            // if reset has been initiated by this operator,
            // we know that the producer client implementation is replaced by a new instance.
            // This instance does not process any tuples anymore.
            return;
        }
        TupleProcessing pt = new TupleProcessing (associatedTuple, producerRecord, producerGeneration, maxProducerGenerations, this, this.errorCategorizer);
        try {
            waitForPermissionAndSendRecords (pt);
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
        if (resetInitiatedOnce.get()) {
            // if reset has been initiated by this operator,
            // we know that the producer client implementation is replaced by a new instance.
            // This instance does not process any tuples anymore.
            return;
        }
        TupleProcessing pt = new TupleProcessing (associatedTuple, records, producerGeneration, maxProducerGenerations, this, this.errorCategorizer);
        try {
            waitForPermissionAndSendRecords (pt);
        } catch (InterruptedException ie) {
            trace.log (DEBUG_LEVEL, "processRecords() interrupted.");
        }
    }

    private void waitForPermissionAndSendRecords (TupleProcessing pt) throws InterruptedException {
        final long tupleSeqNo = pt.getSeqNumber();
        if (trace.isTraceEnabled())
            trace.trace ("processing tuple # " + tupleSeqNo);
        synchronized (pendingTuplesMonitor) {
            int n = 0;
            while (pendingTuples.size() >= maxPendingTuples) {
                if (n++ == 0) nQueueFullPause.increment();
                pendingTuplesMonitor.wait (10000L);
            }
        }
        synchronized (recoveryPendingMonitor) {
            // add pending tuple to the "queue". Must synchronize as the removal is done in a producer's callback thread
            if (trace.isTraceEnabled())
                trace.trace ("queuing tuple # " + tupleSeqNo);

            synchronized (pendingTuples) {
                pendingTuples.put (tupleSeqNo, pt);
                nPendingTuples.setValue (pendingTuples.size());
            }
            if (trace.isTraceEnabled())
                trace.trace ("queued tuple # " + tupleSeqNo);
            for (RecordProduceAttempt pr: pt.getPendingRecords()) {
                if (trace.isTraceEnabled())
                    trace.trace ("sending record # " + pr.getProducerRecordSeqNumber() + " @tuple # " + tupleSeqNo);
                try {
                    Future <RecordMetadata> future = send (pr.getRecord(), pr.getCallback());
                    pr.setFuture (future);
                    if (trace.isTraceEnabled())
                        trace.trace ("sent: record # " + pr.getProducerRecordSeqNumber() + " @tuple # " + tupleSeqNo);
                }
                catch (Exception e) {
                    trace.warn ("Failed sending record " + pr.getProducerRecordSeqNumber() + ": " + e.getMessage());
                    initiateRecovery();
                }
            }
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.ClientCallback#tupleProcessed(long)
     */
    @Override
    public void tupleProcessed (long seqNumber) {
        if (trace.isTraceEnabled())
            trace.trace ("tuple # " + seqNumber + " processed - de-queueing");
        TupleProcessing tp = null;
        int nPendingTp;
        synchronized (pendingTuples) {
            tp = pendingTuples.remove (seqNumber);
            nPendingTp = pendingTuples.size();
            if (this.tupleProcessedHook != null && tp != null) {
                this.tupleProcessedHook.onTupleProduced (tp.getTuple());
            }
        }
        nPendingTuples.setValue (nPendingTp);
        // notify tuple processing
        synchronized (pendingTuplesMonitor) {
            pendingTuplesMonitor.notifyAll();
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.ClientCallback#tupleFailedFinally(long, Set, Exception, boolean)
     */
    @Override
    public void tupleFailedFinally (long seqNumber, Set<String> failedTopics, Exception lastException, boolean initiateRecovery) {
        if (trace.isDebugEnabled())
            trace.debug ("tuple # " + seqNumber + " failed - de-queueing");
        TupleProcessing tp = null;
        int nPendingTp;
        synchronized (pendingTuples) {
            tp = pendingTuples.remove (seqNumber);
            nPendingTp = pendingTuples.size();
            if (tp != null) {
                if (this.tupleProcessedHook != null) {
                    this.tupleProcessedHook.onTupleFailed (tp.getTuple(), tp.getFailure());
                }
                nFailedTuples.increment();
            }
        }
        nPendingTuples.setValue (nPendingTp);
        synchronized (pendingTuplesMonitor) {
            pendingTuplesMonitor.notifyAll();
        }
        if (initiateRecovery) {
            initiateRecovery();
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.ClientCallback#tupleFailedTemporarily(long, java.lang.Exception)
     */
    @Override
    public void tupleFailedTemporarily (long seqNumber, Exception exception) {
        initiateRecovery();
    }

    /**
     * Tries to cancel all send requests that are not yet done.
     * 
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#tryCancelOutstandingSendRequests(boolean)
     */
    @Override
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        synchronized (pendingTuples) {
            pendingTuples.forEach ((seqNo, tp) -> {
                tp.getPendingRecords().forEach (pr -> {
                    Future<?> future = pr.getFuture();
                    if (future != null && !future.isDone()) {
                        future.cancel (mayInterruptIfRunning);
                    }
                });
            });
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#drain()
     */
    @Override
    public void drain() throws Exception {
        if (trace.isEnabledFor (DEBUG_LEVEL)) trace.log (DEBUG_LEVEL, getThisClassName() + " -- DRAIN"); //$NON-NLS-1$
        flush();
        // wait that pendingTuples map gets empty ...
        synchronized (pendingTuplesMonitor) {
            while (true) {
                // check while condition in synchronized manner:
                int sz;
                synchronized (pendingTuples) {
                    sz = pendingTuples.size();
                    if (sz == 0) break;
                }
                trace.info ("waiting to get all pending tuples processed; #tuples = " + sz);
                pendingTuplesMonitor.wait (10000L);
            }
            trace.debug ("all pending tuples processed");
        }
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#checkpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        if (trace.isEnabledFor (DEBUG_LEVEL)) trace.log (DEBUG_LEVEL, getThisClassName() + " -- CHECKPOINT id=" + checkpoint.getSequenceId()); //$NON-NLS-1$
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.producer.KafkaProducerClient#reset(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void reset (Checkpoint checkpoint) throws Exception {
        if (trace.isEnabledFor (DEBUG_LEVEL)) {
            trace.log (DEBUG_LEVEL, getThisClassName() + " -- RESET id=" + (checkpoint == null? -1L: checkpoint.getSequenceId())); //$NON-NLS-1$
        }
    }


    private void initiateConsistentRegionResetOnce() {
        if (!resetInitiatedOnce.getAndSet (true)) {
            tryCancelOutstandingSendRequests (/*mayInterruptIfRunning = */true);
            pendingTuples.forEach((seqNo, tp) -> {
                tp.getPendingRecords().clear();
            });
            pendingTuples.clear();
            synchronized (pendingTuplesMonitor) {
                pendingTuplesMonitor.notifyAll();
            }
            nPendingTuples.setValue (0L);
            try {
                crContext.reset();
            }
            catch (IOException ioe) {
                producer.close (Duration.ofMillis (0L));
                // stop the PE, the runtime may re-launch it
                System.exit (1);
            }
        }
    }


    /**
     * When in CR, triggers reset of the consistent region, when in autonomous region,
     * adds an event to the internal recovery queue.
     */
    private void initiateRecovery() {
        if (inConsistentRegion) {
            initiateConsistentRegionResetOnce();
        } else {
            trace.info ("sending producer recovery event.");
            recoveryQueue.offer (new RecoveryEvent());
        }
    }
}
