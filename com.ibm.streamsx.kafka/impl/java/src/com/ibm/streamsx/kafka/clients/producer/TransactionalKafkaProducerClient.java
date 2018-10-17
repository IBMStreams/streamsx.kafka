package com.ibm.streamsx.kafka.clients.producer;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

public class TransactionalKafkaProducerClient extends KafkaProducerClient {

    // default value of server config transaction.max.timeout.ms
    private static final long TRANSACTION_MAX_TIMEOUT_MS = 900000l;

    private static final Logger logger = Logger.getLogger(TransactionalKafkaProducerClient.class);

    private List<Future<RecordMetadata>> futuresList;
    private String transactionalId;
    private final boolean lazyTransactionBegin;
    private AtomicBoolean transactionInProgress = new AtomicBoolean (false);

    public <K, V> TransactionalKafkaProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            KafkaOperatorProperties kafkaProperties, boolean lazyTransactionBegin) throws Exception {
        super(operatorContext, keyClass, valueClass, kafkaProperties);
        logger.debug("ExaxtlyOnceKafkaProducerClient starting...");
        this.lazyTransactionBegin = lazyTransactionBegin;
        // If this variable has not been set before, then set it to the current end offset.
        // Otherwise, this variable will be overridden with the value is retrieved
        this.futuresList = Collections.synchronizedList(new ArrayList<Future<RecordMetadata>>());
        initTransactions();
        if (!lazyTransactionBegin) {
            // begin a new transaction before the operator starts processing tuples
            checkAndBeginTransaction();
        }
    }

    @Override
    protected void configureProperties() throws Exception {
        super.configureProperties();

        // Need to generate a transactional.id that is unique but persists 
        // across operator instances. In order to guarantee this, we will
        // store the transactional.id in the JCP
        ControlPlaneContext jcpContext = operatorContext.getOptionalContext(ControlPlaneContext.class);
        ControlVariableAccessor<String> transactionalIdCV = jcpContext.createStringControlVariable("transactional_id", false, getRandomId("tid-"));
        transactionalId = transactionalIdCV.sync().getValue();
        logger.debug("Transactional ID = " + transactionalId);

        // adjust transaction timeout transaction.timeout.ms
        ConsistentRegionContext crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        long drainTimeoutMillis = (long) (crContext.getDrainTimeout() * 1000.0);
        long minTransactionTimeout = drainTimeoutMillis + 120000l;
        if (minTransactionTimeout > TRANSACTION_MAX_TIMEOUT_MS) minTransactionTimeout = TRANSACTION_MAX_TIMEOUT_MS;
        if (kafkaProperties.containsKey (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
            long propValue = Long.valueOf (kafkaProperties.getProperty (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG));
            if (propValue < minTransactionTimeout) {
                this.kafkaProperties.put (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "" + minTransactionTimeout);
                logger.warn (MessageFormat.format ("producer config ''{0}'' has been increased from {1} to {2}.",
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, propValue, minTransactionTimeout));
            }
        }
        else {
            this.kafkaProperties.put (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "" + minTransactionTimeout);
            logger.info (MessageFormat.format ("producer config ''{0}'' has been set to {1}.", ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, minTransactionTimeout));
        }

        // The "enable.idempotence" property is required in order to guarantee idempotence
        this.kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // The "transactional.id" property is mandatory in order to support transactions.
        this.kafkaProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    private void initTransactions() {
        // Initialize the transactions. Previously uncommitted 
        // transactions will be aborted. 
        logger.debug("Initializating transactions...");
        producer.initTransactions();
        logger.debug("Transaction initialization finished.");
    }

    private void beginTransaction() {
        if (logger.isDebugEnabled()) logger.debug("Starting new transaction");
        producer.beginTransaction();
    }

    private void abortTransaction() {
        if (logger.isDebugEnabled()) logger.debug("Aborting transaction");
        producer.abortTransaction();
    }


    /**
     * Begins a transaction if no transaction is already in progress.
     * Uses member variable `transactionInProgress`.
     */
    private void checkAndBeginTransaction() {
        if (transactionInProgress.get()) return;
        synchronized (this) {
            if (!transactionInProgress.get()) {
                beginTransaction();
                transactionInProgress.set (true);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Future<RecordMetadata> send (ProducerRecord record) throws Exception {
        Future<RecordMetadata> future = super.send(record);
        futuresList.add(future);
        return future;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public boolean processTuple(ProducerRecord producerRecord) throws Exception {
        // send always within a transaction
        checkAndBeginTransaction();
        this.send(producerRecord);
        return true;
    }


    /**
     * Makes all buffered records immediately available to send and blocks until completion of the associated requests.
     * 
     * @throws InterruptedException. If flush is interrupted, an InterruptedException is thrown.
     */
    @Override
    public synchronized void flush() {
        super.flush();
        // post-condition is, that all futures are in done state.
        // No need to wait by calling future.get() on all futures in futuresList
        futuresList.clear();
    }

    @Override
    public void drain() throws Exception {
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- DRAIN");
        flush();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws Exception {
        final long currentSequenceId = checkpoint.getSequenceId();
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- CHECKPOINT id=" + currentSequenceId);

        // check 'transactionInProgress' for true and set atomically to false
        if (transactionInProgress.compareAndSet (true, false)) {
            logger.debug ("Committing transaction...");
            producer.commitTransaction();
        }
        else {
            logger.debug ("No transaction in progress. Nothing to commit.");
        }
        assert (transactionInProgress.get() == false);
        if (!lazyTransactionBegin) {
            checkAndBeginTransaction();
        }
    }

    /**
     * Tries to cancel all send requests that are not yet done.
     */
    @Override
    public void tryCancelOutstandingSendRequests (boolean mayInterruptIfRunning) {
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- trying to cancel requests");
        int nCancelled = 0;
        for (Future<RecordMetadata> future : futuresList) {
            if (!future.isDone() && future.cancel (mayInterruptIfRunning)) ++nCancelled;
        }
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- number of cancelled send requests: " + nCancelled); //$NON-NLS-1$
        futuresList.clear();
    }

    @Override
    public void reset (Checkpoint checkpoint) throws Exception {
        if (logger.isDebugEnabled()) logger.debug("TransactionalKafkaProducerClient -- RESET id=" + checkpoint.getSequenceId());

        // check 'transactionInProgress' for true and set atomically to false
        if (transactionInProgress.compareAndSet (true, false)) {
            // abort the current transaction
            abortTransaction();
        }
        assert (transactionInProgress.get() == false);
        if (!lazyTransactionBegin) {
            checkAndBeginTransaction();
        }
        setSendException(null);
    }
}
