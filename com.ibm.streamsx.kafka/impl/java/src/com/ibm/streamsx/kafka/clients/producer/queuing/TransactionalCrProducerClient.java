/**
 * 
 */
package com.ibm.streamsx.kafka.clients.producer.queuing;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * Kafka producer client for consistent region, which uses a Kafka transaction for every consistent interval.
 * 
 * @author IBM Kafka toolkit team
 * @since toolkit version 2.2
 */
public class TransactionalCrProducerClient extends TrackingProducerClient {
    // default value of server config transaction.max.timeout.ms
    private static final long TRANSACTION_MAX_TIMEOUT_MS = 900000l;
    private static final Logger trace = Logger.getLogger(TransactionalCrProducerClient.class);

    private String transactionalId;
    private AtomicBoolean transactionInProgress = new AtomicBoolean (false);

    /**
     * @param operatorContext
     * @param keyClass
     * @param valueClass
     * @param guaranteeRecordOrder
     * @param kafkaProperties
     * @throws Exception
     */
    public <K, V> TransactionalCrProducerClient(OperatorContext operatorContext, Class<K> keyClass, Class<V> valueClass,
            boolean guaranteeRecordOrder, KafkaOperatorProperties kafkaProperties) throws Exception {
        super(operatorContext, keyClass, valueClass, guaranteeRecordOrder, kafkaProperties);
        initTransactions();
    }

    /**
     * 
     */
    @Override
    protected void configureProperties() throws Exception {
        super.configureProperties();

        // Need to generate a transactional.id that is unique but persists 
        // across operator re-launches. In order to guarantee this, we will
        // store the transactional.id in the JCP
        ControlPlaneContext jcpContext = operatorContext.getOptionalContext(ControlPlaneContext.class);
        ControlVariableAccessor<String> transactionalIdCV = jcpContext.createStringControlVariable ("transactional_id", false, getRandomId("tid-"));
        transactionalId = transactionalIdCV.sync().getValue();
        trace.debug ("Transactional ID = " + transactionalId);

        // adjust transaction timeout transaction.timeout.ms
        ConsistentRegionContext crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        long drainTimeoutMillis = (long) (crContext.getDrainTimeout() * 1000.0);
        long minTransactionTimeout = drainTimeoutMillis + 120000l;
        if (minTransactionTimeout > TRANSACTION_MAX_TIMEOUT_MS) minTransactionTimeout = TRANSACTION_MAX_TIMEOUT_MS;
        if (kafkaProperties.containsKey (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
            long propValue = Long.valueOf (kafkaProperties.getProperty (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG));
            if (propValue < minTransactionTimeout) {
                this.kafkaProperties.setProperty (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "" + minTransactionTimeout);
                trace.warn (MessageFormat.format ("producer config ''{0}'' has been increased from {1} to {2}.",
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, propValue, minTransactionTimeout));
            }
        }
        else {
            this.kafkaProperties.setProperty (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "" + minTransactionTimeout);
        }

        // The "enable.idempotence" property is required in order to guarantee idempotence
        this.kafkaProperties.setProperty (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // --- begin adjustment for enable.idempotence = true
        // Note that enabling idempotence requires max.in.flight.requests.per.connection 
        // to be less than or equal to 5, retries to be greater than 0 and acks must be 'all'.
        if (kafkaProperties.containsKey (ProducerConfig.ACKS_CONFIG)) {
            final String acks =  kafkaProperties.getProperty (ProducerConfig.ACKS_CONFIG);
            if (!(acks.equals("all") || acks.equals("-1"))) {
                trace.warn (MessageFormat.format ("producer config ''{0}'' has been changed from {1} to {2} for enable.idempotence=true.",
                        ProducerConfig.ACKS_CONFIG, acks, "all"));
                this.kafkaProperties.setProperty (ProducerConfig.ACKS_CONFIG, "all");
            }
        }
        else this.kafkaProperties.setProperty (ProducerConfig.ACKS_CONFIG, "all");
        if (kafkaProperties.containsKey (ProducerConfig.RETRIES_CONFIG)) {
            final long retries =  Long.parseLong (kafkaProperties.getProperty (ProducerConfig.RETRIES_CONFIG).trim());
            if (retries < 1l) {
                trace.warn (MessageFormat.format ("producer config ''{0}'' has been changed from {1} to {2} for enable.idempotence=true.",
                        ProducerConfig.RETRIES_CONFIG, retries, "1"));
                this.kafkaProperties.setProperty (ProducerConfig.RETRIES_CONFIG, "1");
            }
        }
        // we have enabled retries for idempotence.
        // This requires max.in.flight.requests.per.connection = 1 when guaranteeOrdering is true.
        if (kafkaProperties.containsKey (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            final long maxInFlightRequests = Long.parseLong (kafkaProperties.getProperty (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).trim());
            if (guaranteeOrdering && maxInFlightRequests > 1) {
                // we ensured that retries is > 0 for idempotence.
                // max.in.flight.requests.per.connection must be 1 to guarantee record sequence
                final String val = "1";
                trace.warn (MessageFormat.format ("producer config ''{0}'' has been reduced from {1} to {2} for for guaranteed retention of record order per topic partition when retries > 0.",
                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, val));
                this.kafkaProperties.setProperty (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, val);
            } else if (maxInFlightRequests > 5l) {
                final String val = "5";
                trace.warn (MessageFormat.format ("producer config ''{0}'' has been reduced from {1} to {2} for enable.idempotence=true.",
                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests, val));
                this.kafkaProperties.setProperty (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, val);
            }
        }
        else {
            // property not set:
            if (guaranteeOrdering) {
                // we ensured that retries is > 0 for idempotence.
                // max.in.flight.requests.per.connection must be 1 to guarantee record sequence
                this.kafkaProperties.setProperty (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
            }
        }
        // --- end adjustment for enable.idempotence = true
        // The "transactional.id" property is mandatory in order to support transactions.
        this.kafkaProperties.setProperty (ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }

    /**
     * Returns the transactional ID used in the Kafka producer
     * @return
     */
    public String getTransactionalId() {
        return transactionalId;
    }

    private void initTransactions() {
        // Initialize the transactions. Previously uncommitted 
        // transactions will be aborted. 
        trace.debug ("Initializating transactions...");
        producer.initTransactions();
        trace.debug ("Transaction initialization finished.");
    }

    private void beginTransaction() {
        if (trace.isEnabledFor (DEBUG_LEVEL)) trace.log (DEBUG_LEVEL, "Starting new transaction");
        producer.beginTransaction();
    }

    private void abortTransaction() {
        if (trace.isEnabledFor (DEBUG_LEVEL)) trace.log (DEBUG_LEVEL, "Aborting transaction");
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


    /**
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.TrackingProducerClient#processRecord(org.apache.kafka.clients.producer.ProducerRecord, com.ibm.streams.operator.Tuple)
     */
    @Override
    public void processRecord(ProducerRecord<?, ?> producerRecord, Tuple associatedTuple) throws Exception {
        // send always within a transaction
        checkAndBeginTransaction();
        super.processRecord(producerRecord, associatedTuple);
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.TrackingProducerClient#processRecords(java.util.List, com.ibm.streams.operator.Tuple)
     */
    @Override
    public void processRecords(List<ProducerRecord<?, ?>> records, Tuple associatedTuple) throws Exception {
        // send always within a transaction
        checkAndBeginTransaction();
        super.processRecords(records, associatedTuple);
    }


    @Override
    public void checkpoint (Checkpoint checkpoint) throws Exception {
        final long currentSequenceId = checkpoint.getSequenceId();
        if (trace.isEnabledFor (DEBUG_LEVEL)) trace.log (DEBUG_LEVEL, "TransactionalKafkaProducerClient -- CHECKPOINT id=" + currentSequenceId);

        // check 'transactionInProgress' for true and set atomically to false
        if (transactionInProgress.compareAndSet (true, false)) {
            trace.log (DEBUG_LEVEL, "Committing transaction...");
            producer.commitTransaction();
        }
        else {
            trace.log (DEBUG_LEVEL, "No transaction in progress. Nothing to commit.");
        }
        assert (transactionInProgress.get() == false);
    }


    @Override
    public void reset (Checkpoint checkpoint) throws Exception {
        if (trace.isEnabledFor (DEBUG_LEVEL)) {
            trace.log (DEBUG_LEVEL, getThisClassName() + " -- RESET id=" + (checkpoint == null? -1L: checkpoint.getSequenceId())); //$NON-NLS-1$
        }
        // check 'transactionInProgress' for true and set atomically to false
        if (transactionInProgress.compareAndSet (true, false)) {
            // abort the current transaction
            abortTransaction();
        }
        assert (transactionInProgress.get() == false);
    }
}
