/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class is the base class for Consumer clients that are used when not in a consistent region.
 * This class provides default implementations for all checkpoint and consistent region related methods from
 * @link {@link ConsumerClient}.
 */
public abstract class AbstractNonCrKafkaConsumerClient extends AbstractKafkaConsumerClient {

    private static final Logger trace = Logger.getLogger(AbstractNonCrKafkaConsumerClient.class);
    private long commitCount = 2000l; 
    private long nSubmittedRecords = 0l;
    private OffsetManager offsetManager = null;

    
    /**
     * @param operatorContext
     * @param keyClass
     * @param valueClass
     * @param kafkaProperties
     * @throws KafkaConfigurationException
     */
    public <K, V> AbstractNonCrKafkaConsumerClient (OperatorContext operatorContext, Class<K> keyClass,
            Class<V> valueClass, KafkaOperatorProperties kafkaProperties) throws KafkaConfigurationException {
        super (operatorContext, keyClass, valueClass, kafkaProperties);
        ConsistentRegionContext crContext = operatorContext.getOptionalContext (ConsistentRegionContext.class);
        if (crContext != null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' is used in a consistent region. This consumer client implementation (" 
                    + this.getClass() + ") does not support CR.");
        }

        // Test for enable.auto.commit -- should always be set to false by a base class.
        if (!kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) || kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true")) {
            throw new KafkaConfigurationException ("enable.auto.commit is unset (defaults to true) or has the value true. It must be false.");
        }
        offsetManager = new OffsetManager();
    }

    /**
     * Get the offsetManager instance
     * @return the offsetManager
     */
    protected OffsetManager getOffsetManager() {
        return offsetManager;
    }

    /**
     * @return the nSubmittedRecords
     */
    protected long getnSubmittedRecords() {
        return nSubmittedRecords;
    }

    /**
     * @param nSubmittedRecords the nSubmittedRecords to set
     */
    protected void setnSubmittedRecords (long n) {
        this.nSubmittedRecords = n;
    }

    /**
     * Validates the setup of the consumer client by calling the {@link #validate()} method, 
     * creates the Kafka consumer object and starts the consumer and event thread.
     * This method ensures that the event thread is running when it returns.
     * Methods that overwrite this method must call super.startConsumer().
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#startConsumer()
     * @see AbstractKafkaConsumerClient#startConsumer()
     */
    @Override
    public void startConsumer() throws InterruptedException, KafkaClientInitializationException {
        super.startConsumer();
        // offsetManager is null when auto-commit is enabled
        if (offsetManager != null) {
            offsetManager.setOffsetConsumer(getConsumer());
        }
    }

    /**
     * @param commitCount the commitCount to set
     */
    public void setCommitCount(long commitCount) {
        this.commitCount = commitCount;
    }
    
    
    /**
     * see {@link AbstractKafkaConsumerClient#validate()}
     */
    @Override
    protected void validate() throws Exception {
        if (commitCount <= 0) {
            throw new KafkaConfigurationException (Messages.getString ("INVALID_PARAMETER_VALUE_GT", new Integer(0)));
        }
    }
    
    /**
     * This implementation counts submitted tuples and commits the offsets when {@link #commitCount} has reached and auto-commit is disabled. 
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#postSubmit(org.apache.kafka.clients.consumer.ConsumerRecord)
     */
    @Override
    public void postSubmit (ConsumerRecord<?, ?> submittedRecord) {
        // collect submitted offsets per topic partition for periodic commit.
        try {
            synchronized (offsetManager) {
                offsetManager.savePosition(submittedRecord.topic(), submittedRecord.partition(), submittedRecord.offset() +1l, /*autoCreateTopci=*/true);
            }
        } catch (Exception e) {
            // is not caught when autoCreateTopic is 'true'
            trace.error(e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException (e);
        }
        if (++nSubmittedRecords  >= commitCount) {
            if (trace.isDebugEnabled()) {
                trace.debug("commitCount (" + commitCount + ") reached. Preparing to commit offsets ...");
            }
            // commit asynchronous, partition by partition.
            // asynchronous commit implies that the operator is not restarted when commit fails.
            final boolean commitSync = false;
            final boolean commitPartitionWise = true;
            CommitInfo offsets = new CommitInfo (commitSync, commitPartitionWise);

            synchronized (offsetManager) {
                for (TopicPartition tp: offsetManager.getMappedTopicPartitions()) {
                    offsets.put (tp, offsetManager.getOffset(tp.topic(), tp.partition()));
                }
            }
            try {
                nSubmittedRecords = 0l;
                if (!offsets.isEmpty()) {
                    // sendCommitEvent terminates the poll loop, throws InterruptedException:
                    sendCommitEvent (offsets);
                    // when committing offsets for one partition fails, the reason can be that we are not 
                    // assigned to the partition any more when building a consumer group.
                    // Then a different (or the same) consumer starts reading the records again creating duplicates within the application.
                    // This is normal Kafka methodology.
                    sendStartPollingEvent();
                }
            } catch (InterruptedException e) {
                // is not thrown when asynchronously committed; can be silently ignored.
                // Only when we decide to change to synchronous commit, we can end up here.
                // Then it is ok, NOT to start polling again. 
            }
        }
    }
    
    
    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long, boolean)
     */
    @Override
    protected int pollAndEnqueue (long pollTimeout, boolean isThrottled) throws InterruptedException, SerializationException {
        if (trace.isTraceEnabled()) trace.trace("Polling for records..."); //$NON-NLS-1$
        ConsumerRecords<?, ?> records = getConsumer().poll (pollTimeout);
        int numRecords = records == null? 0: records.count();
        if (trace.isTraceEnabled() && numRecords == 0) trace.trace("# polled records: " + (records == null? "0 (records == null)": "0"));
        if (numRecords > 0) {
            if (trace.isDebugEnabled()) trace.debug("# polled records: " + numRecords);
            records.forEach(cr -> {
                if (trace.isTraceEnabled()) {
                    trace.trace (cr.topic() + "-" + cr.partition() + " key=" + cr.key() + " - offset=" + cr.offset()); //$NON-NLS-1$
                }
                getMessageQueue().add(cr);
            });
        }
        return numRecords;
    }
    
    
    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onDrain()
     */
    @Override
    public void onDrain() throws Exception {
        throw new Exception ("onDrain(): consistent region is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetToInitEvent()
     */
    @Override
    protected void processResetToInitEvent() {
        throw new RuntimeException ("resetToInitialState(): consistent region is not supported by this consumer client: " + getThisClassName());
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetEvent(Checkpoint)
     */
    @Override
    protected void processResetEvent(Checkpoint checkpoint) {
        trace.error ("'config checkpoint' is not supported by the " + getOperatorContext().getKind() + " operator.");
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processCheckpointEvent(Checkpoint)
     */
    @Override
    protected void processCheckpointEvent (Checkpoint data) {
        throw new RuntimeException ("onReset(): consistent region and 'config checkpoint' is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpointRetire(long)
     */
    @Override
    public void onCheckpointRetire(long id) {
        throw new RuntimeException ("onCheckpointRetire(): consistent region is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpoint(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onCheckpoint (Checkpoint checkpoint) throws InterruptedException {
        throw new RuntimeException ("consistent region and 'config checkpoint' is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onReset(com.ibm.streams.operator.state.Checkpoint)
     */
    @Override
    public void onReset(Checkpoint checkpoint) throws InterruptedException {
        throw new RuntimeException ("onReset(): consistent region and 'config checkpoint' is not supported by this consumer client: " + getThisClassName());
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onResetToInitialState()
     */
    @Override
    public void onResetToInitialState() throws InterruptedException {
        throw new RuntimeException ("onResetToInitialState(): consistent region is not supported by this consumer client: " + getThisClassName());
    }
}
