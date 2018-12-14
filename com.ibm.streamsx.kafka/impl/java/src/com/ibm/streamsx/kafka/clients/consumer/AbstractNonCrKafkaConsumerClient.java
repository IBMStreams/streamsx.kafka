/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;


import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.Checkpoint;
import com.ibm.streams.operator.state.CheckpointContext;
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
    /** Start position where each subscribed topic is consumed from */
    private StartPosition initialStartPosition = StartPosition.Default;
    private final CheckpointContext chkptContext;
    
    /**
     * Maps TopicPartition to Boolean CV via accessor.
     * The boolean indicates if offsets for the partition have ever been committed or not.
     * The control variables are created with initial values of 'false' and can toggle only
     * once to 'true' and keep this value.
     */
    private Map<TopicPartition, ControlVariableAccessor<java.lang.Boolean>> committedMap;

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
        chkptContext = operatorContext.getOptionalContext (CheckpointContext.class);
        
        if (crContext != null) {
            throw new KafkaConfigurationException ("The operator '" + operatorContext.getName() + "' is used in a consistent region. This consumer client implementation (" 
                    + this.getClass() + ") does not support CR.");
        }

        // Test for enable.auto.commit -- should always be set to false by a base class.
        if (!kafkaProperties.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) || kafkaProperties.getProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equalsIgnoreCase ("true")) {
            throw new KafkaConfigurationException ("enable.auto.commit is unset (defaults to true) or has the value true. It must be false.");
        }
        offsetManager = new OffsetManager();
        committedMap = Collections.synchronizedMap (new HashMap<>());
    }

    /**
     * Returns the checkpoint context if there is one
     * @return the checkpoint context or null if 'config checkpoint' is not configured.
     * @see #isCheckpointEnabled()
     */
    protected final CheckpointContext getChkptContext() {
        return chkptContext;
    }
    
    /**
     * Returns true, when checkpointing is enabled, false otherwise.
     */
    protected final boolean isCheckpointEnabled() {
        if (chkptContext == null) return false;
        return chkptContext.isEnabled();
    }

    /**
     * @return the initialStartPosition
     */
    public final StartPosition getInitialStartPosition() {
        return initialStartPosition;
    }

    /**
     * @param p the initial start position to set
     */
    protected void setInitialStartPosition (StartPosition p) {
        this.initialStartPosition = p;
    }

    /**
     * Checks if an offset for the given topic partition has already been committed for the client's consumer group within the job.
     * The function accesses a job scoped control variable to check this information.
     * The control variable is created with initial value of 'false' if it does not yet exist.
     * @param tp the topic partition
     * @return the value of the control variable
     * @throws InterruptedException the call has been interrupted synchronizing the value from the JCP with the accessor
     * @see #setCommittedForPartitionTrue(TopicPartition)
     */
    protected boolean isCommittedForPartition (TopicPartition tp) throws InterruptedException {
        ControlVariableAccessor<Boolean> cv = this.committedMap.get (tp);
        if (cv == null) {
            final String cvName = createControlVariableName (tp);
            trace.info ("creating control variable: " + cvName);
            cv = getJcpContext().createBooleanControlVariable (cvName, true, false);
            this.committedMap.put (tp, cv);
        }

        final boolean result = cv.sync().getValue();
        trace.info (MessageFormat.format ("partition {0} previously committed: {1}", tp, result));
        return result;
    }

    /**
     * Marks a topic partition as committed for the client's consumer group.
     * This flag is stored in a job scoped control variable for the consumer group and partition.
     * When the topic partition is already marked, this call does not update the control variable once more.
     * @param tp the topic partition to be marked
     * @return the previous value
     * @throws InterruptedException the call has been interrupted synchronizing the value from the JCP with the accessor
     * @throws IOException the value could not be updated in the JCP
     * @see #isCommittedForPartition(TopicPartition)
     */
    protected boolean setCommittedForPartitionTrue (TopicPartition tp) throws InterruptedException, IOException {
        ControlVariableAccessor<Boolean> cv = this.committedMap.get (tp);
        boolean previous = false;
        if (cv == null) {
            final String cvName = createControlVariableName (tp);
            trace.info ("creating control variable: " + cvName);
            cv = getJcpContext().createBooleanControlVariable (cvName, true, false);
            cv.sync();
            this.committedMap.put (tp, cv);
        }
        // here we do not need to sync() before we get the value. The boolean CVs go always from false to true.
        // When we see the wrong value, we set true once more.
        previous = cv.getValue();
//        trace.info (MessageFormat.format ("{0} previously committed: {1}", tp, previous));
        if (!previous) {
            cv.setValue (true);
            trace.info (MessageFormat.format ("partition {0} marked as committed", tp));
        }
        return previous;
    }

    /**
     * Creates a name for a control variable which is unique for the consumer client's group-ID and the given topic partition.
     * @param tp the topic partition
     * @return the concatenation of 'getGroupId()' and the tp.toString()
     * @see #getGroupId()
     */
    private String createControlVariableName (TopicPartition tp) {
        return getGroupId() + tp.topic() + tp.partition();
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
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long)
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
     * Marks all topic partition as committed in a JCP control variable for partition and group.id.
     * 
     * @param offsets a map that maps partitions to offsets 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#postOffsetCommit(java.util.Map)
     */
    @Override
    protected void postOffsetCommit (Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (initialStartPosition == StartPosition.Default) return;
        for (TopicPartition tp: offsets.keySet()) {
            try {
                setCommittedForPartitionTrue (tp);
            }
            catch (InterruptedException e) {
                trace.info ("interrupted while updating control variable for " + tp);
            }
            catch (IOException e) {
                trace.error (e);
                e.printStackTrace();
            }
        }
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
