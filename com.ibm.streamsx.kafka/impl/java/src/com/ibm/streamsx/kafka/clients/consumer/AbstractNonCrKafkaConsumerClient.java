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
package com.ibm.streamsx.kafka.clients.consumer;


import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.control.Controllable;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;
import com.ibm.streams.operator.state.CheckpointContext;
import com.ibm.streams.operator.state.CheckpointContext.Kind;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.kafka.KafkaClientInitializationException;
import com.ibm.streamsx.kafka.KafkaConfigurationException;
import com.ibm.streamsx.kafka.MissingJobControlPlaneException;
import com.ibm.streamsx.kafka.MsgFormatter;
import com.ibm.streamsx.kafka.clients.OffsetManager;
import com.ibm.streamsx.kafka.i18n.Messages;
import com.ibm.streamsx.kafka.operators.AbstractKafkaConsumerOperator;
import com.ibm.streamsx.kafka.properties.KafkaOperatorProperties;

/**
 * This class is the base class for Consumer clients that are used when not in a consistent region.
 * This class provides default implementations for all checkpoint and consistent region related methods from
 * @link {@link ConsumerClient}.
 */
public abstract class AbstractNonCrKafkaConsumerClient extends AbstractKafkaConsumerClient implements Controllable {

    private static final Logger trace = Logger.getLogger(AbstractNonCrKafkaConsumerClient.class);
    protected static final long JCP_CONNECT_TIMEOUT_MILLIS = 30000;

    private CommitMode commitMode;
    private long commitPeriodMillis = 10000;
    private long commitCount = 2000l; 
    private long nSubmittedRecords = 0l;
    private long nextCommitTime = 0l;
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
    private AtomicBoolean jcpConnected = new AtomicBoolean (false);
    private CountDownLatch jmxConnectLatch;

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
     * Returns checkpoint kind. This method works around a bug in the Streams runtime.
     * It evaluates checkpoint interval, which is < 0 when operator driven.
     * // getChkptContext().getKind() is not reported properly. Streams Build 20180710104900 (4.3.0.0) never returns OPERATOR_DRIVEN
     * 
     * @return null if checkpointing is not enabled or CheckpointContext.Kind dependent on period.
     */
    protected final CheckpointContext.Kind getCheckpointKind() {
        if (!isCheckpointEnabled()) return null;
        double period = chkptContext.getCheckpointingInterval();
        return period > 0.0? Kind.PERIODIC: Kind.OPERATOR_DRIVEN;
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
        trace.info (MsgFormatter.format ("partition {0} previously committed: {1}", tp, result));
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
        // here we do not need to sync() (expensive) before we get the value. The boolean CVs go always from false to true.
        // When we see the wrong value, we set true once more.
        previous = cv.getValue();
        if (!previous) {
            cv.setValue (true);
            trace.info (MsgFormatter.format ("partition {0} marked as committed", tp));
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
        return getGroupId() + tp.topic() + "-" + tp.partition();
    }

    /**
     * Get the offsetManager instance
     * @return the offsetManager
     */
    protected OffsetManager getOffsetManager() {
        return offsetManager;
    }

    /**
     * Resets the commit period, either tuple counter or next time period due time.
     * @param now current time
     */
    protected void resetCommitPeriod (long now) {
        switch (commitMode) {
        case Time:
            this.nextCommitTime = now + this.commitPeriodMillis;
            break;
        case TupleCount:
            //this.nSubmittedRecords = 0l;
            break;
        default:
            ;
            //ignore
        }
        this.nSubmittedRecords = 0l;
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
        resetCommitPeriod (System.currentTimeMillis());
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
     * @param period the commit period in milliseconds to set
     */
    public void setCommitPeriodMillis (long period) {
        this.commitPeriodMillis = period;
    }

    /**
     * @param mode the commit mode to set
     */
    public void setCommitMode (CommitMode mode) {
        this.commitMode = mode;
    }

    /**
     * see {@link AbstractKafkaConsumerClient#validate()}
     */
    @Override
    protected void validate() throws Exception {
        switch (commitMode) {
        case Time:
            if (commitPeriodMillis < 100) {
                throw new KafkaConfigurationException (Messages.getString ("INVALID_PARAMETER_VALUE_GT",
                        AbstractKafkaConsumerOperator.COMMIT_PERIOD_PARAM, new Double ((double)commitPeriodMillis/1000.0), new Double(0.1)));
            }
            break;
        case TupleCount:
            if (commitCount <= 0) {
                throw new KafkaConfigurationException (Messages.getString ("INVALID_PARAMETER_VALUE_GT",
                        AbstractKafkaConsumerOperator.COMMIT_COUNT_PARAM, new Long (commitCount), new Long (0)));
            }
            break;
        default:
            break;
        }
    }

    /**
     * Here we commit offsets. We commit offsets here instead of in {@link #postSubmit(ConsumerRecord)}
     * because time based policy is not invoked when no tuples are submitted.
     * 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#preDeQueueForSubmit()
     */
    @Override
    protected void preDeQueueForSubmit() {
        final boolean doCommit;
        long now = 0l;
        switch (commitMode) {
        case Time:
            now = System.currentTimeMillis();
            doCommit = now >= nextCommitTime;
            if (doCommit && trace.isDebugEnabled()) {
                trace.log (DEBUG_LEVEL, "commitPeriod (" + commitPeriodMillis + " ms) has passed. Preparing to commit offsets ...");
            }
            break;
        case TupleCount:
            doCommit = nSubmittedRecords  >= commitCount;
            if (doCommit && trace.isDebugEnabled()) {
                trace.log (DEBUG_LEVEL, "commitCount (" + commitCount + ") reached. Preparing to commit offsets ...");
            }
            break;
        default:
            doCommit = false;
        }
        if (doCommit) {
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
                if (!offsets.isEmpty()) {
                    // sendCommitEvent terminates the poll loop, throws InterruptedException:
                    sendCommitEvent (offsets);
                    // when committing offsets for one partition fails, the reason can be that we are not 
                    // assigned to the partition any more when building a consumer group.
                    // Then a different (or the same) consumer starts reading the records again creating duplicates within the application.
                    // This is normal Kafka methodology.
                    if (isSubscribedOrAssigned()) {
                        sendStartPollingEvent();
                    }
                }
            } catch (InterruptedException e) {
                // is not thrown when asynchronously committed; can be silently ignored.
                // Only when we decide to change to synchronous commit, we can end up here.
                // Then it is ok, NOT to start polling again. 
            } finally {
                resetCommitPeriod (now);
            }
        }
    }

    /**
     * This implementation counts submitted tuples (for commit) and stores the offsets of submitted tuples in the 'offsetManager'. 
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#postSubmit(org.apache.kafka.clients.consumer.ConsumerRecord)
     */
    @Override
    public void postSubmit (ConsumerRecord<?, ?> submittedRecord) {
        ++nSubmittedRecords;
        // collect submitted offsets per topic partition for periodic commit.
        try {
            synchronized (offsetManager) {
                final String topic = submittedRecord.topic();
                final int partition = submittedRecord.partition();
                if (getAssignedPartitions().contains (new TopicPartition (topic, partition))) {
                    offsetManager.savePosition (topic, partition, submittedRecord.offset() +1l, /*autoCreateTopci=*/true);
                }
            }
        } catch (Exception e) {
            // is not caught when autoCreateTopic is 'true'
            trace.error(e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException (e);
        }
    }


    /**
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#pollAndEnqueue(long, boolean)
     */
    @Override
    protected EnqueResult pollAndEnqueue (long pollTimeout, boolean isThrottled) throws InterruptedException, SerializationException {
        if (trace.isTraceEnabled()) trace.trace("Polling for records..."); //$NON-NLS-1$
        ConsumerRecords<?, ?> records = getConsumer().poll (Duration.ofMillis (pollTimeout));
        int numRecords = records == null? 0: records.count();
        if (trace.isTraceEnabled() && numRecords == 0) trace.trace("# polled records: " + (records == null? "0 (records == null)": "0"));
        EnqueResult r = new EnqueResult (numRecords);
        if (numRecords > 0) {
            records.forEach(cr -> {
                if (trace.isTraceEnabled()) {
                    trace.trace (cr.topic() + "-" + cr.partition() + " key=" + cr.key() + " - offset=" + cr.offset()); //$NON-NLS-1$
                }
                final int vsz = cr.serializedValueSize();
                final int ksz = cr.serializedKeySize();
                if (vsz > 0) r.incrementSumValueSize (vsz);
                if (ksz > 0) r.incrementSumKeySize (ksz);
                getMessageQueue().add (cr);
            });
        }
        return r;
    }


    /**
     * Marks all topic partition from the parameter map (keys) as committed in a JCP control variable for partition and group.id.
     * 
     * @param offsets a map that maps partitions to offsets 
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#postOffsetCommit(java.util.Map)
     */
    @Override
    protected void postOffsetCommit (Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (initialStartPosition == StartPosition.Default) return;
        if (!canUseJobControlPlane()) return;
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
     * Empty default implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.AbstractKafkaConsumerClient#processResetToInitEvent()
     */
    @Override
    protected void processResetToInitEvent() {
        trace.log (DEBUG_LEVEL, "processResetToInitEvent");
    }



    /**
     * Empty default implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onCheckpointRetire(long)
     */
    @Override
    public void onCheckpointRetire(long id) {
        trace.log (DEBUG_LEVEL, "onCheckpointRetire, id = " + id);
    }


    /**
     * Empty default implementation
     * @see com.ibm.streamsx.kafka.clients.consumer.ConsumerClient#onResetToInitialState()
     */
    @Override
    public void onResetToInitialState() throws InterruptedException {
        trace.log (DEBUG_LEVEL, "onResetToInitialState");
    }


    /**
     * Tries to connect the client with the JobControlPlane.
     * This method is typically invoked only once at initialization. It sets a member variable,
     * which can be queried with {@link #canUseJobControlPlane()}.
     * 
     * @param connectTimeoutMillis The timeout to connect with the JCP in milliseconds.
     * @return true, if the client can be connected, false when a timeout occurred.
     * @see #canUseJobControlPlane()
     */
    private boolean tryConnectJobControlPlane (long connectTimeoutMillis) {
        if (jcpConnected.get()) return true;
        jmxConnectLatch = new CountDownLatch (1);
        getJcpContext().connect (this);
        try {
            jmxConnectLatch.await (connectTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
        return jcpConnected.get();
    }


    /**
     * Tests for a connection establishment with the JCP operator and throws an exception if it cannot be connected. 
     * @param connectTimeoutMillis The connect timeout in milliseconds
     * @param startPos the initial start position, used for the exception message only
     * @throws MissingJobControlPlaneException The connection cannot be created
     */
    protected void testForJobControlPlaneOrThrow (long connectTimeoutMillis, StartPosition startPos) throws MissingJobControlPlaneException {
        if (!tryConnectJobControlPlane (connectTimeoutMillis)) {
            trace.error (MsgFormatter.format ("Could not connect to the JobControlPlane "
                    + "within {0} milliseconds. Make sure that the operator graph contains "
                    + "a JobControlPlane operator to support startPosition {1}.", connectTimeoutMillis, startPos));
            throw new MissingJobControlPlaneException (Messages.getString ("JCP_REQUIRED_NOCR_STARTPOS_NOT_DEFAULT", startPos));
        }
    }


    /**
     * Checks if a JobControlPlane can be used after {@link #tryConnectJobControlPlane(long)} has been invoked.
     * @return true, when the JobControlPlain could be connected, false otherwise.
     */
    protected boolean canUseJobControlPlane() {
        return jcpConnected.get();
    }
    // ------- Controllable implementation -----------
    /*
     * The Controllable implementation is used to detect a JobControlPlane operator in the graph.
     * In the startConsumer operator we try to connect the Controllable to the JCP. When the setup() happens in time,
     * there is evidence of a JCP. When a timeout happened, we assume there is no JCP.
     */

    /**
     * This is an empty implementation.
     * @see com.ibm.streams.operator.control.Controllable#event(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext, com.ibm.streams.operator.control.Controllable.EventType)
     */
    @Override
    public void event (MBeanServerConnection jcp, OperatorContext context, Controllable.EventType eventType) {
    }

    /**
     * Returns always true when the initial start position is not Default, false fi the initial start position is Default.
     * @see #getInitialStartPosition()
     * @see com.ibm.streams.operator.control.Controllable#isApplicable(com.ibm.streams.operator.OperatorContext)
     */
    @Override
    public boolean isApplicable (OperatorContext context) {
        return getInitialStartPosition() != StartPosition.Default;
    }

    /**
     * @see com.ibm.streams.operator.control.Controllable#setup(javax.management.MBeanServerConnection, com.ibm.streams.operator.OperatorContext)
     */
    @Override
    public void setup (MBeanServerConnection jcp, OperatorContext context) throws InstanceNotFoundException, Exception {
        jcpConnected.set (true);
        jmxConnectLatch.countDown();
    }
}
