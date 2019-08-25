package com.ibm.streamsx.kafka.clients.consumer;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.control.ControlPlaneContext;
import com.ibm.streams.operator.control.variable.ControlVariableAccessor;

/**
 * This class contains the initial offsets for topic partitions.
 * Instances of this class are backed by shared JCP control variables, 
 * one for every topic partition per consumer group. 
 * It uses control variables to manage initial occurrence of topic partitions.
 * 
 * @author The IBM Kafka toolkit maintainers
 * @since v1.8.0
 */
public class CVOffsetAccessor {

    /** Return value when no offset stored in a control variable */
    public final static long NO_OFFSET = Long.MIN_VALUE;
    private static final Logger trace = Logger.getLogger (CVOffsetAccessor.class);
    private final static String CV_NAME_PREFIX = "initialoffs";
    /** Add and remove a constant value from the content of the long control variables to avoid creating a CV with initial value 0L */
    private final static long OFFSET_WORKAROUND_ZERO = 1L;

    private final ControlPlaneContext jcpContext;
    private final String groupId;

    private Map <TopicPartition, ControlVariableAccessor <Long>> cvMap = new HashMap<>();
    private Map <TopicPartition, Long> cache = new HashMap<>();

    /**
     * Constructs a new CVOffsetAccessor instance
     * 
     * @param jcpContext
     * @param groupId
     */
    public CVOffsetAccessor (ControlPlaneContext jcpContext, final String groupId) {
        this.jcpContext = jcpContext;
        this.groupId = groupId;
    }

    /**
     * Creates a name for a control variable which is unique for the consumer client's group-ID and the given topic partition.
     * @param tp the topic partition
     * @return the concatenation of {@value #CV_NAME_PREFIX}-'GID'groupId-'T'topic-'P'partition#
     */
    private String createControlVariableName (final TopicPartition tp) {
        return MessageFormat.format ("{0}-GID{1}-T{2}-P{3}", CV_NAME_PREFIX, groupId, tp.topic(), tp.partition());
    }

    /**
     * Test if an offset is already created in a CV for a given topic partition. If not, the fetch offset
     * can be overridden by seeking the consumer to an initial position and fetching its initial fetch offset.
     * 
     * The test is achieved by
     * <ul>
     * <li>checking the cache, if there is a hit, return true</li>
     * <li>try to create a control variable
     *     with initial offset -1 (which is not valid offset) and read out the content.
     *     If it is not -1, cache the offset for the partition and return true.
     *     if the read value is -1, the CV could be successfully created. Return false
     * </ul>
     * @see #saveOffset(TopicPartition, long, boolean)
     * @param tp the topic partition
     * @return true, if there is an initial offset for the partition, false otherwise
     * @throws InterruptedException Thread interrupted syncing the CV 
     */
    public synchronized boolean hasOffset (final TopicPartition tp) throws InterruptedException {
        if (cache.containsKey (tp)) return true;
        ControlVariableAccessor <Long> cv = cvMap.get (tp);
        if (cv == null) {
            final String cvName = createControlVariableName(tp);
            trace.info ("trying to create control variable: " + cvName);
            cv = jcpContext.createLongControlVariable (cvName, true, NO_OFFSET + OFFSET_WORKAROUND_ZERO);
            this.cvMap.put (tp, cv);
        }
        final long result = cv.sync().getValue().longValue() - OFFSET_WORKAROUND_ZERO;
        final boolean hasOffset = result != NO_OFFSET;
        trace.info (MessageFormat.format ("offset from CV for partition {0}: {1}", tp, (hasOffset? "" + result: "NO_OFFSET")));
        if (hasOffset) {
            // offset present, cache value
            cache.put (tp, new Long (result));
        }
        return hasOffset;
    }

    /**
     * Saves an offset for a topic partition in a control variable.
     *
     * @param tp the topic partition
     * @param offset the new offset
     * @param overwriteValidOffset if set to true, a valid existing offset is overwritten, if false, the offset is only overwritten wqhen there was no valid offset before.
     * @return the previous value, if there was no previous value, {@value #NO_OFFSET} is returned.
     * @throws InterruptedException Thread interrupted syncing the CV
     * @throws IOException Setting the value in the CV failed
     * @throws IllegalArgumentException offset < 0
     */
    public synchronized long saveOffset (final TopicPartition tp, final long offset, final boolean overwriteValidOffset) throws InterruptedException, IOException {
        trace.info (MessageFormat.format ("saveOffset: {0}, {1}, {2}", tp, offset, overwriteValidOffset));
        if (offset < 0) throw new IllegalArgumentException ("offset == " + offset);
        ControlVariableAccessor <Long> cv = cvMap.get (tp);
        if (cv == null) {
            final String cvName = createControlVariableName (tp);
            trace.info (MessageFormat.format ("trying to create control variable {0} for offset {1}", cvName, offset));
            cv = jcpContext.createLongControlVariable (cvName, true, offset + OFFSET_WORKAROUND_ZERO);
            this.cvMap.put (tp, cv);
        }
        final long previousVal = cv.sync().getValue().longValue() - OFFSET_WORKAROUND_ZERO;
        if (previousVal == offset) {
            // CV just now created or offset unchanged
            cache.put (tp, new Long (offset));
        }
        else {
            if (previousVal == NO_OFFSET || overwriteValidOffset) {
                // write the new value into cache and CV
                cv.setValue (new Long (offset + OFFSET_WORKAROUND_ZERO));
                cache.put (tp, new Long (offset));
                trace.info (MessageFormat.format ("offset {0} saved in CV {1} and cached for TP {2}", offset, cv.getName(), tp));
            }
            else {
                // old value was not NO_OFFSET and overwriting is not allowed.
                // cache old value
                cache.put (tp, new Long (previousVal));
            }
        }
        return previousVal;
    }

    /**
     * Gets an offset for a topic partition.
     *
     * @param tp the topic partition
     * @return the offset, if there was no value, {@value #NO_OFFSET}, which is Long.MIN_VALUE, is returned.
     * @throws InterruptedException Thread interrupted syncing the CV
     */
    public synchronized long getOffset (final TopicPartition tp) throws InterruptedException, IOException {
        trace.info (MessageFormat.format ("getOffset: {0}", tp));
        Long offs = cache.get (tp);
        if (offs != null) {
            trace.info (MessageFormat.format ("offset for {0} cached: {1}", tp, offs));
            return offs.longValue();
        }

        ControlVariableAccessor <Long> cv = cvMap.get (tp);
        if (cv == null) {
            final String cvName = createControlVariableName(tp);
            trace.info ("trying to create control variable: " + cvName);
            cv = jcpContext.createLongControlVariable (cvName, true, NO_OFFSET + OFFSET_WORKAROUND_ZERO);
            this.cvMap.put (tp, cv);
        }
        long offsCv = cv.sync().getValue().longValue() - OFFSET_WORKAROUND_ZERO;
        cache.put (tp, new Long (offsCv));
        trace.info (MessageFormat.format ("offset for {0} read from CV: {1}", tp, (offsCv == NO_OFFSET? "NO_OFFSET": "" + offsCv)));
        return offsCv;
    }

    /**
     * Creates a map that maps topic partitions to offsets in the control variables.
     * For partitions that have no initial offset, no mapping is created.
     * @param partitions The partitions for which the mappings are created.
     * @return A map that maps topic partitions to offsets. The map can be empty, but the returned value is never null.
     * @throws IOException 
     * @throws InterruptedException 
     */
    public synchronized Map <TopicPartition, Long> createOffsetMap (final Collection <TopicPartition> partitions) throws InterruptedException, IOException {
        Map <TopicPartition, Long> m = new HashMap<>(partitions.size());
        for (TopicPartition tp: partitions) {
            final long offs = getOffset (tp);
            if (offs != NO_OFFSET) m.put (tp, new Long (offs));
        }
        return m;
    }
}
