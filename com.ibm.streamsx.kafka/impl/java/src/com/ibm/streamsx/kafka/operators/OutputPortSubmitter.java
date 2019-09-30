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
package com.ibm.streamsx.kafka.operators;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streamsx.kafka.KafkaOperatorException;
import com.ibm.streamsx.kafka.clients.producer.FailureDescription;
import com.ibm.streamsx.kafka.clients.producer.TupleProcessedHook;

/**
 * This class represents a Hook that submits tuples to an output port.
 * @author The IBM Kafka toolkit team
 */
public class OutputPortSubmitter implements TupleProcessedHook {

    private static final Logger trace = Logger.getLogger (OutputPortSubmitter.class);

    private final StreamingOutput<OutputTuple> out;
    private final boolean submitOnlyErrors;
    private final Gson gson;
    private int tupleAttrIndex = -1;
    private int stringAttrIndex = -1;
    private final int outQueueCapacity;
    private final long outQueueOfferTimeoutMillis;
    private final BlockingQueue<OutputTuple> outQueue;
    private boolean isRunning = false;
    private Thread tupleSubmitter;
    private final OperatorContext opCtxt;
    private final Object queueMonitor = new Object();
    private final AtomicInteger nQt = new AtomicInteger();
    private final AtomicBoolean reset = new AtomicBoolean (false);

    /**
     * Runnable target for the tuple submission.
     */
    private class TupleSubmitter implements Runnable {
        @Override
        public void run() {
            while (isRunning) {
                OutputTuple oTuple;
                try {
                    oTuple = outQueue.take();
                } catch (InterruptedException e) {
                    continue;
                }
                // here we have taken a tuple and MUST decrement nQt.
                try {
                    if (!reset.get()) out.submit(oTuple);
                } catch (Exception e) {
                    trace.error ("Failed to submit tuple: " + e);
                    continue;
                }
                finally {
                    if (nQt.decrementAndGet() == 0) {
                        synchronized (queueMonitor) {
                            queueMonitor.notifyAll();
                        }
                    }
                }
            }
            trace.info ("Tuple submitter thread ended");
        }
    }

    /**
     * Constructs a new ErrorPortSubmitter.
     * @param opContext the operator context
     * @param outQueueCapacity the capacity of the output queue.
     * @param outQueueOfferTimeoutMs the timeout in milliseconds for placing output tuples into the queue when the queue is full.
     * @throws KafkaOperatorException unsupported output port schema
     */
    public OutputPortSubmitter (OperatorContext opContext, int outQueueCapacity, long outQueueOfferTimeoutMs, boolean submitOnlyErrors) throws KafkaOperatorException {
        if (opContext.getNumberOfStreamingOutputs() == 0) {
            throw new KafkaOperatorException ("Missing output port for operator " + opContext.getKind());
        }
        this.submitOnlyErrors = submitOnlyErrors;
        this.outQueueOfferTimeoutMillis = outQueueOfferTimeoutMs;
        this.outQueueCapacity = outQueueCapacity;
        this.out = opContext.getStreamingOutputs().get(0);
        this.gson = (new GsonBuilder()).enableComplexMapKeySerialization().create();
        this.outQueue = new LinkedBlockingQueue<> (outQueueCapacity);
        this.opCtxt = opContext;
        StreamSchema inPortSchema = opContext.getStreamingInputs().get(0).getStreamSchema();
        StreamSchema outSchema = out.getStreamSchema();
        // find the output attributes, we can assign to
        int nTupleAttrs = 0;
        int nStringAttrs = 0;
        for (String outAttrName: outSchema.getAttributeNames()) {
            Attribute attr = outSchema.getAttribute (outAttrName);
            MetaType metaType = attr.getType().getMetaType();
            switch (metaType) {
            case TUPLE:
                ++nTupleAttrs;
                TupleType tupleType = (TupleType) attr.getType();
                StreamSchema tupleSchema = tupleType.getTupleSchema();
                if (tupleSchema.equals (inPortSchema)) {
                    tupleAttrIndex = attr.getIndex();
                }
                break;
            case RSTRING:
            case USTRING:
                ++nStringAttrs;
                stringAttrIndex = attr.getIndex();
                break;
            default:
                trace.warn ("unsupported attribute type in output port: " + metaType + " for attribute '" + outAttrName + "'");
            }
        }
        if (nTupleAttrs > 1 || nStringAttrs > 1)
            throw new KafkaOperatorException ("Unsupported output port schema: " + outSchema);
    }

    /**
     * ends the tuple submitter thread 
     */
    public void stop() {
        isRunning = false;
    }

    /**
     * creates a thread and starts the thread that reads the outgoing queue and submits tuples.
     */
    public void start() {
        stop();
        this.tupleSubmitter = opCtxt.getThreadFactory().newThread (new TupleSubmitter());
        isRunning = true;
        this.tupleSubmitter.start();
    }

    /**
     * @see com.ibm.streamsx.kafka.clients.producer.TupleProcessedHook#onTupleProduced(com.ibm.streams.operator.Tuple)
     */
    @Override
    public void onTupleProduced (Tuple tuple) {
        if (submitOnlyErrors) return;
        enqueueOTuple (tuple, "");
    }

    /**
     * Creates the error output tuple and places it into a queue.
     * 
     * @see com.ibm.streamsx.kafka.clients.producer.TupleProcessedHook#onTupleFailed(com.ibm.streams.operator.Tuple, com.ibm.streamsx.kafka.clients.producer.FailureDescription)
     */
    @Override
    public void onTupleFailed (Tuple inTuple, FailureDescription failure) {
        if (stringAttrIndex >= 0)
            enqueueOTuple (inTuple, gson.toJson (failure));
        else
            enqueueOTuple (inTuple, null);
    }

    /**
     * ensure that the hook has processed everything
     */
    public void flush() {
        synchronized (queueMonitor) {
            try {
                while (this.nQt.get() > 0) {
                    queueMonitor.wait();
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public void reset() {
        reset.set (true);
        //        if (tupleSubmitter != null) tupleSubmitter.interrupt();
        flush();
        reset.set (false);
    }

    /**
     * Creates an output tuple and offers it to the output queue
     * @param inTuple
     * @param failure
     */
    private void enqueueOTuple (Tuple inTuple, String stringAttrVal) {
        OutputTuple outTuple = out.newTuple();
        if (tupleAttrIndex >= 0)
            outTuple.assignTuple (tupleAttrIndex, inTuple);
        if (stringAttrIndex >= 0)
            outTuple.setString (stringAttrIndex, stringAttrVal);
        try {
            this.nQt.incrementAndGet();
            if (!outQueue.offer (outTuple, outQueueOfferTimeoutMillis, TimeUnit.MILLISECONDS)) {
                trace.error ("Output port queue congested (size = " + outQueueCapacity + "). Output tuple discarded.");
                // tuple NOT offered. revert the previous increment
                if (this.nQt.decrementAndGet() == 0) {
                    synchronized (queueMonitor) {
                        queueMonitor.notifyAll();
                    }
                }
            }
        } catch (InterruptedException e) {
            trace.info ("Interrupted inserting output tuple into output queue. Output tuple discarded.");
        }
    }
}
