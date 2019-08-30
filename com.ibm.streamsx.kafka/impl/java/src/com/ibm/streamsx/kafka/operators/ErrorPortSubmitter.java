package com.ibm.streamsx.kafka.operators;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
import com.ibm.streamsx.kafka.clients.producer.queuing.FailureDescription;
import com.ibm.streamsx.kafka.clients.producer.queuing.TupleProcessedHook;

/**
 * This class represents a Hook that submits tuples to an output port.
 * @author The IBM Kafka toolkit team
 */
public class ErrorPortSubmitter implements TupleProcessedHook {

    private static final long OUT_QUEUE_OFFER_TIMEOUT_MS = 5000;
    private static final int OUTPUT_QUEUE_CAPACITY = 5000;
    private static final Logger trace = Logger.getLogger (ErrorPortSubmitter.class);

    private final StreamingOutput<OutputTuple> out;
    private final Gson gson;
    private int tupleAttrIndex = -1;
    private int stringAttrIndex = -1;
    private final BlockingQueue<OutputTuple> outQueue;
    private final Thread tupleSubmitter;
    private final OperatorContext opCtxt;

    /**
     * Runnable target for the tuple submission.
     */
    private class TupleSubmitter implements Runnable {
        @Override
        public void run() {
            while (!tupleSubmitter.isInterrupted()) {
                try {
                    OutputTuple oTuple = outQueue.take();
                    out.submit(oTuple);
                } catch (InterruptedException e) {
                    trace.info ("Tuple submitter interrupted. Thread ended");
                    return;
                } catch (Exception e) {
                    trace.error ("Failed to submit tuple: " + e);
                }
            }
            trace.info ("Tuple submitter thread ended");
        }
    }

    /**
     * Constructs a new ErrorPortSubmitter.
     * @param opContext the operator context
     * @param tupleAttrIndex  the attribute index of the input tuple attribute. If there is no such index, specify -1.
     * @param stringAttrIndex the attribute index of the JSON error description rstring attribute. If there is no such index, specify -1.
     * @throws KafkaOperatorException unsupported output port schema
     */
    public ErrorPortSubmitter (OperatorContext opContext /* int tupleAttrIndex, int stringAttrIndex*/) throws KafkaOperatorException {
        this.out = opContext.getStreamingOutputs().get(0);
        this.gson = (new GsonBuilder()).enableComplexMapKeySerialization().create();
        //        this.tupleAttrIndex = tupleAttrIndex;
        //        this.stringAttrIndex = stringAttrIndex;
        this.outQueue = new LinkedBlockingQueue<> (OUTPUT_QUEUE_CAPACITY);
        this.opCtxt = opContext;
        this.tupleSubmitter = opCtxt.getThreadFactory().newThread (new TupleSubmitter());
        this.tupleSubmitter.start();
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
     * Empty implementation.
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.TupleProcessedHook#onTupleProduced(com.ibm.streams.operator.Tuple)
     */
    @Override
    public void onTupleProduced (Tuple tuple) { }

    /**
     * Creates the error output tuple and places it into a queue.
     * 
     * @see com.ibm.streamsx.kafka.clients.producer.queuing.TupleProcessedHook#onTupleFailed(com.ibm.streams.operator.Tuple, com.ibm.streamsx.kafka.clients.producer.queuing.FailureDescription)
     */
    @Override
    public void onTupleFailed (Tuple inTuple, FailureDescription failure) {
        final String failureJson = gson.toJson (failure);
        OutputTuple outTuple = out.newTuple();
        if (tupleAttrIndex >= 0)
            outTuple.assignTuple (tupleAttrIndex, inTuple);
        if (stringAttrIndex >= 0)
            outTuple.setString (stringAttrIndex, failureJson);
        try {
            if (!outQueue.offer (outTuple, OUT_QUEUE_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                trace.error ("Output port queue congested (size = " + OUTPUT_QUEUE_CAPACITY + "). Output tuple discarded.");
            }
        } catch (InterruptedException e) {
            trace.warn ("Interrupted inserting tuple into output queue. Output tuple discarded.");
        }
    }
}
