/**
 * 
 */
package com.ibm.streamsx.kafka.clients.consumer;

/**
 * This class represents the result of enqueueing a batch of consumer records into the message queue.
 * This class is not thread-safe.
 * 
 * @author IBM Kafka-Toolkit Maintainers
 */
public class EnqueResult {

    private int numRecords;
    private long sumKeySize;
    private long sumValueSize;

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "n=" + numRecords + "; kSz=" + sumKeySize + "; vSz = " + sumValueSize;
    }


    /**
     * Constructs a new instance
     */
    public EnqueResult() {
        this (0,0L,0L);
    }


    /**
     * @param numRecords
     */
    public EnqueResult (int numRecords) {
        this (numRecords, 0L, 0L);
    }


    /**
     * @param numRecords
     * @param sumKeySize
     * @param sumValueSize
     */
    public EnqueResult (int numRecords, long sumKeySize, long sumValueSize) {
        this.numRecords = numRecords;
        this.sumKeySize = sumKeySize;
        this.sumValueSize = sumValueSize;
    }


    /**
     * @param numRecords the numRecords to set
     */
    public void setNumRecords (int numRecords) {
        this.numRecords = numRecords;
    }


    /**
     * @return the numRecords
     */
    public int getNumRecords() {
        return numRecords;
    }

    /**
     * @return the sumKeySize
     */
    public long getSumKeySize() {
        return sumKeySize;
    }

    /**
     * @return the sumValueSize
     */
    public long getSumValueSize() {
        return sumValueSize;
    }

    /**
     * @return the size of all keys and values
     */
    public long getSumTotalSize() {
        return sumValueSize + sumKeySize;
    }

    /**
     * increments the sum of value sizes. 
     * @param inc the increment
     * @return the new value
     */
    public long incrementSumValueSize (long inc) {
        sumValueSize += inc;
        return sumValueSize;
    }

    /**
     * increments the sum of key sizes. 
     * @param inc the increment
     * @return the new value
     */
    public long incrementSumKeySize (long inc) {
        sumKeySize += inc;
        return sumKeySize;
    }
}
