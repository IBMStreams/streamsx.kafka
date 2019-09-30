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
