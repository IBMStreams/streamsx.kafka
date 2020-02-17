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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author
 */
public class TimeWindowExceptionFilter {

    private Map<String, Queue<Long>> partitions = new HashMap<>();
    private final long timeWindowMillis;
    private final int maxExcPerWindow;
    private boolean partitioned = false;

    /**
     * @param timeWindowMillis The time window will never be larger than this value in milliseconds. 
     * @param maxExcPerWindow  The maximum number of exceptions that are suppressed in the time window
     *                         before {@link #filter(Exception)} returns false.
     */
    public TimeWindowExceptionFilter(long timeWindowMillis, int maxExcPerWindow) {
        this.timeWindowMillis = timeWindowMillis;
        this.maxExcPerWindow = maxExcPerWindow;
    }

    /**
     * Default constructor with 1 suppressed exception per 10 seconds
     */
    public TimeWindowExceptionFilter() {
        this (10000L, 1);
    }

    /**
     * @return the partitioned
     */
    public boolean isPartitioned() {
        return partitioned;
    }

    /**
     * removes the history
     */
    public void reset() {
        if (partitions.size() > 0) partitions.clear();
    }

    /**
     * @param partitioned the partitioned to set
     */
    public void setPartitioned (boolean partitioned) {
        this.partitioned = partitioned;
    }

    /**
     * Filters the exception and returns true, as long as the maximum number of
     * exceptions per time window is NOT yet reached, false otherwise
     * @param e
     */
    public boolean filter (Exception e) {
        Long now = System.currentTimeMillis();
        final String key = partitioned? e.getClass().getName(): "x";
        Queue<Long> q = partitions.get (key);
        if (q == null) {
            q = new LinkedList<Long>();
            partitions.put (key, q);
        }
        q.offer (now);
        Long oldest = q.peek();
        while (now.longValue() - oldest.longValue() > timeWindowMillis) {
            q.poll();
            oldest = q.peek();
        }
        return q.size() <= maxExcPerWindow;
    }
}
