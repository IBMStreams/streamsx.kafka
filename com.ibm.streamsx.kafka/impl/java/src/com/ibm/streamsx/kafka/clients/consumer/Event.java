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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This class represents an event to control the consumer thread.
 * Objects of this type go into the event queue. Events can have a
 * data portion that is processed by the consumer thread, and can have
 * a countdown latch for thread synchronization.
 * 
 * @author IBM Streams toolkit team.
 */
public class Event {

    public static enum EventType {
        START_POLLING, STOP_POLLING, CHECKPOINT, RESET, RESET_TO_INIT, SHUTDOWN, CONTROLPORT_EVENT, COMMIT_OFFSETS;
    };

    private EventType eventType;
    private Object data;
    private CountDownLatch latch;

    /**
     * Constructs a new Event without latch and without additional data
     * @param eventType
     */
    public Event (EventType eventType) {
        this (eventType, null, false);
    }

    /**
     * Constructs a new event without latch with data
     * @param eventType the type of the event
     * @param data the event type specific data
     */
    public Event(EventType eventType, Object data) {
        this (eventType, data, false);
    }

    /**
     * Constructs a new Event without additional data
     * @param eventType
     * @param withLatch if true, a CountDownLatch is associated with the event.
     */
    public Event (EventType eventType, boolean withLatch) {
        this (eventType, null, withLatch);
    }

    /**
     * Constructs a new event with data
     * @param eventType the type of the event
     * @param data the event type specific data
     * @param withLatch if true, a CountDownLatch is associated with the event.
     */
    public Event(EventType eventType, Object data, boolean withLatch) {
        if (eventType == null) throw new IllegalArgumentException("eventType == null");
        this.eventType = eventType;
        this.data = data;
        this.latch = withLatch? new CountDownLatch(1): null;
    }

    /**
     * Waits that the latch is counted down.
     * If the event is created without a latch, the method returns immediately.
     * @throws InterruptedException
     */
    public void await() throws InterruptedException {
        if (latch != null) latch.await();
    }

    /**
     * Waits that the latch is counted down.
     * If the event is created without a latch, the method returns immediately.
     * @param timeout
     * @param timeUnit
     * @throws InterruptedException
     */
    public void await(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (latch != null) latch.await (timeout, timeUnit);
    }

    /**
     * Counts down the latch by 1, so that {@link #await()} returns.
     */
    public void countDownLatch() {
        if (latch != null) latch.countDown();
    }

    /**
     * @return the latch or null if the event is created without a latch
     */
    public final CountDownLatch getLatch() {
        return latch;
    }

    public Object getData() {
        return data;
    }

    public EventType getEventType() {
        return eventType;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (data == null) return eventType.toString();
        return "{" + eventType.toString() + ", (data)}";
    }
}
