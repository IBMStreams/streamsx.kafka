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
package com.ibm.streamsx.kafka.test.utils;

import java.io.ObjectStreamException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import com.ibm.streamsx.topology.function.UnaryOperator;

public class Delay<T> implements UnaryOperator<T> {
    private static final long serialVersionUID = 1L;

    private long time;
    private transient CountDownLatch delayLatch;
    private transient TimerTask task;

    public Delay(long time) {
        this.time = time;
    }

    public Object readResolve() throws ObjectStreamException {
        delayLatch = new CountDownLatch(1);

        task = new TimerTask() {

            @Override
            public void run() {
                delayLatch.countDown();
            }
        };

        return this;
    }

    @Override
    public T apply(T tuple) {
        if(delayLatch.getCount() > 0) {
            Timer t = new Timer();
            t.schedule(task, time);
            try {
                // latch will be released once 
                // the task runs after the specified delay
                delayLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return tuple;
    }

}
