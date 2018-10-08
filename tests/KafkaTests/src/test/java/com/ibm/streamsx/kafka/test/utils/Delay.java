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
