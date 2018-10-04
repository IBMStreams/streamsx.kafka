package com.ibm.streamsx.kafka.test.utils;

import java.io.Serializable;

public class Message<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;
    private K key;
    private V value;

    public Message(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

}
