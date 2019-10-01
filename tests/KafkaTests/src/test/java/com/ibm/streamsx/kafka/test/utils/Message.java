package com.ibm.streamsx.kafka.test.utils;

import java.io.Serializable;

/**
 * immutable class that holds a key and a value (message)
 *
 * @param <K> The key class
 * @param <V> The message class
 */
public class Message<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final K key;
    private final V value;

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
