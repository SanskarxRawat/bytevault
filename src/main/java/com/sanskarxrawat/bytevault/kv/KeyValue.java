package com.sanskarxrawat.bytevault.kv;

public abstract class KeyValue<K, V> {
    protected final K key;
    protected final V value;

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    protected KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }
}