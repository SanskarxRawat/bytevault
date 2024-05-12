package com.sanskarxrawat.bytevault.kv;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface KeyValueStore<K,V> {

    V get(K key) throws IOException, InterruptedException, ExecutionException;

    void set(K key, V value) throws IOException;

    void remove(K key) throws IOException;
}
