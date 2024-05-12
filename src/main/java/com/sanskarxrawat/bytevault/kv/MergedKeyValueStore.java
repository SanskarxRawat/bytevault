package com.sanskarxrawat.bytevault.kv;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface MergedKeyValueStore <K,V> extends KeyValueStore<K,V>{

    void merge() throws IOException, ExecutionException, InterruptedException;

}
