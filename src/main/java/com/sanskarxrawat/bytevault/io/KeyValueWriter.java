package com.sanskarxrawat.bytevault.io;

import com.sanskarxrawat.bytevault.kv.KeyValue;

import java.io.IOException;

public interface KeyValueWriter<T extends KeyValue, R extends KeyValueWriteResult> {
    R write(T data) throws IOException;
}
