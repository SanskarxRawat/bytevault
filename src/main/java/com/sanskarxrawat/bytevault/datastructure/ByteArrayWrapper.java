package com.sanskarxrawat.bytevault.datastructure;

import java.util.Arrays;

public record ByteArrayWrapper(byte[] array) implements Comparable<ByteArrayWrapper> {

    @Override
    public int compareTo(ByteArrayWrapper other) {
        return Arrays.compare(array, other.array);
    }
}
