package com.sanskarxrawat.bytevault.io;

public record WriteResult(String writeFilePath, int valueByteOffset) implements KeyValueWriteResult {

}
