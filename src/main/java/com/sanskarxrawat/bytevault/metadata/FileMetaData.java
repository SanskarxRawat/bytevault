package com.sanskarxrawat.bytevault.metadata;


public class FileMetaData {
    private final String filePath;
    private final int valueByteOffset;
    private final int valueSize;
    private final long timestamp;

    public FileMetaData(String filePath, int valueByteOffset, int valueSize, long timestamp) {
        this.filePath = filePath;
        this.valueByteOffset = valueByteOffset;
        this.valueSize = valueSize;
        this.timestamp = timestamp;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getValueByteOffset() {
        return valueByteOffset;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
