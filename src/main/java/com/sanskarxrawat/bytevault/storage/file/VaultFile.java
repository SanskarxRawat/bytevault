package com.sanskarxrawat.bytevault.storage.file;


import java.io.File;

import static com.sanskarxrawat.bytevault.constant.VaultConstants.DELIMITER;
import static com.sanskarxrawat.bytevault.constant.VaultConstants.FILE_LOG_PREFIX;

public final class VaultFile implements FileManager<Long> {

    @Override
    public String format(Long timestamp) {
        return FILE_LOG_PREFIX + timestamp;
    }

    @Override
    public Integer compare(File f1, File f2, SortType sortType) {
        long l1 = getTimestamp(f1);
        long l2 = getTimestamp(f2);
        return SortType.DESC.equals(sortType) ? Long.compare(l2, l1) : Long.compare(l1, l2);    }

    @Override
    public Long parseFromFileName(String fileName) {
        int delimiterIndex = fileName.indexOf(DELIMITER);
        if (delimiterIndex == -1) {
            throw new IllegalArgumentException("Invalid file name format: " + fileName);
        }
        return Long.parseLong(fileName.substring(delimiterIndex + 1));
    }

    private long getTimestamp(File file) {
        return parseFromFileName(file.getName());
    }
}
