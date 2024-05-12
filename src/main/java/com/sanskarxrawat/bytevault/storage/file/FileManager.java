package com.sanskarxrawat.bytevault.storage.file;

import java.io.File;

public interface FileManager<T> {
    enum SortType {
        ASC,
        DESC
    }

    String format(T obj);

    Integer compare(File f1, File f2, SortType sortType);

    T parseFromFileName(String fileName);
}
