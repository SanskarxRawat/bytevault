package com.sanskarxrawat.bytevault.exception.file;

public class FileDeleteException extends FileException {
    public FileDeleteException(String msg) {
        super(msg);
    }

    public FileDeleteException(String msg, Throwable t) {
        super(msg, t);
    }
}
