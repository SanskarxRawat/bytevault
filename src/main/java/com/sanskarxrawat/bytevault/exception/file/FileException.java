package com.sanskarxrawat.bytevault.exception.file;

public class FileException extends RuntimeException {
    public FileException(String msg) {
        super(msg);
    }

    public FileException(String msg, Throwable t) {
        super(msg, t);
    }
}
