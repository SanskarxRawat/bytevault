package com.sanskarxrawat.bytevault.exception.file;

public class FileReadException extends FileException{
    public FileReadException(String msg) {
        super(msg);
    }

    public FileReadException(String msg, Throwable t) {
        super(msg, t);
    }
}
