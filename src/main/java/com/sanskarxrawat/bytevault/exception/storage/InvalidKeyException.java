package com.sanskarxrawat.bytevault.exception.storage;

public class InvalidKeyException extends StorageException {
    public InvalidKeyException(String msg) {
        super(msg);
    }

    public InvalidKeyException(String msg, Throwable t) {
        super(msg, t);
    }
}
