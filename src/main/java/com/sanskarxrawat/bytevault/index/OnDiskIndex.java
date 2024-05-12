package com.sanskarxrawat.bytevault.index;

import com.sanskarxrawat.bytevault.datastructure.BTree;
import com.sanskarxrawat.bytevault.datastructure.ByteArrayWrapper;
import com.sanskarxrawat.bytevault.metadata.FileMetaData;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


public class OnDiskIndex {
    private static final String INDEX_FILE_NAME = "index.db";
    private final BTree<ByteArrayWrapper, FileMetaData> btree;
    private final Executor executor;

    public OnDiskIndex(String storageDir) {
        File indexFile = new File(storageDir, INDEX_FILE_NAME);
        this.btree = new BTree<>(indexFile);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void put(ByteArrayWrapper key, FileMetaData metaData) {
        executor.execute(() -> {
            try {
                btree.insert(key, metaData);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public FileMetaData get(ByteArrayWrapper key) {
        return btree.get(key);
    }

    public void delete(ByteArrayWrapper key) {
        executor.execute(() -> btree.delete(key));
    }

    public void close() throws IOException {
        btree.close();
    }
}