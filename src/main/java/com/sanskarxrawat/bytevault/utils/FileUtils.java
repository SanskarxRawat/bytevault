package com.sanskarxrawat.bytevault.utils;

import com.sanskarxrawat.bytevault.exception.file.FileReadException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class FileUtils {

    private static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private FileUtils() {
    }

    public static Future<byte[]> readBytesAsync(RandomAccessFile randomAccessFile, long byteOffset, int byteLength) {
        return executorService.submit(() -> readBytes(randomAccessFile, byteOffset, byteLength));
    }

    public static byte[] readBytes(RandomAccessFile randomAccessFile, long byteOffset, int byteLength) throws InterruptedException {
        final byte[] data = new byte[byteLength];
        Runnable readTask = () -> {
            try {
                randomAccessFile.seek(byteOffset);
                randomAccessFile.readFully(data, 0, byteLength);
            } catch (IOException e) {
                throw new FileReadException(e.getMessage());
            }
        };
        // Run the read task synchronously using a virtual thread
        Thread.startVirtualThread(readTask).join();
        return data;
    }

    public static Future<Void> createFileIfNotExistsAsync(String filePath, boolean isDirectory) {
        return executorService.submit(() -> {
            createFileIfNotExists(filePath, isDirectory);
            return null;
        });
    }

    public static void createFileIfNotExists(String filePath, boolean isDirectory) throws IOException {
        final File file = new File(filePath);
        if (isDirectory) {
            file.mkdir();
        } else {
            if (!file.exists()) {
                assert file.createNewFile();
            }
        }
    }
}