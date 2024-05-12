package com.sanskarxrawat.bytevault.io;


import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.sanskarxrawat.bytevault.exception.file.FileDeleteException;
import com.sanskarxrawat.bytevault.log.FileLog;
import com.sanskarxrawat.bytevault.storage.file.FileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class VaultIO implements FileIO{

    private static final Logger LOGGER = LoggerFactory.getLogger(VaultIO.class);
    private static final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final String fileLogDirectory;
    private File activeFile;
    private final FileManager fileManager;
    private final Integer fileSizeLimit;
    private final Map<String, RandomAccessFile> fileAccessors;


    public VaultIO(String fileLogDirectory,FileManager fileManager,Integer fileSizeLimit) throws IOException {
        this.fileLogDirectory = fileLogDirectory;
        this.fileManager = fileManager;
        this.fileSizeLimit = fileSizeLimit;
        this.fileAccessors = new ConcurrentHashMap<>();
        createNewActiveFile(fileLogDirectory);
    }
    @Override
    public Future<String> read(String filepath, int offset, int length) throws IOException, InterruptedException {
        return executorService.submit(() -> readAsync(filepath, offset, length));
    }

    @Override
    public void removeFile(File file) {
        if (fileAccessors.containsKey(file.getPath())) {
            RandomAccessFile randomAccessFile = fileAccessors.remove(file.getPath());
            try {
                randomAccessFile.getChannel().close();
                randomAccessFile.close();
            } catch (IOException e) {
                throw new FileDeleteException("Failed to delete file: "+file.getPath());
            }
        }
    }

    @Override
    public WriteResult writeTo(FileLog data, File file, FileChannel channel) throws IOException {
        final byte[] fileLogBytes = data.toBytes();
        final int valueByteOffset = (int)channel.size() + 20 + data.getKey().getBytes().length;
        channel.write(ByteBuffer.wrap(Bytes.concat(Ints.toByteArray(fileLogBytes.length), fileLogBytes)));
        return new WriteResult(file.getPath(), valueByteOffset);
    }

    @Override
    public File[] getStoredFiles(String storageDirectory) {
        final File storageDir = new File(storageDirectory);
        return storageDir.listFiles();
    }

    private void createNewActiveFile(String fileLogDirectory) throws IOException {
        activeFile = new File(fileLogDirectory + FileSystems.getDefault().getSeparator() + fileManager.format(System.currentTimeMillis()));
        if (!activeFile.createNewFile()) {
            throw new IOException("Failed to create new active file: " + activeFile.getPath());
        }
        fileAccessors.put(activeFile.getPath(), new RandomAccessFile(activeFile.getPath(), "rw"));
    }

    private String readAsync(String filepath, int offset, int length) throws IOException {
        RandomAccessFile accessFile = getFileAccessor(filepath);
        byte[] data = new byte[length];
        accessFile.seek(offset);
        accessFile.readFully(data, 0, length);
        return new String(data, StandardCharsets.UTF_8);
    }

    private RandomAccessFile getFileAccessor(String filepath) throws IOException {
        return fileAccessors.computeIfAbsent(filepath, key -> {
            try {
                return new RandomAccessFile(filepath, "rw");
            } catch (IOException e) {
                throw new RuntimeException("Failed to create RandomAccessFile for path: " + filepath, e);
            }
        });
    }

    @Override
    public WriteResult write(FileLog data) throws IOException {
        if (this.activeFile.length() >= this.fileSizeLimit) {
            createNewActiveFile(this.fileLogDirectory);
        }
        return writeTo(data, this.activeFile, this.fileAccessors.get(this.activeFile.getPath()).getChannel());
    }
}
