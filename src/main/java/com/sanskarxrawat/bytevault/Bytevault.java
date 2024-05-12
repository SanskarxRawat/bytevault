package com.sanskarxrawat.bytevault;


import com.google.common.primitives.Ints;
import com.sanskarxrawat.bytevault.config.VaultConfig;
import com.sanskarxrawat.bytevault.datastructure.ByteArrayWrapper;
import com.sanskarxrawat.bytevault.exception.storage.InvalidKeyException;
import com.sanskarxrawat.bytevault.index.OnDiskIndex;
import com.sanskarxrawat.bytevault.io.FileIO;
import com.sanskarxrawat.bytevault.io.VaultIO;
import com.sanskarxrawat.bytevault.io.WriteResult;
import com.sanskarxrawat.bytevault.kv.MergedKeyValueStore;
import com.sanskarxrawat.bytevault.log.FileLog;
import com.sanskarxrawat.bytevault.log.FileLogConstants;
import com.sanskarxrawat.bytevault.metadata.FileMetaData;
import com.sanskarxrawat.bytevault.storage.file.FileManager;
import com.sanskarxrawat.bytevault.storage.file.VaultFile;
import com.sanskarxrawat.bytevault.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.sanskarxrawat.bytevault.log.FileLogConstants.COMPACT_FILE_SUFFIX;

public class Bytevault implements MergedKeyValueStore<String,String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bytevault.class);
    private OnDiskIndex onDiskIndex;
    private final FileIO fileIO;
    private final Boolean isCacheEnabled;
    private final String STORAGE_DIRECTORY;

    private static final FileManager<?> VAULT_FILE=new VaultFile();
    public Bytevault(VaultConfig vaultConfig) throws IOException {
        FileUtils.createFileIfNotExists(vaultConfig.getStorageDir(),Boolean.TRUE);
        this.isCacheEnabled=vaultConfig.isCacheEnabled();
        this.STORAGE_DIRECTORY=vaultConfig.getStorageDir();
        if(this.isCacheEnabled){
            this.onDiskIndex=new OnDiskIndex(vaultConfig.getStorageDir());
        }
        FileManager<?> VAULT_FILE = new VaultFile();
        this.fileIO=new VaultIO(vaultConfig.getStorageDir(), VAULT_FILE, vaultConfig.getFileSizeLimit());
    }


    @Override
    public String get(String key) throws IOException, InterruptedException, ExecutionException {
        ByteArrayWrapper byteArrayWrapper=new ByteArrayWrapper(key.getBytes());
        FileMetaData fileMetaData=onDiskIndex.get(byteArrayWrapper);

        if(Objects.isNull(fileMetaData)){
            throw new InvalidKeyException(String.format("Key is not existed, key=%s", key));
        }

        String value=fileIO.read(fileMetaData.getFilePath(), fileMetaData.getValueByteOffset(), fileMetaData.getValueSize()).get();
        if (FileLogConstants.TOMBSTONE.equals(value)) {
            onDiskIndex.delete(byteArrayWrapper);
            throw new InvalidKeyException(String.format("Key was Deleted, key=%s", key));
        }
        return value;
    }

    @Override
    public void set(String key, String value) throws IOException {
        final FileLog fileLog = new FileLog(System.currentTimeMillis(), key.getBytes().length, FileLogConstants.TOMBSTONE.getBytes().length,
                key, FileLogConstants.TOMBSTONE);
        WriteResult writeResult=fileIO.write(fileLog);
        FileMetaData fileMetaData=new FileMetaData(writeResult.writeFilePath(), writeResult.valueByteOffset(), fileLog.getValueSize(), System.currentTimeMillis());
        ByteArrayWrapper byteArrayWrapper=new ByteArrayWrapper(key.getBytes());
        if(isCacheEnabled){
            onDiskIndex.put(byteArrayWrapper,fileMetaData);
        }
    }

    @Override
    public void remove(String key) throws IOException {
        ByteArrayWrapper byteArrayWrapper=new ByteArrayWrapper(key.getBytes());
        FileMetaData fileMetaData=onDiskIndex.get(byteArrayWrapper);

        if(Objects.isNull(fileMetaData)){
            throw new InvalidKeyException(String.format("Key is not existed, key=%s", key));
        }
        final FileLog fileLog = new FileLog(System.currentTimeMillis(), key.getBytes().length, FileLogConstants.TOMBSTONE.getBytes().length,
                key, FileLogConstants.TOMBSTONE);
        fileIO.write(fileLog);
        if(isCacheEnabled){
            onDiskIndex.delete(byteArrayWrapper);
        }
    }

    @Override
    public void merge() throws IOException, ExecutionException, InterruptedException {
        List<Path> mergeAbleFiles=getMergeAbleFiles();
        if (mergeAbleFiles.size() <= 1) {
            return;
        }

        Map<ByteArrayWrapper, FileMetaData> mergedIndex = buildMergedIndex(mergeAbleFiles);
        Path compactedFilePath = createCompactedFile(mergedIndex, mergeAbleFiles.get(mergeAbleFiles.size() - 1));

        updateOnDiskIndex(mergedIndex, compactedFilePath);
        deleteFiles(mergeAbleFiles);
        renameCompactedFile(compactedFilePath);
    }

    private List<Path> getMergeAbleFiles() throws IOException {
        try (Stream<Path> files = Files.list(Path.of(STORAGE_DIRECTORY))) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().endsWith(COMPACT_FILE_SUFFIX))
                    .sorted(Comparator.comparingLong(this::getFileLastModifiedTime))
                    .collect(Collectors.toList());
        }
    }

    private Map<ByteArrayWrapper, FileMetaData> buildMergedIndex(List<Path> mergeAbleFiles) throws IOException, InterruptedException, ExecutionException {
        Map<ByteArrayWrapper, FileMetaData> mergedIndex = new HashMap<>();
        for (Path filePath : mergeAbleFiles) {
            byte[] fileBytes = Files.readAllBytes(filePath);
            int byteCursor = 0;
            while (byteCursor < fileBytes.length) {
                int fileLogByteSize = Ints.fromByteArray(fileBytes);
                byteCursor += 4;

                byte[] fileLogBytes = Arrays.copyOfRange(fileBytes, byteCursor, byteCursor + fileLogByteSize);
                FileLog fileLog = FileLog.valueOf(fileLogBytes);

                int valueByteOffset = byteCursor + 16 + fileLog.getKeySize();
                ByteArrayWrapper key = new ByteArrayWrapper(fileLog.getKey().getBytes());
                Future<String> valueFuture = fileIO.read(filePath.toString(), valueByteOffset, fileLog.getValueSize());
                String value = valueFuture.get(); // Blocks until the value is read

                if (!FileLogConstants.TOMBSTONE.equals(value)) {
                    mergedIndex.put(key, new FileMetaData(filePath.toString(), valueByteOffset, fileLog.getValueSize(), fileLog.getTimestamp()));
                } else {
                    mergedIndex.remove(key);
                }

                byteCursor += fileLogByteSize;
            }
        }
        return mergedIndex;
    }

    private Path createCompactedFile(Map<ByteArrayWrapper, FileMetaData> mergedIndex, Path lastMergedFile) throws IOException, InterruptedException, ExecutionException {
        Path compactedFilePath = lastMergedFile.resolveSibling(lastMergedFile.getFileName() + COMPACT_FILE_SUFFIX);
        Files.createFile(compactedFilePath);
        for (Map.Entry<ByteArrayWrapper, FileMetaData> entry : mergedIndex.entrySet()) {
            ByteArrayWrapper key = entry.getKey();
            FileMetaData metaData = entry.getValue();
            Future<String> valueFuture = readFromFile(metaData.getFilePath(), metaData.getValueByteOffset(), metaData.getValueSize());
            String value = valueFuture.get(); // Blocks until the value is read

            FileLog fileLog = new FileLog(metaData.getTimestamp(), key.array().length, metaData.getValueSize(), new String(key.array(), StandardCharsets.UTF_8),value);
            WriteResult writeResult = fileIO.write(fileLog);
            FileMetaData updatedMetaData = new FileMetaData(compactedFilePath.toString(), writeResult.valueByteOffset(), metaData.getValueSize(),metaData.getTimestamp());
            mergedIndex.put(key, updatedMetaData);
        }
        return compactedFilePath;
    }

    private void updateOnDiskIndex(Map<ByteArrayWrapper, FileMetaData> mergedIndex, Path compactedFilePath) {
        for (Map.Entry<ByteArrayWrapper, FileMetaData> entry : mergedIndex.entrySet()) {
            onDiskIndex.put(entry.getKey(), entry.getValue());
        }
    }

    private void deleteFiles(List<Path> filePaths) throws IOException {
        for (Path filePath : filePaths) {
            Files.deleteIfExists(filePath);
        }
    }

    private void renameCompactedFile(Path compactedFilePath) throws IOException {
        Path targetFilePath = compactedFilePath.resolveSibling(compactedFilePath.getFileName().toString().replace(COMPACT_FILE_SUFFIX, ""));
        Files.move(compactedFilePath, targetFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

    private long getFileLastModifiedTime(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            LOGGER.error("Error getting last modified time for file: {}", file, e);
            return 0;
        }
    }

    private Future<String> readFromFile(String filePath, int valueByteOffset, int valueSize) throws IOException {
        try {
            return fileIO.read(filePath, valueByteOffset, valueSize);
        } catch (InterruptedException e) {
            throw new IOException("Error reading from file: " + filePath, e);
        }
    }
}