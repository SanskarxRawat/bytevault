package com.sanskarxrawat.bytevault.io;

import com.sanskarxrawat.bytevault.log.FileLog;
import com.sanskarxrawat.bytevault.storage.file.VaultFile;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.Future;

public interface FileIO extends KeyValueWriter<FileLog,WriteResult> {

    Future<String> read(String filepath, int offset, int length) throws IOException, InterruptedException;

    void removeFile(File file);

    WriteResult writeTo(FileLog data, File file, FileChannel channel) throws IOException;

    File[] getStoredFiles(String storageDirectory);

}
