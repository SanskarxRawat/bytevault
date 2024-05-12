package com.sanskarxrawat.bytevault.config;

import com.sanskarxrawat.bytevault.constant.VaultConstants;
import com.sanskarxrawat.bytevault.exception.storage.InvalidConfigStorageException;

import static com.sanskarxrawat.bytevault.constant.VaultConstants.*;

public class VaultConfig {

    private VaultConfig() {
    }

    private String storageDir;
    private Integer fileSizeLimit;
    private Boolean cacheEnabled;
    private Integer cacheSize;
    private Integer mergePeriodMils;

    protected VaultConfig(Builder builder) {
        this.storageDir = builder.storageDir;
        this.fileSizeLimit = builder.fileSizeLimit;
        this.cacheEnabled = builder.cacheEnabled;
        this.cacheSize = builder.cacheSize;
        this.mergePeriodMils = builder.mergePeriodMils;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getStorageDir() {
        return storageDir;
    }

    public Integer getFileSizeLimit() {
        return fileSizeLimit;
    }

    public Boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public Integer getCacheSize() {
        return cacheSize;
    }

    public Integer getMergePeriodMils() {
        return mergePeriodMils;
    }

    public static class Builder {

        private String storageDir = VaultConstants.DEFAULT_STORAGE_DIR;
        private Integer fileSizeLimit = DEFAULT_FILE_SIZE_LIMIT;
        private Boolean cacheEnabled = DEFAULT_CACHE_ENABLED;
        private Integer cacheSize = DEFAULT_CACHE_SIZE;
        private Integer mergePeriodMils = DEFAULT_MERGE_PERIOD_MILS;

        public Builder storageDir(String storageDir) {
            this.storageDir = storageDir;
            return this;
        }

        public Builder fileSizeLimit(int fileSizeLimit) {
            this.fileSizeLimit = fileSizeLimit;
            return this;
        }

        public Builder cacheEnabled(boolean cacheEnabled) {
            this.cacheEnabled = cacheEnabled;
            return this;
        }

        public Builder cacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder mergePeriodMils(int mergePeriodMils) {
            this.mergePeriodMils = mergePeriodMils;
            return this;
        }

        public VaultConfig build() {
            if (storageDir == null || storageDir.isEmpty()) {
                throw new InvalidConfigStorageException("Invalid config storageDir = " + storageDir);
            } else if (fileSizeLimit <= 0) {
                throw new InvalidConfigStorageException("Invalid config fileSizeLimit = " + fileSizeLimit);
            } else if (cacheEnabled && cacheSize <= 0) {
                throw new InvalidConfigStorageException("Invalid config cacheSize = " + cacheSize);
            } else if (mergePeriodMils <= 0) {
                throw new InvalidConfigStorageException("Invalid config mergePeriodMils = " + mergePeriodMils);
            }

            return new VaultConfig(this);
        }

    }
}