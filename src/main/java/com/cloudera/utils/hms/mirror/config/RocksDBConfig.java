/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.mirror.domain.support.RocksDBColumnFamily;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RocksDB Configuration for HMS-Mirror application.
 * 
 * This configuration provides embedded RocksDB storage with support for:
 * - Multiple concurrent sessions
 * - Multi-threaded access
 * - Column families for different data types (Configurations, Sessions, Connections, Datasets)
 * - Automatic lifecycle management (startup/shutdown)
 * - Lock handling with informative error messages
 * 
 * Configuration properties:
 * - hms-mirror.rocksdb.enabled: Enable/disable RocksDB (default: false)
 * - hms-mirror.rocksdb.path: Database path (default: ${user.home}/.hms-mirror/data)
 * - hms-mirror.rocksdb.max-background-jobs: Background compaction threads (default: 4)
 * - hms-mirror.rocksdb.write-buffer-size: Memory buffer size in bytes (default: 64MB)
 * - hms-mirror.rocksdb.max-write-buffers: Number of write buffers (default: 3)
 */
@Configuration
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class RocksDBConfig {
    
    static {
        System.out.println("=== RocksDBConfig CLASS LOADED ===");
    }

    @Value("${hms-mirror.rocksdb.path:${user.home}/.hms-mirror/data}")
    private String rocksDBPath;

    @Value("${hms-mirror.rocksdb.max-background-jobs:4}")
    private int maxBackgroundJobs;

    @Value("${hms-mirror.rocksdb.write-buffer-size:67108864}")  // 64MB default
    private long writeBufferSize;

    @Value("${hms-mirror.rocksdb.max-write-buffers:3}")
    private int maxWriteBuffers;

    @Value("${hms-mirror.rocksdb.test-mode:false}")
    private boolean testMode;

    private RocksDB rocksDB;
    private ColumnFamilyHandle configurationsColumnFamily;
    private ColumnFamilyHandle sessionsColumnFamily;
    private ColumnFamilyHandle connectionsColumnFamily;
    private ColumnFamilyHandle datasetsColumnFamily;
    private ColumnFamilyHandle jobsColumnFamily;
    
    // Thread-safety for cleanup
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final ReentrantLock cleanupLock = new ReentrantLock();

    public RocksDBConfig() {
        System.out.println("=== RocksDBConfig CONSTRUCTOR CALLED ===");
        log.info("=== RocksDBConfig constructor called - class is being instantiated ===");
    }

    @PostConstruct
    public void init() {
        System.out.println("=== RocksDBConfig @PostConstruct STARTING ===");
        log.info("=============================================================");
        log.info("=== RocksDB Configuration @PostConstruct Starting ===");
        log.info("=============================================================");
        
        final int maxRetries = 15;
        final long retryDelayMs = 100;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("RocksDB initialization attempt {} of {}", attempt, maxRetries);
                initializeRocksDB();
                
                log.info("=============================================================");
                log.info("✅ RocksDB INITIALIZATION COMPLETE (attempt {})", attempt);
                log.info("Database is ready for multi-session and multi-threaded access");
                log.info("Available REST endpoints:");
                log.info("  - Health: /api/v1/rocksdb/health");
                log.info("  - Statistics: /api/v1/rocksdb/statistics");
                log.info("  - Data API: /api/v1/rocksdb/data/*");
                log.info("=============================================================");
                
                System.out.println("=== RocksDBConfig @PostConstruct COMPLETED SUCCESSFULLY ===");
                return; // Success - exit retry loop
                
            } catch (RocksDBException e) {
                if (attempt == maxRetries) {
                    // Last attempt failed - log final error and throw
                    if (e.getMessage() != null && e.getMessage().contains("lock")) {
                        log.error("=============================================================");
                        log.error("❌ RocksDB INITIALIZATION FAILED - DATABASE LOCKED (after {} attempts)", maxRetries);
                        log.error("=============================================================");
                        log.error("Another instance of hms-mirror may be running or was not properly shut down.");
                        log.error("To resolve this issue:");
                        log.error("1. Check for running hms-mirror processes: ps aux | grep hms-mirror");
                        log.error("2. Stop any running instances gracefully");
                        log.error("3. If no processes are running, remove the lock file:");
                        log.error("   rm -f {}LOCK", rocksDBPath.endsWith("/") ? rocksDBPath : rocksDBPath + "/");
                        log.error("4. Restart the application");
                        log.error("=============================================================");
                    } else {
                        log.error("❌ Failed to initialize RocksDB after {} attempts", maxRetries, e);
                    }
                    throw new RuntimeException("RocksDB initialization failed after " + maxRetries + " attempts", e);
                } else {
                    // Retry attempt failed - log warning and continue
                    log.warn("RocksDB initialization attempt {} failed: {} - retrying in {}ms...", 
                             attempt, e.getMessage(), retryDelayMs);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("RocksDB initialization interrupted during retry", ie);
                    }
                }
            }
        }
    }

    private void initializeRocksDB() throws RocksDBException {
        // Load the RocksDB C++ library
        RocksDB.loadLibrary();
        log.info("✓ RocksDB native library loaded successfully");

        // Resolve and log the full data directory path
        File dataDir = new File(rocksDBPath);
        String absolutePath = dataDir.getAbsolutePath();
        log.info("RocksDB data directory: {}", absolutePath);

        // Create data directory if it doesn't exist
        if (!dataDir.exists()) {
            if (dataDir.mkdirs()) {
                log.info("✓ Created RocksDB data directory: {}", absolutePath);
            } else {
                throw new RuntimeException("Failed to create RocksDB data directory: " + absolutePath);
            }
        } else {
            log.info("✓ Using existing RocksDB data directory: {}", absolutePath);
        }

        // Check for stale lock file and attempt cleanup in test environments
        File lockFile = new File(dataDir, "LOCK");
        log.info("RocksDB lock file location: {}", lockFile.getAbsolutePath());
        
        if (lockFile.exists() && isTestEnvironment()) {
            log.warn("Stale lock file found in test environment: {}", lockFile.getAbsolutePath());
            if (lockFile.delete()) {
                log.info("✓ Removed stale lock file for test environment");
            } else {
                log.warn("⚠ Could not remove stale lock file - will attempt normal initialization");
            }
        }

        // Log configuration parameters
        log.info("RocksDB Configuration:");
        log.info("  - Max background jobs: {}", maxBackgroundJobs);
        log.info("  - Write buffer size: {} bytes ({} MB)", writeBufferSize, writeBufferSize / (1024 * 1024));
        log.info("  - Max write buffers: {}", maxWriteBuffers);
        log.info("  - Parallelism: {} threads", Runtime.getRuntime().availableProcessors());

        // Configure column families using centralized enum
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = RocksDBColumnFamily.getAllDescriptors();

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        // Configure RocksDB options for multi-threaded access
        DBOptions dbOptions = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setMaxBackgroundJobs(maxBackgroundJobs)
            .setIncreaseParallelism(Runtime.getRuntime().availableProcessors());

        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
            .setWriteBufferSize(writeBufferSize)
            .setMaxWriteBufferNumber(maxWriteBuffers);

        log.info("Opening RocksDB database...");
        // Open RocksDB with column families
        rocksDB = RocksDB.open(dbOptions, rocksDBPath, columnFamilyDescriptors, columnFamilyHandles);

        // Store column family handles
        configurationsColumnFamily = columnFamilyHandles.get(1); // configurations
        sessionsColumnFamily = columnFamilyHandles.get(2); // sessions
        connectionsColumnFamily = columnFamilyHandles.get(3); // connections
        datasetsColumnFamily = columnFamilyHandles.get(4); // datasets
        jobsColumnFamily = columnFamilyHandles.get(5); // jobs

        log.info("✓ RocksDB database opened successfully");
        log.info("✓ Column families initialized: default, configurations, sessions, connections, datasets, jobs");
    }

    @PreDestroy
    public void cleanup() {
        // Use atomic operation to ensure cleanup happens only once
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("RocksDB cleanup already in progress or completed");
            return;
        }
        
        cleanupLock.lock();
        try {
            log.info("=============================================================");
            log.info("=== RocksDB Configuration @PreDestroy Starting ===");
            log.info("=============================================================");
            
            // Safe shutdown of RocksDB with proper resource management
            performSafeShutdown();
            
        } catch (Exception e) {
            log.error("Error during RocksDB cleanup", e);
        } finally {
            cleanupLock.unlock();
        }
        
        log.info("=============================================================");
        log.info("✅ RocksDB SHUTDOWN COMPLETE");
        log.info("=============================================================");
    }
    
    /**
     * Perform safe shutdown of RocksDB resources without accessing potentially invalid handles
     */
    private void performSafeShutdown() {
        try {
            // Cancel background work BEFORE checking properties to avoid race conditions
            if (rocksDB != null && rocksDB.isOwningHandle()) {
                try {
                    log.info("Cancelling RocksDB background jobs before shutdown");
                    rocksDB.cancelAllBackgroundWork(true); // true = wait for jobs to finish
                    log.info("✓ Background jobs cancelled");
                } catch (Exception e) {
                    log.warn("Could not cancel background work: {}", e.getMessage());
                }
                
                // Force flush any pending writes
                try {
                    rocksDB.flush(new FlushOptions().setWaitForFlush(true));
                    log.info("✓ Forced flush of pending writes");
                } catch (Exception e) {
                    log.warn("Could not flush pending writes: {}", e.getMessage());
                }
            }
            
            // Close column families in reverse order
            closeColumnFamilyHandle("jobs", jobsColumnFamily);
            closeColumnFamilyHandle("datasets", datasetsColumnFamily);
            closeColumnFamilyHandle("connections", connectionsColumnFamily);
            closeColumnFamilyHandle("sessions", sessionsColumnFamily);
            closeColumnFamilyHandle("configurations", configurationsColumnFamily);
            
            // Null out column family references before closing main DB
            jobsColumnFamily = null;
            datasetsColumnFamily = null;
            connectionsColumnFamily = null;
            sessionsColumnFamily = null;
            configurationsColumnFamily = null;
            
            // Close the main RocksDB instance
            if (rocksDB != null) {
                try {
                    if (rocksDB.isOwningHandle()) {
                        rocksDB.close();
                        log.info("✓ RocksDB database closed successfully");
                    } else {
                        log.warn("RocksDB handle was already closed");
                    }
                } catch (Exception e) {
                    log.warn("Error closing RocksDB: {}", e.getMessage());
                } finally {
                    rocksDB = null;
                }
            }
            
            // Force garbage collection to help release native resources
            System.gc();
            log.info("✓ Requested garbage collection to release native resources");
            
            // Wait a brief moment for native cleanup
            Thread.sleep(100);
            
            // Check lock file status and attempt cleanup for test scenarios
            performLockFileCleanup();
            
        } catch (Exception e) {
            log.error("Error during safe RocksDB shutdown", e);
        }
    }
    
    /**
     * Safely close a column family handle
     */
    private void closeColumnFamilyHandle(String name, ColumnFamilyHandle handle) {
        if (handle != null) {
            try {
                if (handle.isOwningHandle()) {
                    handle.close();
                    log.info("✓ Closed {} column family", name);
                } else {
                    log.warn("{} column family handle was already closed", name);
                }
            } catch (Exception e) {
                log.warn("Error closing {} column family: {}", name, e.getMessage());
            }
        }
    }
    
    /**
     * Perform lock file cleanup for test environments
     */
    private void performLockFileCleanup() {
        try {
            File dataDir = new File(rocksDBPath);
            File lockFile = new File(dataDir, "LOCK");
            
            if (lockFile.exists()) {
                log.info("RocksDB lock file still exists at: {}", lockFile.getAbsolutePath());
                
                // In test scenarios, try to remove stale lock file after a brief wait
                boolean isTestEnvironment = isTestEnvironment();
                if (isTestEnvironment) {
                    log.info("Test environment detected - attempting lock file cleanup");
                    Thread.sleep(200); // Additional wait for native cleanup
                    
                    if (lockFile.delete()) {
                        log.info("✓ Successfully removed stale lock file for test environment");
                    } else {
                        log.warn("⚠ Could not remove lock file - may be held by another process");
                    }
                } else {
                    log.info("Production environment - lock file will be automatically removed when JVM exits");
                }
            } else {
                log.info("✓ RocksDB lock file already cleaned up");
            }
            
            log.info("RocksDB data directory: {}", dataDir.getAbsolutePath());
            log.info("All RocksDB resources have been properly released");
        } catch (Exception e) {
            log.warn("Error during lock file cleanup: {}", e.getMessage());
        }
            
    }

    private boolean isTestEnvironment() {
        // Check explicit test mode configuration first
        if (testMode) {
            return true;
        }
        
        // Check for common test indicators
        String[] testClassPaths = {"junit", "testng", "surefire", "test"};
        String classPath = System.getProperty("java.class.path", "");
        for (String testPath : testClassPaths) {
            if (classPath.toLowerCase().contains(testPath)) {
                return true;
            }
        }
        
        // Check for test system properties
        return System.getProperty("test.environment") != null || 
               System.getProperty("maven.test.skip") != null ||
               Boolean.parseBoolean(System.getProperty("spring.test.context", "false"));
    }

    @Bean
    public RocksDB rocksDB() {
        if (isShutdown.get()) {
            log.warn("RocksDB bean requested after shutdown - returning null");
            return null;
        }
        return rocksDB;
    }

    @Bean("configurationsColumnFamily")
    public ColumnFamilyHandle configurationsColumnFamily() {
        if (isShutdown.get()) {
            log.warn("Configurations column family bean requested after shutdown - returning null");
            return null;
        }
        return configurationsColumnFamily;
    }

    @Bean("sessionsColumnFamily") 
    public ColumnFamilyHandle sessionsColumnFamily() {
        if (isShutdown.get()) {
            log.warn("Sessions column family bean requested after shutdown - returning null");
            return null;
        }
        return sessionsColumnFamily;
    }

    @Bean("connectionsColumnFamily")
    public ColumnFamilyHandle connectionsColumnFamily() {
        if (isShutdown.get()) {
            log.warn("Connections column family bean requested after shutdown - returning null");
            return null;
        }
        return connectionsColumnFamily;
    }

    @Bean("datasetsColumnFamily")
    public ColumnFamilyHandle datasetsColumnFamily() {
        if (isShutdown.get()) {
            log.warn("Datasets column family bean requested after shutdown - returning null");
            return null;
        }
        return datasetsColumnFamily;
    }

    @Bean("jobsColumnFamily")
    public ColumnFamilyHandle jobsColumnFamily() {
        if (isShutdown.get()) {
            log.warn("Jobs column family bean requested after shutdown - returning null");
            return null;
        }
        return jobsColumnFamily;
    }

    @Bean("rocksDBObjectMapper")
    public ObjectMapper rocksDBObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}