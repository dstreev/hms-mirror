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

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-friendly RocksDB manager that ensures proper cleanup in test environments
 * where Spring shutdown hooks may not execute reliably.
 */
@Component
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class RocksDBTestManager {

    private final RocksDBConfig rocksDBConfig;
    private final AtomicBoolean cleanupExecuted = new AtomicBoolean(false);
    private final Thread shutdownHook;

    @Autowired
    public RocksDBTestManager(RocksDBConfig rocksDBConfig) {
        this.rocksDBConfig = rocksDBConfig;
        
        // Register JVM shutdown hook as backup for test environments
        this.shutdownHook = new Thread(this::forceCleanup, "RocksDB-Cleanup-Hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        
        log.info("RocksDBTestManager initialized with JVM shutdown hook");
    }

    /**
     * Force cleanup of RocksDB resources. Safe to call multiple times.
     * This method should be called from test cleanup methods (@AfterEach, @AfterAll, etc.)
     */
    public void forceCleanup() {
        if (cleanupExecuted.compareAndSet(false, true)) {
            log.info("RocksDBTestManager: Force cleaning up RocksDB resources");
            try {
                // Call the RocksDB cleanup directly
                rocksDBConfig.cleanup();
                
                // Additional aggressive cleanup for tests
                performAdditionalTestCleanup();
                
                log.info("RocksDBTestManager: Force cleanup completed successfully");
            } catch (Exception e) {
                log.error("RocksDBTestManager: Error during force cleanup", e);
            }
        } else {
            log.debug("RocksDBTestManager: Cleanup already executed, skipping");
        }
    }

    /**
     * Reset the cleanup state for reuse in test scenarios.
     * Call this from test setup methods (@BeforeEach) if you need to reuse RocksDB.
     */
    public void resetCleanupState() {
        cleanupExecuted.set(false);
        log.debug("RocksDBTestManager: Cleanup state reset");
    }

    /**
     * Perform additional cleanup specific to test environments
     */
    private void performAdditionalTestCleanup() {
        try {
            // Force garbage collection to release native resources
            System.gc();
            Thread.sleep(100);
            
            // Additional aggressive lock file cleanup for tests
            String rocksDBPath = getRocksDBPath();
            if (rocksDBPath != null) {
                File lockFile = new File(rocksDBPath, "LOCK");
                if (lockFile.exists()) {
                    log.warn("RocksDBTestManager: Attempting to remove persistent lock file for tests");
                    // Wait a bit longer for native cleanup
                    Thread.sleep(200);
                    
                    if (lockFile.delete()) {
                        log.info("RocksDBTestManager: Successfully removed persistent lock file");
                    } else {
                        log.warn("RocksDBTestManager: Could not remove persistent lock file: {}", lockFile.getAbsolutePath());
                    }
                }
                
                // Optionally clean up the entire data directory for fresh tests
                if (isAggressiveCleanupEnabled()) {
                    File dataDir = new File(rocksDBPath);
                    if (dataDir.exists() && dataDir.isDirectory()) {
                        log.info("RocksDBTestManager: Performing aggressive cleanup of data directory");
                        deleteDirectory(dataDir);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("RocksDBTestManager: Error during additional test cleanup", e);
        }
    }

    private String getRocksDBPath() {
        try {
            // Use reflection to access the private rocksDBPath field
            var field = RocksDBConfig.class.getDeclaredField("rocksDBPath");
            field.setAccessible(true);
            return (String) field.get(rocksDBConfig);
        } catch (Exception e) {
            log.warn("RocksDBTestManager: Could not access rocksDBPath field", e);
            return null;
        }
    }

    private boolean isAggressiveCleanupEnabled() {
        return Boolean.parseBoolean(System.getProperty("hms-mirror.rocksdb.aggressive-test-cleanup", "false"));
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    if (!file.delete()) {
                        log.warn("RocksDBTestManager: Could not delete file: {}", file.getAbsolutePath());
                    }
                }
            }
        }
        if (!directory.delete()) {
            log.warn("RocksDBTestManager: Could not delete directory: {}", directory.getAbsolutePath());
        }
    }

    @PreDestroy
    public void springCleanup() {
        log.info("RocksDBTestManager: Spring @PreDestroy called");
        forceCleanup();
        
        // Remove shutdown hook since Spring cleanup executed
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            log.debug("RocksDBTestManager: Removed JVM shutdown hook");
        } catch (IllegalStateException e) {
            // JVM is already shutting down, hook will execute anyway
            log.debug("RocksDBTestManager: Could not remove shutdown hook (JVM shutting down)");
        }
    }
}