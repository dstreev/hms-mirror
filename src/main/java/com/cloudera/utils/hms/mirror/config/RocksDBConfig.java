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

/**
 * RocksDB Configuration for HMS-Mirror application.
 * 
 * This configuration provides embedded RocksDB storage with support for:
 * - Multiple concurrent sessions
 * - Multi-threaded access
 * - Column families for different data types (Configurations, Sessions, Connections)
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

    private RocksDB rocksDB;
    private ColumnFamilyHandle configurationsColumnFamily;
    private ColumnFamilyHandle sessionsColumnFamily;
    private ColumnFamilyHandle connectionsColumnFamily;

    public RocksDBConfig() {
        System.out.println("=== RocksDBConfig CONSTRUCTOR CALLED ===");
        log.info("=== RocksDBConfig constructor called - class is being instantiated ===");
    }

    @PostConstruct
    public void init() {
        System.out.println("=== RocksDBConfig @PostConstruct STARTING ===");
        try {
            log.info("=============================================================");
            log.info("=== RocksDB Configuration @PostConstruct Starting ===");
            log.info("=============================================================");
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

            // Log lock file location for troubleshooting
            File lockFile = new File(dataDir, "LOCK");
            log.info("RocksDB lock file location: {}", lockFile.getAbsolutePath());

            // Log configuration parameters
            log.info("RocksDB Configuration:");
            log.info("  - Max background jobs: {}", maxBackgroundJobs);
            log.info("  - Write buffer size: {} bytes ({} MB)", writeBufferSize, writeBufferSize / (1024 * 1024));
            log.info("  - Max write buffers: {}", maxWriteBuffers);
            log.info("  - Parallelism: {} threads", Runtime.getRuntime().availableProcessors());

            // Configure column families
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                new ColumnFamilyDescriptor("configurations".getBytes()),
                new ColumnFamilyDescriptor("sessions".getBytes()),
                new ColumnFamilyDescriptor("connections".getBytes())
            );

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

            log.info("✓ RocksDB database opened successfully");
            log.info("✓ Column families initialized: default, configurations, sessions, connections");

        } catch (RocksDBException e) {
            if (e.getMessage() != null && e.getMessage().contains("lock")) {
                log.error("=============================================================");
                log.error("❌ RocksDB INITIALIZATION FAILED - DATABASE LOCKED");
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
                log.error("❌ Failed to initialize RocksDB", e);
            }
            throw new RuntimeException("RocksDB initialization failed", e);
        }
        
        log.info("=============================================================");
        log.info("✅ RocksDB INITIALIZATION COMPLETE");
        log.info("Database is ready for multi-session and multi-threaded access");
        log.info("Available REST endpoints:");
        log.info("  - Health: /api/v1/rocksdb/health");
        log.info("  - Statistics: /api/v1/rocksdb/statistics");
        log.info("  - Data API: /api/v1/rocksdb/data/*");
        log.info("=============================================================");
        
        System.out.println("=== RocksDBConfig @PostConstruct COMPLETED SUCCESSFULLY ===");
    }

    @PreDestroy
    public void cleanup() {
        log.info("=============================================================");
        log.info("=== RocksDB Configuration @PreDestroy Starting ===");
        log.info("=============================================================");
        
        try {
            // Close column families in reverse order
            if (connectionsColumnFamily != null) {
                connectionsColumnFamily.close();
                log.info("✓ Closed connections column family");
            }
            if (sessionsColumnFamily != null) {
                sessionsColumnFamily.close();
                log.info("✓ Closed sessions column family");
            }
            if (configurationsColumnFamily != null) {
                configurationsColumnFamily.close();
                log.info("✓ Closed configurations column family");
            }
            
            // Close the main RocksDB instance
            if (rocksDB != null) {
                rocksDB.close();
                log.info("✓ RocksDB database closed successfully");
            }
            
            // Log lock file status after shutdown
            File dataDir = new File(rocksDBPath);
            File lockFile = new File(dataDir, "LOCK");
            
            if (lockFile.exists()) {
                log.info("RocksDB lock file still exists at: {}", lockFile.getAbsolutePath());
                log.info("This is normal - the lock file will be automatically removed when the JVM exits");
            } else {
                log.info("RocksDB lock file not found at: {}", lockFile.getAbsolutePath());
            }
            
            log.info("RocksDB data directory: {}", dataDir.getAbsolutePath());
            log.info("All RocksDB resources have been properly released");
            
        } catch (Exception e) {
            log.error("Error during RocksDB shutdown", e);
        }
        
        log.info("=============================================================");
        log.info("✅ RocksDB SHUTDOWN COMPLETE");
        log.info("=============================================================");
    }

    @Bean
    public RocksDB rocksDB() {
        return rocksDB;
    }

    @Bean("configurationsColumnFamily")
    public ColumnFamilyHandle configurationsColumnFamily() {
        return configurationsColumnFamily;
    }

    @Bean("sessionsColumnFamily") 
    public ColumnFamilyHandle sessionsColumnFamily() {
        return sessionsColumnFamily;
    }

    @Bean("connectionsColumnFamily")
    public ColumnFamilyHandle connectionsColumnFamily() {
        return connectionsColumnFamily;
    }

    @Bean("rocksDBObjectMapper")
    public ObjectMapper rocksDBObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}