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

package com.cloudera.utils.hms.mirror.service;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Service for managing RocksDB health, statistics, and operations.
 * 
 * Provides methods for:
 * - Health monitoring
 * - Statistics collection
 * - Compaction management
 * - Storage size tracking
 * - Performance metrics
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class RocksDBManagementService {

    private final RocksDB rocksDB;

    @Autowired
    public RocksDBManagementService(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    /**
     * Check if RocksDB is healthy and accessible.
     */
    public boolean isHealthy() {
        try {
            // Simple health check - try to access database properties
            rocksDB.getProperty("rocksdb.stats");
            return true;
        } catch (Exception e) {
            log.warn("RocksDB health check failed", e);
            return false;
        }
    }

    /**
     * Get comprehensive RocksDB statistics and health information.
     */
    public Map<String, Object> getStatistics() throws RocksDBException {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("healthy", isHealthy());
        stats.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        // Database properties
        addDatabaseProperties(stats);
        
        // Performance metrics
        addPerformanceMetrics(stats);
        
        // Storage information
        addStorageInfo(stats);
        
        // Compaction information
        addCompactionInfo(stats);
        
        return stats;
    }

    /**
     * Get storage size information for all column families.
     */
    public Map<String, Long> getStorageSizes() throws RocksDBException {
        Map<String, Long> sizes = new HashMap<>();
        
        // Total database size
        String totalSizeStr = rocksDB.getProperty("rocksdb.total-sst-files-size");
        if (totalSizeStr != null) {
            sizes.put("total_sst_files_size", Long.parseLong(totalSizeStr));
        }
        
        // Live data size
        String liveDataSizeStr = rocksDB.getProperty("rocksdb.estimate-live-data-size");
        if (liveDataSizeStr != null) {
            sizes.put("live_data_size", Long.parseLong(liveDataSizeStr));
        }
        
        // Memory usage
        String memUsageStr = rocksDB.getProperty("rocksdb.cur-size-all-mem-tables");
        if (memUsageStr != null) {
            sizes.put("memory_usage", Long.parseLong(memUsageStr));
        }
        
        return sizes;
    }

    /**
     * Trigger manual compaction to optimize storage.
     */
    public void triggerCompaction() throws RocksDBException {
        log.info("Triggering RocksDB manual compaction");
        rocksDB.compactRange();
        log.info("Manual compaction completed");
    }

    /**
     * Trigger compaction for a specific column family.
     */
    public void triggerCompaction(ColumnFamilyHandle columnFamily) throws RocksDBException {
        String cfName = getCfName(columnFamily);
        log.info("Triggering manual compaction for column family: {}", cfName);
        rocksDB.compactRange(columnFamily);
        log.info("Manual compaction completed for column family: {}", cfName);
    }

    /**
     * Get column family specific statistics.
     */
    public Map<String, Object> getColumnFamilyStatistics(ColumnFamilyHandle columnFamily) throws RocksDBException {
        Map<String, Object> stats = new HashMap<>();
        String cfName = getCfName(columnFamily);
        
        stats.put("name", cfName);
        
        // Column family specific properties
        String[] properties = {
            "rocksdb.num-files-at-level0",
            "rocksdb.num-files-at-level1", 
            "rocksdb.num-files-at-level2",
            "rocksdb.estimate-num-keys",
            "rocksdb.estimate-table-readers-mem",
            "rocksdb.cur-size-active-mem-table",
            "rocksdb.size-all-mem-tables"
        };
        
        for (String property : properties) {
            try {
                String value = rocksDB.getProperty(columnFamily, property);
                if (value != null) {
                    stats.put(property.replace("rocksdb.", ""), value);
                }
            } catch (RocksDBException e) {
                log.debug("Could not get property {} for column family {}", property, cfName);
            }
        }
        
        return stats;
    }

    /**
     * Get performance metrics for monitoring.
     */
    public Map<String, Object> getPerformanceMetrics() throws RocksDBException {
        Map<String, Object> metrics = new HashMap<>();
        
        // Read/write statistics
        String readBytes = rocksDB.getProperty("rocksdb.total-bytes-read");
        String writtenBytes = rocksDB.getProperty("rocksdb.total-bytes-written");
        
        if (readBytes != null) metrics.put("total_bytes_read", Long.parseLong(readBytes));
        if (writtenBytes != null) metrics.put("total_bytes_written", Long.parseLong(writtenBytes));
        
        // Number of keys
        String numKeys = rocksDB.getProperty("rocksdb.estimate-num-keys");
        if (numKeys != null) metrics.put("estimated_keys", Long.parseLong(numKeys));
        
        // Background errors
        String bgErrors = rocksDB.getProperty("rocksdb.background-errors");
        if (bgErrors != null) metrics.put("background_errors", Long.parseLong(bgErrors));
        
        return metrics;
    }

    /**
     * Check if compaction is pending.
     */
    public boolean isCompactionPending() throws RocksDBException {
        String compactionPending = rocksDB.getProperty("rocksdb.compaction-pending");
        return "1".equals(compactionPending);
    }

    /**
     * Get formatted storage size in human-readable format.
     */
    public String getFormattedStorageSize() throws RocksDBException {
        Map<String, Long> sizes = getStorageSizes();
        Long totalSize = sizes.get("total_sst_files_size");
        
        if (totalSize == null || totalSize == 0) {
            return "0 B";
        }
        
        return formatBytes(totalSize);
    }

    /**
     * Validate database integrity.
     */
    public Map<String, Object> validateIntegrity() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Check if database is accessible
            boolean healthy = isHealthy();
            result.put("accessible", healthy);
            
            if (healthy) {
                // Check for background errors
                String bgErrors = rocksDB.getProperty("rocksdb.background-errors");
                boolean hasErrors = bgErrors != null && !bgErrors.equals("0");
                result.put("has_background_errors", hasErrors);
                result.put("background_errors", bgErrors);
                
                // Check compaction status
                result.put("compaction_pending", isCompactionPending());
                
                result.put("status", "HEALTHY");
            } else {
                result.put("status", "UNHEALTHY");
            }
            
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("error", e.getMessage());
            log.error("Error validating RocksDB integrity", e);
        }
        
        result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        return result;
    }

    private void addDatabaseProperties(Map<String, Object> stats) throws RocksDBException {
        Map<String, String> dbProps = new HashMap<>();
        
        String[] properties = {
            "rocksdb.stats",
            "rocksdb.sstables",
            "rocksdb.num-files-at-level0",
            "rocksdb.num-files-at-level1",
            "rocksdb.num-files-at-level2",
            "rocksdb.background-errors",
            "rocksdb.compaction-pending"
        };
        
        for (String property : properties) {
            try {
                String value = rocksDB.getProperty(property);
                if (value != null) {
                    dbProps.put(property.replace("rocksdb.", ""), value);
                }
            } catch (RocksDBException e) {
                log.debug("Could not get property: {}", property);
            }
        }
        
        stats.put("database_properties", dbProps);
    }

    private void addPerformanceMetrics(Map<String, Object> stats) throws RocksDBException {
        stats.put("performance_metrics", getPerformanceMetrics());
    }

    private void addStorageInfo(Map<String, Object> stats) throws RocksDBException {
        Map<String, Long> sizes = getStorageSizes();
        stats.put("storage_info", sizes);
        stats.put("formatted_size", getFormattedStorageSize());
    }

    private void addCompactionInfo(Map<String, Object> stats) throws RocksDBException {
        Map<String, Object> compactionInfo = new HashMap<>();
        compactionInfo.put("pending", isCompactionPending());
        
        String pendingStr = rocksDB.getProperty("rocksdb.compaction-pending");
        if (pendingStr != null) {
            compactionInfo.put("pending_count", Integer.parseInt(pendingStr));
        }
        
        stats.put("compaction_info", compactionInfo);
    }

    private String getCfName(ColumnFamilyHandle columnFamily) {
        try {
            return new String(columnFamily.getName());
        } catch (RocksDBException e) {
            return "unknown";
        }
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        
        String[] units = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = bytes;
        
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        
        return String.format("%.2f %s", size, units[unitIndex]);
    }
}