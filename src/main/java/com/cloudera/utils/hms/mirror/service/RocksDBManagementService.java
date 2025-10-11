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
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupEngineOptions;
import org.rocksdb.RestoreOptions;
import org.rocksdb.Env;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    
    @Value("${hms-mirror.rocksdb.path:${user.home}/.hms-mirror/data}")
    private String rocksDBPath;

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

    /**
     * Get list of column families with their metadata.
     */
    public Map<String, Object> getColumnFamilies(ColumnFamilyHandle defaultCF, 
                                                 ColumnFamilyHandle configurationsCF,
                                                 ColumnFamilyHandle sessionsCF,
                                                 ColumnFamilyHandle connectionsCF) throws RocksDBException {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> columnFamilies = new ArrayList<>();
        
        // Add default column family
        Map<String, Object> defaultInfo = new HashMap<>();
        defaultInfo.put("name", "default");
        defaultInfo.put("keysCount", getEstimatedKeyCount(defaultCF));
        columnFamilies.add(defaultInfo);
        
        // Add configurations column family
        Map<String, Object> configInfo = new HashMap<>();
        configInfo.put("name", "configurations");
        configInfo.put("keysCount", getEstimatedKeyCount(configurationsCF));
        columnFamilies.add(configInfo);
        
        // Add sessions column family
        Map<String, Object> sessionsInfo = new HashMap<>();
        sessionsInfo.put("name", "sessions");
        sessionsInfo.put("keysCount", getEstimatedKeyCount(sessionsCF));
        columnFamilies.add(sessionsInfo);
        
        // Add connections column family
        Map<String, Object> connectionsInfo = new HashMap<>();
        connectionsInfo.put("name", "connections");
        connectionsInfo.put("keysCount", getEstimatedKeyCount(connectionsCF));
        columnFamilies.add(connectionsInfo);
        
        result.put("columnFamilies", columnFamilies);
        return result;
    }
    
    /**
     * Get keys for a specific column family.
     */
    public Map<String, Object> getKeys(ColumnFamilyHandle columnFamily, String prefix) throws RocksDBException {
        Map<String, Object> result = new HashMap<>();
        List<String> keys = new ArrayList<>();
        
        // Use iterator to get keys
        try (org.rocksdb.RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();
            
            int count = 0;
            int maxKeys = 1000; // Limit to prevent memory issues
            
            while (iterator.isValid() && count < maxKeys) {
                byte[] keyBytes = iterator.key();
                String key = new String(keyBytes);
                
                if (prefix == null || prefix.isEmpty() || key.startsWith(prefix)) {
                    keys.add(key);
                    count++;
                }
                
                iterator.next();
            }
        }
        
        result.put("keys", keys);
        result.put("count", keys.size());
        return result;
    }
    
    /**
     * Get value for a specific key in a column family.
     */
    public Map<String, Object> getValue(ColumnFamilyHandle columnFamily, String key) throws RocksDBException {
        Map<String, Object> result = new HashMap<>();
        
        byte[] valueBytes = rocksDB.get(columnFamily, key.getBytes());
        
        if (valueBytes != null) {
            String value = new String(valueBytes);
            result.put("key", key);
            result.put("value", value);
            result.put("exists", true);
            result.put("size", valueBytes.length);
        } else {
            result.put("key", key);
            result.put("exists", false);
        }
        
        return result;
    }
    
    /**
     * Flush all memtables to disk.
     */
    public void flushMemTables() throws RocksDBException {
        log.info("Flushing RocksDB memtables");
        rocksDB.flush(new org.rocksdb.FlushOptions());
        log.info("Memtables flushed successfully");
    }
    
    /**
     * Clear all data in the database.
     */
    public void clearAllData(ColumnFamilyHandle defaultCF, 
                            ColumnFamilyHandle configurationsCF,
                            ColumnFamilyHandle sessionsCF,
                            ColumnFamilyHandle connectionsCF) throws RocksDBException {
        log.warn("Clearing all RocksDB data");
        
        // Delete all ranges for each column family
        rocksDB.deleteRange(defaultCF, new byte[0], new byte[]{(byte)0xFF});
        rocksDB.deleteRange(configurationsCF, new byte[0], new byte[]{(byte)0xFF});
        rocksDB.deleteRange(sessionsCF, new byte[0], new byte[]{(byte)0xFF});
        rocksDB.deleteRange(connectionsCF, new byte[0], new byte[]{(byte)0xFF});
        
        // Flush to ensure data is cleared
        flushMemTables();
        
        log.warn("All RocksDB data cleared");
    }
    
    private long getEstimatedKeyCount(ColumnFamilyHandle columnFamily) {
        try {
            String numKeys = rocksDB.getProperty(columnFamily, "rocksdb.estimate-num-keys");
            return numKeys != null ? Long.parseLong(numKeys) : 0;
        } catch (RocksDBException e) {
            log.debug("Could not get key count for column family", e);
            return 0;
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
    
    /**
     * Create a backup of the RocksDB database.
     */
    public Map<String, Object> createBackup() throws RocksDBException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
        
        // Determine backup directory relative to RocksDB data directory
        File dataDir = new File(rocksDBPath);
        File backupDir = new File(dataDir.getParent(), "backup/" + timestamp);
        
        log.info("Creating RocksDB backup to: {}", backupDir.getAbsolutePath());
        
        // Create backup directory
        if (!backupDir.mkdirs()) {
            throw new RuntimeException("Failed to create backup directory: " + backupDir.getAbsolutePath());
        }
        
        BackupEngineOptions backupOptions = new BackupEngineOptions(backupDir.getAbsolutePath());
        
        try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions)) {
            // Create the backup
            backupEngine.createNewBackup(rocksDB);
            
            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("backup_path", backupDir.getAbsolutePath());
            result.put("timestamp", timestamp);
            result.put("message", "Backup created successfully");
            
            log.info("RocksDB backup completed successfully to: {}", backupDir.getAbsolutePath());
            return result;
            
        } catch (Exception e) {
            log.error("Failed to create RocksDB backup", e);
            
            // Clean up failed backup directory
            try {
                deleteDirectory(backupDir);
            } catch (Exception cleanupEx) {
                log.warn("Failed to clean up backup directory after failure: {}", backupDir.getAbsolutePath(), cleanupEx);
            }
            
            throw new RocksDBException("Failed to create backup: " + e.getMessage());
        } finally {
            backupOptions.close();
        }
    }
    
    /**
     * List available backups.
     */
    public Map<String, Object> listBackups() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> backups = new ArrayList<>();
        
        try {
            File dataDir = new File(rocksDBPath);
            File backupRootDir = new File(dataDir.getParent(), "backup");
            
            if (backupRootDir.exists() && backupRootDir.isDirectory()) {
                File[] backupDirs = backupRootDir.listFiles(File::isDirectory);
                if (backupDirs != null) {
                    for (File backupDir : backupDirs) {
                        Map<String, Object> backupInfo = new HashMap<>();
                        backupInfo.put("name", backupDir.getName());
                        backupInfo.put("path", backupDir.getAbsolutePath());
                        backupInfo.put("created", backupDir.lastModified());
                        backupInfo.put("size", getDirectorySize(backupDir));
                        backupInfo.put("formatted_size", formatBytes(getDirectorySize(backupDir)));
                        backups.add(backupInfo);
                    }
                }
                
                // Sort backups by creation time (newest first)
                backups.sort((a, b) -> Long.compare((Long) b.get("created"), (Long) a.get("created")));
            }
            
            result.put("backups", backups);
            result.put("backup_count", backups.size());
            result.put("backup_root", backupRootDir.getAbsolutePath());
            
        } catch (Exception e) {
            log.error("Error listing backups", e);
            result.put("error", "Failed to list backups: " + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Delete a specific backup.
     */
    public Map<String, Object> deleteBackup(String backupName) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            File dataDir = new File(rocksDBPath);
            File backupDir = new File(dataDir.getParent(), "backup/" + backupName);
            
            if (!backupDir.exists()) {
                result.put("status", "ERROR");
                result.put("message", "Backup not found: " + backupName);
                return result;
            }
            
            if (!backupDir.getName().equals(backupName)) {
                result.put("status", "ERROR");
                result.put("message", "Invalid backup name");
                return result;
            }
            
            deleteDirectory(backupDir);
            
            result.put("status", "SUCCESS");
            result.put("message", "Backup deleted successfully: " + backupName);
            log.info("Deleted RocksDB backup: {}", backupDir.getAbsolutePath());
            
        } catch (Exception e) {
            log.error("Error deleting backup: {}", backupName, e);
            result.put("status", "ERROR");
            result.put("message", "Failed to delete backup: " + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * Restore database from a backup.
     * WARNING: This operation requires the application to be restarted for the restore to take effect.
     */
    public Map<String, Object> restoreBackup(String backupName) throws RocksDBException {
        Map<String, Object> result = new HashMap<>();
        
        try {
            File dataDir = new File(rocksDBPath);
            File backupDir = new File(dataDir.getParent(), "backup/" + backupName);
            
            // Validate backup exists
            if (!backupDir.exists() || !backupDir.isDirectory()) {
                result.put("status", "ERROR");
                result.put("message", "Backup not found: " + backupName);
                return result;
            }
            
            // Validate backup name for security
            if (!backupDir.getName().equals(backupName) || backupName.contains("..") || backupName.contains("/")) {
                result.put("status", "ERROR");
                result.put("message", "Invalid backup name");
                return result;
            }
            
            log.info("Starting RocksDB restore from backup: {}", backupDir.getAbsolutePath());
            
            // Create a temporary restore directory
            File tempRestoreDir = new File(dataDir.getParent(), "temp_restore_" + System.currentTimeMillis());
            if (!tempRestoreDir.mkdirs()) {
                throw new RuntimeException("Failed to create temporary restore directory: " + tempRestoreDir.getAbsolutePath());
            }
            
            BackupEngineOptions backupOptions = new BackupEngineOptions(backupDir.getAbsolutePath());
            RestoreOptions restoreOptions = new RestoreOptions(false); // Don't keep log files
            
            try (BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions)) {
                // Get backup info to find the latest backup ID
                var backupInfo = backupEngine.getBackupInfo();
                if (backupInfo.isEmpty()) {
                    result.put("status", "ERROR");
                    result.put("message", "No backup found in backup directory");
                    return result;
                }
                
                // Use the latest backup (backups are sorted by ID)
                int latestBackupId = backupInfo.get(backupInfo.size() - 1).backupId();
                
                // Restore to temporary directory first
                backupEngine.restoreDbFromBackup(latestBackupId, tempRestoreDir.getAbsolutePath(), 
                                                tempRestoreDir.getAbsolutePath(), restoreOptions);
                
                log.info("Backup restored to temporary directory: {}", tempRestoreDir.getAbsolutePath());
                
                // Since RocksDB is currently open, we need to prepare for atomic replacement
                // Create backup of current data directory
                File currentBackupDir = new File(dataDir.getParent(), "current_backup_" + System.currentTimeMillis());
                
                // Move current data directory to backup location
                if (dataDir.exists()) {
                    if (!dataDir.renameTo(currentBackupDir)) {
                        // Cleanup and fail
                        deleteDirectory(tempRestoreDir);
                        throw new RuntimeException("Failed to backup current data directory");
                    }
                    log.info("Current data backed up to: {}", currentBackupDir.getAbsolutePath());
                }
                
                // Move restored data to final location
                if (!tempRestoreDir.renameTo(dataDir)) {
                    // Try to restore original data directory
                    if (currentBackupDir.exists()) {
                        currentBackupDir.renameTo(dataDir);
                    }
                    throw new RuntimeException("Failed to move restored data to final location");
                }
                
                // Clean up the current backup (optional - could keep for safety)
                try {
                    deleteDirectory(currentBackupDir);
                    log.info("Cleaned up temporary current backup");
                } catch (Exception e) {
                    log.warn("Failed to clean up temporary current backup: {}", currentBackupDir.getAbsolutePath(), e);
                }
                
                result.put("status", "SUCCESS");
                result.put("message", "Database restore completed successfully. Application restart required to use restored data.");
                result.put("backup_name", backupName);
                result.put("backup_path", backupDir.getAbsolutePath());
                result.put("restored_to", dataDir.getAbsolutePath());
                result.put("restart_required", true);
                
                log.info("RocksDB restore completed successfully from backup: {}. Application restart required.", backupName);
                
            } catch (Exception e) {
                // Cleanup temporary directory on failure
                try {
                    deleteDirectory(tempRestoreDir);
                } catch (Exception cleanupEx) {
                    log.warn("Failed to clean up temporary restore directory: {}", tempRestoreDir.getAbsolutePath(), cleanupEx);
                }
                throw e;
            } finally {
                backupOptions.close();
                restoreOptions.close();
            }
            
        } catch (Exception e) {
            log.error("Failed to restore RocksDB from backup: {}", backupName, e);
            result.put("status", "ERROR");
            result.put("message", "Failed to restore backup: " + e.getMessage());
        }
        
        return result;
    }
    
    private void deleteDirectory(File directory) throws Exception {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        if (!file.delete()) {
                            throw new Exception("Failed to delete file: " + file.getAbsolutePath());
                        }
                    }
                }
            }
            if (!directory.delete()) {
                throw new Exception("Failed to delete directory: " + directory.getAbsolutePath());
            }
        }
    }
    
    private long getDirectorySize(File directory) {
        long size = 0;
        try {
            if (directory.exists() && directory.isDirectory()) {
                File[] files = directory.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isDirectory()) {
                            size += getDirectorySize(file);
                        } else {
                            size += file.length();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error calculating directory size for: {}", directory.getAbsolutePath(), e);
        }
        return size;
    }
}