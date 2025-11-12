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

package com.cloudera.utils.hms.mirror.web.controller.api.v1;

import com.cloudera.utils.hms.mirror.domain.support.RocksDBColumnFamily;
import com.cloudera.utils.hms.mirror.service.ConfigurationManagementService;
import com.cloudera.utils.hms.mirror.service.RocksDBManagementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for RocksDB management operations.
 * Provides endpoints for monitoring health, statistics, compaction, and other management tasks.
 */
@CrossOrigin
@RestController
@RequestMapping("/api/v1/rocksdb")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "RocksDB Management", description = "RocksDB health monitoring and management operations")
@Slf4j
public class RocksDBManagementController {

    private final RocksDBManagementService managementService;
    private final ConfigurationManagementService configurationManagementService;
    private final ColumnFamilyHandle configurationsColumnFamily;
    private final ColumnFamilyHandle sessionsColumnFamily;
    private final ColumnFamilyHandle connectionsColumnFamily;
    private final ColumnFamilyHandle datasetsColumnFamily;
    private final ColumnFamilyHandle jobsColumnFamily;
    private final ColumnFamilyHandle conversionResultColumnFamily;
    private final ColumnFamilyHandle runStatusColumnFamily;

    @Autowired
    public RocksDBManagementController(RocksDBManagementService managementService,
                                     ConfigurationManagementService configurationManagementService,
                                     @Qualifier("configurationsColumnFamily") ColumnFamilyHandle configurationsColumnFamily,
                                     @Qualifier("sessionsColumnFamily") ColumnFamilyHandle sessionsColumnFamily,
                                     @Qualifier("connectionsColumnFamily") ColumnFamilyHandle connectionsColumnFamily,
                                     @Qualifier("datasetsColumnFamily") ColumnFamilyHandle datasetsColumnFamily,
                                     @Qualifier("jobsColumnFamily") ColumnFamilyHandle jobsColumnFamily,
                                     @Qualifier("conversionResultColumnFamily") ColumnFamilyHandle conversionResultColumnFamily,
                                     @Qualifier("runStatusColumnFamily") ColumnFamilyHandle runStatusColumnFamily) {
        this.managementService = managementService;
        this.configurationManagementService = configurationManagementService;
        this.configurationsColumnFamily = configurationsColumnFamily;
        this.sessionsColumnFamily = sessionsColumnFamily;
        this.connectionsColumnFamily = connectionsColumnFamily;
        this.datasetsColumnFamily = datasetsColumnFamily;
        this.jobsColumnFamily = jobsColumnFamily;
        this.conversionResultColumnFamily = conversionResultColumnFamily;
        this.runStatusColumnFamily = runStatusColumnFamily;
    }

    @GetMapping("/health")
    @Operation(summary = "Check RocksDB health status", 
               description = "Returns the current health status of the RocksDB instance")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Health status retrieved successfully"),
        @ApiResponse(responseCode = "503", description = "RocksDB is unhealthy")
    })
    public ResponseEntity<Map<String, Object>> getHealth() {
        try {
            boolean healthy = managementService.isHealthy();
            Map<String, Object> result = Map.of(
                "healthy", healthy,
                "status", healthy ? "UP" : "DOWN"
            );
            
            return healthy ? 
                ResponseEntity.ok(result) : 
                ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(result);
        } catch (Exception e) {
            log.error("Error checking RocksDB health", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("healthy", false, "status", "ERROR", "error", e.getMessage()));
        }
    }

    @GetMapping("/statistics")
    @Operation(summary = "Get comprehensive RocksDB statistics", 
               description = "Returns detailed statistics about RocksDB performance, storage, and health")
    @ApiResponse(responseCode = "200", description = "Statistics retrieved successfully",
                 content = @Content(schema = @Schema(implementation = Map.class)))
    public ResponseEntity<Map<String, Object>> getStatistics() {
        try {
            Map<String, Object> statistics = managementService.getStatistics();
            return ResponseEntity.ok(statistics);
        } catch (RocksDBException e) {
            log.error("Error retrieving RocksDB statistics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve statistics: " + e.getMessage()));
        }
    }

    @GetMapping("/storage-sizes")
    @Operation(summary = "Get storage size information", 
               description = "Returns storage size information for the RocksDB instance")
    @ApiResponse(responseCode = "200", description = "Storage sizes retrieved successfully")
    public ResponseEntity<Map<String, Long>> getStorageSizes() {
        try {
            Map<String, Long> sizes = managementService.getStorageSizes();
            return ResponseEntity.ok(sizes);
        } catch (RocksDBException e) {
            log.error("Error retrieving storage sizes", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of());
        }
    }

    @GetMapping("/storage-size/formatted")
    @Operation(summary = "Get formatted storage size", 
               description = "Returns human-readable storage size for the RocksDB instance")
    @ApiResponse(responseCode = "200", description = "Formatted storage size retrieved successfully")
    public ResponseEntity<Map<String, String>> getFormattedStorageSize() {
        try {
            String formattedSize = managementService.getFormattedStorageSize();
            return ResponseEntity.ok(Map.of("formatted_size", formattedSize));
        } catch (RocksDBException e) {
            log.error("Error retrieving formatted storage size", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve storage size: " + e.getMessage()));
        }
    }

    @GetMapping("/performance-metrics")
    @Operation(summary = "Get performance metrics", 
               description = "Returns performance metrics for monitoring RocksDB operations")
    @ApiResponse(responseCode = "200", description = "Performance metrics retrieved successfully")
    public ResponseEntity<Map<String, Object>> getPerformanceMetrics() {
        try {
            Map<String, Object> metrics = managementService.getPerformanceMetrics();
            return ResponseEntity.ok(metrics);
        } catch (RocksDBException e) {
            log.error("Error retrieving performance metrics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve performance metrics: " + e.getMessage()));
        }
    }

    @PostMapping("/compaction")
    @Operation(summary = "Trigger manual compaction", 
               description = "Triggers a manual compaction for all column families to optimize storage")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Compaction triggered successfully"),
        @ApiResponse(responseCode = "500", description = "Failed to trigger compaction")
    })
    public ResponseEntity<Map<String, String>> triggerCompaction() {
        try {
            managementService.triggerCompaction();
            return ResponseEntity.ok(Map.of("message", "Manual compaction triggered successfully"));
        } catch (RocksDBException e) {
            log.error("Error triggering compaction", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to trigger compaction: " + e.getMessage()));
        }
    }

    @PostMapping("/compaction/{columnFamily}")
    @Operation(summary = "Trigger compaction for specific column family",
               description = "Triggers a manual compaction for a specific column family")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Column family compaction triggered successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid column family name"),
        @ApiResponse(responseCode = "500", description = "Failed to trigger compaction")
    })
    public ResponseEntity<Map<String, String>> triggerColumnFamilyCompaction(
            @Parameter(description = "Column family name (configurations, sessions, connections, datasets, jobs)")
            @PathVariable String columnFamily) {
        try {
            ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
            if (handle == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid column family: " + columnFamily));
            }

            managementService.triggerCompaction(handle);
            return ResponseEntity.ok(Map.of("message", 
                "Manual compaction triggered successfully for column family: " + columnFamily));
        } catch (RocksDBException e) {
            log.error("Error triggering compaction for column family: {}", columnFamily, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to trigger compaction: " + e.getMessage()));
        }
    }

    @GetMapping("/compaction-status")
    @Operation(summary = "Check compaction status", 
               description = "Returns whether compaction is currently pending")
    @ApiResponse(responseCode = "200", description = "Compaction status retrieved successfully")
    public ResponseEntity<Map<String, Object>> getCompactionStatus() {
        try {
            boolean pending = managementService.isCompactionPending();
            return ResponseEntity.ok(Map.of(
                "compaction_pending", pending,
                "status", pending ? "PENDING" : "IDLE"
            ));
        } catch (RocksDBException e) {
            log.error("Error checking compaction status", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to check compaction status: " + e.getMessage()));
        }
    }

    @GetMapping("/column-family/{columnFamily}/statistics")
    @Operation(summary = "Get column family specific statistics",
               description = "Returns statistics for a specific column family")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Column family statistics retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid column family name")
    })
    public ResponseEntity<Map<String, Object>> getColumnFamilyStatistics(
            @Parameter(description = "Column family name (configurations, sessions, connections, datasets, jobs)")
            @PathVariable String columnFamily) {
        try {
            ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
            if (handle == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid column family: " + columnFamily));
            }

            Map<String, Object> statistics = managementService.getColumnFamilyStatistics(handle);
            return ResponseEntity.ok(statistics);
        } catch (RocksDBException e) {
            log.error("Error retrieving statistics for column family: {}", columnFamily, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve column family statistics: " + e.getMessage()));
        }
    }

    @GetMapping("/integrity")
    @Operation(summary = "Validate database integrity", 
               description = "Performs integrity checks on the RocksDB instance")
    @ApiResponse(responseCode = "200", description = "Integrity validation completed")
    public ResponseEntity<Map<String, Object>> validateIntegrity() {
        Map<String, Object> result = managementService.validateIntegrity();
        
        // Return appropriate HTTP status based on validation result
        String status = (String) result.get("status");
        if ("HEALTHY".equals(status)) {
            return ResponseEntity.ok(result);
        } else if ("UNHEALTHY".equals(status)) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(result);
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @GetMapping("/column-families")
    @Operation(summary = "List all column families", 
               description = "Returns a list of all column families with their metadata")
    @ApiResponse(responseCode = "200", description = "Column families retrieved successfully")
    public ResponseEntity<Map<String, Object>> getColumnFamilies() {
        try {
            // Use centralized enum to get column families
            Map<String, Object> result = new HashMap<>();
            List<Map<String, Object>> columnFamilies = new ArrayList<>();
            
            // Iterate through all column families from enum
            for (RocksDBColumnFamily cf : RocksDBColumnFamily.getAllColumnFamilies()) {
                Map<String, Object> cfInfo = new HashMap<>();
                cfInfo.put("name", cf.getDisplayName());
                cfInfo.put("keysCount", 0); // Will be populated when we can actually query
                columnFamilies.add(cfInfo);
            }
            
            result.put("columnFamilies", columnFamilies);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error retrieving column families", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve column families: " + e.getMessage()));
        }
    }

    @GetMapping("/data/{columnFamily}/keys")
    @Operation(summary = "Get keys for a column family",
               description = "Returns keys for the specified column family, optionally filtered by prefix")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Keys retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid column family name")
    })
    public ResponseEntity<Map<String, Object>> getKeys(
            @Parameter(description = "Column family name (default, configurations, sessions, connections, datasets, jobs)")
            @PathVariable String columnFamily,
            @Parameter(description = "Key prefix filter")
            @RequestParam(value = "prefix", required = false) String prefix) {
        try {
            ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
            if (handle == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid column family: " + columnFamily));
            }

            Map<String, Object> result = managementService.getKeys(handle, prefix);
            return ResponseEntity.ok(result);
        } catch (RocksDBException e) {
            log.error("Error retrieving keys for column family: {}", columnFamily, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve keys: " + e.getMessage()));
        }
    }

    @GetMapping("/data/{columnFamily}")
    @Operation(summary = "Get value for a specific key",
               description = "Returns the value for the specified key in the given column family")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Value retrieved successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid column family name or missing key parameter"),
        @ApiResponse(responseCode = "404", description = "Key not found")
    })
    public ResponseEntity<Map<String, Object>> getValue(
            @Parameter(description = "Column family name (default, configurations, sessions, connections, datasets, jobs)")
            @PathVariable String columnFamily,
            @Parameter(description = "The key to retrieve")
            @RequestParam String key) {
        try {
            if (key == null || key.trim().isEmpty()) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Missing required parameter: key"));
            }
            
            ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
            if (handle == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid column family: " + columnFamily));
            }

            Map<String, Object> result = managementService.getValue(handle, key);
            
            if (!(Boolean) result.get("exists")) {
                return ResponseEntity.notFound().build();
            }
            
            return ResponseEntity.ok(result);
        } catch (RocksDBException e) {
            log.error("Error retrieving value for key {} in column family: {}", key, columnFamily, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to retrieve value: " + e.getMessage()));
        }
    }

    @PostMapping("/maintenance/{action}")
    @Operation(summary = "Perform maintenance operations", 
               description = "Performs maintenance operations like flush or clear")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Maintenance operation completed successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid action"),
        @ApiResponse(responseCode = "500", description = "Failed to perform maintenance operation")
    })
    public ResponseEntity<Map<String, String>> performMaintenance(
            @Parameter(description = "Maintenance action (flush, clear)")
            @PathVariable String action) {
        try {
            switch (action.toLowerCase()) {
                case "flush":
                    managementService.flushMemTables();
                    return ResponseEntity.ok(Map.of("message", "Memtables flushed successfully"));
                case "clear":
                    // For now, just clear the available column families
                    return ResponseEntity.ok(Map.of("message", "Clear operation not yet implemented for all column families"));
                default:
                    return ResponseEntity.badRequest()
                        .body(Map.of("error", "Invalid action: " + action));
            }
        } catch (RocksDBException e) {
            log.error("Error performing maintenance action: {}", action, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to perform maintenance: " + e.getMessage()));
        }
    }

    @PostMapping("/backup")
    @Operation(summary = "Create database backup", 
               description = "Creates a timestamped backup of the RocksDB database")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Backup created successfully"),
        @ApiResponse(responseCode = "500", description = "Failed to create backup")
    })
    public ResponseEntity<Map<String, Object>> createBackup() {
        try {
            Map<String, Object> result = managementService.createBackup();
            return ResponseEntity.ok(result);
        } catch (RocksDBException e) {
            log.error("Error creating backup", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("status", "ERROR", "error", "Failed to create backup: " + e.getMessage()));
        }
    }

    @GetMapping("/backups")
    @Operation(summary = "List available backups", 
               description = "Returns a list of all available database backups")
    @ApiResponse(responseCode = "200", description = "Backups listed successfully")
    public ResponseEntity<Map<String, Object>> listBackups() {
        Map<String, Object> result = managementService.listBackups();
        return ResponseEntity.ok(result);
    }

    @DeleteMapping("/backup/{backupName}")
    @Operation(summary = "Delete a backup", 
               description = "Deletes a specific backup by name")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Backup deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Backup not found"),
        @ApiResponse(responseCode = "500", description = "Failed to delete backup")
    })
    public ResponseEntity<Map<String, Object>> deleteBackup(
            @Parameter(description = "Backup name (timestamp)")
            @PathVariable String backupName) {
        Map<String, Object> result = managementService.deleteBackup(backupName);
        
        String status = (String) result.get("status");
        if ("SUCCESS".equals(status)) {
            return ResponseEntity.ok(result);
        } else if (result.get("message") != null && result.get("message").toString().contains("not found")) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/restore/{backupName}")
    @Operation(summary = "Restore database from backup", 
               description = "Restores the RocksDB database from a specific backup. WARNING: Application restart required.")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Backup restored successfully"),
        @ApiResponse(responseCode = "404", description = "Backup not found"),
        @ApiResponse(responseCode = "500", description = "Failed to restore backup")
    })
    public ResponseEntity<Map<String, Object>> restoreBackup(
            @Parameter(description = "Backup name (timestamp) to restore from")
            @PathVariable String backupName) {
        try {
            Map<String, Object> result = managementService.restoreBackup(backupName);
            
            String status = (String) result.get("status");
            if ("SUCCESS".equals(status)) {
                return ResponseEntity.ok(result);
            } else if (result.get("message") != null && result.get("message").toString().contains("not found")) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
        } catch (RocksDBException e) {
            log.error("Error restoring from backup: {}", backupName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("status", "ERROR", "error", "Failed to restore backup: " + e.getMessage()));
        }
    }

    @PostMapping("/scan")
    @Operation(summary = "Scan keys with optional prefix filtering", 
               description = "Scans keys in the specified column family with optional prefix filtering and limit")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Scan completed successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid column family or missing parameters"),
        @ApiResponse(responseCode = "500", description = "Failed to scan keys")
    })
    public ResponseEntity<Map<String, Object>> scanKeys(@RequestBody Map<String, Object> request) {
        try {
            String columnFamily = (String) request.get("columnFamily");
            String prefix = (String) request.get("prefix");
            Integer limit = request.get("limit") != null ? (Integer) request.get("limit") : 100;
            
            if (columnFamily == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Missing required parameter: columnFamily"));
            }
            
            ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
            if (handle == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid column family: " + columnFamily));
            }
            
            // For now, delegate to the existing getKeys method
            Map<String, Object> result = managementService.getKeys(handle, prefix);
            
            // Limit the results if requested
            if (result.containsKey("keys")) {
                @SuppressWarnings("unchecked")
                List<String> keys = (List<String>) result.get("keys");
                if (keys.size() > limit) {
                    keys = keys.subList(0, limit);
                    result.put("keys", keys);
                    result.put("truncated", true);
                    result.put("limit", limit);
                }
            }
            
            return ResponseEntity.ok(result);
        } catch (RocksDBException e) {
            log.error("Error scanning keys", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to scan keys: " + e.getMessage()));
        } catch (Exception e) {
            log.error("Error processing scan request", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to process scan request: " + e.getMessage()));
        }
    }

    @PostMapping("/put")
    @Operation(summary = "Store key-value pair", 
               description = "Stores a key-value pair in the specified column family")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Value stored successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid column family or missing parameters"),
        @ApiResponse(responseCode = "500", description = "Failed to store value")
    })
    public ResponseEntity<Map<String, String>> putValue(@RequestBody Map<String, String> request) {
        try {
            String columnFamily = request.get("columnFamily");
            String key = request.get("key");
            String value = request.get("value");
            
            if (columnFamily == null || key == null || value == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Missing required parameters: columnFamily, key, value"));
            }
            
            ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
            if (handle == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid column family: " + columnFamily));
            }
            
            managementService.putValue(handle, key, value);
            return ResponseEntity.ok(Map.of("message", "Value stored successfully"));
        } catch (Exception e) {
            log.error("Error storing value", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to store value: " + e.getMessage()));
        }
    }

    // Configuration-specific endpoints for YAML-based storage


    private ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) {
        switch (columnFamilyName.toLowerCase()) {
            case "configurations":
                return configurationsColumnFamily;
            case "sessions":
                return sessionsColumnFamily;
            case "connections":
                return connectionsColumnFamily;
            case "datasets":
                return datasetsColumnFamily;
            case "jobs":
                return jobsColumnFamily;
            case "conversionresult":
                return conversionResultColumnFamily;
            case "runstatus":
                return runStatusColumnFamily;
            default:
                return null;
        }
    }
    

}