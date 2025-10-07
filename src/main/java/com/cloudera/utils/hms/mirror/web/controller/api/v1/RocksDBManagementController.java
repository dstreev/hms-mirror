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

import java.util.Map;

/**
 * REST controller for RocksDB management operations.
 * Provides endpoints for monitoring health, statistics, compaction, and other management tasks.
 */
@RestController
@RequestMapping("/api/v1/rocksdb")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "RocksDB Management", description = "RocksDB health monitoring and management operations")
@Slf4j
public class RocksDBManagementController {

    private final RocksDBManagementService managementService;
    private final ColumnFamilyHandle configurationsColumnFamily;
    private final ColumnFamilyHandle sessionsColumnFamily;
    private final ColumnFamilyHandle connectionsColumnFamily;

    @Autowired
    public RocksDBManagementController(RocksDBManagementService managementService,
                                     @Qualifier("configurationsColumnFamily") ColumnFamilyHandle configurationsColumnFamily,
                                     @Qualifier("sessionsColumnFamily") ColumnFamilyHandle sessionsColumnFamily,
                                     @Qualifier("connectionsColumnFamily") ColumnFamilyHandle connectionsColumnFamily) {
        this.managementService = managementService;
        this.configurationsColumnFamily = configurationsColumnFamily;
        this.sessionsColumnFamily = sessionsColumnFamily;
        this.connectionsColumnFamily = connectionsColumnFamily;
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
            @Parameter(description = "Column family name (configurations, sessions, connections)")
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
            @Parameter(description = "Column family name (configurations, sessions, connections)")
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

    private ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) {
        switch (columnFamilyName.toLowerCase()) {
            case "configurations":
                return configurationsColumnFamily;
            case "sessions":
                return sessionsColumnFamily;
            case "connections":
                return connectionsColumnFamily;
            default:
                return null;
        }
    }
}