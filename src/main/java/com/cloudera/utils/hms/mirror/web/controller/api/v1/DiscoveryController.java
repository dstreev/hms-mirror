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

import com.cloudera.utils.hms.mirror.service.DiscoveryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for discovering databases and tables from HiveServer2 connections.
 * Provides endpoints to query metadata without requiring a full migration session.
 */
@CrossOrigin
@RestController
@RequestMapping("/api/v1/connections")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Discovery", description = "Discover databases and tables from connections")
@RequiredArgsConstructor
@Slf4j
public class DiscoveryController {

    @NonNull
    private final DiscoveryService discoveryService;

    /**
     * Get list of databases from a connection.
     *
     * @param connectionKey The connection key to query
     * @return Response containing list of databases
     */
    @GetMapping(value = "/{connectionKey}/databases", produces = "application/json")
    @Operation(
        summary = "Get databases from connection",
        description = "Fetches the list of databases from a successfully tested HiveServer2 connection"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Databases retrieved successfully",
                     content = @Content(schema = @Schema(implementation = DatabaseListResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid connection or connection not tested"),
        @ApiResponse(responseCode = "404", description = "Connection not found"),
        @ApiResponse(responseCode = "500", description = "Failed to fetch databases")
    })
    public ResponseEntity<Map<String, Object>> getDatabases(
            @Parameter(description = "Connection key", required = true)
            @PathVariable String connectionKey) {

        log.info("DiscoveryController.getDatabases() called - connectionKey: {}", connectionKey);

        try {
            List<String> databases = discoveryService.getDatabases(connectionKey);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("databases", databases);
            response.put("count", databases.size());

            log.info("Successfully retrieved {} databases from connection: {}", databases.size(), connectionKey);
            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.error("Invalid argument for getDatabases: {}", e.getMessage());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);

        } catch (IllegalStateException e) {
            log.error("Invalid state for getDatabases: {}", e.getMessage());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);

        } catch (Exception e) {
            log.error("Error fetching databases from connection {}", connectionKey, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to fetch databases: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Get list of tables from a specific database in a connection.
     *
     * @param connectionKey The connection key to query
     * @param databaseName The database name to query tables from
     * @return Response containing list of tables
     */
    @GetMapping(value = "/{connectionKey}/databases/{databaseName}/tables", produces = "application/json")
    @Operation(
        summary = "Get tables from database",
        description = "Fetches the list of tables from a specific database in a successfully tested HiveServer2 connection"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Tables retrieved successfully",
                     content = @Content(schema = @Schema(implementation = TableListResponse.class))),
        @ApiResponse(responseCode = "400", description = "Invalid connection, database name, or connection not tested"),
        @ApiResponse(responseCode = "404", description = "Connection not found"),
        @ApiResponse(responseCode = "500", description = "Failed to fetch tables")
    })
    public ResponseEntity<Map<String, Object>> getTables(
            @Parameter(description = "Connection key", required = true)
            @PathVariable String connectionKey,
            @Parameter(description = "Database name", required = true)
            @PathVariable String databaseName) {

        log.info("DiscoveryController.getTables() called - connectionKey: {}, databaseName: {}",
                 connectionKey, databaseName);

        try {
            List<String> tables = discoveryService.getTables(connectionKey, databaseName);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("tables", tables);
            response.put("count", tables.size());
            response.put("databaseName", databaseName);

            log.info("Successfully retrieved {} tables from database {} in connection: {}",
                     tables.size(), databaseName, connectionKey);
            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.error("Invalid argument for getTables: {}", e.getMessage());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(e.getMessage().contains("not found") ?
                    HttpStatus.NOT_FOUND : HttpStatus.BAD_REQUEST).body(errorResponse);

        } catch (IllegalStateException e) {
            log.error("Invalid state for getTables: {}", e.getMessage());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);

        } catch (Exception e) {
            log.error("Error fetching tables from database {} in connection {}",
                      databaseName, connectionKey, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to fetch tables: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    // Response schemas for Swagger documentation
    private static class DatabaseListResponse {
        public String status;
        public List<String> databases;
        public int count;
    }

    private static class TableListResponse {
        public String status;
        public List<String> tables;
        public int count;
        public String databaseName;
    }
}
