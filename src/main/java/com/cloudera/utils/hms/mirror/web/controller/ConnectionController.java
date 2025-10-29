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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConnectionRepository;
import com.cloudera.utils.hms.mirror.service.ConnectionService;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionRequest;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/connections")
@Getter
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class ConnectionController {

    @NonNull
    private final ConnectionService connectionService;
    @NonNull
    private final ConnectionRepository connectionRepository;

    private Map<String, Object> convertConnectionToMap(ConnectionDto conn) {
        Map<String, Object> connData = new HashMap<>();
        connData.put("id", conn.getId());
        connData.put("name", conn.getName());
        connData.put("description", conn.getDescription());
        connData.put("environment", conn.getEnvironment() != null ? conn.getEnvironment().name() : null);
        
        // Create config object that UI expects
        Map<String, Object> config = new HashMap<>();
        config.put("platformType", conn.getPlatformType() != null ? conn.getPlatformType() : "");
        config.put("hcfsNamespace", conn.getHcfsNamespace() != null ? conn.getHcfsNamespace() : "");
        config.put("createIfNotExists", conn.isCreateIfNotExists());
        config.put("enableAutoTableStats", conn.isEnableAutoTableStats());
        config.put("enableAutoColumnStats", conn.isEnableAutoColumnStats());

        // Add HiveServer2 config
        Map<String, Object> hiveServer2 = new HashMap<>();
        hiveServer2.put("uri", conn.getHs2Uri() != null ? conn.getHs2Uri() : "");
//        hiveServer2.put("driverClassName", conn.getHs2DriverClassName() != null ? conn.getHs2DriverClassName() : "");
//        hiveServer2.put("jarFile", conn.getHs2JarFile() != null ? conn.getHs2JarFile() : "");
//        hiveServer2.put("disconnected", conn.isHs2Disconnected());
        // Always include connectionProperties
        Map<String, Object> hs2ConnectionProps = new HashMap<>();
        if (conn.getHs2ConnectionProperties() != null) {
            hs2ConnectionProps.putAll(conn.getHs2ConnectionProperties());
        }
        // Ensure user and password are always present
        hs2ConnectionProps.putIfAbsent("user", "");
        hs2ConnectionProps.putIfAbsent("password", "");
        hiveServer2.put("connectionProperties", hs2ConnectionProps);
        config.put("hiveServer2", hiveServer2);
        
        // Add Metastore Direct config
        Map<String, Object> metastoreDirect = new HashMap<>();
        if (conn.isMetastoreDirectEnabled()) {
            metastoreDirect.put("uri", conn.getMetastoreDirectUri() != null ? conn.getMetastoreDirectUri() : "");
            metastoreDirect.put("type", conn.getMetastoreDirectType() != null ? conn.getMetastoreDirectType() : "");
            
            // Always include connectionProperties
            Map<String, Object> connectionProps = new HashMap<>();
            connectionProps.put("user", conn.getMetastoreDirectUsername() != null ? conn.getMetastoreDirectUsername() : "");
            connectionProps.put("password", conn.getMetastoreDirectPassword() != null ? conn.getMetastoreDirectPassword() : "");
            metastoreDirect.put("connectionProperties", connectionProps);
            
            // Always include connectionPool with default values
            Map<String, Object> connectionPool = new HashMap<>();
            connectionPool.put("min", conn.getMetastoreDirectMinConnections() != null ? conn.getMetastoreDirectMinConnections() : 3);
            connectionPool.put("max", conn.getMetastoreDirectMaxConnections() != null ? conn.getMetastoreDirectMaxConnections() : 5);
            metastoreDirect.put("connectionPool", connectionPool);
        } else {
            // Add empty metastoreDirect structure for consistency
            metastoreDirect.put("uri", "");
            metastoreDirect.put("type", "");
            metastoreDirect.put("connectionProperties", Map.of("user", "", "password", ""));
            metastoreDirect.put("connectionPool", Map.of("min", 3, "max", 5));
        }
        config.put("metastoreDirect", metastoreDirect);
        
        // Add partition discovery config
        Map<String, Object> partitionDiscovery = new HashMap<>();
        partitionDiscovery.put("auto", conn.isPartitionDiscoveryAuto());
        partitionDiscovery.put("initMSCK", conn.isPartitionDiscoveryInitMSCK());
        partitionDiscovery.put("partitionBucketLimit", conn.getPartitionBucketLimit() != null ? conn.getPartitionBucketLimit() : 100);
        config.put("partitionDiscovery", partitionDiscovery);
        
        // Add top-level connectionPool that some UI components might expect
        Map<String, Object> topLevelConnectionPool = new HashMap<>();
        if (conn.isMetastoreDirectEnabled() && conn.getMetastoreDirectMinConnections() != null && conn.getMetastoreDirectMaxConnections() != null) {
            topLevelConnectionPool.put("min", conn.getMetastoreDirectMinConnections());
            topLevelConnectionPool.put("max", conn.getMetastoreDirectMaxConnections());
        } else {
            topLevelConnectionPool.put("min", 3);
            topLevelConnectionPool.put("max", 5);
        }
        config.put("connectionPool", topLevelConnectionPool);
        
        connData.put("config", config);
        
        // Add test status and timestamps using safe string formatting
        String testStatus = "never_tested";
        if (conn.getTestResults() != null && conn.getTestResults().getStatus() != null) {
            testStatus = conn.getTestResults().getStatus().name().toLowerCase();
        }
        connData.put("testStatus", testStatus);
        
        // Format dates as strings to avoid serialization issues
        if (conn.getCreated() != null) {
            connData.put("created", conn.getCreated().toString());
        }
        if (conn.getModified() != null) {
            connData.put("modified", conn.getModified().toString());
        }
        
        return connData;
    }

    @GetMapping(produces = "application/json")
    public ResponseEntity<Map<String, Object>> getConnections(
            @RequestParam(value = "search", required = false) String search,
            @RequestParam(value = "environment", required = false) String environment,
            @RequestParam(value = "status", required = false, defaultValue = "all") String status) {
        
        try {
            log.info("ConnectionController.getConnections() called - search: {}, environment: {}, status: {}", 
                     search, environment, status);
            
            // Step 1: Try to retrieve connections from service
            List<ConnectionDto> connectionDtos = getConnectionService().getFilteredConnections(search, environment, status);
            log.info("Successfully retrieved {} connections from service", connectionDtos.size());
            
            // Step 2: Try to create a minimal response with just count first
            Map<String, Object> response = new HashMap<>();
            response.put("count", connectionDtos.size());
            response.put("filters", Map.of(
                    "search", search != null ? search : "",
                    "environment", environment != null ? environment : "",
                    "status", status
            ));
            
            // Step 3: Build connection data with config object that UI expects
            List<Map<String, Object>> detailedConnections = connectionDtos.stream()
                    .map(conn -> {
                        Map<String, Object> connData = new HashMap<>();
                        connData.put("id", conn.getId());
                        connData.put("name", conn.getName());
                        connData.put("description", conn.getDescription());
                        connData.put("environment", conn.getEnvironment() != null ? conn.getEnvironment().name() : null);
                        
                        // Create config object that UI expects
                        Map<String, Object> config = new HashMap<>();
                        config.put("platformType", conn.getPlatformType() != null ? conn.getPlatformType() : "");
                        config.put("hcfsNamespace", conn.getHcfsNamespace() != null ? conn.getHcfsNamespace() : "");
                        config.put("createIfNotExists", conn.isCreateIfNotExists());
                        config.put("enableAutoTableStats", conn.isEnableAutoTableStats());
                        config.put("enableAutoColumnStats", conn.isEnableAutoColumnStats());

                        // Add HiveServer2 config
                        Map<String, Object> hiveServer2 = new HashMap<>();
                        hiveServer2.put("uri", conn.getHs2Uri() != null ? conn.getHs2Uri() : "");
//                        hiveServer2.put("driverClassName", conn.getHs2DriverClassName() != null ? conn.getHs2DriverClassName() : "");
//                        hiveServer2.put("jarFile", conn.getHs2JarFile() != null ? conn.getHs2JarFile() : "");
//                        hiveServer2.put("disconnected", conn.isHs2Disconnected());
                        // Always include connectionProperties
                        Map<String, Object> hs2ConnectionProps = new HashMap<>();
                        if (conn.getHs2ConnectionProperties() != null) {
                            hs2ConnectionProps.putAll(conn.getHs2ConnectionProperties());
                        }
                        // Ensure user and password are always present
                        hs2ConnectionProps.putIfAbsent("user", "");
                        hs2ConnectionProps.putIfAbsent("password", "");
                        hiveServer2.put("connectionProperties", hs2ConnectionProps);
                        config.put("hiveServer2", hiveServer2);
                        
                        // Add Metastore Direct config if enabled
                        if (conn.isMetastoreDirectEnabled()) {
                            Map<String, Object> metastoreDirect = new HashMap<>();
                            metastoreDirect.put("uri", conn.getMetastoreDirectUri() != null ? conn.getMetastoreDirectUri() : "");
                            metastoreDirect.put("type", conn.getMetastoreDirectType() != null ? conn.getMetastoreDirectType() : "");
                            
                            // Always include connectionProperties
                            Map<String, Object> connectionProps = new HashMap<>();
                            connectionProps.put("user", conn.getMetastoreDirectUsername() != null ? conn.getMetastoreDirectUsername() : "");
                            connectionProps.put("password", conn.getMetastoreDirectPassword() != null ? conn.getMetastoreDirectPassword() : "");
                            metastoreDirect.put("connectionProperties", connectionProps);
                            
                            // Always include connectionPool with default values
                            Map<String, Object> connectionPool = new HashMap<>();
                            connectionPool.put("min", conn.getMetastoreDirectMinConnections() != null ? conn.getMetastoreDirectMinConnections() : 3);
                            connectionPool.put("max", conn.getMetastoreDirectMaxConnections() != null ? conn.getMetastoreDirectMaxConnections() : 5);
                            metastoreDirect.put("connectionPool", connectionPool);
                            
                            config.put("metastoreDirect", metastoreDirect);
                        } else {
                            // Add empty metastoreDirect structure for consistency
                            Map<String, Object> metastoreDirect = new HashMap<>();
                            metastoreDirect.put("uri", "");
                            metastoreDirect.put("type", "");
                            metastoreDirect.put("connectionProperties", Map.of("user", "", "password", ""));
                            metastoreDirect.put("connectionPool", Map.of("min", 3, "max", 5));
                            config.put("metastoreDirect", metastoreDirect);
                        }
                        
                        // Add partition discovery config
                        Map<String, Object> partitionDiscovery = new HashMap<>();
                        partitionDiscovery.put("auto", conn.isPartitionDiscoveryAuto());
                        partitionDiscovery.put("initMSCK", conn.isPartitionDiscoveryInitMSCK());
                        partitionDiscovery.put("partitionBucketLimit", conn.getPartitionBucketLimit() != null ? conn.getPartitionBucketLimit() : 100);
                        config.put("partitionDiscovery", partitionDiscovery);
                        
                        // Add top-level connectionPool that some UI components might expect
                        Map<String, Object> topLevelConnectionPool = new HashMap<>();
                        if (conn.isMetastoreDirectEnabled() && conn.getMetastoreDirectMinConnections() != null && conn.getMetastoreDirectMaxConnections() != null) {
                            topLevelConnectionPool.put("min", conn.getMetastoreDirectMinConnections());
                            topLevelConnectionPool.put("max", conn.getMetastoreDirectMaxConnections());
                        } else {
                            topLevelConnectionPool.put("min", 3);
                            topLevelConnectionPool.put("max", 5);
                        }
                        config.put("connectionPool", topLevelConnectionPool);
                        
                        connData.put("config", config);
                        
                        // Add test status and timestamps using safe string formatting
                        String testStatus = "never_tested";
                        if (conn.getTestResults() != null && conn.getTestResults().getStatus() != null) {
                            testStatus = conn.getTestResults().getStatus().name().toLowerCase();
                        }
                        connData.put("testStatus", testStatus);
                        
                        // Format dates as strings to avoid serialization issues
                        if (conn.getCreated() != null) {
                            connData.put("created", conn.getCreated().toString());
                        }
                        if (conn.getModified() != null) {
                            connData.put("modified", conn.getModified().toString());
                        }
                        
                        return connData;
                    })
                    .toList();
            
            response.put("connections", detailedConnections);
            log.info("Built response with {} detailed connections", detailedConnections.size());
            
            return ResponseEntity.ok(response);
            
//        } catch (RocksDBException e) {
//            log.error("RocksDB error retrieving connections", e);
//            Map<String, Object> errorResponse = new HashMap<>();
//            errorResponse.put("error", "Failed to retrieve connections: " + e.getMessage());
//            errorResponse.put("connections", List.of());
//            errorResponse.put("count", 0);
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        } catch (Exception e) {
            log.error("Unexpected error in getConnections", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Unexpected error: " + e.getMessage());
            errorResponse.put("connections", List.of());
            errorResponse.put("count", 0);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping(value = "/{id}", produces = "application/json")
    public ResponseEntity<Map<String, Object>> getConnection(@PathVariable("id") String id) {
        try {
            log.info("Getting connection by ID: {}", id);
            
            Optional<ConnectionDto> connection = getConnectionService().getConnectionById(id);
            if (connection.isPresent()) {
                Map<String, Object> connectionData = convertConnectionToMap(connection.get());
                return ResponseEntity.ok(connectionData);
            } else {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Connection not found: " + id);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            }
            
        } catch (RepositoryException e) {
            log.error("Error retrieving connection {}", id, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to retrieve connection: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(produces = "application/json", consumes = "application/json")
    public ResponseEntity<Map<String, Object>> createConnection(@RequestBody ConnectionRequest request) {
        try {
            log.info("Creating new connection: {}", request.getName());
            
            ConnectionDto connectionDto = request.toConnection();
            ConnectionDto savedConnectionDto = getConnectionService().createConnection(connectionDto);
            
            Map<String, Object> response = new HashMap<>();
            response.put("connection", savedConnectionDto);
            response.put("message", "Connection created successfully");
            
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
            
        } catch (RepositoryException e) {
            log.error("Error creating connection", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to create connection: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        } catch (Exception e) {
            log.error("Unexpected error creating connection", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Unexpected error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }
    }

    @PutMapping(value = "/{id}", produces = "application/json", consumes = "application/json")
    public ResponseEntity<Map<String, Object>> updateConnection(@PathVariable("id") String id, @RequestBody ConnectionRequest request) {
        try {
            log.info("Updating connection: {}", id);
            
            ConnectionDto connectionDto = request.toConnection();
            ConnectionDto updatedConnectionDto = getConnectionService().updateConnection(id, connectionDto);
            
            Map<String, Object> response = new HashMap<>();
            response.put("connection", updatedConnectionDto);
            response.put("message", "Connection updated successfully");
            
            return ResponseEntity.ok(response);
            
        } catch (RepositoryException e) {
            log.error("Error updating connection {}", id, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to update connection: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        } catch (Exception e) {
            log.error("Unexpected error updating connection {}", id, e);
            Map<String, Object> errorResponse = new HashMap<>();
            if (e.getMessage() != null && e.getMessage().contains("not found")) {
                errorResponse.put("error", "Connection not found: " + id);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            } else {
                errorResponse.put("error", "Unexpected error: " + e.getMessage());
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
            }
        }
    }

    @DeleteMapping(value = "/{id}", produces = "application/json")
    public ResponseEntity<Map<String, Object>> deleteConnection(@PathVariable("id") String id) {
        try {
            log.info("Deleting connection: {}", id);
            
            boolean deleted = getConnectionRepository().deleteById(id);
            Map<String, Object> response = new HashMap<>();
            
            if (deleted) {
                response.put("message", "Connection deleted successfully");
                return ResponseEntity.ok(response);
            } else {
                response.put("error", "Connection not found: " + id);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }
            
        } catch (RepositoryException e) {
            log.error("Error deleting connection {}", id, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to delete connection: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(value = "/{id}/test", produces = "application/json")
    public ResponseEntity<Map<String, Object>> testConnection(@PathVariable("id") String id) {
        try {
            log.info("Testing connection: {}", id);
            
            boolean testResult = getConnectionService().testConnection(id);
            Map<String, Object> response = new HashMap<>();
            response.put("success", testResult);
            response.put("message", testResult ? "Connection test successful" : "Connection test failed");
            
            return ResponseEntity.ok(response);
            
        } catch (RepositoryException e) {
            log.error("Error testing connection {}", id, e);
            Map<String, Object> errorResponse = new HashMap<>();
            if (e.getMessage() != null && e.getMessage().contains("not found")) {
                errorResponse.put("error", "Connection not found: " + id);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            } else {
                errorResponse.put("error", "Failed to test connection: " + e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
            }
        }
    }

    /*
    @PostMapping(value = "/{id}/set-default", produces = "application/json")
    public ResponseEntity<Map<String, Object>> setDefaultConnection(@PathVariable("id") String id) {
        try {
            log.info("Setting connection as default: {}", id);
            
            connectionService.setDefaultConnection(id);
            Map<String, Object> response = new HashMap<>();
            response.put("message", "Connection set as default successfully");
            
            return ResponseEntity.ok(response);
            
        } catch (RocksDBException e) {
            log.error("Error setting default connection {}", id, e);
            Map<String, Object> errorResponse = new HashMap<>();
            if (e.getMessage() != null && e.getMessage().contains("not found")) {
                errorResponse.put("error", "Connection not found: " + id);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            } else {
                errorResponse.put("error", "Failed to set default connection: " + e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
            }
        }
    }
     */

//    @GetMapping(value = "/default", produces = "application/json")
//    public ResponseEntity<ConnectionDto> getDefaultConnection() {
//        try {
//            log.info("Getting default connection");
//
//            Optional<ConnectionDto> defaultConnection = getConnectionService().getDefaultConnection();
//            if (defaultConnection.isPresent()) {
//                return ResponseEntity.ok(defaultConnection.get());
//            } else {
//                return ResponseEntity.notFound().build();
//            }
//
//        } catch (RocksDBException e) {
//            log.error("Error retrieving default connection", e);
//            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
//        }
//    }

}