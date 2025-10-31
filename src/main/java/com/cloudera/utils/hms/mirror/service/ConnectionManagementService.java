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

import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for managing HMS Mirror Connection persistence and retrieval.
 * This service acts as a dedicated layer for connection CRUD operations,
 * delegating persistence to the ConnectionRepository.
 *
 * Connections define cluster configurations with metadata about HiveServer2
 * and optional direct metastore access for HMS Mirror operations.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class ConnectionManagementService {

    private final ConnectionRepository connectionRepository;

    /**
     * Lists all connections with their metadata.
     *
     * @return Map containing connection listing results
     */
    public Map<String, Object> list() {
        log.debug("Listing all connections");
        try {
            List<ConnectionDto> connections = getAllConnections();

            // Convert to the format expected by frontend: {key: ConnectionDto}
            Map<String, ConnectionDto> dataMap = new HashMap<>();
            for (ConnectionDto connectionDto : connections) {
                dataMap.put(connectionDto.getKey(), connectionDto);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("data", dataMap);
            return result;

        } catch (Exception e) {
            log.error("Error listing connections", e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to list connections: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Lists connections with filtering applied.
     *
     * @param search      Search term for name/description
     * @param environment Environment filter
     * @param status      Test status filter
     * @return Map containing filtered connection listing results
     */
    public Map<String, Object> listFiltered(String search, String environment, String status) {
        log.debug("Listing filtered connections - search: {}, environment: {}, status: {}",
                  search, environment, status);
        try {
            List<ConnectionDto> connections = getFilteredConnections(search, environment, status);

            // Convert to the format expected by frontend: {key: ConnectionDto}
            Map<String, ConnectionDto> dataMap = new HashMap<>();
            for (ConnectionDto connectionDto : connections) {
                dataMap.put(connectionDto.getKey(), connectionDto);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("data", dataMap);
            result.put("filters", Map.of(
                "search", search != null ? search : "",
                "environment", environment != null ? environment : "",
                "status", status != null ? status : "all"
            ));
            return result;

        } catch (Exception e) {
            log.error("Error listing filtered connections", e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to list connections: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Loads a specific connection by key.
     *
     * @param key The connection key
     * @return Map containing the connection load results
     */
    public Map<String, Object> load(String key) {
        log.debug("Loading connection: {}", key);
        try {
            Optional<ConnectionDto> connectionOpt = connectionRepository.findById(key);

            Map<String, Object> result = new HashMap<>();
            if (connectionOpt.isPresent()) {
                result.put("status", "SUCCESS");
                result.put("data", connectionOpt.get());
                result.put("key", key);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Connection not found: " + key);
            }

            return result;

        } catch (Exception e) {
            log.error("Error loading connection {}", key, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to load connection: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Saves a new connection.
     *
     * @param connectionDto The connection DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> save(ConnectionDto connectionDto) {
        log.debug("Saving connection: {}", connectionDto.getName());
        try {
            // Check if a connection with this key already exists
            if (connectionRepository.existsById(connectionDto.getKey())) {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "ERROR");
                result.put("message", "Connection with key '" + connectionDto.getKey() + "' already exists");
                return result;
            }

            // Save using repository (timestamps are handled by repository layer)
            connectionRepository.save(connectionDto);

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Connection saved successfully");
            result.put("key", connectionDto.getKey());
            return result;

        } catch (Exception e) {
            log.error("Error saving connection {}", connectionDto.getName(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save connection: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Updates an existing connection.
     *
     * @param connectionDto The updated connection DTO
     * @return Map containing the update operation results
     */
    public Map<String, Object> update(ConnectionDto connectionDto) {
        log.debug("Updating connection: {}", connectionDto.getKey());
        try {
            // Check if connection exists first
            Optional<ConnectionDto> existingConnectionOpt = connectionRepository.findById(connectionDto.getKey());
            Map<String, Object> result = new HashMap<>();

            if (existingConnectionOpt.isPresent()) {
                // Preserve original creation date if it exists
                ConnectionDto existingConnection = existingConnectionOpt.get();
                if (existingConnection.getCreated() != null) {
                    connectionDto.setCreated(existingConnection.getCreated());
                }

                // Save the updated connection
                connectionRepository.save(connectionDto);

                result.put("status", "SUCCESS");
                result.put("message", "Connection updated successfully");
                result.put("key", connectionDto.getKey());
                return result;
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Connection not found: " + connectionDto.getKey());
                return result;
            }

        } catch (Exception e) {
            log.error("Error updating connection {}", connectionDto.getKey(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to update connection: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Deletes a connection by key.
     *
     * @param key The connection key
     * @return Map containing the delete operation results
     */
    public Map<String, Object> delete(String key) {
        log.debug("Deleting connection: {}", key);
        try {
            // Check if the connection exists first
            Map<String, Object> result = new HashMap<>();

            if (connectionRepository.existsById(key)) {
                // Delete the connection
                connectionRepository.deleteById(key);
                result.put("status", "SUCCESS");
                result.put("message", "Connection deleted successfully");
                result.put("key", key);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Connection not found: " + key);
            }

            return result;

        } catch (Exception e) {
            log.error("Error deleting connection {}", key, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to delete connection: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Tests a connection by key.
     *
     * @param key The connection key to test
     * @return Map containing the test operation results
     */
    public Map<String, Object> test(String key) {
        log.debug("Testing connection: {}", key);
        try {
            boolean testResult = connectionRepository.testConnection(key);

            Map<String, Object> result = new HashMap<>();
            result.put("status", testResult ? "SUCCESS" : "FAILED");
            result.put("message", testResult ? "Connection test successful" : "Connection test failed");
            result.put("key", key);
            result.put("testPassed", testResult);

            return result;

        } catch (RepositoryException e) {
            log.error("Error testing connection {}", key, e);
            Map<String, Object> errorResult = new HashMap<>();

            if (e.getMessage() != null && e.getMessage().contains("not found")) {
                errorResult.put("status", "NOT_FOUND");
                errorResult.put("message", "Connection not found: " + key);
            } else {
                errorResult.put("status", "ERROR");
                errorResult.put("message", "Failed to test connection: " + e.getMessage());
            }

            return errorResult;
        }
    }

    /**
     * Gets the default connection.
     *
     * @return Map containing the default connection
     */
    public Map<String, Object> getDefault() {
        log.debug("Getting default connection");
        try {
            Optional<ConnectionDto> defaultConnectionOpt = connectionRepository.findDefaultConnection();

            Map<String, Object> result = new HashMap<>();
            if (defaultConnectionOpt.isPresent()) {
                result.put("status", "SUCCESS");
                result.put("data", defaultConnectionOpt.get());
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "No default connection configured");
            }

            return result;

        } catch (Exception e) {
            log.error("Error getting default connection", e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to get default connection: " + e.getMessage());
            return errorResult;
        }
    }

    // ============================================================================
    // Internal helper methods (maintain existing functionality)
    // ============================================================================

    private List<ConnectionDto> getAllConnections() throws RepositoryException {
        Map<String, ConnectionDto> connectionMap = connectionRepository.findAll();
        return new ArrayList<>(connectionMap.values()).stream()
                .sorted(this::compareConnections)
                .collect(Collectors.toList());
    }

    private List<ConnectionDto> getFilteredConnections(String search, String environment, String status) throws RepositoryException {
        List<ConnectionDto> connectionDtos = getAllConnections();

        return connectionDtos.stream()
                .filter(conn -> matchesSearch(conn, search))
                .filter(conn -> matchesEnvironment(conn, environment))
                .filter(conn -> matchesStatus(conn, status))
                .sorted(this::compareConnections)
                .collect(Collectors.toList());
    }

    private boolean matchesSearch(ConnectionDto conn, String search) {
        if (search == null || search.trim().isEmpty()) {
            return true;
        }
        String searchLower = search.toLowerCase();
        return conn.getName().toLowerCase().contains(searchLower) ||
               (conn.getDescription() != null && conn.getDescription().toLowerCase().contains(searchLower));
    }

    private boolean matchesEnvironment(ConnectionDto conn, String environment) {
        if (environment == null || environment.trim().isEmpty()) {
            return true;
        }
        return conn.getEnvironment() != null && conn.getEnvironment().name().equals(environment);
    }

    private boolean matchesStatus(ConnectionDto conn, String status) {
        if (status == null || status.trim().isEmpty() || "all".equals(status)) {
            return true;
        }

        ConnectionDto.ConnectionTestResults.TestStatus testStatus =
                conn.getTestResults() != null ? conn.getTestResults().getStatus() :
                ConnectionDto.ConnectionTestResults.TestStatus.NEVER_TESTED;

        switch (status) {
            case "success":
                return testStatus == ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS;
            case "failed":
                return testStatus == ConnectionDto.ConnectionTestResults.TestStatus.FAILED;
            case "never_tested":
                return testStatus == ConnectionDto.ConnectionTestResults.TestStatus.NEVER_TESTED;
            default:
                return true;
        }
    }

    private int compareConnections(ConnectionDto a, ConnectionDto b) {
        // Default connections first
        if (a.isDefault() && !b.isDefault()) return -1;
        if (!a.isDefault() && b.isDefault()) return 1;

        // Then by name
        return a.getName().compareToIgnoreCase(b.getName());
    }
}
