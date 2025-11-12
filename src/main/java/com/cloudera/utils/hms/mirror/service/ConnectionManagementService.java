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
import com.cloudera.utils.hms.mirror.domain.support.DriverType;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;

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
@Slf4j
public class ConnectionManagementService {

    private final ConnectionRepository connectionRepository;
    private final DriverUtilsService driverUtilsService;
    private final PasswordService passwordService;

    public ConnectionManagementService(ConnectionRepository connectionRepository,
                                      @Autowired(required = false) DriverUtilsService driverUtilsService,
                                      @Autowired(required = false) PasswordService passwordService) {
        this.connectionRepository = connectionRepository;
        this.driverUtilsService = driverUtilsService;
        this.passwordService = passwordService;
    }

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
            Optional<ConnectionDto> connectionOpt = connectionRepository.findByKey(key);

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
                result.put("status", "CONFLICT");
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
            Optional<ConnectionDto> existingConnectionOpt = connectionRepository.findByKey(connectionDto.getKey());
            Map<String, Object> result = new HashMap<>();

            if (existingConnectionOpt.isPresent()) {
                // Preserve original creation date
                ConnectionDto existingConnection = existingConnectionOpt.get();
                if (existingConnection.getCreated() != null) {
                    connectionDto.setCreated(existingConnection.getCreated());
                }

                // Check if configuration has changed - if so, reset test results
                // Configuration changes invalidate previous test results
                boolean configChanged = hasConfigurationChanged(existingConnection, connectionDto);

                if (configChanged) {
                    // Reset test results when configuration changes
                    log.info("Connection configuration changed for {}. Resetting test results.", connectionDto.getKey());
                    connectionDto.setHcfsTestResults(null);
                    connectionDto.setHs2TestResults(null);
                    connectionDto.setMetastoreDirectTestResults(null);
                } else {
                    // Preserve test results from existing connection to prevent loss during updates.
                    // Test results should only be updated by the test() endpoint, never by save/update operations.
                    // This ensures that if a user tests a connection, then makes non-configuration changes and saves,
                    // the test results are preserved rather than being reset to null.
                    if (existingConnection.getHcfsTestResults() != null) {
                        connectionDto.setHcfsTestResults(existingConnection.getHcfsTestResults());
                    }
                    if (existingConnection.getHs2TestResults() != null) {
                        connectionDto.setHs2TestResults(existingConnection.getHs2TestResults());
                    }
                    if (existingConnection.getMetastoreDirectTestResults() != null) {
                        connectionDto.setMetastoreDirectTestResults(existingConnection.getMetastoreDirectTestResults());
                    }
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
     * Tests a connection by attempting to establish both HiveServer2 and Metastore Direct connections.
     * Updates the ConnectionDto's testResults field with the outcome.
     *
     * @param key The connection key to test
     * @return Map containing the test operation results
     */
    public Map<String, Object> test(String key) {
        log.info("Testing connection: {}", key);

        try {
            // Load the connection
            Optional<ConnectionDto> connectionOpt = connectionRepository.findByKey(key);
            if (!connectionOpt.isPresent()) {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "NOT_FOUND");
                result.put("message", "Connection not found: " + key);
                return result;
            }

            ConnectionDto connectionDto = connectionOpt.get();
            LocalDateTime testStartTime = LocalDateTime.now();
            long startMillis = System.currentTimeMillis();

            StringBuilder detailsBuilder = new StringBuilder();
            boolean hs2Success = false;
            boolean metastoreSuccess = false;
            boolean overallSuccess = false;
            String errorMessage = null;

            // Test HiveServer2 connection if configured
            if (!isBlank(connectionDto.getHs2Uri())) {
                log.info("Testing HiveServer2 connection for: {}", key);
                try {
                    hs2Success = testHS2Connection(connectionDto);
                    if (hs2Success) {
                        detailsBuilder.append("HS2 connection: SUCCESS\n");
                        log.info("HS2 connection test passed for: {}", key);
                    } else {
                        detailsBuilder.append("HS2 connection: FAILED\n");
                        log.warn("HS2 connection test failed for: {}", key);
                    }
                } catch (Exception e) {
                    hs2Success = false;
                    detailsBuilder.append("HS2 connection: FAILED - ").append(e.getMessage()).append("\n");
                    errorMessage = "HS2 connection failed: " + e.getMessage();
                    log.error("HS2 connection test error for: {}", key, e);
                }
            } else {
                detailsBuilder.append("HS2 connection: NOT CONFIGURED\n");
                log.debug("HS2 connection not configured for: {}", key);
                // If HS2 is not configured, consider it success for overall test
                hs2Success = false;
            }

            // Test Metastore Direct connection if configured
            if (connectionDto.isMetastoreDirectEnabled() &&
                !isBlank(connectionDto.getMetastoreDirectUri())) {
                log.info("Testing Metastore Direct connection for: {}", key);
                try {
                    metastoreSuccess = testMetastoreDirectConnection(connectionDto);
                    if (metastoreSuccess) {
                        detailsBuilder.append("Metastore Direct connection: SUCCESS\n");
                        log.info("Metastore Direct connection test passed for: {}", key);
                    } else {
                        detailsBuilder.append("Metastore Direct connection: FAILED\n");
                        log.warn("Metastore Direct connection test failed for: {}", key);
                    }
                } catch (Exception e) {
                    metastoreSuccess = false;
                    detailsBuilder.append("Metastore Direct connection: FAILED - ").append(e.getMessage()).append("\n");
                    if (errorMessage == null) {
                        errorMessage = "Metastore Direct connection failed: " + e.getMessage();
                    } else {
                        errorMessage += "; Metastore Direct connection failed: " + e.getMessage();
                    }
                    log.error("Metastore Direct connection test error for: {}", key, e);
                }
            } else {
                detailsBuilder.append("Metastore Direct connection: NOT CONFIGURED\n");
                log.debug("Metastore Direct connection not configured for: {}", key);
                // If Metastore Direct is not configured, consider it success for overall test
                metastoreSuccess = false;
            }

            // Overall test is successful if all configured connections succeed
            overallSuccess = hs2Success && metastoreSuccess;

            // Calculate duration
            long endMillis = System.currentTimeMillis();
            double durationSeconds = (endMillis - startMillis) / 1000.0;

            // Update individual test results for HiveServer2
            if (!isBlank(connectionDto.getHs2Uri())) {
                String hs2Details = detailsBuilder.toString().lines()
                        .filter(line -> line.contains("HS2 connection"))
                        .findFirst()
                        .orElse("HS2 connection: NOT TESTED");

                ConnectionDto.ConnectionTestResults hs2TestResults = ConnectionDto.ConnectionTestResults.builder()
                        .status(hs2Success ? ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS :
                                ConnectionDto.ConnectionTestResults.TestStatus.FAILED)
                        .lastTested(testStartTime)
                        .duration(durationSeconds)
                        .errorMessage(hs2Success ? null : (errorMessage != null && errorMessage.contains("HS2") ? errorMessage : "HS2 connection failed"))
                        .details(hs2Details)
                        .build();

                connectionDto.setHs2TestResults(hs2TestResults);
            }

            // Update individual test results for Metastore Direct
            if (connectionDto.isMetastoreDirectEnabled() && !isBlank(connectionDto.getMetastoreDirectUri())) {
                String metastoreDetails = detailsBuilder.toString().lines()
                        .filter(line -> line.contains("Metastore Direct"))
                        .findFirst()
                        .orElse("Metastore Direct connection: NOT TESTED");

                ConnectionDto.ConnectionTestResults metastoreTestResults = ConnectionDto.ConnectionTestResults.builder()
                        .status(metastoreSuccess ? ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS :
                                ConnectionDto.ConnectionTestResults.TestStatus.FAILED)
                        .lastTested(testStartTime)
                        .duration(durationSeconds)
                        .errorMessage(metastoreSuccess ? null : (errorMessage != null && errorMessage.contains("Metastore") ? errorMessage : "Metastore Direct connection failed"))
                        .details(metastoreDetails)
                        .build();

                connectionDto.setMetastoreDirectTestResults(metastoreTestResults);
            }

            // Save updated connection
            connectionRepository.save(connectionDto);

            // Build response
            Map<String, Object> result = new HashMap<>();
            result.put("status", overallSuccess ? "SUCCESS" : "FAILED");
            result.put("message", overallSuccess ? "Connection test successful" :
                       "Connection test failed: " + errorMessage);
            result.put("key", key);
            result.put("testPassed", overallSuccess);
            result.put("duration", durationSeconds);
            result.put("details", detailsBuilder.toString());

            log.info("Connection test completed for: {} - Status: {}, Duration: {}s",
                     key, overallSuccess ? "SUCCESS" : "FAILED", durationSeconds);

            return result;

        } catch (Exception e) {
            log.error("Error testing connection {}", key, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to test connection: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Tests HiveServer2 JDBC connection.
     * Properly loads and registers the HiveServer2 driver before testing.
     *
     * @param connectionDto The connection configuration
     * @return true if connection is successful, false otherwise
     */
    private boolean testHS2Connection(ConnectionDto connectionDto) throws SQLException {
        String uri = connectionDto.getHs2Uri();
        String username = connectionDto.getHs2Username();
        String password = connectionDto.getHs2Password();

        // Note: Password decryption is not performed during connection testing
        // The password should be stored in decrypted form in the ConnectionDto

        Properties props = new Properties();
        if (username != null && !username.isEmpty()) {
            props.setProperty("user", username);
        }
        if (password != null && !password.isEmpty()) {
            props.setProperty("password", password);
        }

        // Add any additional connection properties
        if (connectionDto.getHs2ConnectionProperties() != null) {
            connectionDto.getHs2ConnectionProperties().forEach((k, v) -> {
                if (!"password".equals(k) && !"user".equals(k)) { // Don't override user/password
                    props.setProperty(k, v);
                }
            });
        }

        // Load and register HiveServer2 driver (required for JDBC connection)
        Driver hs2Driver = null;
        List<DriverType> driversByPlatform = DriverType.findByPlatformType(connectionDto.getPlatformType());
        // TODO: At some point we may want to offer multiple options.
        DriverType driverType = driversByPlatform.get(0);

        if (driverUtilsService != null && driverType != null) {
            log.debug("Loading HiveServer2 driver: {}", driverType);
            // Use Environment.LEFT as a placeholder - it's only used for logging
            hs2Driver = driverUtilsService.getHs2Driver(connectionDto, Environment.LEFT);
            if (hs2Driver == null) {
                log.error("Failed to load HiveServer2 driver for type: {}", driverType);
                throw new SQLException("Failed to load HiveServer2 driver: " + driverType);
            }
            log.debug("HiveServer2 driver loaded successfully");
        }

        Connection conn = null;
        Statement stmt = null;
        try {
            // Register driver if loaded
            if (hs2Driver != null) {
                log.debug("Registering HiveServer2 driver");
                DriverManager.registerDriver(hs2Driver);
            }

            log.debug("Attempting HS2 connection to: {}", uri);
            conn = DriverManager.getConnection(uri, props);

            // Validate connection with a simple query
            stmt = conn.createStatement();
            stmt.execute("SELECT 1");

            log.debug("HS2 connection validated successfully");
            return true;
        } finally {
            // Clean up resources
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.warn("Error closing statement", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Error closing connection", e);
                }
            }
            // Deregister driver
            if (hs2Driver != null) {
                try {
                    log.debug("Deregistering HiveServer2 driver");
                    DriverManager.deregisterDriver(hs2Driver);
                } catch (SQLException e) {
                    log.warn("Error deregistering HiveServer2 driver", e);
                }
            }
        }
    }

    /**
     * Tests Metastore Direct JDBC connection.
     *
     * @param connectionDto The connection configuration
     * @return true if connection is successful, false otherwise
     */
    private boolean testMetastoreDirectConnection(ConnectionDto connectionDto) throws SQLException {
        String uri = connectionDto.getMetastoreDirectUri();
        String username = connectionDto.getMetastoreDirectUsername();
        String password = connectionDto.getMetastoreDirectPassword();

        // Note: Password decryption is not performed during connection testing
        // The password should be stored in decrypted form in the ConnectionDto

        Properties props = new Properties();
        if (username != null && !username.isEmpty()) {
            props.setProperty("user", username);
        }
        if (password != null && !password.isEmpty()) {
            props.setProperty("password", password);
        }

        // Add any additional connection properties
        if (connectionDto.getMetastoreDirectConnectionProperties() != null) {
            connectionDto.getMetastoreDirectConnectionProperties().forEach((k, v) -> {
                if (!"password".equals(k) && !"user".equals(k)) { // Don't override user/password
                    props.setProperty(k, v);
                }
            });
        }

        Connection conn = null;
        Statement stmt = null;
        try {
            log.debug("Attempting Metastore Direct connection to: {}", uri);
            conn = DriverManager.getConnection(uri, props);

            // Validate connection with a simple query
            stmt = conn.createStatement();
            stmt.execute("SELECT 1");

            log.debug("Metastore Direct connection validated successfully");
            return true;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.warn("Error closing statement", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Error closing connection", e);
                }
            }
        }
    }

    /**
     *
     * Gets the default connection.
     *
     * @return Map containing the default connection
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
     */

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

        // Check all test results to determine overall status
        ConnectionDto.ConnectionTestResults.TestStatus overallStatus = determineOverallTestStatus(conn);

        switch (status) {
            case "success":
                return overallStatus == ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS;
            case "failed":
                return overallStatus == ConnectionDto.ConnectionTestResults.TestStatus.FAILED;
            case "never_tested":
                return overallStatus == ConnectionDto.ConnectionTestResults.TestStatus.NEVER_TESTED;
            default:
                return true;
        }
    }

    private ConnectionDto.ConnectionTestResults.TestStatus determineOverallTestStatus(ConnectionDto conn) {
        List<ConnectionDto.ConnectionTestResults> allResults = new ArrayList<>();
        if (conn.getHcfsTestResults() != null) allResults.add(conn.getHcfsTestResults());
        if (conn.getHs2TestResults() != null) allResults.add(conn.getHs2TestResults());
        if (conn.getMetastoreDirectTestResults() != null) allResults.add(conn.getMetastoreDirectTestResults());

        if (allResults.isEmpty()) {
            return ConnectionDto.ConnectionTestResults.TestStatus.NEVER_TESTED;
        }

        // If any test failed, overall status is FAILED
        boolean anyFailed = allResults.stream()
                .anyMatch(r -> r.getStatus() == ConnectionDto.ConnectionTestResults.TestStatus.FAILED);
        if (anyFailed) {
            return ConnectionDto.ConnectionTestResults.TestStatus.FAILED;
        }

        // If all tests succeeded, overall status is SUCCESS
        boolean allSuccess = allResults.stream()
                .allMatch(r -> r.getStatus() == ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS);
        if (allSuccess) {
            return ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS;
        }

        // Otherwise, never tested
        return ConnectionDto.ConnectionTestResults.TestStatus.NEVER_TESTED;
    }

    private int compareConnections(ConnectionDto a, ConnectionDto b) {
        // Sort by name
        // TODO: Add default connection support when isDefault() method is implemented
        return a.getName().compareToIgnoreCase(b.getName());
    }

    /**
     * Checks if the connection configuration has changed between existing and new connection.
     * Configuration changes include changes to URIs, credentials, or connection settings.
     *
     * @param existing The existing connection
     * @param updated The updated connection
     * @return true if configuration has changed, false otherwise
     */
    private boolean hasConfigurationChanged(ConnectionDto existing, ConnectionDto updated) {
        // Check HCFS Namespace changes
        if (!Objects.equals(existing.getHcfsNamespace(), updated.getHcfsNamespace())) {
            return true;
        }

        // Check HiveServer2 configuration changes
        if (!Objects.equals(existing.getHs2Uri(), updated.getHs2Uri()) ||
            !Objects.equals(existing.getHs2Username(), updated.getHs2Username()) ||
            !Objects.equals(existing.getHs2Password(), updated.getHs2Password())) {
            return true;
        }

        // Check Metastore Direct configuration changes
        if (existing.isMetastoreDirectEnabled() != updated.isMetastoreDirectEnabled()) {
            return true;
        }

        if (existing.isMetastoreDirectEnabled() && updated.isMetastoreDirectEnabled()) {
            if (!Objects.equals(existing.getMetastoreDirectUri(), updated.getMetastoreDirectUri()) ||
                !Objects.equals(existing.getMetastoreDirectUsername(), updated.getMetastoreDirectUsername()) ||
                !Objects.equals(existing.getMetastoreDirectPassword(), updated.getMetastoreDirectPassword())) {
                return true;
            }
        }

        return false;
    }
}
