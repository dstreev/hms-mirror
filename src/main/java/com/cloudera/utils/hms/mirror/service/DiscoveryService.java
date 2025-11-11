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
import com.cloudera.utils.hms.mirror.repository.ConnectionRepository;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service for discovering databases and tables from HiveServer2 connections.
 * Provides methods to query metadata about databases and tables without
 * requiring a full migration session.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DiscoveryService {

    @NonNull
    private final ConnectionRepository connectionRepository;
    @NonNull
    private final DriverUtilsService driverUtilsService;

    /**
     * Fetch list of databases from a connection.
     *
     * @param connectionKey The connection key to query
     * @return List of database names
     * @throws Exception if connection fails or query cannot be executed
     */
    public List<String> getDatabases(String connectionKey) throws Exception {
        log.info("Fetching databases for connection: {}", connectionKey);

        // Load the connection
        Optional<ConnectionDto> connectionOpt = connectionRepository.findByKey(connectionKey);
        if (!connectionOpt.isPresent()) {
            throw new IllegalArgumentException("Connection not found: " + connectionKey);
        }

        ConnectionDto connectionDto = connectionOpt.get();

        // Verify HS2 is configured
        if (isBlank(connectionDto.getHs2Uri())) {
            throw new IllegalStateException("HiveServer2 URI not configured for connection: " + connectionKey);
        }

        // Verify HS2 connection was tested successfully
        if (connectionDto.getHs2TestResults() == null ||
            connectionDto.getHs2TestResults().getStatus() != ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS) {
            throw new IllegalStateException("HiveServer2 connection has not been successfully tested. Please test the connection first.");
        }

        List<String> databases = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Create connection
            conn = createHS2Connection(connectionDto);

            // Execute SHOW DATABASES
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW DATABASES");

            // Collect database names
            while (rs.next()) {
                String dbName = rs.getString(1);
                databases.add(dbName);
            }

            log.info("Found {} databases for connection: {}", databases.size(), connectionKey);
            return databases;

        } finally {
            closeResources(rs, stmt, conn);
        }
    }

    /**
     * Fetch list of tables from a specific database in a connection.
     *
     * @param connectionKey The connection key to query
     * @param databaseName The database name to query tables from
     * @return List of table names
     * @throws Exception if connection fails or query cannot be executed
     */
    public List<String> getTables(String connectionKey, String databaseName) throws Exception {
        log.info("Fetching tables for database {} in connection: {}", databaseName, connectionKey);

        // Load the connection
        Optional<ConnectionDto> connectionOpt = connectionRepository.findByKey(connectionKey);
        if (!connectionOpt.isPresent()) {
            throw new IllegalArgumentException("Connection not found: " + connectionKey);
        }

        ConnectionDto connectionDto = connectionOpt.get();

        // Verify HS2 is configured
        if (isBlank(connectionDto.getHs2Uri())) {
            throw new IllegalStateException("HiveServer2 URI not configured for connection: " + connectionKey);
        }

        // Verify HS2 connection was tested successfully
        if (connectionDto.getHs2TestResults() == null ||
            connectionDto.getHs2TestResults().getStatus() != ConnectionDto.ConnectionTestResults.TestStatus.SUCCESS) {
            throw new IllegalStateException("HiveServer2 connection has not been successfully tested. Please test the connection first.");
        }

        // Validate database name to prevent SQL injection
        if (isBlank(databaseName) || !databaseName.matches("^[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Invalid database name: " + databaseName);
        }

        List<String> tables = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Create connection
            conn = createHS2Connection(connectionDto);

            // Execute SHOW TABLES IN database
            stmt = conn.createStatement();
            String query = "SHOW TABLES IN " + databaseName;
            rs = stmt.executeQuery(query);

            // Collect table names
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }

            log.info("Found {} tables in database {} for connection: {}", tables.size(), databaseName, connectionKey);
            return tables;

        } finally {
            closeResources(rs, stmt, conn);
        }
    }

    /**
     * Create an HS2 JDBC connection from a ConnectionDto.
     * Uses the same pattern as ConnectionManagementService.testHS2Connection.
     *
     * @param connectionDto The connection configuration
     * @return A JDBC Connection
     * @throws SQLException if connection cannot be established
     */
    private Connection createHS2Connection(ConnectionDto connectionDto) throws SQLException {
        String uri = connectionDto.getHs2Uri();
        String username = connectionDto.getHs2Username();
        String password = connectionDto.getHs2Password();

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

        // Load and register HiveServer2 driver
        Driver hs2Driver = null;
        List<DriverType> driversByPlatform = DriverType.findByPlatformType(connectionDto.getPlatformType());
        if (driversByPlatform != null && !driversByPlatform.isEmpty()) {
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
        }

        try {
            // Register driver if loaded
            if (hs2Driver != null) {
                log.debug("Registering HiveServer2 driver");
                DriverManager.registerDriver(hs2Driver);
            }

            log.debug("Attempting HS2 connection to: {}", uri);
            Connection conn = DriverManager.getConnection(uri, props);
            log.debug("HS2 connection established successfully");

            // Store the driver reference for cleanup (attach as connection property/wrapper if needed)
            // For now, we'll handle driver deregistration in closeResources if needed

            return conn;
        } catch (SQLException e) {
            // Deregister driver on failure
            if (hs2Driver != null) {
                try {
                    DriverManager.deregisterDriver(hs2Driver);
                } catch (SQLException de) {
                    log.warn("Error deregistering driver", de);
                }
            }
            throw e;
        }
    }

    /**
     * Close JDBC resources safely.
     *
     * @param rs ResultSet to close
     * @param stmt Statement to close
     * @param conn Connection to close
     */
    private void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Error closing ResultSet", e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.warn("Error closing Statement", e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("Error closing Connection", e);
            }
        }
    }
}
