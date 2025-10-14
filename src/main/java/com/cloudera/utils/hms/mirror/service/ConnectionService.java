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

import com.cloudera.utils.hms.mirror.domain.core.Connection;
import com.cloudera.utils.hms.mirror.repository.ConnectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class ConnectionService {

    private final ConnectionRepository connectionRepository;

    public List<Connection> getAllConnections() throws RocksDBException {
        Map<String, Connection> connectionMap = connectionRepository.findAll();
        return new ArrayList<>(connectionMap.values());
    }

    public List<Connection> getFilteredConnections(String search, String environment, String status) throws RocksDBException {
        List<Connection> connections = getAllConnections();
        
        return connections.stream()
                .filter(conn -> matchesSearch(conn, search))
                .filter(conn -> matchesEnvironment(conn, environment))
                .filter(conn -> matchesStatus(conn, status))
                .sorted(this::compareConnections)
                .collect(Collectors.toList());
    }

    public Optional<Connection> getConnectionById(String id) throws RocksDBException {
        return connectionRepository.findById(id);
    }

    public Connection createConnection(Connection connection) throws RocksDBException {
        // Use the connection name as the key in RocksDB
        String id = connection.getName();
        
        // Check if a connection with this name already exists
        if (connectionRepository.existsById(id)) {
            throw new RuntimeException("Connection with name '" + id + "' already exists");
        }
        
        connection.setId(id);
        
        // If this is the first connection, make it default
        if (getAllConnections().isEmpty()) {
            connection.setDefault(true);
        }
        
        log.info("Saving connection to RocksDB with key: {}", id);
        return connectionRepository.save(id, connection);
    }

    public Connection updateConnection(String id, Connection connection) throws RocksDBException {
        Optional<Connection> existingOpt = connectionRepository.findById(id);
        if (existingOpt.isEmpty()) {
            throw new RuntimeException("Connection not found: " + id);
        }
        
        Connection existing = existingOpt.get();
        connection.setId(id);
        connection.setCreated(existing.getCreated());
        
        return connectionRepository.save(id, connection);
    }

    public boolean deleteConnection(String id) throws RocksDBException {
        Optional<Connection> connectionOpt = connectionRepository.findById(id);
        if (connectionOpt.isEmpty()) {
            return false;
        }
        
        Connection connection = connectionOpt.get();
        boolean wasDeleted = connectionRepository.deleteById(id);
        
        // If we deleted the default connection, set another one as default
        if (wasDeleted && connection.isDefault()) {
            List<Connection> remaining = getAllConnections();
            if (!remaining.isEmpty()) {
                connectionRepository.setAsDefault(remaining.get(0).getId());
            }
        }
        
        return wasDeleted;
    }

    public Optional<Connection> getDefaultConnection() throws RocksDBException {
        return connectionRepository.findDefaultConnection();
    }

    public void setDefaultConnection(String id) throws RocksDBException {
        connectionRepository.setAsDefault(id);
    }

    public boolean testConnection(String id) throws RocksDBException {
        return connectionRepository.testConnection(id);
    }

    public List<Connection> getConnectionsByEnvironment(Connection.Environment environment) throws RocksDBException {
        return connectionRepository.findByEnvironment(environment);
    }

    public Connection duplicateConnection(String sourceId, String newName) throws RocksDBException {
        Optional<Connection> sourceOpt = connectionRepository.findById(sourceId);
        if (sourceOpt.isEmpty()) {
            throw new RuntimeException("Source connection not found: " + sourceId);
        }
        
        Connection source = sourceOpt.get();
        Connection duplicate = Connection.builder()
                .name(newName)
                .description(source.getDescription() + " (Copy)")
                .environment(source.getEnvironment())
                .platformType(source.getPlatformType())
                .hcfsNamespace(source.getHcfsNamespace())
                .hs2Uri(source.getHs2Uri())
                .hs2Username(source.getHs2Username())
                .hs2Password(source.getHs2Password())
                .hs2DriverClassName(source.getHs2DriverClassName())
                .hs2JarFile(source.getHs2JarFile())
                .hs2Disconnected(source.isHs2Disconnected())
                .hs2ConnectionProperties(source.getHs2ConnectionProperties())
                .metastoreDirectEnabled(source.isMetastoreDirectEnabled())
                .metastoreDirectUri(source.getMetastoreDirectUri())
                .metastoreDirectType(source.getMetastoreDirectType())
                .metastoreDirectUsername(source.getMetastoreDirectUsername())
                .metastoreDirectPassword(source.getMetastoreDirectPassword())
                .metastoreDirectMinConnections(source.getMetastoreDirectMinConnections())
                .metastoreDirectMaxConnections(source.getMetastoreDirectMaxConnections())
                .connectionPoolLib(source.getConnectionPoolLib())
                .partitionDiscoveryAuto(source.isPartitionDiscoveryAuto())
                .partitionDiscoveryInitMSCK(source.isPartitionDiscoveryInitMSCK())
                .partitionBucketLimit(source.getPartitionBucketLimit())
                .createIfNotExists(source.isCreateIfNotExists())
                .enableAutoTableStats(source.isEnableAutoTableStats())
                .enableAutoColumnStats(source.isEnableAutoColumnStats())
                .isDefault(false)
                .build();
        
        return createConnection(duplicate);
    }

    private boolean matchesSearch(Connection conn, String search) {
        if (search == null || search.trim().isEmpty()) {
            return true;
        }
        String searchLower = search.toLowerCase();
        return conn.getName().toLowerCase().contains(searchLower) ||
               (conn.getDescription() != null && conn.getDescription().toLowerCase().contains(searchLower));
    }

    private boolean matchesEnvironment(Connection conn, String environment) {
        if (environment == null || environment.trim().isEmpty()) {
            return true;
        }
        return conn.getEnvironment() != null && conn.getEnvironment().name().equals(environment);
    }

    private boolean matchesStatus(Connection conn, String status) {
        if (status == null || status.trim().isEmpty() || "all".equals(status)) {
            return true;
        }
        
        Connection.ConnectionTestResults.TestStatus testStatus = 
                conn.getTestResults() != null ? conn.getTestResults().getStatus() : 
                Connection.ConnectionTestResults.TestStatus.NEVER_TESTED;
        
        switch (status) {
            case "success":
                return testStatus == Connection.ConnectionTestResults.TestStatus.SUCCESS;
            case "failed":
                return testStatus == Connection.ConnectionTestResults.TestStatus.FAILED;
            case "never_tested":
                return testStatus == Connection.ConnectionTestResults.TestStatus.NEVER_TESTED;
            default:
                return true;
        }
    }

    private int compareConnections(Connection a, Connection b) {
        // Default connections first
        if (a.isDefault() && !b.isDefault()) return -1;
        if (!a.isDefault() && b.isDefault()) return 1;
        
        // Then by name
        return a.getName().compareToIgnoreCase(b.getName());
    }

    private String generateConnectionId(String name) {
        String baseId = name.toLowerCase()
                .replaceAll("[^a-z0-9]", "_")
                .replaceAll("_+", "_")
                .replaceAll("^_|_$", "");
        
        if (baseId.isEmpty()) {
            baseId = "connection";
        }
        
        String id = baseId;
        int counter = 1;
        
        try {
            while (connectionRepository.existsById(id)) {
                id = baseId + "_" + counter++;
            }
        } catch (RocksDBException e) {
            log.warn("Error checking for existing connection ID, using UUID fallback", e);
            return UUID.randomUUID().toString();
        }
        
        return id;
    }
}