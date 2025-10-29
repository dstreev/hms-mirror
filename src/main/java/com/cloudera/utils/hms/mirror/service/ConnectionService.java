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

@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class ConnectionService {

    private final ConnectionRepository connectionRepository;

    public List<ConnectionDto> getAllConnections() throws RepositoryException {
        Map<String, ConnectionDto> connectionMap = connectionRepository.findAll();
        return new ArrayList<>(connectionMap.values());
    }

    public List<ConnectionDto> getFilteredConnections(String search, String environment, String status) throws RepositoryException {
        List<ConnectionDto> connectionDtos = getAllConnections();
        
        return connectionDtos.stream()
                .filter(conn -> matchesSearch(conn, search))
                .filter(conn -> matchesEnvironment(conn, environment))
                .filter(conn -> matchesStatus(conn, status))
                .sorted(this::compareConnections)
                .collect(Collectors.toList());
    }

    public Optional<ConnectionDto> getConnectionById(String id) throws RepositoryException {
        return connectionRepository.findById(id);
    }

    public ConnectionDto createConnection(ConnectionDto connectionDto) throws RepositoryException {
        // Use the connection name as the key in RocksDB
        String id = connectionDto.getName();
        
        // Check if a connection with this name already exists
        if (connectionRepository.existsById(id)) {
            throw new RuntimeException("Connection with name '" + id + "' already exists");
        }
        
        connectionDto.setId(id);
        
        // If this is the first connection, make it default
        if (getAllConnections().isEmpty()) {
            connectionDto.setDefault(true);
        }
        
        log.info("Saving connection to RocksDB with key: {}", id);
        return connectionRepository.save(id, connectionDto);
    }

    public ConnectionDto updateConnection(String id, ConnectionDto connectionDto) throws RepositoryException {
        Optional<ConnectionDto> existingOpt = connectionRepository.findById(id);
        if (existingOpt.isEmpty()) {
            throw new RuntimeException("Connection not found: " + id);
        }
        
        ConnectionDto existing = existingOpt.get();
        connectionDto.setId(id);
        connectionDto.setCreated(existing.getCreated());
        
        return connectionRepository.save(id, connectionDto);
    }

    public boolean deleteConnection(String id) throws RepositoryException {
        Optional<ConnectionDto> connectionOpt = connectionRepository.findById(id);
        if (connectionOpt.isEmpty()) {
            return false;
        }
        
        ConnectionDto connectionDto = connectionOpt.get();
        boolean wasDeleted = connectionRepository.deleteById(id);

        return wasDeleted;
    }

    public Optional<ConnectionDto> getDefaultConnection() throws RepositoryException {
        return connectionRepository.findDefaultConnection();
    }

    public boolean testConnection(String id) throws RepositoryException {
        return connectionRepository.testConnection(id);
    }

//    public List<ConnectionDto> getConnectionsByEnvironment(ConnectionDto.Environment environment) throws RocksDBException {
//        return connectionRepository.findByEnvironment(environment);
//    }
//
//    public ConnectionDto duplicateConnection(String sourceId, String newName) throws RocksDBException {
//        Optional<ConnectionDto> sourceOpt = connectionRepository.findById(sourceId);
//        if (sourceOpt.isEmpty()) {
//            throw new RuntimeException("Source connection not found: " + sourceId);
//        }
//
//        ConnectionDto source = sourceOpt.get();
//        ConnectionDto duplicate = ConnectionDto.builder()
//                .name(newName)
//                .description(source.getDescription() + " (Copy)")
//                .environment(source.getEnvironment())
//                .platformType(source.getPlatformType())
//                .hcfsNamespace(source.getHcfsNamespace())
//                .hs2Uri(source.getHs2Uri())
//                .hs2Username(source.getHs2Username())
//                .hs2Password(source.getHs2Password())
////                .hs2DriverClassName(source.getHs2DriverClassName())
////                .hs2JarFile(source.getHs2JarFile())
////                .hs2Disconnected(source.isHs2Disconnected())
//                .hs2ConnectionProperties(source.getHs2ConnectionProperties())
//                .metastoreDirectEnabled(source.isMetastoreDirectEnabled())
//                .metastoreDirectUri(source.getMetastoreDirectUri())
//                .metastoreDirectType(source.getMetastoreDirectType())
//                .metastoreDirectUsername(source.getMetastoreDirectUsername())
//                .metastoreDirectPassword(source.getMetastoreDirectPassword())
//                .metastoreDirectMinConnections(source.getMetastoreDirectMinConnections())
//                .metastoreDirectMaxConnections(source.getMetastoreDirectMaxConnections())
//                .partitionDiscoveryAuto(source.isPartitionDiscoveryAuto())
//                .partitionDiscoveryInitMSCK(source.isPartitionDiscoveryInitMSCK())
//                .partitionBucketLimit(source.getPartitionBucketLimit())
//                .createIfNotExists(source.isCreateIfNotExists())
//                .enableAutoTableStats(source.isEnableAutoTableStats())
//                .enableAutoColumnStats(source.isEnableAutoColumnStats())
//                .isDefault(false)
//                .build();
//
//        return createConnection(duplicate);
//    }

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

    /*
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
     */
}