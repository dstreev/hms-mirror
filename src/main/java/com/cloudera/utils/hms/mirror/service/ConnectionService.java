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
 * @deprecated Use ConnectionManagementService instead. This service delegates to
 * ConnectionManagementService and exists only for backward compatibility.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
@Deprecated
public class ConnectionService {

    private final ConnectionManagementService connectionManagementService;
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

    public Optional<ConnectionDto> getConnectionByKey(String key) throws RepositoryException {
        return connectionRepository.findById(key);
    }

    public ConnectionDto createConnection(ConnectionDto connectionDto) throws RepositoryException {
        // Check if a connection with this name already exists
        if (connectionRepository.existsById(connectionDto.getKey())) {
            throw new RuntimeException("Connection with name '" + connectionDto.getKey() + "' already exists");
        }

        log.info("Saving connection to RocksDB with key: {}", connectionDto.getKey());
        return connectionRepository.save(connectionDto);
    }

    public ConnectionDto updateConnection(ConnectionDto connectionDto) throws RepositoryException {
        Optional<ConnectionDto> existingOpt = connectionRepository.findById(connectionDto.getKey());
        if (existingOpt.isEmpty()) {
            throw new RuntimeException("Connection not found: " + connectionDto.getKey());
        }

        return connectionRepository.save(connectionDto);
    }

    public boolean deleteConnection(String key) throws RepositoryException {
        Optional<ConnectionDto> connectionOpt = connectionRepository.findById(key);
        if (connectionOpt.isEmpty()) {
            return false;
        }

        ConnectionDto connectionDto = connectionOpt.get();
        boolean wasDeleted = connectionRepository.deleteById(key);

        return wasDeleted;
    }

    public Optional<ConnectionDto> getDefaultConnection() throws RepositoryException {
        return connectionRepository.findDefaultConnection();
    }

    public boolean testConnection(String key) throws RepositoryException {
        return connectionRepository.testConnection(key);
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
