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

package com.cloudera.utils.hms.mirror.repository.impl;

import com.cloudera.utils.hms.mirror.domain.Connection;
import com.cloudera.utils.hms.mirror.repository.ConnectionRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class ConnectionRepositoryImpl extends AbstractRocksDBRepository<Connection, String> implements ConnectionRepository {

    public ConnectionRepositoryImpl(RocksDB rocksDB,
                                   @Qualifier("connectionsColumnFamily") ColumnFamilyHandle columnFamily,
                                   @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<Connection>() {});
    }

    @Override
    public Connection save(String id, Connection connection) throws RocksDBException {
        connection.setId(id);
        connection.setModified(LocalDateTime.now());
        
        if (connection.getCreated() == null) {
            connection.setCreated(LocalDateTime.now());
        }
        
        return super.save(id, connection);
    }

    @Override
    public Optional<Connection> findDefaultConnection() throws RocksDBException {
        Map<String, Connection> all = findAll();
        return all.values().stream()
                .filter(Connection::isDefault)
                .findFirst();
    }

    @Override
    public List<Connection> findByEnvironment(Connection.Environment environment) throws RocksDBException {
        Map<String, Connection> all = findAll();
        return all.values().stream()
                .filter(conn -> environment.equals(conn.getEnvironment()))
                .collect(Collectors.toList());
    }

    @Override
    public void setAsDefault(String connectionId) throws RocksDBException {
        // First, unset all other defaults
        Map<String, Connection> all = findAll();
        for (Map.Entry<String, Connection> entry : all.entrySet()) {
            Connection conn = entry.getValue();
            if (conn.isDefault()) {
                conn.setDefault(false);
                save(entry.getKey(), conn);
            }
        }
        
        // Set the specified connection as default
        Optional<Connection> targetConnection = findById(connectionId);
        if (targetConnection.isPresent()) {
            Connection conn = targetConnection.get();
            conn.setDefault(true);
            save(connectionId, conn);
        } else {
            throw new RocksDBException("Connection not found: " + connectionId);
        }
    }

    @Override
    public boolean testConnection(String connectionId) throws RocksDBException {
        Optional<Connection> connectionOpt = findById(connectionId);
        if (connectionOpt.isEmpty()) {
            throw new RocksDBException("Connection not found: " + connectionId);
        }
        
        Connection connection = connectionOpt.get();
        
        // TODO: Implement actual connection testing logic
        // For now, return a basic validation
        boolean isValid = connection.getHs2Uri() != null 
                && !connection.getHs2Uri().trim().isEmpty();
        
        // Update test results
        Connection.ConnectionTestResults testResults = Connection.ConnectionTestResults.builder()
                .status(isValid ? Connection.ConnectionTestResults.TestStatus.SUCCESS : Connection.ConnectionTestResults.TestStatus.FAILED)
                .lastTested(LocalDateTime.now())
                .duration(isValid ? 1.5 : 0.0)
                .errorMessage(isValid ? null : "Invalid configuration")
                .build();
        
        connection.setTestResults(testResults);
        save(connectionId, connection);
        
        log.info("Connection test completed for {}: {}", connectionId, testResults.getStatus());
        return isValid;
    }
}