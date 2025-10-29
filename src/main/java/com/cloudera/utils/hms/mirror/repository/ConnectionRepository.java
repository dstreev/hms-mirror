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

package com.cloudera.utils.hms.mirror.repository;

import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Connection persistence operations.
 * Extends the base RocksDBRepository with Connection-specific operations.
 */
public interface ConnectionRepository extends RocksDBRepository<ConnectionDto, String> {

    /**
     * Find the default connection.
     *
     * @return Optional containing the default connection if found
     * @throws RepositoryException if there's an error accessing the repository
     */
    Optional<ConnectionDto> findDefaultConnection() throws RepositoryException;

    /**
     * Find all connections for a specific environment.
     *
     * @param environment The environment to filter by
     * @return List of connections for the specified environment
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<ConnectionDto> findByEnvironment(ConnectionDto.Environment environment) throws RepositoryException;

    /**
     * Test a connection.
     *
     * @param connectionId The ID of the connection to test
     * @return true if the connection is successful, false otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean testConnection(String connectionId) throws RepositoryException;
}