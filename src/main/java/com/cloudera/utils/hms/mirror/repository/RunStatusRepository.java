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

import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;

import java.util.List;

/**
 * Repository interface for RunStatus persistence operations.
 * Extends the base RocksDBRepository with RunStatus-specific operations.
 *
 * RunStatus objects are stored with a composite key pattern:
 * {conversionResultKey}/runStatus
 *
 * This allows RunStatus to be associated with a specific ConversionResult
 * while being stored in its own repository for efficient access.
 */
public interface RunStatusRepository extends RocksDBRepository<RunStatus, String> {
    /**
     * Save a RunStatus for a specific ConversionResult key.
     * The actual key used will be: {conversionResultKey}/runStatus
     *
     * @param conversionResultKey The key of the parent ConversionResult
     * @param runStatus The RunStatus to save
     * @return The saved RunStatus
     * @throws RepositoryException if there's an error accessing the repository
     */
    RunStatus saveByKey(String conversionResultKey, RunStatus runStatus) throws RepositoryException;

    /**
     * Find a RunStatus by its associated ConversionResult key.
     * Looks up using the key: {conversionResultKey}/runStatus
     *
     * @param conversionResultKey The key of the parent ConversionResult
     * @return The RunStatus if found, empty Optional otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    java.util.Optional<RunStatus> findByKey(String conversionResultKey) throws RepositoryException;

    /**
     * Delete a RunStatus by its associated ConversionResult key.
     * Deletes using the key: {conversionResultKey}/runStatus
     *
     * @param conversionResultKey The key of the parent ConversionResult
     * @return true if the RunStatus existed and was deleted, false otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean deleteByKey(String conversionResultKey) throws RepositoryException;

    /**
     * Check if a RunStatus exists for a specific ConversionResult key.
     *
     * @param conversionResultKey The key of the parent ConversionResult
     * @return true if a RunStatus exists, false otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean existsByKey(String conversionResultKey) throws RepositoryException;

    /**
     * Find all RunStatus objects and return them as a list.
     *
     * @return List of all RunStatus objects
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<RunStatus> findAllAsList() throws RepositoryException;


}
