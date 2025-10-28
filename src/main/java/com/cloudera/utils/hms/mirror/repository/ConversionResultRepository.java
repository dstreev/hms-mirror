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

import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;

import java.util.Date;
import java.util.List;

/**
 * Repository interface for ConversionResult persistence operations.
 *
 * ConversionResult objects are stored in the repository.
 * The key is ConversionResult.key which follows the format "yyyyMMdd_HHmmss".
 *
 * This repository provides CRUD operations and date-based filtering for
 * querying conversion results within specific time ranges.
 */
public interface ConversionResultRepository extends RocksDBRepository<ConversionResult, String> {

    /**
     * Save a ConversionResult using its internal key field.
     *
     * @param conversionResult The ConversionResult to save
     * @return The saved ConversionResult
     * @throws RepositoryException if there's an error accessing the repository
     */
    ConversionResult save(ConversionResult conversionResult) throws RepositoryException;

    /**
     * Find all ConversionResults between two dates (inclusive).
     * Uses the key format "yyyyMMdd_HHmmss" for date comparison.
     *
     * @param startDate The start date (inclusive)
     * @param endDate The end date (inclusive)
     * @return List of ConversionResults created between the specified dates
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<ConversionResult> findByDateRange(Date startDate, Date endDate) throws RepositoryException;

    /**
     * Find all ConversionResults created after a specific date.
     *
     * @param startDate The start date (inclusive)
     * @return List of ConversionResults created on or after the specified date
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<ConversionResult> findByDateAfter(Date startDate) throws RepositoryException;

    /**
     * Find all ConversionResults created before a specific date.
     *
     * @param endDate The end date (inclusive)
     * @return List of ConversionResults created on or before the specified date
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<ConversionResult> findByDateBefore(Date endDate) throws RepositoryException;

    /**
     * Find all ConversionResults and return them as a list.
     *
     * @return List of all ConversionResults
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<ConversionResult> findAllAsList() throws RepositoryException;

    /**
     * Delete ConversionResults older than a specific date.
     * Useful for cleanup/purge operations.
     *
     * @param beforeDate The cutoff date
     * @return Number of ConversionResults deleted
     * @throws RepositoryException if there's an error accessing the repository
     */
    int deleteByDateBefore(Date beforeDate) throws RepositoryException;
}
