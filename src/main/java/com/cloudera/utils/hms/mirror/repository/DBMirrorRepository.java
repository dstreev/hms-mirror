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

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Repository for managing DBMirror instances.
 *
 * The key for a DBMirror is a composite of the ConversionResult key and the DBMirror name,
 * formatted as "conversionResultKey/databaseName".
 * The value is the DBMirror instance serialized.
 */
public interface DBMirrorRepository extends RocksDBRepository<DBMirror, String> {
    String DATABASE_PREFIX = "/database/";
    String KEY_PREFIX = "db:";

    /**
     * Find all DBMirror instances for a specific ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @return Map of database names to DBMirror instances
     * @throws RepositoryException if there's an error accessing the repository
     */
    Map<String, DBMirror> findByConversionKey(String conversionResultKey) throws RepositoryException;

    /**
     * Save a DBMirror instance for a specific ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param dbMirror The DBMirror instance to save
     * @return The saved DBMirror instance
     * @throws RepositoryException if there's an error accessing the repository
     */
    DBMirror save(String conversionResultKey, DBMirror dbMirror) throws RepositoryException;

    /**
     * Delete all DBMirror instances for a specific ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @throws RepositoryException if there's an error accessing the repository
     */
    void deleteByConversionKey(String conversionResultKey) throws RepositoryException;

    /**
     * Delete a specific DBMirror instance by name.
     * This method matches the save signature pattern by taking the same identifiers needed to build the composite key.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @return true if the DBMirror existed and was deleted, false if it didn't exist
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean deleteByName(String conversionResultKey, String databaseName) throws RepositoryException;

    /**
     * Find all database names for a specific ConversionResult.
     * Returns a list of database names (extracted from DBMirror.getName()).
     *
     * @param conversionResultKey The key of the ConversionResult
     * @return List of database names
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<String> listNamesByKey(String conversionResultKey) throws RepositoryException;

    /**
     * Find a specific DBMirror instance by ConversionResult key and database name.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @return Optional containing the DBMirror if found, empty otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    Optional<DBMirror> findByName(String conversionResultKey, String databaseName) throws RepositoryException;

    /**
     * Build a composite key from ConversionResult key and database name.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @return The composite key in format "conversionResultKey/databaseName"
     */
    static String buildKey(String conversionResultKey, String databaseName) {
        String key = conversionResultKey + DATABASE_PREFIX + databaseName;
        return key;
    }

    /**
     * Build a prefixed key from ConversionResult key and database name.
     * Used for storing and searching with the prefix.
     *
     * @param conversionResultKey
     * @param databaseName
     * @return
     */
    static String buildPrefixedKey(String conversionResultKey, String databaseName) {
        return KEY_PREFIX + buildKey(conversionResultKey, databaseName);
    }


    static String buildSearchPrefix(String conversionResultKey) {
        return KEY_PREFIX + conversionResultKey + DATABASE_PREFIX;
    }

}
