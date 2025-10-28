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

import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;

import java.util.Map;

/**
 * Repository for managing TableMirror instances.
 *
 * The key for a TableMirror is a composite of the ConversionResult key, database name, and table name,
 * formatted as "conversionResultKey/database/databaseName/table/tableName".
 * The value is the TableMirror instance serialized.
 */
public interface TableMirrorRepository extends RocksDBRepository<TableMirror, String> {
    String DATABASE_PREFIX = "/database/";
    String TABLE_PREFIX = "/table/";

    /**
     * Find a specific TableMirror by name within a database.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @param tableName The name of the table
     * @return The TableMirror instance, or null if not found
     * @throws RepositoryException if there's an error accessing the repository
     */
    TableMirror findByName(String conversionResultKey, String databaseName, String tableName) throws RepositoryException;

    /**
     * Find all TableMirror instances for a specific database within a ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @return Map of table names to TableMirror instances
     * @throws RepositoryException if there's an error accessing the repository
     */
    Map<String, TableMirror> findByDatabase(String conversionResultKey, String databaseName) throws RepositoryException;

    /**
     * Save a TableMirror instance for a specific database and table.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @param tableMirror The TableMirror instance to save (table name extracted from tableMirror.getName())
     * @return The saved TableMirror instance
     * @throws RepositoryException if there's an error accessing the repository
     */
    TableMirror save(String conversionResultKey, String databaseName, TableMirror tableMirror)
            throws RepositoryException;

    /**
     * Delete a specific TableMirror instance by name.
     * This method matches the save signature pattern by taking the same identifiers needed to build the composite key.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @param tableName The name of the table
     * @return true if the TableMirror existed and was deleted, false if it didn't exist
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean deleteByName(String conversionResultKey, String databaseName, String tableName) throws RepositoryException;

    /**
     * Delete all TableMirror instances for a specific database.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @throws RepositoryException if there's an error accessing the repository
     */
    void deleteByDatabase(String conversionResultKey, String databaseName) throws RepositoryException;

    /**
     * Build a composite key from ConversionResult key, database name, and table name.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @param tableName The name of the table
     * @return The composite key in format "conversionResultKey/database/databaseName/table/tableName"
     */
    static String buildKey(String conversionResultKey, String databaseName, String tableName) {
        return conversionResultKey + DATABASE_PREFIX + databaseName + TABLE_PREFIX + tableName;
    }

    /**
     * Build a database prefix for iteration.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @return The database prefix in format "conversionResultKey/database/databaseName/table/"
     */
    static String buildPrefixKey(String conversionResultKey, String databaseName) {
        return conversionResultKey + DATABASE_PREFIX + databaseName + TABLE_PREFIX;
    }
}
