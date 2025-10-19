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
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Map;

/**
 * Repository for managing DBMirror instances in RocksDB.
 *
 * The key for a DBMirror is a composite of the ConversionResult key and the DBMirror name,
 * formatted as "conversionResultKey/databaseName".
 * The value is the DBMirror instance serialized as YAML.
 */
public interface DBMirrorRepository extends RocksDBRepository<DBMirror, String> {
    String DATABASE_PREFIX = "/database/";

    /**
     * Find all DBMirror instances for a specific ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @return Map of database names to DBMirror instances
     * @throws RocksDBException if there's an error accessing RocksDB
     */
    Map<String, DBMirror> findByConversionResult(String conversionResultKey) throws RocksDBException;

    /**
     * Save a DBMirror instance for a specific ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @param dbMirror The DBMirror instance to save
     * @return The saved DBMirror instance
     * @throws RocksDBException if there's an error accessing RocksDB
     */
    DBMirror save(String conversionResultKey, String databaseName, DBMirror dbMirror) throws RocksDBException;

    /**
     * Delete all DBMirror instances for a specific ConversionResult.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @throws RocksDBException if there's an error accessing RocksDB
     */
    void deleteByConversionResult(String conversionResultKey) throws RocksDBException;

    /**
     * Build a composite key from ConversionResult key and database name.
     *
     * @param conversionResultKey The key of the ConversionResult
     * @param databaseName The name of the database
     * @return The composite key in format "conversionResultKey/databaseName"
     */
    static String buildKey(String conversionResultKey, String databaseName) {
        return conversionResultKey + DATABASE_PREFIX + databaseName;
    }
}
