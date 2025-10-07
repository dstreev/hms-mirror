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

import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic repository interface for RocksDB operations.
 * 
 * @param <T> The entity type
 * @param <ID> The identifier type (typically String)
 */
public interface RocksDBRepository<T, ID> {

    /**
     * Save or update an entity.
     */
    T save(ID id, T entity) throws RocksDBException;

    /**
     * Find an entity by its identifier.
     */
    Optional<T> findById(ID id) throws RocksDBException;

    /**
     * Check if an entity exists by its identifier.
     */
    boolean existsById(ID id) throws RocksDBException;

    /**
     * Find all entities in the column family.
     */
    Map<ID, T> findAll() throws RocksDBException;

    /**
     * Find all entity IDs in the column family.
     */
    List<ID> findAllIds() throws RocksDBException;

    /**
     * Delete an entity by its identifier.
     */
    boolean deleteById(ID id) throws RocksDBException;

    /**
     * Delete all entities in the column family.
     */
    void deleteAll() throws RocksDBException;

    /**
     * Count the number of entities in the column family.
     */
    long count() throws RocksDBException;

    /**
     * Get the size in bytes of the column family.
     */
    long getSizeInBytes() throws RocksDBException;
}