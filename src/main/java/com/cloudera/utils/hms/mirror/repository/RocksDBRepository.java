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

import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic repository interface for persistence operations.
 * This interface is implementation-agnostic and can be backed by any persistence layer.
 *
 * @param <T> The entity type
 * @param <ID> The identifier type (typically String)
 */
public interface RocksDBRepository<T, ID> {

    /**
     * Save or update an entity.
     *
     * @param id The entity identifier
     * @param entity The entity to save
     * @return The saved entity
     * @throws RepositoryException if there's an error accessing the repository
     */
//    T save(ID id, T entity) throws RepositoryException;

    /**
     * Find an entity by its identifier.
     *
     * @param id The entity identifier
     * @return Optional containing the entity if found, empty otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    Optional<T> findByKey(ID id) throws RepositoryException;

    /**
     * Check if an entity exists by its identifier.
     *
     * @param id The entity identifier
     * @return true if the entity exists, false otherwise
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean existsById(ID id) throws RepositoryException;

    /**
     * Find all entities in the repository.
     *
     * @return Map of all entities keyed by their identifier
     * @throws RepositoryException if there's an error accessing the repository
     */
    Map<ID, T> findAll() throws RepositoryException;

    /**
     * Find all entity IDs in the repository.
     *
     * @return List of all entity identifiers
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<ID> findAllIds() throws RepositoryException;

    boolean delete(T entity) throws RepositoryException;

    /**
     * Delete an entity by its identifier.
     *
     * @param id The entity identifier
     * @return true if the entity was deleted, false if it didn't exist
     * @throws RepositoryException if there's an error accessing the repository
     */
    boolean deleteById(ID id) throws RepositoryException;

    /**
     * Delete all entities in the repository.
     *
     * @throws RepositoryException if there's an error accessing the repository
     */
    void deleteAll() throws RepositoryException;

    /**
     * Count the number of entities in the repository.
     *
     * @return The total count of entities
     * @throws RepositoryException if there's an error accessing the repository
     */
    long count() throws RepositoryException;

    /**
     * Get the size in bytes of the repository.
     *
     * @return The size in bytes
     * @throws RepositoryException if there's an error accessing the repository
     */
    long getSizeInBytes() throws RepositoryException;

    /**
     * Find all key suffixes that match a given prefix.
     * Returns the portion of each key that comes after the prefix.
     *
     * For example, if keys are "parent1/child1", "parent1/child2", "parent2/child3"
     * and prefix is "parent1/", the result will be ["child1", "child2"].
     *
     * @param prefix The key prefix to search for
     * @return List of key suffixes (key minus the prefix) for all matching keys
     * @throws RepositoryException if there's an error accessing the repository
     */
    List<String> findKeySuffixesByPrefix(String prefix) throws RepositoryException;
}