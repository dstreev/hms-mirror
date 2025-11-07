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

package com.cloudera.utils.hms.mirror.repository.rocksDbImpl;

import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.RocksDBRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Abstract base implementation for RocksDB repositories.
 * Provides common functionality for JSON serialization/deserialization and basic CRUD operations.
 * Wraps RocksDB-specific exceptions in generic RepositoryException.
 *
 * @param <T>  The entity type
 * @param <ID> The identifier type (typically String)
 */
@Slf4j
public abstract class AbstractRocksDBRepository<T, ID> implements RocksDBRepository<T, ID> {

    protected final RocksDB rocksDB;
    protected final ColumnFamilyHandle columnFamily;
    protected final ObjectMapper objectMapper;
    protected final TypeReference<T> typeReference;

    public AbstractRocksDBRepository(RocksDB rocksDB,
                                     ColumnFamilyHandle columnFamily,
                                     ObjectMapper objectMapper,
                                     TypeReference<T> typeReference) {
        this.rocksDB = rocksDB;
        this.columnFamily = columnFamily;
        this.objectMapper = objectMapper;
        this.typeReference = typeReference;
    }

    //    @Override
    protected T save(ID id, T entity) throws RepositoryException {
        try {
            byte[] key = serializeKey(id);
            byte[] value = objectMapper.writeValueAsBytes(entity);
            rocksDB.put(columnFamily, key, value);
            log.debug("Saved entity with id: {}", id);
            return entity;
        } catch (JsonProcessingException e) {
            throw new RepositoryException("Failed to serialize entity: " + entity.getClass().getSimpleName(), e);
        } catch (RocksDBException e) {
            throw new RepositoryException("Failed to save entity " + entity.getClass().getSimpleName() +
                    " with id: " + id, e);
        }
    }

    //    @Override
    public Optional<T> findByKey(ID id) throws RepositoryException {
        try {
            byte[] key = serializeKey(id);
            byte[] value = rocksDB.get(columnFamily, key);

            if (value == null) {
                log.debug("No value found for id: {}", id);
                return Optional.empty();
            }

            log.debug("Attempting to deserialize entity with id: {}", id);
            log.debug("Value byte array length: {}", value.length);

            if (log.isTraceEnabled()) {
                String valueAsString = new String(value, StandardCharsets.UTF_8);
                log.trace("Raw value for id {}: {}", id, valueAsString.substring(0, Math.min(500, valueAsString.length())));
            }

            T entity = objectMapper.readValue(value, typeReference);
            log.debug("Successfully deserialized entity with id: {}", id);
            return Optional.of(entity);
        } catch (java.io.IOException e) {
            log.error("Failed to deserialize entity with id: {}. Error: {}", id, e.getMessage(), e);
            log.error("Exception type: {}", e.getClass().getName());
            throw new RepositoryException("Failed to deserialize entity with id: " + id +
                ". Exception: " + e.getClass().getSimpleName() + " - " + e.getMessage(), e);
        } catch (RocksDBException e) {
            log.error("RocksDB error when finding entity with id: {}", id, e);
            throw new RepositoryException("Failed to find entity with id: " + id, e);
        }
    }

    @Override
    public boolean existsById(ID id) throws RepositoryException {
        try {
            byte[] key = serializeKey(id);
            byte[] value = rocksDB.get(columnFamily, key);
            return value != null;
        } catch (RocksDBException e) {
            throw new RepositoryException("Failed to check existence of entity with id: " + id, e);
        }
    }

    @Override
    public Map<ID, T> findAll() throws RepositoryException {
        Map<ID, T> results = new HashMap<>();

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                ID id = deserializeKey(iterator.key());
                byte[] valueBytes = iterator.value();

                log.debug("Attempting to deserialize entity with id: {}", id);
                log.debug("Value byte array length: {}", valueBytes != null ? valueBytes.length : 0);

                if (valueBytes != null && log.isTraceEnabled()) {
                    String valueAsString = new String(valueBytes, StandardCharsets.UTF_8);
                    log.trace("Raw value for id {}: {}", id, valueAsString.substring(0, Math.min(500, valueAsString.length())));
                }

                try {
                    T entity = objectMapper.readValue(valueBytes, typeReference);
                    results.put(id, entity);
                    log.debug("Successfully deserialized entity with id: {}", id);
                } catch (java.io.IOException e) {
                    log.error("Failed to deserialize entity with id: {}. Error: {}", id, e.getMessage(), e);
                    log.error("Exception type: {}", e.getClass().getName());
                    if (valueBytes != null) {
                        String valuePreview = new String(valueBytes, StandardCharsets.UTF_8);
                        log.error("Value preview (first 1000 chars): {}",
                            valuePreview.substring(0, Math.min(1000, valuePreview.length())));
                    }
                    throw new RepositoryException("Failed to deserialize entity with id: " + id +
                        ". Exception: " + e.getClass().getSimpleName() + " - " + e.getMessage(), e);
                }

                iterator.next();
            }
            return results;
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error during findAll() iteration", e);
            throw new RepositoryException("Unexpected error during findAll(): " + e.getMessage(), e);
        }
    }

    @Override
    public List<ID> findAllIds() throws RepositoryException {
        List<ID> ids = new ArrayList<>();

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                ID id = deserializeKey(iterator.key());
                ids.add(id);
                iterator.next();
            }
        }

        return ids;
    }

    @Override
    public boolean deleteById(ID id) throws RepositoryException {
        try {
            byte[] key = serializeKey(id);
            boolean existed = existsById(id);
            rocksDB.delete(columnFamily, key);
            log.debug("Deleted entity with id: {}", id);
            return existed;
        } catch (RocksDBException e) {
            throw new RepositoryException("Failed to delete entity with id: " + id, e);
        }
    }

    @Override
    public void deleteAll() throws RepositoryException {
        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                rocksDB.delete(columnFamily, iterator.key());
                iterator.next();
            }
            log.debug("Deleted all entities from column family");
        } catch (RocksDBException e) {
            throw new RepositoryException("Failed to delete all entities", e);
        }
    }

    @Override
    public long count() throws RepositoryException {
        long count = 0;

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                count++;
                iterator.next();
            }
        }

        return count;
    }

    @Override
    public long getSizeInBytes() throws RepositoryException {
        try {
            String sizeStr = rocksDB.getProperty(columnFamily, "rocksdb.estimate-live-data-size");
            return sizeStr != null ? Long.parseLong(sizeStr) : 0L;
        } catch (RocksDBException e) {
            throw new RepositoryException("Failed to get repository size", e);
        }
    }

    @Override
    public List<String> findKeySuffixesByPrefix(String prefix) throws RepositoryException {
        List<String> keySuffixes = new ArrayList<>();
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seek(prefixBytes);

            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);

                // Stop if we've moved past keys with this prefix
                if (!key.startsWith(prefix)) {
                    break;
                }

                // Extract the suffix (part after the prefix)
                String suffix = key.substring(prefix.length());
                keySuffixes.add(suffix);

                iterator.next();
            }
        }

        log.debug("Found {} key suffixes for prefix: {}", keySuffixes.size(), prefix);
        return keySuffixes;
    }

    /**
     * Serialize the key to bytes.
     * Default implementation assumes String keys.
     */
    protected byte[] serializeKey(ID id) {
        return id.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserialize the key from bytes.
     * Default implementation returns String keys.
     * Subclasses should override if using different key types.
     */
    @SuppressWarnings("unchecked")
    protected ID deserializeKey(byte[] keyBytes) {
        return (ID) new String(keyBytes, StandardCharsets.UTF_8);
    }
}