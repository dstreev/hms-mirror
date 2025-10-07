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
 * 
 * @param <T> The entity type
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

    @Override
    public T save(ID id, T entity) throws RocksDBException {
        try {
            byte[] key = serializeKey(id);
            byte[] value = objectMapper.writeValueAsBytes(entity);
            rocksDB.put(columnFamily, key, value);
            log.debug("Saved entity with id: {}", id);
            return entity;
        } catch (JsonProcessingException e) {
            throw new RocksDBException("Failed to serialize entity: " + e.getMessage());
        }
    }

    @Override
    public Optional<T> findById(ID id) throws RocksDBException {
        try {
            byte[] key = serializeKey(id);
            byte[] value = rocksDB.get(columnFamily, key);
            
            if (value == null) {
                return Optional.empty();
            }
            
            T entity = objectMapper.readValue(value, typeReference);
            return Optional.of(entity);
        } catch (java.io.IOException e) {
            throw new RocksDBException("Failed to deserialize entity: " + e.getMessage());
        }
    }

    @Override
    public boolean existsById(ID id) throws RocksDBException {
        byte[] key = serializeKey(id);
        byte[] value = rocksDB.get(columnFamily, key);
        return value != null;
    }

    @Override
    public Map<ID, T> findAll() throws RocksDBException {
        Map<ID, T> results = new HashMap<>();
        
        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();
            
            while (iterator.isValid()) {
                ID id = deserializeKey(iterator.key());
                T entity = objectMapper.readValue(iterator.value(), typeReference);
                results.put(id, entity);
                iterator.next();
            }
        } catch (java.io.IOException e) {
            throw new RocksDBException("Failed to deserialize entities: " + e.getMessage());
        }
        
        return results;
    }

    @Override
    public List<ID> findAllIds() throws RocksDBException {
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
    public boolean deleteById(ID id) throws RocksDBException {
        byte[] key = serializeKey(id);
        boolean existed = existsById(id);
        rocksDB.delete(columnFamily, key);
        log.debug("Deleted entity with id: {}", id);
        return existed;
    }

    @Override
    public void deleteAll() throws RocksDBException {
        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();
            
            while (iterator.isValid()) {
                rocksDB.delete(columnFamily, iterator.key());
                iterator.next();
            }
        }
        log.debug("Deleted all entities from column family");
    }

    @Override
    public long count() throws RocksDBException {
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
    public long getSizeInBytes() throws RocksDBException {
        String sizeStr = rocksDB.getProperty(columnFamily, "rocksdb.estimate-live-data-size");
        return sizeStr != null ? Long.parseLong(sizeStr) : 0L;
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