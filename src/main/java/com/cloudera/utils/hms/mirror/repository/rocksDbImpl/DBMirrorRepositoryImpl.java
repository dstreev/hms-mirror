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

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of DBMirrorRepository that persists DBMirror instances to RocksDB.
 * <p>
 * Keys are composite: "conversionResultKey/databaseName"
 * Values are DBMirror instances serialized as YAML.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true")
@Slf4j
public class DBMirrorRepositoryImpl extends AbstractRocksDBRepository<DBMirror, String>
        implements DBMirrorRepository {

    public DBMirrorRepositoryImpl(RocksDB rocksDB,
                                  @Qualifier("conversionResultColumnFamily") ColumnFamilyHandle columnFamily,
                                  @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<DBMirror>() {
        });
    }

    @Override
    public boolean delete(DBMirror dbMirror) throws RepositoryException {
        // TODO: Need to cleanup the TableMirror References too.
        return deleteById(dbMirror.getKey());
    }

    @Override
    public DBMirror save(String conversionResultKey, DBMirror dbMirror)
            throws RepositoryException {
        String compositeKey = DBMirrorRepository.buildKey(conversionResultKey, dbMirror.getName());
        dbMirror.setKey(compositeKey);
        String storagePrefix = DBMirrorRepository.buildPrefixedKey(conversionResultKey, dbMirror.getName());
        // The object key and the storage key are different because the storage key contains the KEY_PREFIX
        //   in order to identify the object type. Since we are storing multiple object types in this column Family.
        //  EG: conv: for ConversionResult db: for DBMirror and tbl: for TableMirror.
        return super.save(storagePrefix, dbMirror);
    }

    @Override
    public void deleteByConversionKey(String conversionResultKey) throws RepositoryException {
        // Use full prefix including KEY_PREFIX to avoid picking up non-DBMirror records

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            // Seek directly to the full prefix for efficiency
            String searchPrefix = DBMirrorRepository.buildSearchPrefix(conversionResultKey);
            iterator.seek(searchPrefix.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Verify the key starts with our full prefix - stop if we've moved past our prefix range
                if (!key.startsWith(searchPrefix)) {
                    break;
                }

                rocksDB.delete(columnFamily, iterator.key());
                iterator.next();
            }
        } catch (RocksDBException e) {
            // todo: missing id.
            throw new RepositoryException("Failed to delete entity: ", e);
        }
    }

    @Override
    public Map<String, DBMirror> findByConversionKey(String conversionResultKey) throws RepositoryException {
        Map<String, DBMirror> result = new HashMap<>();
        // Use full prefix including KEY_PREFIX to avoid picking up non-DBMirror records
        String searchPrefix = DBMirrorRepository.buildSearchPrefix(conversionResultKey);

        log.debug("Finding DBMirrors by conversion result key prefix: {}", searchPrefix);
        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            // Seek directly to the full prefix for efficiency
            iterator.seek(searchPrefix.getBytes());
            while (iterator.isValid()) {
                String key = new String(iterator.key());
                // Verify the key starts with our full prefix - stop if we've moved past our prefix range
                if (!key.startsWith(searchPrefix)) {
                    break;
                }
                String databaseName = key.substring(searchPrefix.length());
                try {
                    DBMirror dbMirror = objectMapper.readValue(iterator.value(), typeReference);
                    result.put(databaseName, dbMirror);
                } catch (Exception e) {
                    log.error("Failed to deserialize DBMirror for key: {}", key, e);
                } finally {
                    iterator.next();
                }
            }
        }
        return result;
    }

    @Override
    public boolean deleteByName(String conversionResultKey, String databaseName) throws RepositoryException {
        String compositeKey = DBMirrorRepository.buildPrefixedKey(conversionResultKey, databaseName);;
        log.debug("Deleting DBMirror by composite key: {}", compositeKey);
        return deleteById(compositeKey);
    }

    @Override
    public List<String> listNamesByKey(String conversionResultKey) throws RepositoryException {
        // Use full prefix including KEY_PREFIX to avoid picking up non-DBMirror records
        String searchPrefix = DBMirrorRepository.buildSearchPrefix(conversionResultKey);
        List<String> databaseNames = findKeySuffixesByPrefix(searchPrefix);
        log.debug("Found {} database names for ConversionResult key: {}", databaseNames.size(), conversionResultKey);
        return databaseNames;
    }

    @Override
    public Optional<DBMirror> findByName(String conversionResultKey, String databaseName)
            throws RepositoryException {
        String compositeKey = DBMirrorRepository.buildPrefixedKey(conversionResultKey, databaseName);
        log.debug("Finding DBMirror by composite key: {}", compositeKey);
        return this.findByKey(compositeKey);
    }
}
