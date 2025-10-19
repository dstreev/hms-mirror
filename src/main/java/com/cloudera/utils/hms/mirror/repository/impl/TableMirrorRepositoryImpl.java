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

import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
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
import java.util.Map;

/**
 * Implementation of TableMirrorRepository that persists TableMirror instances to RocksDB.
 *
 * Keys are composite: "conversionResultKey/database/databaseName/table/tableName"
 * Values are TableMirror instances serialized as YAML.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true")
@Slf4j
public class TableMirrorRepositoryImpl extends AbstractRocksDBRepository<TableMirror, String>
        implements TableMirrorRepository {

    public TableMirrorRepositoryImpl(RocksDB rocksDB,
                                      @Qualifier("conversionResultColumnFamily") ColumnFamilyHandle columnFamily,
                                      @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<TableMirror>() {});
    }

    @Override
    public TableMirror findByName(String conversionResultKey, String databaseName, String tableName)
            throws RocksDBException {
        String compositeKey = TableMirrorRepository.buildKey(conversionResultKey, databaseName, tableName);
        return findById(compositeKey).orElse(null);
    }

    @Override
    public Map<String, TableMirror> findByDatabase(String conversionResultKey, String databaseName)
            throws RocksDBException {
        Map<String, TableMirror> result = new HashMap<>();
        String prefix = TableMirrorRepository.buildDatabasePrefix(conversionResultKey, databaseName);

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seek(prefix.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've moved past keys for this database
                if (!key.startsWith(prefix)) {
                    break;
                }

                // Extract table name from composite key
                String tableName = key.substring(prefix.length());

                try {
                    TableMirror tableMirror = objectMapper.readValue(iterator.value(), typeReference);
                    result.put(tableName, tableMirror);
                } catch (Exception e) {
                    log.error("Failed to deserialize TableMirror for key: {}", key, e);
                }

                iterator.next();
            }
        }

        return result;
    }

    @Override
    public TableMirror save(String conversionResultKey, String databaseName, String tableName,
                            TableMirror tableMirror) throws RocksDBException {
        String compositeKey = TableMirrorRepository.buildKey(conversionResultKey, databaseName, tableName);
        return save(compositeKey, tableMirror);
    }

    @Override
    public void deleteByDatabase(String conversionResultKey, String databaseName) throws RocksDBException {
        String prefix = TableMirrorRepository.buildDatabasePrefix(conversionResultKey, databaseName);

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seek(prefix.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've moved past keys for this database
                if (!key.startsWith(prefix)) {
                    break;
                }

                rocksDB.delete(columnFamily, iterator.key());
                iterator.next();
            }
        }
    }
}
