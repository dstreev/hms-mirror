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

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
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
import java.util.Map;

/**
 * Implementation of DBMirrorRepository that persists DBMirror instances to RocksDB.
 *
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
        super(rocksDB, columnFamily, objectMapper, new TypeReference<DBMirror>() {});
    }

    @Override
    public Map<String, DBMirror> findByConversionResult(String conversionResultKey) throws RocksDBException {
        Map<String, DBMirror> result = new HashMap<>();
        String prefix = conversionResultKey + DATABASE_PREFIX;

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seek(prefix.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've moved past keys for this conversion result
                if (!key.startsWith(prefix)) {
                    break;
                }

                // Extract database name from composite key
                String databaseName = key.substring(prefix.length());

                try {
                    DBMirror dbMirror = objectMapper.readValue(iterator.value(), typeReference);
                    result.put(databaseName, dbMirror);
                } catch (Exception e) {
                    log.error("Failed to deserialize DBMirror for key: {}", key, e);
                }

                iterator.next();
            }
        }

        return result;
    }

    @Override
    public DBMirror save(String conversionResultKey, String databaseName, DBMirror dbMirror)
            throws RocksDBException {
        String compositeKey = DBMirrorRepository.buildKey(conversionResultKey, databaseName);
        return save(compositeKey, dbMirror);
    }

    @Override
    public void deleteByConversionResult(String conversionResultKey) throws RocksDBException {
        String prefix = conversionResultKey + DATABASE_PREFIX;

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seek(prefix.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've moved past keys for this conversion result
                if (!key.startsWith(prefix)) {
                    break;
                }

                rocksDB.delete(columnFamily, iterator.key());
                iterator.next();
            }
        }
    }
}
