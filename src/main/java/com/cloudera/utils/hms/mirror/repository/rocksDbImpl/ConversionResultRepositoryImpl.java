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

import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * RocksDB implementation of ConversionResultRepository.
 * Handles persistence of ConversionResult entities in RocksDB.
 * <p>
 * ConversionResult objects are stored in the conversionResult column family.
 * The key is ConversionResult.key which follows the format "yyyyMMdd_HHmmss".
 * <p>
 * This implementation provides efficient date-based querying by leveraging
 * the lexicographic ordering of the date-formatted keys.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class ConversionResultRepositoryImpl extends AbstractRocksDBRepository<ConversionResult, String>
        implements ConversionResultRepository {

    private static final DateFormat KEY_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");

    public ConversionResultRepositoryImpl(RocksDB rocksDB,
                                          @Qualifier("conversionResultColumnFamily") ColumnFamilyHandle columnFamily,
                                          @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<ConversionResult>() {
        });
    }

    @Override
    public boolean delete(ConversionResult conversionResult) throws RepositoryException {
        // TODO: Need to cleanup the DBMirror and TableMirror References too.
        return deleteById(conversionResult.getKey());
    }

    @Override
    public ConversionResult save(ConversionResult conversionResult) throws RepositoryException {
        if (conversionResult.getKey() == null || conversionResult.getKey().isEmpty()) {
            // Generate a new key if not present
            conversionResult.setKey(KEY_DATE_FORMAT.format(new Date()));
        }
        log.debug("Saving ConversionResult with key: {}", conversionResult.getKey());

        LocalDateTime currentTime = LocalDateTime.now();
        if (conversionResult.getCreated() == null) {
            conversionResult.setCreated(currentTime);
        }
        conversionResult.setModified(currentTime);

        return super.save(conversionResult.getKey(), conversionResult);
    }

    @Override
    public List<ConversionResult> findByDateRange(Date startDate, Date endDate) throws RepositoryException {
        String startKey = KEY_DATE_FORMAT.format(startDate);
        String endKey = KEY_DATE_FORMAT.format(endDate);

        log.debug("Finding ConversionResults between {} and {}", startKey, endKey);

        List<ConversionResult> results = new ArrayList<>();

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            // Seek to the start key
            iterator.seek(startKey.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've moved past the end date
                if (key.compareTo(endKey) > 0) {
                    break;
                }

                try {
                    ConversionResult result = objectMapper.readValue(iterator.value(), typeReference);
                    results.add(result);
                } catch (Exception e) {
                    log.error("Failed to deserialize ConversionResult for key: {}", key, e);
                }

                iterator.next();
            }
        }

        log.debug("Found {} ConversionResults in date range", results.size());
        return results;
    }

    @Override
    public List<ConversionResult> findByDateAfter(Date startDate) throws RepositoryException {
        String startKey = KEY_DATE_FORMAT.format(startDate);

        log.debug("Finding ConversionResults after {}", startKey);

        List<ConversionResult> results = new ArrayList<>();

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            // Seek to the start key
            iterator.seek(startKey.getBytes());

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                try {
                    ConversionResult result = objectMapper.readValue(iterator.value(), typeReference);
                    results.add(result);
                } catch (Exception e) {
                    log.error("Failed to deserialize ConversionResult for key: {}", key, e);
                }

                iterator.next();
            }
        }

        log.debug("Found {} ConversionResults after start date", results.size());
        return results;
    }

    @Override
    public List<ConversionResult> findByDateBefore(Date endDate) throws RepositoryException {
        String endKey = KEY_DATE_FORMAT.format(endDate);

        log.debug("Finding ConversionResults before {}", endKey);

        List<ConversionResult> results = new ArrayList<>();

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've moved past the end date
                if (key.compareTo(endKey) > 0) {
                    break;
                }

                try {
                    ConversionResult result = objectMapper.readValue(iterator.value(), typeReference);
                    results.add(result);
                } catch (Exception e) {
                    log.error("Failed to deserialize ConversionResult for key: {}", key, e);
                }

                iterator.next();
            }
        }

        log.debug("Found {} ConversionResults before end date", results.size());
        return results;
    }

    @Override
    public List<ConversionResult> findAllAsList() throws RepositoryException {
        List<ConversionResult> results = new ArrayList<>();

        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                try {
                    ConversionResult result = objectMapper.readValue(iterator.value(), typeReference);
                    results.add(result);
                } catch (Exception e) {
                    log.error("Failed to deserialize ConversionResult for key: {}", key, e);
                }

                iterator.next();
            }
        }

        log.debug("Found {} total ConversionResults", results.size());
        return results;
    }

    @Override
    public int deleteByDateBefore(Date beforeDate) throws RepositoryException {
        String cutoffKey = KEY_DATE_FORMAT.format(beforeDate);

        log.debug("Deleting ConversionResults before {}", cutoffKey);

        int deletedCount = 0;
        List<String> keysToDelete = new ArrayList<>();

        // First pass: collect keys to delete
        try (RocksIterator iterator = rocksDB.newIterator(columnFamily)) {
            iterator.seekToFirst();

            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Stop if we've reached entries at or after the cutoff date
                if (key.compareTo(cutoffKey) >= 0) {
                    break;
                }

                keysToDelete.add(key);
                iterator.next();
            }
        }

        // Second pass: delete collected keys
        for (String key : keysToDelete) {
            try {
                rocksDB.delete(columnFamily, key.getBytes());
            } catch (RocksDBException e) {
                throw new RepositoryException(e);
            }
            deletedCount++;
        }

        log.info("Deleted {} ConversionResults before {}", deletedCount, cutoffKey);
        return deletedCount;
    }
}
