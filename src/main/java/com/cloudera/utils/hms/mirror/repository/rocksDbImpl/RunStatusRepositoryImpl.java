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

import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.repository.RunStatusRepository;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * RocksDB implementation of RunStatusRepository.
 * Handles persistence of RunStatus entities in RocksDB.
 *
 * RunStatus objects are stored in the same column family as ConversionResult
 * (conversionResultColumnFamily) with composite keys in the format:
 * {conversionResultKey}/runStatus
 *
 * This allows efficient association with ConversionResult objects by storing
 * them together in the same column family, enabling atomic operations and
 * maintaining data locality.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class RunStatusRepositoryImpl extends AbstractRocksDBRepository<RunStatus, String> implements RunStatusRepository {

    private static final String KEY_SEPARATOR = "/";
    private static final String RUN_STATUS_SUFFIX = "runStatus";

    public RunStatusRepositoryImpl(RocksDB rocksDB,
                                   @Qualifier("conversionResultColumnFamily") ColumnFamilyHandle columnFamily,
                                   @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<RunStatus>() {});
    }

    /**
     * Build a composite key from a ConversionResult key.
     * Format: {conversionResultKey}/runStatus
     *
     * @param conversionResultKey The ConversionResult key
     * @return The composite key for RunStatus storage
     */
    private String buildCompositeKey(String conversionResultKey) {
        return conversionResultKey + KEY_SEPARATOR + RUN_STATUS_SUFFIX;
    }

    @Override
    public RunStatus saveByKey(String conversionResultKey, RunStatus runStatus) throws RepositoryException {
        String compositeKey = buildCompositeKey(conversionResultKey);
        log.debug("Saving RunStatus with composite key: {}", compositeKey);
        return save(compositeKey, runStatus);
    }

    @Override
    public Optional<RunStatus> findByKey(String conversionResultKey) throws RepositoryException {
        String compositeKey = buildCompositeKey(conversionResultKey);
        log.debug("Finding RunStatus by composite key: {}", compositeKey);
        return findById(compositeKey);
    }

    @Override
    public boolean deleteByKey(String conversionResultKey) throws RepositoryException {
        String compositeKey = buildCompositeKey(conversionResultKey);
        log.debug("Deleting RunStatus by composite key: {}", compositeKey);
        return deleteById(compositeKey);
    }

    @Override
    public boolean existsByKey(String conversionResultKey) throws RepositoryException {
        String compositeKey = buildCompositeKey(conversionResultKey);
        return existsById(compositeKey);
    }

    @Override
    public List<RunStatus> findAllAsList() throws RepositoryException {
        Map<String, RunStatus> allRunStatuses = findAll();
        return new ArrayList<>(allRunStatuses.values());
    }

    /**
     * Override save to ensure users understand the composite key pattern.
     * Direct save() calls should use the full composite key format.
     *
     * @param id The full composite key: {conversionResultKey}/runStatus
     * @param entity The RunStatus to save
     * @return The saved RunStatus
     * @throws RepositoryException if there's an error accessing RocksDB
     */
    @Override
    public RunStatus save(String id, RunStatus entity) throws RepositoryException {
        // Validate that the key follows the expected format
        if (!id.contains(KEY_SEPARATOR + RUN_STATUS_SUFFIX)) {
            log.warn("RunStatus key '{}' does not follow the expected composite key format. " +
                    "Consider using saveByKey() instead.", id);
        }
        return super.save(id, entity);
    }
}
