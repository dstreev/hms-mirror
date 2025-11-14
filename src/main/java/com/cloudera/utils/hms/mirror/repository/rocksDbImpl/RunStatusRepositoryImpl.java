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
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.RunStatusRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
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
 * <p>
 * RunStatus objects are stored in the same column family as ConversionResult
 * (conversionResultColumnFamily) with composite keys in the format:
 * {conversionResultKey}/runStatus
 * <p>
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
                                   @Qualifier("runStatusColumnFamily") ColumnFamilyHandle columnFamily,
                                   @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<RunStatus>() {
        });
    }

    @Override
    public boolean delete(RunStatus runStatus) throws RepositoryException {
        // TODO: Need to cleanup the TableMirror References too.
        return deleteById(runStatus.getKey());
    }

    /**
     * Build a composite key from a ConversionResult key.
     * Format: {conversionResultKey}/runStatus
     *
     * @param conversionResultKey The ConversionResult key
     * @return The composite key for RunStatus storage
     */
    private String buildCompositeKey(String conversionResultKey) {
        return conversionResultKey;
    }

    @Override
    public RunStatus save(String conversionResultKey, RunStatus runStatus) throws RepositoryException {
        String compositeKey = buildCompositeKey(conversionResultKey);
        runStatus.setKey(compositeKey);
        log.debug("Saving RunStatus with composite key: {}", compositeKey);
        return super.save(compositeKey, runStatus);
    }

    @Override
    public RunStatus save(RunStatus runStatus) throws RepositoryException {
        String compositeKey = runStatus.getKey();
        if (compositeKey == null) {
            throw new RepositoryException("RunStatus does not have a key");
        }
        return super.save(compositeKey, runStatus);
    }
    @Override
    public Optional<RunStatus> findByKey(String conversionResultKey) throws RepositoryException {
        String compositeKey = buildCompositeKey(conversionResultKey);
        log.debug("Finding RunStatus by composite key: {}", compositeKey);
        return super.findByKey(compositeKey);
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

}
