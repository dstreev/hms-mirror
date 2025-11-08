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

import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.DatasetRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * RocksDB implementation of DatasetRepository.
 * Handles persistence of Dataset entities in RocksDB.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class DatasetRepositoryImpl extends AbstractRocksDBRepository<DatasetDto, String> implements DatasetRepository {

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public DatasetRepositoryImpl(RocksDB rocksDB,
                                 @Qualifier("datasetsColumnFamily") ColumnFamilyHandle columnFamily,
                                 @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<DatasetDto>() {
        });
    }

    @Override
    public boolean delete(DatasetDto datasetDto) throws RepositoryException {
        return deleteById(datasetDto.getKey());
    }

    @Override
    public DatasetDto save(DatasetDto datasetDto) throws RepositoryException {
        // Set timestamps
        LocalDateTime currentTime = LocalDateTime.now();
        // Save using repository (timestamps and name are handled by repository layer)
        if (datasetDto.getCreated() == null) {
            datasetDto.setCreated(currentTime);
        }
        datasetDto.setModified(currentTime);

        return super.save(datasetDto.getKey(), datasetDto);
    }

    @Override
    public List<DatasetDto> findAllSortedByName() throws RepositoryException {
        Map<String, DatasetDto> allDatasets = findAll();
        return allDatasets.values().stream()
                .sorted(Comparator.comparing(DatasetDto::getName))
                .collect(Collectors.toList());
    }
}
