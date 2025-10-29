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

import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.repository.JobRepository;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RocksDB implementation of JobRepository.
 * Handles persistence of Job entities in RocksDB.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class JobRepositoryImpl extends AbstractRocksDBRepository<JobDto, String> implements JobRepository {

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public JobRepositoryImpl(RocksDB rocksDB,
                            @Qualifier("jobsColumnFamily") ColumnFamilyHandle columnFamily,
                            @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<JobDto>() {});
    }

    @Override
    public JobDto save(JobDto jobDto) throws RepositoryException {
        // Use the job name as the key for consistency with list and load operations
        // The getKey() method returns name-uuid which should not be used as the storage key
        return save(jobDto.getName(), jobDto);
    }

    @Override
    public JobDto save(String id, JobDto jobDto) throws RepositoryException {
        // Set timestamps
        String currentTime = LocalDateTime.now().format(dateFormatter);
        if (jobDto.getCreatedDate() == null) {
            jobDto.setCreatedDate(currentTime);
        }
        jobDto.setModifiedDate(currentTime);

        return super.save(id, jobDto);
    }

    @Override
    public List<JobDto> findAllSortedByName() throws RepositoryException {
        Map<String, JobDto> allJobs = findAll();
        return allJobs.values().stream()
                .sorted(Comparator.comparing(JobDto::getName))
                .collect(Collectors.toList());
    }
}
