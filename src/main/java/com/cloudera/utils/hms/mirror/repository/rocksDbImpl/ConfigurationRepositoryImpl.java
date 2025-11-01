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

import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConfigurationRepository;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * RocksDB implementation of ConfigurationRepository.
 * Handles persistence of Configuration entities in RocksDB.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class ConfigurationRepositoryImpl extends AbstractRocksDBRepository<ConfigLiteDto, String> implements ConfigurationRepository {

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public ConfigurationRepositoryImpl(RocksDB rocksDB,
                                       @Qualifier("configurationsColumnFamily") ColumnFamilyHandle columnFamily,
                                       @Qualifier("rocksDBObjectMapper") ObjectMapper objectMapper) {
        super(rocksDB, columnFamily, objectMapper, new TypeReference<ConfigLiteDto>() {
        });
    }

    @Override
    public boolean delete(ConfigLiteDto configLiteDto) throws RepositoryException {
        return deleteById(configLiteDto.getKey());
    }

    @Override
    public ConfigLiteDto save(ConfigLiteDto configLiteDto) throws RepositoryException {
        // Set timestamps
        LocalDateTime currentTime = LocalDateTime.now();
        if (configLiteDto.getCreated() == null) {
            configLiteDto.setCreated(currentTime);
        }
        configLiteDto.setModified(currentTime);


        // Ensure configuration name matches the id
        if (isBlank(configLiteDto.getKey())) {
            throw new RuntimeException("Configuration key cannot be null or blank");
        }

        return super.save(configLiteDto.getKey(), configLiteDto);
    }

    @Override
    public Optional<ConfigLiteDto> findByKey(String key) throws RepositoryException {
        // Call parent implementation to get the entity
        Optional<ConfigLiteDto> result = super.findByKey(key);

        // If entity exists, ensure the key is set (it's not stored in the JSON value)
        if (result.isPresent()) {
            ConfigLiteDto config = result.get();
            config.setKey(key);
            return Optional.of(config);
        }

        return result;
    }

    @Override
    public Map<String, ConfigLiteDto> findAll() throws RepositoryException {
        // Call parent implementation
        Map<String, ConfigLiteDto> allConfigurations = super.findAll();

        // Set the key on each configuration (keys are not stored in the JSON value)
        allConfigurations.forEach((key, config) -> config.setKey(key));

        return allConfigurations;
    }

    @Override
    public List<ConfigLiteDto> findAllSortedByName() throws RepositoryException {
        Map<String, ConfigLiteDto> allConfigurations = findAll();
        return allConfigurations.values().stream()
                .sorted(Comparator.comparing(ConfigLiteDto::getName))
                .collect(Collectors.toList());
    }
}
