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

package com.cloudera.utils.hms.mirror.repository;

import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import org.rocksdb.RocksDBException;

import java.util.List;

/**
 * Repository interface for Configuration persistence operations.
 * Extends the base RocksDBRepository with Configuration-specific operations.
 */
public interface ConfigurationRepository extends RocksDBRepository<ConfigLiteDto, String> {

    /**
     * Find all configurations and return them as a list sorted by name.
     *
     * @return List of all configurations sorted by name
     * @throws RocksDBException if there's an error accessing RocksDB
     */
    List<ConfigLiteDto> findAllSortedByName() throws RocksDBException;
}
