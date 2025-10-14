/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain.dto;

import com.cloudera.utils.hms.mirror.domain.core.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

/**
 * Lightweight DTO for HMS Mirror Configuration storage in RocksDB.
 * This is a simplified configuration object that contains essential configuration 
 * properties but excludes complex cluster and acceptance objects to maintain
 * clean separation between lite and full configuration representations.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Lightweight Configuration DTO for storage and API transfer")
public class ConfigLiteDto {

    // Basic configuration
    private String name;
    private String comment;

    // Feature flags
    private boolean databaseOnly = Boolean.FALSE;
    private boolean migrateNonNative = Boolean.FALSE;
    private boolean readOnly = Boolean.FALSE;
    private boolean noPurge = Boolean.FALSE;
    private boolean replace = Boolean.FALSE;
    private boolean resetRight = Boolean.FALSE;
    private boolean sync = Boolean.FALSE;
    private boolean quiet = Boolean.FALSE;
    private boolean skipFeatures = Boolean.FALSE;
    private boolean skipLegacyTranslation = Boolean.FALSE;

    @Schema(description = "When we are migrating, some intermediate tables may be created to facilitate the migration. " +
            "These table are removed by default. If you want to keep them, set this to true. " +
            "Note that the implications are additional tables in the target database.")
    private boolean saveWorkingTables = Boolean.FALSE;

    // File and data handling
    private boolean copyAvroSchemaUrls = Boolean.FALSE;
    private boolean dumpTestData = Boolean.FALSE;

    // Sub-configuration objects (preserved as they don't cause flattening issues)
    private HybridConfig hybrid = new HybridConfig();
    private IcebergConversion icebergConversion = new IcebergConversion();
    private MigrateACID migrateACID = new MigrateACID();
    private MigrateVIEW migrateVIEW = new MigrateVIEW();
    private Optimization optimization = new Optimization();
    private TransferConfig transfer = new TransferConfig();
    private TransferOwnership ownershipTransfer = new TransferOwnership();

    // Constructor
    public ConfigLiteDto() {
    }

    // Constructor with name and dataStrategy
    public ConfigLiteDto(String name) {
        this.name = name;
    }
}