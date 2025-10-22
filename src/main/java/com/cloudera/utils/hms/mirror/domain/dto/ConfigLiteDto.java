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
    private String description;
    private String createdDate;
    private String modifiedDate;

    // Feature flags
    private boolean migrateNonNative = Boolean.FALSE;
    private boolean execute = Boolean.FALSE;
    // Handled by the Job 'Disaster Recovery' feature.'
//    private boolean databaseOnly = Boolean.FALSE;
//    private boolean readOnly = Boolean.FALSE;
//    private boolean sync = Boolean.FALSE;

//    private boolean quiet = Boolean.FALSE;
    // These features don't make sense.  May add back later.
//    private boolean skipFeatures = Boolean.FALSE;
//    private boolean skipLegacyTranslation = Boolean.FALSE;

    // Additional cluster settings
    private boolean createIfNotExists = Boolean.TRUE;
    private boolean enableAutoTableStats = Boolean.FALSE;
    private boolean enableAutoColumnStats = Boolean.FALSE;

    @Schema(description = "When we are migrating, some intermediate tables may be created to facilitate the migration. " +
            "These table are removed by default. If you want to keep them, set this to true. " +
            "Note that the implications are additional tables in the target database.")
    private boolean saveWorkingTables = Boolean.FALSE;

    // File and data handling
    private boolean copyAvroSchemaUrls = Boolean.FALSE;
//    private boolean dumpTestData = Boolean.FALSE;
    private String loadTestDataFile = null;

    /**
     * Force external location in table DDLs.
     * If true, table create statements will explicitly set location rather than rely on the database directory.
     */
    private boolean forceExternalLocation = Boolean.FALSE;

    // Sub-configuration objects (preserved as they don't cause flattening issues)
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

    /**
     * Check if loading test data from file.
     *
     * @return true if loadTestDataFile is set
     */
    public boolean isLoadingTestData() {
        return loadTestDataFile != null && !loadTestDataFile.isBlank();
    }

    /**
     * Check if metadata details should be loaded.
     * This is a computed property based on transfer configuration.
     *
     * @return true if metadata details should be loaded
     */
    public Boolean loadMetadataDetails() {
        // When we're ALIGNED and asking for DISTCP, we need to load the partition metadata.
        if (transfer != null && transfer.getStorageMigration() != null) {
            if ((transfer.getStorageMigration().getTranslationType() == com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum.ALIGNED
                    || transfer.getStorageMigration().getTranslationType() == com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum.RELATIVE)
                    && transfer.getStorageMigration().isDistcp()) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    /**
     * Create a deep clone of this ConfigLiteDto.
     * All nested objects are cloned to avoid shared references.
     *
     * @return A deep clone of this ConfigLiteDto
     */
    public ConfigLiteDto deepClone() {
        ConfigLiteDto clone = new ConfigLiteDto();

        // Copy primitive and immutable fields
        clone.name = this.name;
        clone.description = this.description;
        clone.migrateNonNative = this.migrateNonNative;
        clone.execute = this.execute;
        clone.createIfNotExists = this.createIfNotExists;
        clone.enableAutoTableStats = this.enableAutoTableStats;
        clone.enableAutoColumnStats = this.enableAutoColumnStats;
        clone.saveWorkingTables = this.saveWorkingTables;
        clone.copyAvroSchemaUrls = this.copyAvroSchemaUrls;
        clone.loadTestDataFile = this.loadTestDataFile;
        clone.forceExternalLocation = this.forceExternalLocation;

        // Deep clone sub-configuration objects using their existing clone() methods
        if (this.icebergConversion != null) {
            clone.icebergConversion = this.icebergConversion.clone();
        } else {
            clone.icebergConversion = new IcebergConversion();
        }

        if (this.migrateACID != null) {
            clone.migrateACID = this.migrateACID.clone();
        } else {
            clone.migrateACID = new MigrateACID();
        }

        if (this.migrateVIEW != null) {
            clone.migrateVIEW = this.migrateVIEW.clone();
        } else {
            clone.migrateVIEW = new MigrateVIEW();
        }

        if (this.optimization != null) {
            clone.optimization = this.optimization.clone();
        } else {
            clone.optimization = new Optimization();
        }

        if (this.transfer != null) {
            clone.transfer = this.transfer.clone();
        } else {
            clone.transfer = new TransferConfig();
        }

        if (this.ownershipTransfer != null) {
            clone.ownershipTransfer = this.ownershipTransfer.clone();
        } else {
            clone.ownershipTransfer = new TransferOwnership();
        }

        return clone;
    }
}