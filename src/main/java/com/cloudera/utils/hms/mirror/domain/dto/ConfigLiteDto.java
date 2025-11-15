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
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
public class ConfigLiteDto implements Cloneable {

    private static final DateTimeFormatter KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");

    // This would be the top level Key for the RocksDB columnFamily.
    @Schema(description = "Primary key used for RocksDB storage. If not explicitly set, defaults to the configuration name. " +
            "This serves as the unique identifier in the RocksDB column family for configuration persistence",
            accessMode = Schema.AccessMode.READ_WRITE,
            example = "prod-migration-config")
    private String key = null;
//    private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);
    public String getKey() {
        if (key == null) {
            if (name == null) {
                throw new IllegalStateException("name is required");
            } else {
                key = name;
            }
        }
        return key;
    }

    // Basic configuration
    @Schema(description = "Unique name for the configuration. Used as the primary identifier and key in RocksDB storage",
            required = true,
            example = "prod-migration-config")
    private String name;

    @Schema(description = "Optional description explaining the purpose and scope of this configuration",
            example = "Production migration configuration for analytics tables")
    private String description;

    @Schema(description = "Timestamp when this configuration was first created",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;

    @Schema(description = "Timestamp when this configuration was last modified",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;

    // Feature flags
    @Schema(description = "Enable migration of non-native tables (e.g., HBase, Druid-backed tables). " +
            "When false, only standard Hive tables are migrated",
            defaultValue = "false",
            example = "true")
    private boolean migrateNonNative = Boolean.FALSE;
//    private boolean execute = Boolean.FALSE;
    // Handled by the Job 'Disaster Recovery' feature.'
//    private boolean databaseOnly = Boolean.FALSE;
//    private boolean readOnly = Boolean.FALSE;
//    private boolean sync = Boolean.FALSE;

//    private boolean quiet = Boolean.FALSE;
    // These features don't make sense.  May add back later.

    // Additional cluster settings
    @Schema(description = "Create database or table with IF NOT EXISTS clause to prevent errors if they already exist. " +
            "Recommended for idempotent migrations and disaster recovery scenarios",
            defaultValue = "true",
            example = "true")
    private boolean createIfNotExists = Boolean.TRUE;

    @Schema(description = "Enable automatic computation of table-level statistics (e.g., row count, total size) after table creation. " +
            "Improves query optimization but increases migration time",
            defaultValue = "false",
            example = "true")
    private boolean enableAutoTableStats = Boolean.FALSE;

    @Schema(description = "Enable automatic computation of column-level statistics (e.g., min/max values, distinct counts) after table creation. " +
            "Provides more granular optimization but significantly increases migration time",
            defaultValue = "false",
            example = "false")
    private boolean enableAutoColumnStats = Boolean.FALSE;

    @Schema(description = "When we are migrating, some intermediate tables may be created to facilitate the migration. " +
            "These table are removed by default. If you want to keep them, set this to true. " +
            "Note that the implications are additional tables in the target database.",
            defaultValue = "false",
            example = "false")
    private boolean saveWorkingTables = Boolean.FALSE;

    // File and data handling
    @Schema(description = "Copy Avro schema URLs from source tables to target tables. " +
            "Useful when tables reference Avro schemas stored in external schema registries",
            defaultValue = "false",
            example = "true")
    private boolean copyAvroSchemaUrls = Boolean.FALSE;

    /**
     * Force external location in table DDLs.
     * If true, table create statements will explicitly set location rather than rely on the database directory.
     */
    @Schema(description = "Force explicit LOCATION clause in table DDL statements instead of relying on database default directory. " +
            "Ensures precise control over table data locations, particularly important for external tables",
            defaultValue = "false",
            example = "true")
    private boolean forceExternalLocation = Boolean.FALSE;

    // Sub-configuration objects (preserved as they don't cause flattening issues)
    @Schema(description = "Iceberg table conversion configuration. Controls whether and how Hive tables are converted to Iceberg format",
            implementation = IcebergConversion.class)
    private IcebergConversion icebergConversion = new IcebergConversion();

    @Schema(description = "ACID (transactional) table migration configuration. Controls migration strategy for ACID tables including downgrade and in-place operations",
            implementation = MigrateACID.class)
    private MigrateACID migrateACID = new MigrateACID();

    @Schema(description = "View migration configuration. Controls whether Hive views are migrated to the target cluster",
            implementation = MigrateVIEW.class)
    private MigrateVIEW migrateVIEW = new MigrateVIEW();

    @Schema(description = "Query optimization configuration. Controls various performance optimization settings for data migration operations",
            implementation = Optimization.class)
    private Optimization optimization = new Optimization();

    @Schema(description = "Transfer configuration. Defines prefixes, directories, storage migration settings, and warehouse configurations for data transfer",
            implementation = TransferConfig.class)
    private TransferConfig transfer = new TransferConfig();

    @Schema(description = "Ownership transfer configuration. Controls whether database and table ownership is transferred during migration",
            implementation = TransferOwnership.class)
    private TransferOwnership ownershipTransfer = new TransferOwnership();

    // Constructor
    public ConfigLiteDto() {
    }

    // Constructor with name and dataStrategy
    public ConfigLiteDto(String name) {
        this.name = name;
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
            if (transfer.getStorageMigration().getTranslationType() == com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum.ALIGNED
                    || (transfer.getStorageMigration().getTranslationType() == com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum.RELATIVE
                     && transfer.getStorageMigration().isDistcp())
             ) {
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
    public ConfigLiteDto clone() {
        ConfigLiteDto clone = null;
        try {
            clone = (ConfigLiteDto) super.clone();

            // Copy primitive and immutable fields
            clone.name = this.name;
            clone.description = this.description;
            clone.migrateNonNative = this.migrateNonNative;
            clone.createIfNotExists = this.createIfNotExists;
            clone.enableAutoTableStats = this.enableAutoTableStats;
            clone.enableAutoColumnStats = this.enableAutoColumnStats;
            clone.saveWorkingTables = this.saveWorkingTables;
            clone.copyAvroSchemaUrls = this.copyAvroSchemaUrls;
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
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Clone not supported", e);
        }

        return clone;
    }
}