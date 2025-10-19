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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * System-level configuration initialization service.
 * Creates default configurations that should always be available in the system.
 * These configurations are created/updated on every application startup.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class SystemConfigurationInitializationService {

    private final ConfigurationManagementService configurationManagementService;

    @PostConstruct
    public void initializeSystemConfigurations() {
        log.info("Initializing system-level default configurations");

        try {
            createSystemConfigurations();
            log.info("System configuration initialization completed successfully");
        } catch (Exception e) {
            log.error("Failed to initialize system configurations", e);
        }
    }

    private void createSystemConfigurations() {
        log.info("Creating 7 system-level default configurations");

        try {
            // Create 7 system configurations using ConfigLiteDto
            ConfigLiteDto[] configurations = {
                createExternalTableConfiguration(),
                createAcidTablesConfiguration(),
                createViewsConfiguration(),
                createIcebergConversionConfiguration(),
                createAcidToIcebergConfiguration(),
                createAcidDowngradeConfiguration(),
                createAcidDowngradeInplaceConfiguration()
            };

            for (ConfigLiteDto config : configurations) {
                try {
                    Map<String, Object> result = configurationManagementService.saveConfiguration(config.getName(), config);
                    if ("SUCCESS".equals(result.get("status"))) {
                        log.debug("Created/updated system configuration: {}", config.getName());
                    } else {
                        log.warn("Failed to create/update system configuration {}: {}", config.getName(), result.get("message"));
                    }
                } catch (Exception e) {
                    log.warn("Failed to create/update system configuration: {}", config.getName(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to create system configurations", e);
        }
    }

    /**
     * SYS External Table Configuration
     * Used for creating or migrating external tables
     */
    private ConfigLiteDto createExternalTableConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) External Table");
        config.setDescription("(DO NOT CHANGE) System default for external table migrations. Creates external tables that reference data in " +
                "external storage systems. Metadata is transferred but data remains in original location.");
        config.setMigrateNonNative(false);
        config.getMigrateACID().setOn(false);
        config.getMigrateVIEW().setOn(false);

        return config;
    }

    /**
     * SYS ACID Tables Configuration
     * Handles ACID (transactional) table migrations
     */
    private ConfigLiteDto createAcidTablesConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) ACID Tables");
        config.setDescription("(DO NOT CHANGE) System default for ACID (transactional) table migrations. Handles full ACID tables with " +
                "insert, update, delete capabilities. Preserves transactional properties and compaction settings.");
        config.setMigrateNonNative(true);
        config.getMigrateACID().setOn(true);
        config.getMigrateVIEW().setOn(false);

        return config;
    }

    /**
     * SYS Views Configuration
     * Migrates Hive views between environments
     */
    private ConfigLiteDto createViewsConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) Views");
        config.setDescription("(DO NOT CHANGE) System default for view migrations. Transfers Hive views including their SQL definitions " +
                "and dependencies. Handles view rewriting when underlying table references change.");
        config.setMigrateNonNative(false);
        config.getMigrateACID().setOn(false);
        config.getMigrateVIEW().setOn(true);

        return config;
    }

    /**
     * SYS Iceberg Conversion Configuration
     * Converts traditional Hive tables to Iceberg format
     */
    private ConfigLiteDto createIcebergConversionConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) Iceberg Conversion");
        config.setDescription("(DO NOT CHANGE) System default for converting traditional Hive tables to Apache Iceberg format. " +
                "Includes data migration and metadata conversion to leverage Iceberg features like time travel, " +
                "schema evolution, and partition evolution.");
        config.setMigrateNonNative(false);
        config.getMigrateACID().setOn(false);
        config.getMigrateVIEW().setOn(false);
        config.getIcebergConversion().setEnable(true);
        config.getIcebergConversion().setVersion(2);

        return config;
    }

    /**
     * SYS ACID to Iceberg Conversion Configuration
     * Converts ACID tables to Iceberg format
     */
    private ConfigLiteDto createAcidToIcebergConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) ACID to Iceberg Conversion");
        config.setDescription("(DO NOT CHANGE) System default for converting ACID tables to Apache Iceberg format. Migrates transactional " +
                "tables to Iceberg while preserving ACID guarantees. Includes data conversion and compaction.");
        config.setMigrateNonNative(false);
        config.getMigrateACID().setOn(true);
        config.getMigrateVIEW().setOn(false);
        config.getIcebergConversion().setEnable(true);
        config.getIcebergConversion().setVersion(2);

        return config;
    }

    /**
     * SYS ACID Downgrade Configuration
     * Converts ACID tables to non-ACID external tables
     */
    private ConfigLiteDto createAcidDowngradeConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) ACID Downgrade");
        config.setDescription("(DO NOT CHANGE) System default for downgrading ACID tables to non-ACID external tables. Creates new " +
                "external tables with data from ACID tables, removing transactional properties. Useful for moving " +
                "to non-ACID environments or simplifying table management.");
        config.setMigrateNonNative(false);
        config.getMigrateACID().setOn(true);
        config.getMigrateACID().setDowngrade(true);
        config.getMigrateVIEW().setOn(false);

        return config;
    }

    /**
     * SYS ACID Downgrade In-place Configuration
     * Converts ACID tables to non-ACID tables in-place
     */
    private ConfigLiteDto createAcidDowngradeInplaceConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("(System) ACID Downgrade In-place");
        config.setDescription("(DO NOT CHANGE) System default for in-place ACID table downgrade. Converts ACID tables to non-ACID format " +
                "without moving data. Removes transaction metadata and ORC ACID properties while preserving data location. " +
                "Requires careful planning as it modifies tables in place.");
        config.setMigrateNonNative(false);
        config.getMigrateACID().setOn(true);
        config.getMigrateACID().setDowngrade(true);
        config.getMigrateACID().setInplace(true);
        config.getMigrateVIEW().setOn(false);

        return config;
    }
}
