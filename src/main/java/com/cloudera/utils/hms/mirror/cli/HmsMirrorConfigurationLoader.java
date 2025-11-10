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

package com.cloudera.utils.hms.mirror.cli;

import com.cloudera.utils.hms.mirror.domain.legacy.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.service.DomainService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

import java.io.File;
import java.nio.file.FileSystems;
import java.util.*;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Refactored configuration loader that creates a fully configured HmsMirrorConfig bean.
 * This replaces the previous approach of having 100+ CommandLineRunner beans.
 *
 * Initialization Order:
 * 1. Load base config from file (or create empty for setup mode)
 * 2. Apply all command-line properties using Spring Environment
 * 3. Return fully configured HmsMirrorConfig bean ready for injection
 */
@Configuration("hmsMirrorConfigurationLoader")
@Order(1)  // Early initialization to ensure config is ready before other beans
@Slf4j
@Getter
public class HmsMirrorConfigurationLoader {

    public static final String CONFIG_PREFIX = "hms-mirror.config";
    public static final String CONVERSION_PREFIX = "hms-mirror.conversion";

    private final DomainService domainService;
    private final Environment environment;

    public HmsMirrorConfigurationLoader(DomainService domainService, Environment environment) {
        this.domainService = domainService;
        this.environment = environment;
    }

    /**
     * Creates and fully configures HmsMirrorConfig bean for normal execution mode.
     * Loads base config from file, then applies all command-line property overrides.
     */
    @Bean(name = "hmsMirrorConfig")
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "false",
            matchIfMissing = true)
    public HmsMirrorConfig loadAndConfigureHmsMirrorConfig() {
        log.info("Loading HMS-Mirror configuration from file");

        // Step 1: Load base config from file
        HmsMirrorConfig config = loadConfigFromFile();

        // Step 2: Apply all property overrides
        applyAllProperties(config);

        log.info("HMS-Mirror configuration fully loaded and configured");
        return config;
    }

    /**
     * Creates empty HmsMirrorConfig for setup mode.
     * Properties will still be applied to the empty config.
     */
    @Bean(name = "hmsMirrorConfig")
    @Order(1)
    @ConditionalOnProperty(
            name = "hms-mirror.config.setup",
            havingValue = "true")
    public HmsMirrorConfig loadEmptyConfigForSetup() {
        log.info("Creating empty HMS-Mirror configuration for setup mode");

        HmsMirrorConfig config = new HmsMirrorConfig();

        // Apply properties even in setup mode
        applyAllProperties(config);

        log.info("Empty HMS-Mirror configuration created for setup");
        return config;
    }

    /**
     * Loads the base HmsMirrorConfig from YAML file.
     */
    private HmsMirrorConfig loadConfigFromFile() {
        String configPath = environment.getProperty(CONFIG_PREFIX + ".path");
        String configFile = environment.getProperty(CONFIG_PREFIX + ".filename");

        if (configPath == null || configFile == null) {
            log.error("Configuration path and filename must be specified");
            throw new RuntimeException("Configuration path and filename must be specified");
        }

        // Strip quotes from filename
        configFile = configFile.replaceAll("^\"|\"$", "");

        String fullConfigPath;
        if (configFile.startsWith(File.separator)) {
            fullConfigPath = configFile;
        } else {
            fullConfigPath = configPath + File.separator + configFile;
        }

        log.info("Loading configuration from: {}", fullConfigPath);
        HmsMirrorConfig config = domainService.deserializeConfig(fullConfigPath);

        if (config == null) {
            log.error("Couldn't locate configuration file: {}", fullConfigPath);
            throw new RuntimeException("Couldn't locate configuration file: " + fullConfigPath);
        }

        return config;
    }

    /**
     * Applies all command-line properties to the HmsMirrorConfig.
     * This replaces the previous 100+ CommandLineRunner beans with a single comprehensive method.
     * Uses Spring Environment to access all properties.
     */
    private void applyAllProperties(HmsMirrorConfig config) {
        log.info("Applying command-line property overrides to configuration");

        // Test data file
        applyIfPresent(CONVERSION_PREFIX + ".test-filename", value -> {
            String adjustedFilename = value.replaceAll("^\"|\"$", "");
            config.setLoadTestDataFile(adjustedFilename);
            log.info("test-filename: {}", adjustedFilename);
        });

        // Output directory
        String outputDir = environment.getProperty(CONFIG_PREFIX + ".output-dir");
        if (outputDir != null && !outputDir.equals("false")) {
            config.setOutputDirectory(outputDir);
            config.setUserSetOutputDirectory(true);
            log.info("output-dir: {}", outputDir);
            ensureRetryPathExists();
        } else {
            // Use environment variable or default
            String envOutputPath = System.getenv("APP_OUTPUT_PATH");
            if (envOutputPath != null) {
                config.setOutputDirectory(envOutputPath);
                config.setUserSetOutputDirectory(false);
                log.info("output-dir (from env): {}", envOutputPath);
            } else {
                String defaultDir = System.getProperty("user.home") +
                    FileSystems.getDefault().getSeparator() + ".hms-mirror/reports/not-set";
                config.setOutputDirectory(defaultDir);
                config.setUserSetOutputDirectory(false);
                log.info("output-dir (default): {}", defaultDir);
            }
            ensureRetryPathExists();
        }

        // ACID partition count
        applyIntIfPresent(CONFIG_PREFIX + ".acid-partition-count", value -> {
            config.getMigrateACID().setPartitionLimit(value);
            log.info("acid-partition-count: {}", value);
        });

        // Auto-tune
        applyFlagIfPresent(CONFIG_PREFIX + ".auto-tune", value -> {
            config.getOptimization().setAutoTune(value);
            log.info("auto-tune: {}", value);
        });

        // Avro schema migration
        applyFlagIfPresent(CONFIG_PREFIX + ".avro-schema-migration", value -> {
            config.setCopyAvroSchemaUrls(value);
            log.info("avro-schema-migration: {}", value);
        });

        // Beta
        applyFlagIfPresent(CONFIG_PREFIX + ".beta", value -> {
            config.setBeta(value);
            log.info("beta: {}", value);
        });

        // Create if not exist
        applyFlagIfPresent(CONFIG_PREFIX + ".create-if-not-exist", value -> {
            if (nonNull(config.getCluster(com.cloudera.utils.hms.mirror.domain.support.Environment.LEFT))) {
                config.getCluster(com.cloudera.utils.hms.mirror.domain.support.Environment.LEFT).setCreateIfNotExists(value);
            }
            if (nonNull(config.getCluster(com.cloudera.utils.hms.mirror.domain.support.Environment.RIGHT))) {
                config.getCluster(com.cloudera.utils.hms.mirror.domain.support.Environment.RIGHT).setCreateIfNotExists(value);
            }
            log.info("create-if-not-exist: {}", value);
        });

        // Legacy command line options
        applyIfPresent(CONFIG_PREFIX + ".legacy-command-line-options", value -> {
            config.setCommandLineOptions(value);
            log.info("legacy-command-line-options: {}", value);
        });

        // Compress text output
        applyFlagIfPresent(CONFIG_PREFIX + ".compress-text-output", value -> {
            config.getOptimization().setCompressTextOutput(value);
            log.info("compress-text-output: {}", value);
        });

        // Comment
        applyIfPresent(CONFIG_PREFIX + ".comment", value -> {
            config.setComment(value);
            log.info("comment: {}", value);
        });

        // Data strategy
        applyIfPresent(CONFIG_PREFIX + ".data-strategy", value -> {
            config.setDataStrategy(DataStrategyEnum.valueOf(value));
            log.info("data-strategy: {}", value);
        });

        // Target namespace
        applyIfPresent(CONFIG_PREFIX + ".target-namespace", value -> {
            config.getTransfer().setTargetNamespace(value);
            log.info("target-namespace: {}", value);
        });


        // Database(s)
        applyIfPresent(CONFIG_PREFIX + ".database", value -> {
            List<String> databases = Arrays.asList(value.split(","));
            Set<String> dbSet = new TreeSet<>(databases);
            config.setDatabases(dbSet);
            log.info("database: {}", value);
        });

        // Database only
        applyBooleanIfPresent(CONFIG_PREFIX + ".database-only", value -> {
            config.setDatabaseOnly(value);
            log.info("database-only: {}", value);
        });

        // Consolidate tables for distcp
        applyFlagIfPresent(CONFIG_PREFIX + ".consolidate-tables-for-distcp", value -> {
            config.getTransfer().getStorageMigration().setConsolidateTablesForDistcp(value);
            log.info("consolidate-tables-for-distcp: {}", value);
        });

        // Consolidate DB create statements
        applyFlagIfPresent(CONFIG_PREFIX + ".consolidate-db-create-statements", value -> {
            config.setConsolidateDBCreateStatements(value);
            log.info("consolidate-db-create-statements: {}", value);
        });

        // Database prefix
        applyIfPresent(CONFIG_PREFIX + ".db-prefix", value -> {
            config.setDbPrefix(value);
            log.info("db-prefix: {}", value);
        });

        // Database skip properties
        applyIfPresent(CONFIG_PREFIX + ".database-skip-properties", value -> {
            log.info("database-skip-properties: {}", value);
            for (String prop: value.split(",")) {
                config.getFilter().getDbPropertySkipList().add(prop);
            }
            log.info("database-skip-properties: {}", value);
        });

        // Database regex
        applyIfPresent(CONFIG_PREFIX + ".database-regex", value -> {
            config.getFilter().setDbRegEx(value);
            log.info("database-regex: {}", value);
        });

        // Database rename
        applyIfPresent(CONFIG_PREFIX + ".db-rename", value -> {
            config.setDbRename(value);
            log.info("db-rename: {}", value);
        });

        // Distcp
        applyIfPresent(CONFIG_PREFIX + ".distcp", value -> {
            log.info("distcp: {}", value);
            if (Boolean.parseBoolean(value)) {
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
            } else {
                String flowStr = value;
                if (!isBlank(flowStr)) {
                    try {
                        DistcpFlowEnum flow = DistcpFlowEnum.valueOf(flowStr.toUpperCase(Locale.ROOT));
                        config.getTransfer().getStorageMigration().setDataFlow(flow);
                    } catch (IllegalArgumentException iae) {
                        log.error("Optional argument for `distcp` is invalid. Valid values: {}", Arrays.toString(DistcpFlowEnum.values()));
                        throw new RuntimeException("Optional argument for `distcp` is invalid. Valid values: " +
                                Arrays.toString(DistcpFlowEnum.values()), iae);
                    }
                }
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
            }
            log.info("distcp: {}", value);
        });

        // Data movement strategy
        applyIfPresent(CONFIG_PREFIX + ".data-movement-strategy", value -> {
            try {
                DataMovementStrategyEnum strategy = DataMovementStrategyEnum.valueOf(value);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(strategy);
            } catch (IllegalArgumentException iae) {
                log.error("Can't set data-movement-strategy with value: {}", value);
            }
            log.info("data-movement-strategy: {}", value);
        });

        // Downgrade ACID
        applyBooleanIfPresent(CONFIG_PREFIX + ".downgrade-acid", value -> {
            config.getMigrateACID().setDowngrade(value);
            log.info("downgrade-acid: {}", value);
        });

        // Dump source
        applyIfPresent(CONFIG_PREFIX + ".dump-source", value -> {
            if (config.getDataStrategy() == DataStrategyEnum.DUMP) {
                config.setExecute(Boolean.FALSE); // No Actions.
                config.setSync(Boolean.FALSE);

                try {
                    com.cloudera.utils.hms.mirror.domain.support.Environment source = com.cloudera.utils.hms.mirror.domain.support.Environment.valueOf(value.toUpperCase());
                    config.setDumpSource(source);
                } catch (RuntimeException re) {
                    log.error("The `-ds` option should be either: (LEFT|RIGHT). {} is NOT a valid option.", value);
                    throw new RuntimeException("The `-ds` option should be either: (LEFT|RIGHT). " + value +
                            " is NOT a valid option.");
                }
            }
            log.info("dump-source: {}", value);
        });

        // Dump test data
        applyIfPresent(CONFIG_PREFIX + ".dump-test-data", value -> {
            config.setDumpTestData(Boolean.parseBoolean(value));
            log.info("dump-test-data: {}", value);
        });

        // Align locations
        applyBooleanIfPresent(CONFIG_PREFIX + ".align-locations", value -> {
            config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.ALIGNED);
            log.info("align-locations: {}", value);
        });

        // Execute
        applyBooleanIfPresent(CONFIG_PREFIX + ".execute", value -> {
            config.setExecute(value);
            log.info("execute: {}", value);
        });

        // Export partition count
        applyIntIfPresent(CONFIG_PREFIX + ".export-partition-count", value -> {
            config.getHybrid().setExportImportPartitionLimit(value);
            log.info("export-partition-count: {}", value);
        });

        // External warehouse directory
        applyIfPresent(CONFIG_PREFIX + ".external-warehouse-directory", value -> {
            if (isNull(config.getTransfer().getWarehouse())) {
                Warehouse warehouse = new Warehouse();
                warehouse.setSource(WarehouseSource.GLOBAL);
                config.getTransfer().setWarehouse(warehouse);
            }
            String ewdStr = value;
            // Remove/prevent duplicate namespace config.
            if (nonNull(config.getTransfer().getTargetNamespace())) {
                if (ewdStr.startsWith(config.getTransfer().getTargetNamespace())) {
                    ewdStr = ewdStr.substring(config.getTransfer().getTargetNamespace().length());
                    log.warn("External Warehouse Location Modified (stripped duplicate namespace): {}", ewdStr);
                }
            }
            config.getTransfer().getWarehouse().setExternalDirectory(ewdStr);
            log.info("external-warehouse-directory: {}", value);
        });

        // Flip
        applyBooleanIfPresent(CONFIG_PREFIX + ".flip", value -> {
            config.setFlip(value);
            log.info("flip: {}", value);
        });

        // Force external location
        applyBooleanIfPresent(CONFIG_PREFIX + ".force-external-location", value -> {
            config.getTranslator().setForceExternalLocation(value);
            log.info("force-external-location: {}", value);
        });

        // Global location map
        applyIfPresent(CONFIG_PREFIX + ".global-location-map", value -> {
            String[] maps = value.split(",");
            for (String map : maps) {
                config.addGlobalLocationMap(map);
            }
            log.info("global-location-map: {}", value);
        });

        // Iceberg table property overrides
        applyIfPresent(CONFIG_PREFIX + ".iceberg-table-property-overrides", value -> {
            String[] overrides = value.split(",");
            Map<String, String> overridesMap = new HashMap<>();
            for (String override : overrides) {
                String[] parts = override.split("=");
                if (parts.length == 2) {
                    overridesMap.put(parts[0], parts[1]);
                }
            }
            config.getIcebergConversion().setTableProperties(overridesMap);
            log.info("iceberg-table-property-overrides: {}", value);
        });

        // Iceberg version
        applyIntIfPresent(CONFIG_PREFIX + ".iceberg-version", value -> {
            config.getIcebergConversion().setVersion(value);
            log.info("iceberg-version: {}", value);
        });

        // Intermediate storage
        applyIfPresent(CONFIG_PREFIX + ".intermediate-storage", value -> {
            config.getTransfer().setIntermediateStorage(value);
            log.info("intermediate-storage: {}", value);
        });

        // Migrate ACID
        String migrateAcidValue = environment.getProperty(CONFIG_PREFIX + ".migrate-acid");
        if (migrateAcidValue != null) {
            if (Boolean.parseBoolean(migrateAcidValue)) {
                config.getMigrateACID().setOn(Boolean.TRUE);
                config.getMigrateACID().setOnly(Boolean.FALSE);
            } else {
                config.getMigrateACID().setOn(Boolean.TRUE);
                config.getMigrateACID().setOnly(Boolean.FALSE);
                String bucketLimit = migrateAcidValue;
                if (!isBlank(bucketLimit)) {
                    config.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
                }
            }
        }

        // Migrate ACID-Only
        String migrateAcidValue1 = environment.getProperty(CONFIG_PREFIX + ".migrate-acid-only");
        if (migrateAcidValue1 != null) {
            if (Boolean.parseBoolean(migrateAcidValue1)) {
                config.getMigrateACID().setOn(Boolean.TRUE);
                config.getMigrateACID().setOnly(Boolean.TRUE);
            } else {
                config.getMigrateACID().setOn(Boolean.TRUE);
                config.getMigrateACID().setOnly(Boolean.TRUE);
                String bucketLimit1 = migrateAcidValue1;
                if (!isBlank(bucketLimit1)) {
                    config.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit1));
                }
            }
        }

        // In-place
        applyBooleanIfPresent(CONFIG_PREFIX + ".in-place", value -> {
            config.getMigrateACID().setInplace(value);
            log.info("in-place: {}", value);
        });

        // Views only
        applyBooleanIfPresent(CONFIG_PREFIX + ".views-only", value -> {
            config.getMigrateVIEW().setOn(value);
            log.info("views-only: {}", value);
        });

        // No purge
        applyBooleanIfPresent(CONFIG_PREFIX + ".no-purge", value -> {
            config.getMigrateACID().setArtificialBucketThreshold(value ? 0 : 2);
            log.info("no-purge: {}", value);
        });

        // Only acid table property
        applyBooleanIfPresent(CONFIG_PREFIX + ".only-acid-table-property", value -> {
            config.getMigrateACID().setOnly(value);
            log.info("only-acid-table-property: {}", value);
        });

        // Password key
        applyIfPresent(CONFIG_PREFIX + ".password-key", value -> {
            config.setPasswordKey(value);
            log.info("password-key: *****");
        });

        // Property overrides (BOTH sides)
        applyIfPresent(CONFIG_PREFIX + ".property-overrides", value -> {
            log.info("property-overrides: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides)) {
                config.getOptimization().getOverrides().setPropertyOverridesStr(overrides, SideType.BOTH);
            }
        });

        // Property overrides LEFT
        applyIfPresent(CONFIG_PREFIX + ".property-overrides-left", value -> {
            log.info("property-overrides-left: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides)) {
                config.getOptimization().getOverrides().setPropertyOverridesStr(overrides, SideType.LEFT);
            }
        });

        // Property overrides RIGHT
        applyIfPresent(CONFIG_PREFIX + ".property-overrides-right", value -> {
            log.info("property-overrides-right: {}", value);
            String[] overrides = value.split(",");
            if (nonNull(overrides)) {
                config.getOptimization().getOverrides().setPropertyOverridesStr(overrides, SideType.RIGHT);
            }
        });

        // Quiet
        applyBooleanIfPresent(CONFIG_PREFIX + ".quiet", value -> {
            // Set logging level or quiet mode
            log.info("quiet: {}", value);
        });

        // Read only
        applyBooleanIfPresent(CONFIG_PREFIX + ".read-only", value -> {
            config.setReadOnly(value);
            log.info("read-only: {}", value);
        });

        // Replay directory
        // Removed - Not supported.
//        applyIfPresent(CONFIG_PREFIX + ".replay-directory", value -> {
//            config.setReplayDir(value);
//            log.info("replay-directory: {}", value);
//        });

        // Reset right
        applyBooleanIfPresent(CONFIG_PREFIX + ".reset-right", value -> {
            config.setResetRight(value);
            log.info("reset-right: {}", value);
        });

        // Reset to default location
        // Legacy method.
//        applyBooleanIfPresent(CONFIG_PREFIX + ".reset-to-default-location", value -> {
//            config.setResetToDefaultLocation(value);
//            log.info("reset-to-default-location: {}", value);
//        });

        // Right is disconnected
        applyBooleanIfPresent(CONFIG_PREFIX + ".right-is-disconnected", value -> {
//            config.getCluster(com.cloudera.utils.hms.mirror.domain.support.Environment.RIGHT).setHcfsNamespace("DUMMY");
            log.info("right-is-disconnected: {}", value);
        });

        // Save working tables
        applyBooleanIfPresent(CONFIG_PREFIX + ".save-working-tables", value -> {
            config.setSaveWorkingTables(value);
            log.info("save-working-tables: {}", value);
        });

        // Schema only
//        applyBooleanIfPresent(CONFIG_PREFIX + ".schema-only", value -> {
//            if (value) {
//                config.setDataStrategy(DataStrategyEnum.SCHEMA_ONLY);
//            }
//            log.info("schema-only: {}", value);
//        });

        // Skip features
        applyBooleanIfPresent(CONFIG_PREFIX + ".skip-features", value -> {
            config.setSkipFeatures(value);
            log.info("skip-features: {}", value);
        });

        // Skip legacy translation
        applyBooleanIfPresent(CONFIG_PREFIX + ".skip-legacy-translation", value -> {
            config.setSkipLegacyTranslation(value);
            log.info("skip-legacy-translation: {}", value);
        });

        // Skip link check
        applyBooleanIfPresent(CONFIG_PREFIX + ".skip-link-check", value -> {
            config.setSkipLinkCheck(value);
            log.info("skip-link-check: {}", value);
        });

        // Skip optimizations
        applyBooleanIfPresent(CONFIG_PREFIX + ".skip-optimizations", value -> {
            config.getOptimization().setSkip(value);
            log.info("skip-optimizations: {}", value);
        });

        // Skip stats collection
        applyBooleanIfPresent(CONFIG_PREFIX + ".skip-stats-collection", value -> {
            config.getOptimization().setSkipStatsCollection(value);
            log.info("skip-stats-collection: {}", value);
        });

        // Sort dynamic partition inserts
        applyBooleanIfPresent(CONFIG_PREFIX + ".sort-dynamic-partition-inserts", value -> {
            config.getOptimization().setSortDynamicPartitionInserts(value);
            log.info("sort-dynamic-partition-inserts: {}", value);
        });

        // SQL output
        // Removed
//        applyBooleanIfPresent(CONFIG_PREFIX + ".so", value -> {
//            config.setOutputFormatAsSQL(value);
//            log.info("sql-output: {}", value);
//        });

        // SQL partition count
        applyIntIfPresent(CONFIG_PREFIX + ".sql-partition-count", value -> {
            config.getHybrid().setSqlPartitionLimit(value);
            log.info("sql-partition-count: {}", value);
        });

        // Storage migration namespace
        applyIfPresent(CONFIG_PREFIX + ".storage-migration-namespace", value -> {
            config.getTransfer().setTargetNamespace(value);
            log.info("storage-migration-namespace: {}", value);
        });

        // Storage migration strict
        applyBooleanIfPresent(CONFIG_PREFIX + ".storage-migration-strict", value -> {
            config.getTransfer().getStorageMigration().setStrict(value);
            log.info("storage-migration-strict: {}", value);
        });

        // Suppress CLI warnings
        applyBooleanIfPresent(CONFIG_PREFIX + ".suppress-cli-warnings", value -> {
            // Implementation for suppressing warnings
            log.info("suppress-cli-warnings: {}", value);
        });

        // Sync
        applyBooleanIfPresent(CONFIG_PREFIX + ".sync", value -> {
            config.setSync(value);
            log.info("sync: {}", value);
        });

        // Table exclude filter
        applyIfPresent(CONFIG_PREFIX + ".table-exclude-filter", value -> {
            config.getFilter().setTblExcludeRegEx(value);
            log.info("table-exclude-filter: {}", value);
        });

        // Table filter
        applyIfPresent(CONFIG_PREFIX + ".table-filter", value -> {
            config.getFilter().setTblRegEx(value);
            log.info("table-filter: {}", value);
        });

        // Table filter partition count limit
        applyIntIfPresent(CONFIG_PREFIX + ".table-filter-partition-count-limit", value -> {
            config.getFilter().setTblPartitionLimit(value);
            log.info("table-filter-partition-count-limit: {}", value);
        });

        // Table filter size limit
        applyLongIfPresent(CONFIG_PREFIX + ".table-filter-size-limit", value -> {
            config.getFilter().setTblSizeLimit(value * 1024 * 1024); // Convert MB to bytes
            log.info("table-filter-size-limit: {}MB", value);
        });

        // Transfer ownership (main property - sets both database and table)
        applyBooleanIfPresent(CONFIG_PREFIX + ".transfer-ownership", value -> {
            config.getOwnershipTransfer().setDatabase(value);
            config.getOwnershipTransfer().setTable(value);
            log.info("transfer-ownership: {}", value);
        });

        // Transfer ownership - database only
        applyBooleanIfPresent(CONFIG_PREFIX + ".transfer-ownership-database", value -> {
            config.getOwnershipTransfer().setDatabase(value);
            log.info("transfer-ownership-database: {}", value);
        });

        // Transfer ownership - table only
        applyBooleanIfPresent(CONFIG_PREFIX + ".transfer-ownership-table", value -> {
            config.getOwnershipTransfer().setTable(value);
            log.info("transfer-ownership-table: {}", value);
        });

        // Translation type
        applyIfPresent(CONFIG_PREFIX + ".translation-type", value -> {
            config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.valueOf(value));
            log.info("translation-type: {}", value);
        });

        // Warehouse directory
        applyIfPresent(CONFIG_PREFIX + ".warehouse-directory", value -> {
            config.getTransfer().getWarehouse().setManagedDirectory(value);
            log.info("warehouse-directory: {}", value);
        });

        // Warehouse plans
        applyIfPresent(CONFIG_PREFIX + ".warehouse-plans", value -> {
            log.info("warehouse-plan: {}", value);
            String[] warehouseplan = value.split(",");

            if (nonNull(warehouseplan)) {
                // for each plan entry, split on '=' for db=ext_dir:mngd_dir
                for (String plan : warehouseplan) {
                    String[] planParts = plan.split("=");
                    String db = planParts[0];
                    if (planParts.length == 2) {
                        String[] locations = planParts[1].split(":");
                        String ext_dir = locations[0];
                        String mngd_dir = locations[1];
                        config.getTranslator().getWarehouseMapBuilder().addWarehousePlan(db, ext_dir, mngd_dir);
                    } else {
                        log.warn("Invalid Warehouse Plan Entry: {}", plan);
                    }
                }
            }
        });

        log.info("All property overrides applied successfully");
    }

    // Helper methods for applying properties

    private void applyIfPresent(String propertyName, PropertyConsumer consumer) {
        String value = environment.getProperty(propertyName);
        if (value != null && !value.isEmpty()) {
            try {
                consumer.accept(value);
            } catch (Exception e) {
                log.error("Error applying property {}: {}", propertyName, e.getMessage());
            }
        }
    }

    private void applyBooleanIfPresent(String propertyName, BooleanPropertyConsumer consumer) {
        String value = environment.getProperty(propertyName);
        if (value != null && !value.isEmpty()) {
            try {
                consumer.accept(Boolean.parseBoolean(value));
            } catch (Exception e) {
                log.error("Error applying boolean property {}: {}", propertyName, e.getMessage());
            }
        }
    }

    /**
     * Applies a boolean flag property where presence means true.
     * This is for command-line flags that don't require arguments.
     *
     * Behavior:
     * - Property not set: don't apply
     * - Property set to empty string: apply TRUE (flag is present)
     * - Property set to "true": apply TRUE
     * - Property set to "false": apply FALSE
     * - Property set to any other value: apply TRUE (flag is present)
     */
    private void applyFlagIfPresent(String propertyName, BooleanPropertyConsumer consumer) {
        if (environment.containsProperty(propertyName)) {
            String value = environment.getProperty(propertyName);
            try {
                boolean flagValue;
                if (value == null || value.isEmpty()) {
                    // Flag is present without a value, treat as true
                    flagValue = true;
                } else if (value.equalsIgnoreCase("false")) {
                    // Explicitly set to false
                    flagValue = false;
                } else {
                    // Any other value (including "true"), treat as true
                    flagValue = true;
                }
                consumer.accept(flagValue);
            } catch (Exception e) {
                log.error("Error applying flag property {}: {}", propertyName, e.getMessage());
            }
        }
    }

    private void applyIntIfPresent(String propertyName, IntPropertyConsumer consumer) {
        String value = environment.getProperty(propertyName);
        if (value != null && !value.isEmpty()) {
            try {
                consumer.accept(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                log.error("Error parsing integer property {}: {}", propertyName, e.getMessage());
            }
        }
    }

    private void applyLongIfPresent(String propertyName, LongPropertyConsumer consumer) {
        String value = environment.getProperty(propertyName);
        if (value != null && !value.isEmpty()) {
            try {
                consumer.accept(Long.parseLong(value));
            } catch (NumberFormatException e) {
                log.error("Error parsing long property {}: {}", propertyName, e.getMessage());
            }
        }
    }

    private void ensureRetryPathExists() {
        File retryPath = new File(System.getProperty("user.home") +
            FileSystems.getDefault().getSeparator() + ".hms-mirror" +
            FileSystems.getDefault().getSeparator() + "retry");
        if (!retryPath.exists()) {
            retryPath.mkdirs();
            log.debug("Created retry path: {}", retryPath.getAbsolutePath());
        }
    }

    // Functional interfaces for property consumers

    @FunctionalInterface
    private interface PropertyConsumer {
        void accept(String value);
    }

    @FunctionalInterface
    private interface BooleanPropertyConsumer {
        void accept(boolean value);
    }

    @FunctionalInterface
    private interface IntPropertyConsumer {
        void accept(int value);
    }

    @FunctionalInterface
    private interface LongPropertyConsumer {
        void accept(long value);
    }
}