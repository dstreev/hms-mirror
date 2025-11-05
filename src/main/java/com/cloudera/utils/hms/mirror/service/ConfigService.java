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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.core.Cluster;
import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.SQL;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.STORAGE_MIGRATION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service class responsible for managing HMS Mirror configuration operations.
 * This class handles configuration validation, loading, saving, and various configuration-related
 * operations for the HMS Mirror tool. It ensures that configurations are valid for different
 * data strategies and cluster setups.
 * <p>
 * Key responsibilities include:
 * - Configuration validation for different data strategies
 * - Cluster connection validation
 * - Configuration adjustments based on data strategies
 * - Warehouse and namespace validation
 * - Link testing between clusters
 * - Configuration file operations (load/save)
 */
@Service
@Slf4j
@Getter
@RequiredArgsConstructor
public class ConfigService {

    @NonNull
    private final ObjectMapper yamlMapper;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final org.springframework.core.env.Environment springEnv;
    @NonNull
    private final DomainService domainService;
    @NonNull
    private final CliEnvironment cliEnvironment;
//    @NonNull
//    private final ExecuteSessionService executeSessionService;
//    private TranslatorService translatorService;

    /**
     * Constructor for ConfigService.
     *
     * @param yamlMapper ObjectMapper for YAML processing
     * @param springEnv Spring environment
     * @param domainService Service for domain operations
     * @param executeSessionService Service for execute session management
    public ConfigService(@Qualifier("yamlMapper") ObjectMapper yamlMapper,
    org.springframework.core.env.Environment springEnv,
    DomainService domainService,
    ExecuteSessionService executeSessionService) {
    this.yamlMapper = yamlMapper;
    this.springEnv = springEnv;
    this.domainService = domainService;
    this.executeSessionService = executeSessionService;
    log.debug("ConfigService initialized");
    }
     */

    /**
     * TODO: Review
     * Checks if warehouse plans exist in the configuration.
     * This method verifies if either warehouse map builder plans are defined or
     * if warehouse directories (external and managed) are configured.
     *
     * @param session The execute session containing the configuration
     * @return true if warehouse plans exist, false otherwise
     */
    public boolean doWareHousePlansExist() {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        final RunStatus runStatus = conversionResult.getRunStatus();

        AtomicBoolean rtn = new AtomicBoolean(true);
        // Loop through the DatabaseSpecs and ensure the warehouse directories at set.
        conversionResult.getDataset().getDatabases().forEach(db -> {
            if (db.getWarehouse().getExternalDirectory() == null ||
                    db.getWarehouse().getManagedDirectory() == null) {
                runStatus.addError(WAREHOUSE_DETAILS_MISSING, db.getDatabaseName());
                rtn.set(false);
            }
        });

        return rtn.get();
    }


    /**
     * Checks if metastore direct configuration is available for a specific environment.
     *
     * @param session     The execute session containing the configuration
     * @param environment The environment (LEFT/RIGHT) to check
     * @return true if metastore direct is configured, false otherwise
     */
    public boolean isMetastoreDirectConfigured(ConnectionDto connect) {
        if (nonNull(connect)) {
            return !isBlank(connect.getMetastoreDirectUri());
        }
        return false;
    }

    /**
     * Determines if a DistCp plan can be derived from the current configuration.
     */
    public boolean canDeriveDistcpPlan(ConversionResult conversionResult) {
        return conversionResult.getConfig().getTransfer().getStorageMigration().isDistcp();
    }

    /**
     * TODO: Review
     * Converts the configuration to a string representation, masking sensitive information.
     * This method converts the configuration to YAML format and masks passwords for security.
     *
     * @param config The HMS Mirror configuration to convert
     * @return String representation of the configuration with masked sensitive data
     */
    public String configToString(HmsMirrorConfig config) {
        String rtn = null;
        try {
            rtn = yamlMapper.writeValueAsString(config);
            // Blank out passwords
            rtn = rtn.replaceAll("user:\\s\".*\"", "user: \"*****\"");
            rtn = rtn.replaceAll("password:\\s\".*\"", "password: \"*****\"");
        } catch (JsonProcessingException e) {
            log.error("Parsing issue", e);
        }
        return rtn;
    }

    public void addConfigAdjustmentMessage(DataStrategyEnum strategy, String property, String from, String to, String why) {
        String message = MessageFormat.format("Adjusted Data Strategy: {0}  for property {1} from {2} to {3}. Reason: {4}",
                strategy.toString(), property, from, to, why);
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        RunStatus runStatus = conversionResult.getRunStatus();
        runStatus.addConfigMessage(message);
    }


    /**
     * Aligns configuration settings based on the data strategy and other parameters.
     * This method adjusts various configuration settings to ensure they are compatible
     * with the selected data strategy and other configuration options.
     *
     * @return true if alignment was successful without breaking errors, false otherwise
     */
    public boolean alignConfigurationSettings() {
        boolean rtn = Boolean.TRUE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        JobExecution jobExecution = conversionResult.getJobExecution();
        RunStatus runStatus = conversionResult.getRunStatus();

        // Iceberg Conversions is a beta feature.
        if (config.getIcebergConversion().isEnable()) {
            // Check that the DataStategy is either STORAGE_MIGRATION or SQL before allowing the Iceberg Conversion can be set.
            if (!EnumSet.of(STORAGE_MIGRATION, SQL).contains(job.getStrategy())) {
                addConfigAdjustmentMessage(job.getStrategy(),
                        "icebergConversion:enabled",
                        Boolean.toString(config.getIcebergConversion().isEnable()),
                        Boolean.toString(false), "Only available w/ Data Strategies STORAGE_MIGRATION and SQL.");
                config.getIcebergConversion().setEnable(false);
            }
        }

        switch (job.getStrategy()) {
            case DUMP:
                // Translation Type need to be RELATIVE.
                if (config.getTransfer().getStorageMigration().getTranslationType() != TranslationTypeEnum.RELATIVE) {
                    addConfigAdjustmentMessage(job.getStrategy(),
                            "TranslationType",
                            config.getTransfer().getStorageMigration().getTranslationType().toString(),
                            TranslationTypeEnum.RELATIVE.toString(), "Only the RELATIVE Translation Type is supported for DUMP.");
                    config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.RELATIVE);
                }
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.NA);
                // DUMP doesn't require Execute.
                if (jobExecution.isExecute()) {
                    jobExecution.setDryRun(Boolean.TRUE);
                }
                break;
            case SCHEMA_ONLY:
                switch (config.getTransfer().getStorageMigration().getTranslationType()) {
                    case RELATIVE:
                        // Ensure the proper Data Movement Strategy is set. (which is MANUAL)
                        if (config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.SQL) {
                            addConfigAdjustmentMessage(job.getStrategy(),
                                    "DataMovementStrategy",
                                    config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                                    DataMovementStrategyEnum.MANUAL.toString(), "Only the MANUAL Data Movement Strategy is supported for SCHEMA_ONLY with the translationType of RELATIVE.");
                            config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                        }
                        break;
                    case ALIGNED:
                        // Ensure the proper Data Movement Strategy is set. (which is SQL)
                        if (config.getTransfer().getStorageMigration().getDataMovementStrategy() != DataMovementStrategyEnum.DISTCP) {
                            addConfigAdjustmentMessage(job.getStrategy(),
                                    "DataMovementStrategy",
                                    config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                                    DataMovementStrategyEnum.DISTCP.toString(), "Only the DISTCP Data Movement Strategy is supported for SCHEMA_ONLY with the translationType of ALIGNED.");
                            config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                        }
                        break;
                    default:
                        break;
                }

                // Sync process is read-only and no-purge to ensure data is not deleted.
                // Because we'll assume that the data is being copied through other processes.
                if (job.isSync()) {
                    job.setDisasterRecovery(Boolean.TRUE);
//                    config.setNoPurge(Boolean.TRUE);
                }

                break;
            case SQL:
            case HYBRID:
                // Ensure the proper Data Movement Strategy is set. (which is SQL)
                switch (config.getTransfer().getStorageMigration().getDataMovementStrategy()) {
                    case SQL:
                        break;
                    default:
                        addConfigAdjustmentMessage(job.getStrategy(),
                                "DataMovementStrategy",
                                config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                                DataMovementStrategyEnum.SQL.toString(), "Only the SQL Data Movement Strategy is supported for SQL and HYBRID.");
                        config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                        break;
                }
                break;
            case EXPORT_IMPORT:
                if (config.getTransfer().getStorageMigration().getDataMovementStrategy() != DataMovementStrategyEnum.NA) {
                    addConfigAdjustmentMessage(job.getStrategy(),
                            "DataMovementStrategy",
                            config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                            DataMovementStrategyEnum.NA.toString(), "Only the NA Data Movement Strategy is supported for EXPORT_IMPORT.");
                    config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.NA);
                }
                break;
            case STORAGE_MIGRATION:
                if (config.getTransfer().getStorageMigration().getTranslationType() != TranslationTypeEnum.ALIGNED) {
                    addConfigAdjustmentMessage(job.getStrategy(),
                            "TranslationType",
                            config.getTransfer().getStorageMigration().getTranslationType().toString(),
                            TranslationTypeEnum.ALIGNED.toString(), "Only the ALIGNED Translation Type is supported for STORAGE_MIGRATION.");
                    config.getTransfer().getStorageMigration().setTranslationType(TranslationTypeEnum.ALIGNED);
                }

                // For the Aligned Translation Type, we need to ensure the Data Movement Strategy is set to SQL or DISTCP.
                if (config.getIcebergConversion().isEnable() &&
                        config.getTransfer().getStorageMigration().getDataMovementStrategy() != DataMovementStrategyEnum.SQL) {
                    addConfigAdjustmentMessage(job.getStrategy(),
                            "DataMovementStrategy",
                            config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                            DataMovementStrategyEnum.SQL.toString(), "Only the SQL Data Movement Strategy is supported for STORAGE_MIGRATION w/ ICEBERG_CONVERSION enabled.");
                    config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                }

                if (config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.NA) {
                    addConfigAdjustmentMessage(job.getStrategy(),
                            "DataMovementStrategy",
                            config.getTransfer().getStorageMigration().getDataMovementStrategy().toString(),
                            DataMovementStrategyEnum.DISTCP.toString(), "Only the SQL/DISTCP Data Movement Strategy is supported for STORAGE_MIGRATION.");
                    config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                }

                break;
            case COMMON:
                break;
            case LINKED:
                // Sync process is read-only and no-purge to ensure data is not deleted.
                // Because we'll assume that the data is being copied through other processes.
                if (job.isSync()) {
                    job.setDisasterRecovery(Boolean.TRUE);
//                    config.setNoPurge(Boolean.TRUE);
                }
                break;
            default:
                break;
        }
        // If migrateView is on and the data strategy is NOT either DUMP or SCHEMA_ONLY,
        //  change to data strategy to SCHEMA_ONLY.
        if (config.getMigrateVIEW().isOn() &&
                !(job.getStrategy() == DataStrategyEnum.DUMP ||
                        job.getStrategy() == DataStrategyEnum.SCHEMA_ONLY)) {
            addConfigAdjustmentMessage(job.getStrategy(),
                    "DataStrategy",
                    job.getStrategy().toString(),
                    DataStrategyEnum.SCHEMA_ONLY.toString(),
                    "MigrateVIEW is only valid for DUMP and SCHEMA_ONLY Data Strategies.");
            job.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
        }

        // No longer needed.  We're approaching this from a Dataset Perspective
        /*
        if (config.loadMetadataDetails()) {
            switch (config.getDatabaseFilterType()) {
                case WAREHOUSE_PLANS:
                    // Need to ensure we have Warehouse Plans and Databases are in sync.
                    config.getDatabases().clear();
                    if (!config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
                        for (Map.Entry<String, Warehouse> warehousePlan : config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().entrySet()) {
                            config.getDatabases().add(warehousePlan.getKey());
                        }
                    }
                    break;
                case MANUAL:
                case REGEX:
                case UNDETERMINED:
                    break;
            }
        }
         */

        // Check for the unsupport Migration Scenarios
        // ==============================================================================================================
        // We don't support NonLegacy to Legacy Migrations (downgrades)
        // MOVED TO VALIDATION
//        if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isHdpHive3() &&
//                conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
//            addConfigAdjustmentMessage(job.getStrategy(),
//                    "LegacyHive(LEFT)",
//                    Boolean.toString(conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()),
//                    Boolean.FALSE.toString(), "Legacy Hive is not supported for HDP Hive 3.");
////            conversionResult.getConnection(Environment.LEFT).getPlatformType().setLegacyHive(Boolean.FALSE);
//        }
//
//        if (nonNull(conversionResult.getConnection(Environment.RIGHT))
//                && conversionResult.getConnection(Environment.RIGHT).getPlatformType().isHdpHive3() &&
//                conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
//            session.addConfigAdjustmentMessage(config.getDataStrategy(),
//                    "LegacyHive(RIGHT)",
//                    Boolean.toString(conversionResult.getConnection(Environment.RIGHT).isLegacyHive()),
//                    Boolean.FALSE.toString(), "Legacy Hive is not supported for HDP Hive 3.");
//            conversionResult.getConnection(Environment.RIGHT).getPlatformType().setLegacyHive(Boolean.FALSE);
//        }
        // ==============================================================================================================

        return rtn;
    }

    /**
     * Gets the skip stats collection setting based on the configuration and data strategy.
     * This method determines whether statistics collection should be skipped based on
     * various configuration parameters and the selected data strategy.
     *
     * @param config The HMS Mirror configuration
     * @return Boolean indicating whether to skip stats collection
     */
    public Boolean getSkipStatsCollection(HmsMirrorConfig config) {
        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (!config.getOptimization().isSkipStatsCollection()) {
            try {
                switch (config.getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        config.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        break;
                    case STORAGE_MIGRATION:
                        // TODO: This might be helpful to get so we can be more clear with the distcp process. EG: Mapper count.
                        if (config.getTransfer().getStorageMigration().isDistcp()) {
                            config.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return config.getOptimization().isSkipStatsCollection();
    }

    /**
     * Tests the link between clusters to validate namespace availability.
     * This method checks if the configured HCFS namespaces are accessible.
     *
     * @return true if link test passes, false otherwise
     * @throws DisabledException if the CLI interface is disabled
     */
    protected Boolean linkTest() throws DisabledException {
        Boolean rtn = Boolean.TRUE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        if (isNull(cliEnvironment)) {
            return Boolean.TRUE;
        }

        if (conversionResult.getJob().isSkipLinkCheck() || conversionResult.getJob().isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {

            log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");

            // We need to gather and test the configured hcfs namespaces defined in the config.
            // LEFT, RIGHT, and TargetNamespace.
            // If a namespace is NOT defined, record an issue and move on.  Only 'fail' when a defined namespace fails.
            List<String> namespaces = new ArrayList<>();

            if (nonNull(conversionResult.getConnection(Environment.LEFT)) && !isBlank(conversionResult.getConnection(Environment.LEFT).getHcfsNamespace())) {
                namespaces.add(conversionResult.getConnection(Environment.LEFT).getHcfsNamespace());
            }
            if (nonNull(conversionResult.getConnection(Environment.RIGHT)) && !isBlank(conversionResult.getConnection(Environment.RIGHT).getHcfsNamespace())) {
                namespaces.add(conversionResult.getConnection(Environment.RIGHT).getHcfsNamespace());
            }
            namespaces.add(conversionResult.getTargetNamespace());
            for (String namespace : namespaces) {
                try {
                    if (NamespaceUtils.isNamespaceAvailable(cliEnvironment, namespace)) {
                        log.info("Namespace: {} is available.", namespace);
                    } else {
                        log.warn("Namespace: {} is not available.", namespace);
                        rtn = Boolean.FALSE;
                    }
                } catch (DisabledException de) {
                    log.warn("Namespace: {} is not available because the hcfs client has been disabled.", namespace);
                }
            }

        }
        return rtn;
    }

    /**
     * Loads a configuration from the default config directory.
     *
     * @param configFileName The name of the configuration file to load
     * @return The loaded HMS Mirror configuration
     */
    public HmsMirrorConfig loadConfig(String configFileName) {
        HmsMirrorConfig rtn = domainService.deserializeConfig(configFileName);
        return rtn;
    }

    /**
     * Saves the configuration to a file.
     *
     * @param config         The configuration to save
     * @param configFileName The name of the file to save to
     * @param overwrite      Whether to overwrite an existing file
     * @return true if save was successful, false otherwise
     * @throws IOException if there is an error writing the file
     */
    public boolean saveConfig(HmsMirrorConfig config, String configFileName, Boolean overwrite) throws IOException {
        return HmsMirrorConfig.save(config, configFileName, overwrite);
    }

    /**
     * Overlays one configuration onto another, merging their settings.
     *
     * @param config  The base configuration
     * @param overlay The configuration to overlay on top
     */
    public void overlayConfig(HmsMirrorConfig config, HmsMirrorConfig overlay) {
        List<Environment> envs = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment env : envs) {
            if (nonNull(config.getCluster(env))) {
                boolean hasHS2 = nonNull(config.getCluster(env).getHiveServer2());
                boolean hasMS = nonNull(config.getCluster(env).getMetastoreDirect());
                if (nonNull(overlay.getCluster(env))) {
                    // Method 1: Replace the entire cluster.
                    config.getClusters().put(env, overlay.getCluster(env));
                    if (hasHS2) {
                        if (isNull(config.getCluster(env).getHiveServer2())) {
                            config.getCluster(env).setHiveServer2(new HiveServer2Config());
                        }
                    }
                    if (hasMS) {
                        if (isNull(config.getCluster(env).getMetastoreDirect())) {
                            config.getCluster(env).setMetastoreDirect(new DBStore());
                        }
                    }
                }
            }
        }

        // Set Encrypted Status of Passwords.
        config.setEncryptedPasswords(overlay.isEncryptedPasswords());

        setDefaultsForDataStrategy(config);
    }

    /**
     * Sets default configuration values based on the data strategy.
     *
     * @param config The configuration to set defaults for
     */
    public void setDefaultsForDataStrategy(HmsMirrorConfig config) {
        // Set Attribute for the config.
        switch (config.getDataStrategy()) {
            case DUMP:
                break;
            case STORAGE_MIGRATION:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                break;
            case SCHEMA_ONLY:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                break;
            case SQL:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case EXPORT_IMPORT:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.EXPORT_IMPORT);
                break;
            case HYBRID:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.HYBRID);
                break;
            case COMMON:
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case LINKED:
                // Set to ensure 'dr' doesn't delete LINKED tables.
                config.setNoPurge(Boolean.TRUE);
                config.setReadOnly(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                config.getMigrateACID().setOn(Boolean.FALSE);
                break;
            default:
                break;
        }

    }

    /**
     * Creates a new configuration for a specific data strategy with appropriate defaults.
     *
     * @param dataStrategy The data strategy to create configuration for
     * @return A new HMS Mirror configuration
     */
    public HmsMirrorConfig createForDataStrategy(DataStrategyEnum dataStrategy) {
        HmsMirrorConfig rtn = new HmsMirrorConfig();
        rtn.setDataStrategy(dataStrategy);

        switch (dataStrategy) {
            case DUMP:
                rtn.getMigrateACID().setOn(Boolean.TRUE);
                rtn.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                Cluster leftDump = new Cluster();
                leftDump.setEnvironment(Environment.LEFT);
                leftDump.setHiveServer2(new HiveServer2Config());
                leftDump.setMetastoreDirect(new DBStore());
                leftDump.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.LEFT, leftDump);
                break;
            case STORAGE_MIGRATION:
                rtn.getMigrateACID().setOn(Boolean.TRUE);
                Cluster leftSM = new Cluster();
                leftSM.setEnvironment(Environment.LEFT);
                leftSM.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.LEFT, leftSM);
                leftSM.setMetastoreDirect(new DBStore());
                leftSM.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
//                rtn.getTransfer().setTargetNamespace("ofs://NEED_TO_SET_THIS");
                leftSM.setHiveServer2(new HiveServer2Config());
                break;
            case SCHEMA_ONLY:
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
                Cluster leftT = new Cluster();
                leftT.setEnvironment(Environment.LEFT);
                leftT.setLegacyHive(Boolean.TRUE);
                rtn.getClusters().put(Environment.LEFT, leftT);
                leftT.setMetastoreDirect(new DBStore());
                leftT.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                leftT.setHiveServer2(new HiveServer2Config());
                Cluster rightT = new Cluster();
                rightT.setEnvironment(Environment.RIGHT);
                rightT.setHiveServer2(new HiveServer2Config());
                rightT.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.RIGHT, rightT);
                break;
            case COMMON:
                Cluster leftC = new Cluster();
                leftC.setEnvironment(Environment.LEFT);
                leftC.setLegacyHive(Boolean.TRUE);
                rtn.getClusters().put(Environment.LEFT, leftC);
                leftC.setMetastoreDirect(new DBStore());
                leftC.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
//                rtn.getTransfer().setTargetNamespace("hdfs|s3a|ofs://NEED_TO_SET_THIS");
                leftC.setHiveServer2(new HiveServer2Config());
                Cluster rightC = new Cluster();
                rightC.setEnvironment(Environment.RIGHT);
                rightC.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.RIGHT, rightC);
                break;
            default:
                break;
        }

        setDefaultsForDataStrategy(rtn);
        return rtn;
    }

    /**
     * Validates the configuration for cluster connections.
     * This method performs comprehensive validation of connection-related configurations including:
     * - Password encryption and key validation
     * - Cluster configuration validation based on data strategy
     * - HiveServer2 and Metastore connection configuration validation
     * - URI validation for both LEFT and RIGHT clusters
     * - Driver JAR file validation
     * - Kerberos configuration validation across versions
     *
     * @param session The execute session containing the configuration to validate
     * @return Boolean TRUE if all connection configurations are valid, FALSE otherwise.
     *         The method will add appropriate error messages to the session's RunStatus for any validation failures.
     */
    /**
     * Validates the configuration for cluster connections using ConversionResult.
     * This method performs comprehensive validation of connection-related configurations including:
     * - Password encryption and key validation
     * - Cluster configuration validation based on data strategy
     * - HiveServer2 and Metastore connection configuration validation
     * - URI validation for both LEFT and RIGHT clusters
     * - Driver JAR file validation
     * - Kerberos configuration validation across versions
     *
     * @return Boolean TRUE if all connection configurations are valid, FALSE otherwise.
     * The method will add appropriate error messages to the conversionResult's RunStatus for any validation failures.
     */
    public Boolean validateForConnections() {
        Boolean rtn = Boolean.TRUE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        // Get the configuration from the session
//        ExecuteSession session = executeSessionService.getSession();
//        HmsMirrorConfig config = session.getConfig();

        // TODO: Fix for encrypted passwords
//        if (config.isEncryptedPasswords()) {
//            runStatus.addWarning(PASSWORDS_ENCRYPTED);
//            if (isNull(config.getPasswordKey()) || config.getPasswordKey().isEmpty()) {
//                runStatus.addError(PKEY_PASSWORD_CFG);
//                rtn = Boolean.FALSE;
//            }
//        }

        List<Environment> envList = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        Set<Environment> envSet = new TreeSet<>(envList);
        // Validate that the cluster properties are set for the data strategy.
        switch (job.getStrategy()) {
            // Need both clusters defined and HS2 configs set.
            case SQL:
                // Inplace Downgrade is a single cluster effort
                if (config.getMigrateACID().isInplace()) {
                    envSet.remove(Environment.RIGHT);
                    // Drop the Right cluster to prevent confusion.
                    conversionResult.getConnections().remove(Environment.RIGHT);
                }
            case SCHEMA_ONLY:
            case EXPORT_IMPORT:
            case HYBRID:
            case COMMON:
                for (Environment env : envSet) {
                    if (isNull(conversionResult.getConnection(env))) {
                        runStatus.addError(CLUSTER_NOT_DEFINED_OR_CONFIGURED, env);
                        rtn = Boolean.FALSE;
                    } else {
                        if (isNull(conversionResult.getConnection(env).getHs2Uri())) {
                            if (!conversionResult.isMockTestDataset()) {
                                runStatus.addError(HS2_NOT_DEFINED_OR_CONFIGURED, env);
                                rtn = Boolean.FALSE;
                            }
                        } else {
                            // Check the config values.
                            /*
                            Minimum Values:
                            - uri
                            - driverClassName
                            - username (if not kerberized)

                             */
                            ConnectionDto connection = conversionResult.getConnection(env);
                            if (!conversionResult.isMockTestDataset()) {
                                if (isBlank(connection.getHs2Uri())) {
                                    runStatus.addError(MISSING_PROPERTY, "uri", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                                /* We know this from the Platform Type.
                                if (isBlank(hs2.getDriverClassName())) {
                                    runStatus.addError(MISSING_PROPERTY, "driverClassName", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                                */

                                if (!connection.isHs2KerberosConnection() && isBlank(connection.getHs2Username())) {
                                    runStatus.addError(MISSING_PROPERTY, "user", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                            }
                        }
                    }
                }
                break;
            // Need Left cluster defined with HS2 config.
            case STORAGE_MIGRATION:
                // Check for Metastore Direct on LEFT.
            case DUMP:
                // Drop the Right cluster to prevent confusion.
                conversionResult.getConnections().remove(Environment.RIGHT);
                break;

        }

        // Set maxConnections to Concurrency.
        // Don't validate connections or url's if we're working with test data.
        if (!conversionResult.isMockTestDataset()) {
            ConnectionDto leftConnection = conversionResult.getConnection(Environment.LEFT);
            if (!leftConnection.isValidHs2Uri()) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_HS2_URI_INVALID);
            }
            /* We know this from the Platform Type.
            if (isBlank(leftHS2.getJarFile())) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_HS2_DRIVER_JARS);
            }
             */

            if (isBlank(leftConnection.getMetastoreDirectUri())) {
                runStatus.addWarning(LEFT_METASTORE_URI_NOT_DEFINED);
            }

            ConnectionDto rightConnection = conversionResult.getConnection(Environment.RIGHT);

            if (nonNull(rightConnection)) {
                // TODO: Add validation for -rid (right-is-disconnected) option.
                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
                // -
                //
//                if (nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
//                    HiveServer2Config rightHS2 = config.getCluster(Environment.RIGHT).getHiveServer2();

                    /* We know this from the Platform Type.
                    if (isBlank(rightHS2.getJarFile())) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_DRIVER_JARS);
                    }
                     */

                if (job.getStrategy() != STORAGE_MIGRATION
                        && !rightConnection.isValidHs2Uri()) {
                    if (!job.getStrategy().equals(DataStrategyEnum.DUMP)) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_URI_INVALID);
                    }
                } else {


                    if (leftConnection.isHs2KerberosConnection()
                            && rightConnection.isHs2KerberosConnection()
                            && (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()
                            != conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive())) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(KERB_ACROSS_VERSIONS);
                    }
                }
//                }
            } else {
                if (!(job.getStrategy() == STORAGE_MIGRATION
                        || job.getStrategy() == DataStrategyEnum.DUMP)) {
                    if (!config.getMigrateACID().isInplace()) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_DEFINITION_MISSING);
                    }
                }
            }
        }

        return rtn;
    }

    /**
     * Validates the configuration for cluster connections using ExecuteSession.
     *
     * @return Boolean TRUE if all connection configurations are valid, FALSE otherwise
     * @deprecated Use {@link #validateForConnections(ConversionResult)} instead.
     *             This method will be removed in a future release.
     @Deprecated(since = "4.0", forRemoval = true)
     public Boolean validateForConnections(ExecuteSession session) {
     // Delegate to ConversionResult-based method
     if (session.getConversionResult() == null) {
     // Create a temporary ConversionResult with the session's RunStatus
     ConversionResult conversionResult = new ConversionResult();
     conversionResult.setRunStatus(session.getRunStatus());
     return validateForConnections(conversionResult);
     }
     return validateForConnections(session.getConversionResult());
     }
     */

    /**
     * Validates the complete configuration using ConversionResult.
     * This method performs comprehensive validation including:
     * - JAR file validation
     * - Data strategy compatibility checks
     * - Cluster configuration validation
     * - Warehouse and namespace validation
     * - ACID migration validation
     * - Link testing between clusters
     * - Database filtering validation
     *
     * @param cli The CLI environment
     * @return Boolean TRUE if configuration is valid, FALSE otherwise
     */
    public Boolean validate() {
        AtomicReference<Boolean> rtn = new AtomicReference<>(Boolean.TRUE);

        // Get the configuration from the session
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        final RunStatus runStatus = conversionResult.getRunStatus();

        // Reset RunStatus
        runStatus.clearErrors();
        runStatus.clearWarnings();

        // Do some basic validation first.
        // Ensure that we have connections, dataset, and a config
        if (isNull(conversionResult.getConnection(Environment.LEFT))) {
            runStatus.addError(MISSING_CONFIGURATION, "LEFT(Source) connection");
            rtn.set(Boolean.FALSE);
        }

        /*
        RIGHT is NOT required when:
        - Strategy is STORAGE_MIGRATION or DUMP
        - OR ACID in-place downgrade with SQL/EXPORT_IMPORT/HYBRID strategy
         */
        if (isNull(conversionResult.getConnection(Environment.RIGHT)) &&
                !EnumSet.of(DataStrategyEnum.STORAGE_MIGRATION, DataStrategyEnum.DUMP).contains(job.getStrategy()) &&
                !(config.getMigrateACID().isInplace() && config.getMigrateACID().isDowngrade() &&
                        EnumSet.of(DataStrategyEnum.SQL, DataStrategyEnum.EXPORT_IMPORT, DataStrategyEnum.HYBRID).contains(job.getStrategy()))) {
            runStatus.addError(MISSING_CONFIGURATION, "RIGHT(Target) connection");
            rtn.set(Boolean.FALSE);
        }

        if (isNull(conversionResult.getDataset())) {
            runStatus.addError(MISSING_CONFIGURATION, "Dataset");
            rtn.set(Boolean.FALSE);
        } else {
            if (conversionResult.getDataset().getDatabases().isEmpty()) {
                runStatus.addError(MISSING_CONFIGURATION, "Databases");
                rtn.set(Boolean.FALSE);
            }
        }

        // Shortcut the Validation because we are missing required elements.
        if (!rtn.get()) {
            return rtn.get();
        }

        // ==============================================================================================================

        // Validate the jar files listed in the configs for each cluster.
        // Visible Environment Variables:
        // Don't validate when using test data.
        if (!conversionResult.isMockTestDataset()) {
            Environment[] envs = Environment.getVisible();
            /* Under the idea, the jar files will be static or known, so no need to validate them. */
            /*
            for (Environment env : envs) {
                Cluster cluster = config.getCluster(env);
                if (nonNull(cluster)) {
                    if (nonNull(cluster.getHiveServer2())) {
                        if (isBlank(cluster.getHiveServer2().getJarFile())) {
                            runStatus.addError(HS2_DRIVER_JARS_MISSING, env);
                            rtn.set(Boolean.FALSE);
                        } else {
                            String[] jarFiles = cluster.getHiveServer2().getJarFile().split("\\:");
                            // Go through each jar file and validate it exists.
                            for (String jarFile : jarFiles) {
                                if (!new File(jarFile).exists()) {
                                    runStatus.addError(HS2_DRIVER_JAR_NOT_FOUND, jarFile, env);
                                    rtn.set(Boolean.FALSE);
                                }
                            }
                        }
                    }
                }
            }
             */
        }

        // Set distcp options.
        // TODO: Kind of a non-event.
//        canDeriveDistcpPlan(session);

        // When doing STORARE_MIGRATION using DISTCP, we can alter the table location of an ACID table and 'distcp' the
        // data to the new location without any other changes because the filesystem directory names will still match the
        // metastores internal transactional values.
        // BUT, if you've asked to save an ARCHIVE of the table and use 'distcp', we need to 'create' the new table, which
        // will not maintain the ACID properties. This is an invalid state.
        if (job.getStrategy() == STORAGE_MIGRATION) {
            if (config.getTransfer().getStorageMigration().isDistcp() &&
                    config.getMigrateACID().isOn() &&
                    config.getTransfer().getStorageMigration().isCreateArchive()) {
                runStatus.addError(STORAGE_MIGRATION_ACID_ARCHIVE_DISTCP);
                rtn.set(Boolean.FALSE);
            }
        }

        // Check that we're not moving backwards with Hive Migrations.
        if (nonNull(conversionResult.getConnection(Environment.RIGHT))
                && conversionResult.getConnection(Environment.RIGHT).getPlatformType().getHiveVersion().getVersion() <
                conversionResult.getConnection(Environment.LEFT).getPlatformType().getHiveVersion().getVersion()) {
            runStatus.addError(HIVE_DOWNGRADE_REQUESTED);
            rtn.set(Boolean.FALSE);
        }

        if (conversionResult.getDataset().getDatabases().isEmpty()) {
            runStatus.addError(MISC_ERROR, "No databases specified");
            rtn.set(Boolean.FALSE);
        }

        // Both of these can't be set together.
        conversionResult.getDataset().getDatabases().forEach(db -> {
            if (!isBlank(db.getDbRename()) && !isBlank(db.getDbPrefix())) {
                rtn.set(Boolean.FALSE);
                runStatus.addError(CONFLICTING_PROPERTIES, "dbRename", "dbPrefix");
            }
        });

        // Before Validation continues, let's make some adjustments to the configuration to
        // ensure we're in a valid state.
        rtn.set(alignConfigurationSettings());

        switch (job.getStrategy()) {
            case DUMP:
                break;
            case STORAGE_MIGRATION:
                // This strategy not available for Legacy Hive.
                if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                    runStatus.addError(STORAGE_MIGRATION_NOT_AVAILABLE_FOR_LEGACY, Environment.LEFT);
                    rtn.set(Boolean.FALSE);
                }

                // When using the Aligned movement strategy, we MUST have access to the Metastore Direct to pull
                // All the location details for the tables.
                if (conversionResult.getConnection(Environment.LEFT).getMetastoreDirectUri() == null) {
                    runStatus.addError(METASTORE_DIRECT_NOT_DEFINED_OR_CONFIGURED, Environment.LEFT.toString());
                    rtn.set(Boolean.FALSE);
                }

                // When STORAGE_MIGRATION and HDP3, we need to force external location AND
                //   we can't use the ACID options.
                if (conversionResult.getConnection(Environment.LEFT).getPlatformType() == PlatformType.HDP3) {
                    config.setForceExternalLocation(Boolean.TRUE);
                    if (config.getMigrateACID().isOn() &&
                            !config.getTransfer().getStorageMigration().isDistcp()) {
                        runStatus.addError(HIVE3_ON_HDP_ACID_TRANSFERS);
                        rtn.set(Boolean.FALSE);
                    }
                }

                break;
            case SCHEMA_ONLY:
                // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
                if (config.getMigrateACID().isOn() &&
                        config.getMigrateACID().isDowngrade()) {
                    runStatus.addError(ACID_DOWNGRADE_SCHEMA_ONLY);
                    rtn.set(Boolean.FALSE);
                }
                if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE
                        && config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.DISTCP) {
                    runStatus.addWarning(DISTCP_W_RELATIVE);
                }
                if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE
                        && config.getTransfer().getStorageMigration().getDataMovementStrategy() == DataMovementStrategyEnum.MANUAL) {
                    runStatus.addWarning(RELATIVE_MANUAL);
                }
            default:
                // Need to build a conversions supported map. EG: HDP2 -> CDP, etc..
                if (nonNull(conversionResult.getConnection(Environment.RIGHT)) && conversionResult.getConnection(Environment.RIGHT).getPlatformType().isHdpHive3()) {
                    config.setForceExternalLocation(Boolean.TRUE);
                    runStatus.addWarning(HDP3_HIVE);
                }

                // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
                if (!config.getMigrateACID().isInplace()) {
                    if (conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive() &&
                            !conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()
                        // TODO: Review
//                            && !config.isDumpTestData()
                    ) {
                        runStatus.addError(NON_LEGACY_TO_LEGACY);
                        rtn.set(Boolean.FALSE);
                    }
                } else {
                    // Drop the Right cluster to prevent confusion.
                    conversionResult.getConnections().remove(Environment.RIGHT);
                }
        }

        // Ozone Volume Name Check.
        // If the target namespace is set to ofs:// and we have warehouse plans defined for movement, ensure the
        //   volume name is at least 3 characters long.
        // Validate that the targetNamespace isn't null
        if (isNull(conversionResult.getTargetNamespace()) && job.getStrategy() != DataStrategyEnum.DUMP) {
            log.error("Unable to validate Target Namespace.");
            rtn.set(Boolean.FALSE);
            runStatus.addError(TARGET_NAMESPACE_NOT_DEFINED);
        } else {
            if (conversionResult.getTargetNamespace().startsWith("ofs://")) {
                //
                for (DatasetDto.DatabaseSpec dbs : conversionResult.getDataset().getDatabases()) {
                    Warehouse wp = dbs.getWarehouse();
                    String externalDirectory = wp.getExternalDirectory();
                    String managedDirectory = wp.getManagedDirectory();
                    if (nonNull(externalDirectory) && Objects.requireNonNull(NamespaceUtils.getFirstDirectory(externalDirectory)).length() < 3) {
                        runStatus.addError(OZONE_VOLUME_NAME_TOO_SHORT);
                        rtn.set(Boolean.FALSE);
                    }
                    if (nonNull(managedDirectory) && Objects.requireNonNull(NamespaceUtils.getFirstDirectory(managedDirectory)).length() < 3) {
                        runStatus.addError(OZONE_VOLUME_NAME_TOO_SHORT);
                        rtn.set(Boolean.FALSE);
                    }
                }
//            if (!config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
//                RunStatus finalRunStatus = runStatus;
//                config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().forEach((k, v) -> {
//                    String externalDirectory = v.getExternalDirectory();
//                    String managedDirectory = v.getManagedDirectory();
//                    if (nonNull(externalDirectory) && Objects.requireNonNull(NamespaceUtils.getFirstDirectory(externalDirectory)).length() < 3) {
//                        finalRunStatus.addError(OZONE_VOLUME_NAME_TOO_SHORT);
//                        rtn.set(Boolean.FALSE);
//                    }
//                    if (nonNull(managedDirectory) && Objects.requireNonNull(NamespaceUtils.getFirstDirectory(managedDirectory)).length() < 3) {
//                        finalRunStatus.addError(OZONE_VOLUME_NAME_TOO_SHORT);
//                        rtn.set(Boolean.FALSE);
//                    }
//                });
//            }
            }
        }

        // TODO: Need validation of Metastore Direct.
//        if (nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
//            DBStore leftMS = config.getCluster(Environment.LEFT).getMetastoreDirect();
//            if (isBlank(leftMS.getUri())) {
//                runStatus.addWarning(LEFT_METASTORE_URI_NOT_DEFINED);
//            }
//        }

        // Check for valid acid downgrade scenario.
        // Can't downgrade without SQL.
        if (config.getMigrateACID().isInplace()) {
            if (job.getStrategy() != SQL) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn.set(Boolean.FALSE);
            }
        }

        // Messages about what controls the way the databases are filtered.
        // TODO: Since we're using Datasets, I don't think this is needed.
        /*
        switch (job.getStrategy()) {
            case STORAGE_MIGRATION:
                runStatus.addWarning(DATASTRATEGY_FILTER_CONTROLLED_BY, job.getStrategy().toString(), "Warehouse Plans");
                break;
            case DUMP:
            case SCHEMA_ONLY:
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case COMMON:
            case LINKED:
            default:
                switch (config.getDatabaseFilterType()) {
                    case MANUAL:
                        runStatus.addWarning(DATABASE_FILTER_CONTROLLED_BY, "Manual Database Input");
                        break;
                    case WAREHOUSE_PLANS:
                        runStatus.addWarning(DATABASE_FILTER_CONTROLLED_BY, "Warehouse Plans");
                        break;
                    case REGEX:
                        runStatus.addWarning(DATABASE_FILTER_CONTROLLED_BY, "RegEx Filter");
                        break;
                    case UNDETERMINED:
                        break;
                }
                break;
        }
         */

        AtomicBoolean tableFilterWarning = new AtomicBoolean(false);

        if (config.loadMetadataDetails() && config.getTransfer().getStorageMigration().isDistcp()) {
            runStatus.addWarning(ALIGNED_DISTCP_EXECUTE);
            // We can't move forward in this condition without some warehouse plans.
            if (!doWareHousePlansExist()) {
                runStatus.addError(ALIGN_LOCATIONS_WITHOUT_WAREHOUSE_PLANS);
                rtn.set(Boolean.FALSE);
            }

            // Warn if there is any filter of tables.  This could affect distcp jobs
            //    because we're not including everything in the Database.

            conversionResult.getDataset().getDatabases().forEach(db -> {
                if (db.getFilter().isFilterEnabled() || !db.getTables().isEmpty()) {
                    runStatus.addWarning(DISTCP_W_TABLE_FILTERS);
                    rtn.set(Boolean.FALSE);
                    tableFilterWarning.set(true);
                }
            });

            // Need to work through the ACID Inplace Downgrade.
            if (job.getStrategy() == SQL
                    && config.getMigrateACID().isOn()
                    && config.getMigrateACID().isDowngrade()
                    && (!doWareHousePlansExist())) {
                runStatus.addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn.set(Boolean.FALSE);
            }
            if (job.getStrategy() == SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (config.getMigrateACID().isOn() ||
                        config.getMigrateACID().isOnly()) {
                    if (!config.getMigrateACID().isDowngrade() && config.getTransfer().getStorageMigration().isDistcp()) {
                        runStatus.addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn.set(Boolean.FALSE);
                    }
                }
            }
        }

        // TODO: Work to do on Force External Location.
        if (config.isForceExternalLocation()) {
            runStatus.addWarning(RDL_FEL_OVERRIDES);
        }

        // Can't LINK ACID tables. They'll need to turn ACID off.
        if (job.getStrategy() == DataStrategyEnum.LINKED) {
            if (config.getMigrateACID().isOn()) {
                runStatus.addError(LINKED_NO_ACID_SUPPORT);
                rtn.set(Boolean.FALSE);
            }
        }

        if (conversionResult.isMockTestDataset()) {
            runStatus.addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
        }

        if (job.isSync()
                && (tableFilterWarning.get())) {
            runStatus.addWarning(SYNC_TBL_FILTER);
        }

        if (job.isSync()
                && !(job.getStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || job.getStrategy() == DataStrategyEnum.LINKED ||
                job.getStrategy() == SQL ||
                job.getStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                job.getStrategy() == DataStrategyEnum.HYBRID)) {
            runStatus.addError(VALID_SYNC_STRATEGIES);
            rtn.set(Boolean.FALSE);
        }

        if (config.getMigrateACID().isOn()
                && !(job.getStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || job.getStrategy() == DataStrategyEnum.DUMP
                || job.getStrategy() == DataStrategyEnum.EXPORT_IMPORT
                || job.getStrategy() == DataStrategyEnum.HYBRID
                || job.getStrategy() == SQL
                || job.getStrategy() == STORAGE_MIGRATION)) {
            runStatus.addError(VALID_ACID_STRATEGIES);
            rtn.set(Boolean.FALSE);
        }

        if (config.getMigrateACID().isOn()
                && config.getMigrateACID().isInplace()) {
            if (!(job.getStrategy() == SQL)) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn.set(Boolean.FALSE);
            }
            if (isNull(conversionResult.getTargetNamespace())) {
                runStatus.addError(TARGET_NAMESPACE_NOT_DEFINED);
                rtn.set(Boolean.FALSE);
            }
            ;
            if (!isBlank(job.getIntermediateStorage())) {
                runStatus.addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
                rtn.set(Boolean.FALSE);
            }
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addError(DISTCP_W_DA_IP_ACID);
                rtn.set(Boolean.FALSE);
            }
            if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                runStatus.addError(DA_IP_NON_LEGACY);
                rtn.set(Boolean.FALSE);
            }
        }


        // Check to ensure the Target Namespace is available.
        // TODO: Review this.
        /*
        try {
            // Under this condition, there isn't a RIGHT cluster defined. So skip the check.
            String targetNamespace = null;
            switch (job.getStrategy()) {
                case DUMP:
                    // No check needed.
                    break;
                case EXPORT_IMPORT:
                case SQL:
                    if (configLite.getMigrateACID().isInplace()) {
                        break;
                    } else {
                        targetNamespace = configLite.getTargetNamespace();
                    }
                default:
                    targetNamespace = config.getTargetNamespace();
                    break;
            }
        } catch (RequiredConfigurationException e) {
            runStatus.addError(TARGET_NAMESPACE_NOT_DEFINED);
            rtn.set(Boolean.FALSE);
        }
         */

        // Because some just don't get that you can't do this...
        // Check that the external and managed warehouse locations aren't the same.  hive won't allow this.
        // TODO: Fix
        /*
        if (nonNull(config.getTransfer().getWarehouse())) {
            if ((!isBlank(config.getTransfer().getWarehouse().getManagedDirectory())) &&
                    (!isBlank(config.getTransfer().getWarehouse().getExternalDirectory()))) {
                // Make sure these aren't set to the same location.
                if (config.getTransfer().getWarehouse().getManagedDirectory().equals(config.getTransfer().getWarehouse().getExternalDirectory())) {
                    runStatus.addError(WAREHOUSE_DIRS_SAME_DIR, config.getTransfer().getWarehouse().getExternalDirectory()
                            , config.getTransfer().getWarehouse().getManagedDirectory());
                    rtn.set(Boolean.FALSE);
                }
            }
        }
         */

        if (job.getStrategy() == DataStrategyEnum.ACID) {
            runStatus.addError(ACID_NOT_TOP_LEVEL_STRATEGY);
            rtn.set(Boolean.FALSE);
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (job.getStrategy()) {
            case LINKED:
                if (nonNull(conversionResult.getTargetNamespace())) {
                    runStatus.addError(COMMON_STORAGE_WITH_LINKED);
                    rtn.set(Boolean.FALSE);
                }
                if (!isBlank(job.getIntermediateStorage())) {
                    runStatus.addError(INTERMEDIATE_STORAGE_WITH_LINKED);
                    rtn.set(Boolean.FALSE);
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                // Downgrade inplace is a single cluster effort.
                // TODO: Review and Fix
                /*
                if (!config.getMigrateACID().isInplace()) {
                    if (conversionResult.getConnection(Environment.RIGHT).getHiveServer2() != null
                            && !config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                            && isBlank(config.getTransfer().getIntermediateStorage())
                    ) {

                        try {
                            if (!config.getMigrateACID().isInplace() && !linkTest(session, cli)) {
                                runStatus.addError(LINK_TEST_FAILED);
                                rtn.set(Boolean.FALSE);
                            }
                        } catch (DisabledException e) {
                            log.error("Link Test Skipped because the CLI Interface is disabled.");
                            runStatus.addError(LINK_TEST_FAILED);
                            rtn.set(Boolean.FALSE);
                        }
                    } else {
                        runStatus.addWarning(LINK_TEST_SKIPPED_WITH_OPTIONS);
                    }
                }
                 */
                break;
            case SCHEMA_ONLY:
                if (config.isCopyAvroSchemaUrls()) {
                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    // TODO: Review
                        /*
                    try {
                        if (!linkTest(session, cli)) {
                            runStatus.addError(LINK_TEST_FAILED);
                            rtn.set(Boolean.FALSE);
                        }
                    } catch (DisabledException e) {
                        log.error("Link Test Skipped because the CLI Interface is disabled.");
                        runStatus.addError(LINK_TEST_FAILED);
                        rtn.set(Boolean.FALSE);
                    }
                         */
                }
                break;
            case DUMP:
                // TODO: Review, I don't think we need this anymore.
                /*
                if (config.getDumpSource() == Environment.RIGHT) {
                    runStatus.addWarning(DUMP_ENV_FLIP);
                }
                */
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
                    runStatus.addError(LEGACY_HIVE_RIGHT_CLUSTER);
                    rtn.set(Boolean.FALSE);
                }
                break;
        }

        // TODO: Review
        /*
        if (job.isReplace()) {
            if (job.getStrategy() != SQL) {
                runStatus.addError(REPLACE_ONLY_WITH_SQL);
                rtn.set(Boolean.FALSE);
            }
            if (configLite.getMigrateACID().isOn()) {
                if (!configLite.getMigrateACID().isDowngrade()) {
                    runStatus.addError(REPLACE_ONLY_WITH_DA);
                    rtn.set(Boolean.FALSE);
                }
            }
        }
         */

        if (job.isReadOnly()) {
            switch (job.getStrategy()) {
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                case SQL:
                    break;
                default:
                    runStatus.addError(RO_VALID_STRATEGIES);
                    rtn.set(Boolean.FALSE);
            }
        }

        // TODO: Review
        /*
        if (config.getCluster(Environment.RIGHT) != null) {
            if (job.getStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
                    configLite.isCreateIfNotExists()) {
                runStatus.addWarning(CINE_WITH_DATASTRATEGY);
            }
        }
        */

        /* TODO: Review? Wasn't enabled before.
        if (config.getTranslator().getOrderedGlobalLocationMap() != null) {
            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
            //    For items that match /data/mydir maybe confusing as to which one to adjust.
            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
            //      will push longer paths to be evaluated first and once a match is found, skip further checks.

        }
         */

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.


        if (rtn.get()) {
            // Last check for errors.
            if (runStatus.hasErrors()) {
                rtn.set(Boolean.FALSE);
            }
        }
        return rtn.get();
    }

    /**
     * Validates the configuration using ExecuteSession.
     *
     * @param session The execute session containing the configuration to validate
     * @param cli The CLI environment
     * @return Boolean TRUE if configuration is valid, FALSE otherwise
     * @deprecated Use {@link #validate(ConversionResult, CliEnvironment)} instead.
     *             This method will be removed in a future release.
     @Deprecated(since = "4.0", forRemoval = true)
     public Boolean validate(ExecuteSession session, CliEnvironment cli) {
     // Delegate to ConversionResult-based method
     if (session.getConversionResult() == null) {
     // Create a temporary ConversionResult with the session's RunStatus
     ConversionResult conversionResult = new ConversionResult();
     conversionResult.setRunStatus(session.getRunStatus());
     return validate(conversionResult, cli);
     }
     return validate(session.getConversionResult(), cli);
     }
     */

    /**
     * Flips the configuration between LEFT and RIGHT clusters.
     * This method creates a new configuration with the LEFT and RIGHT cluster configurations swapped.
     *
     * @param config The configuration to flip
     * @return The flipped configuration
     */
    public HmsMirrorConfig flipConfig(HmsMirrorConfig config) {
        if (config != null) {
            Cluster leftClone = config.getCluster(Environment.LEFT).clone();
            leftClone.setEnvironment(Environment.RIGHT);
            Cluster rightClone = config.getCluster(Environment.RIGHT).clone();
            rightClone.setEnvironment(Environment.LEFT);
            config.getClusters().put(Environment.RIGHT, leftClone);
            config.getClusters().put(Environment.LEFT, rightClone);
        }
        return config;
    }

}
