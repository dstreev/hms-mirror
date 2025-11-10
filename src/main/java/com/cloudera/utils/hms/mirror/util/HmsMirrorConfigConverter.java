/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.util;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.legacy.Cluster;
import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.legacy.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.PartitionDiscovery;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.JobExecution;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Utility class to convert HmsMirrorConfig to various DTO objects:
 * - ConfigLiteDto
 * - DatasetDto
 * - ConnectionDto (Left and Right)
 * - JobDto
 */
@Slf4j
public class HmsMirrorConfigConverter {

    /**
     * Main conversion method that returns all converted DTOs
     */
    public static ConversionResult convert(HmsMirrorConfig config, String jobName) {
        ConversionResult result = new ConversionResult();
        // Setup Output directories for Report Writing
        result.setUserSetOutputDirectory(config.isUserSetOutputDirectory());
        result.setOutputDirectory(config.getOutputDirectory());
        result.setFinalOutputDirectory(config.getFinalOutputDirectory());

        result.setConfig(toConfigLiteDto(config, result.getKey()));
        result.setDataset(toDatasetDto(config, result.getKey()));
        result.setConnection(Environment.LEFT, toConnectionDto(config, Environment.LEFT));
        result.setConnection(Environment.RIGHT, toConnectionDto(config, Environment.RIGHT));
        result.setJob(toJobDto(config, jobName));
        result.setJobExecution(toJobExecution(config));
        result.setTranslator(config.getTranslator().clone());

        return result;
    }

    /**
     * Convert HmsMirrorConfig to ConfigLiteDto
     */
    public static ConfigLiteDto toConfigLiteDto(HmsMirrorConfig config, String name) {
        ConfigLiteDto dto = new ConfigLiteDto();

        // Basic configuration
        dto.setName(name);
        dto.setDescription(config.getComment());

        // Feature flags
        dto.setMigrateNonNative(config.isMigrateNonNative());
//        dto.setExecute(config.isExecute());
        dto.setCreateIfNotExists(extractCreateIfNotExistsFromCluster(config));
        dto.setEnableAutoTableStats(extractEnableAutoTableStatsFromCluster(config));
        dto.setEnableAutoColumnStats(extractEnableAutoColumnStatsFromCluster(config));
        dto.setSaveWorkingTables(config.isSaveWorkingTables());

        // File and data handling
        dto.setCopyAvroSchemaUrls(config.isCopyAvroSchemaUrls());
//        dto.setLoadTestDataFile(config.getLoadTestDataFile());
        // Note: forceExternalLocation is not available in TransferConfig

        // Sub-configuration objects
        if (nonNull(config.getIcebergConversion())) {
            dto.setIcebergConversion(config.getIcebergConversion().clone());
        }

        if (nonNull(config.getMigrateACID())) {
            dto.setMigrateACID(config.getMigrateACID().clone());
        }

        if (nonNull(config.getMigrateVIEW())) {
            dto.setMigrateVIEW(config.getMigrateVIEW().clone());
        }

        if (nonNull(config.getOptimization())) {
            dto.setOptimization(config.getOptimization().clone());
        }

        // Transfer configuration - includes all TransferConfig fields:
        // - transferPrefix, shadowPrefix, storageMigrationPostfix
        // - exportBaseDirPrefix, remoteWorkingDirectory
        // - intermediateStorage, targetNamespace
        // - storageMigration, warehouse
        if (nonNull(config.getTransfer())) {
            // Clone to avoid sharing the same object reference
            dto.setTransfer(config.getTransfer().clone());
        }

        if (nonNull(config.getOwnershipTransfer())) {
            dto.setOwnershipTransfer(config.getOwnershipTransfer().clone());
        }

        return dto;
    }

    /**
     * Convert HmsMirrorConfig to DatasetDto
     * Creates a dataset based on databases and filter configuration
     */
    public static DatasetDto toDatasetDto(HmsMirrorConfig config, String datasetName) {
        DatasetDto dto = new DatasetDto();

        dto.setName(datasetName);
        dto.setDescription("Dataset converted from HmsMirrorConfig");

        // If specific databases are defined, create database specs for each
        if (nonNull(config.getDatabases()) && !config.getDatabases().isEmpty()) {
            for (String dbName : config.getDatabases()) {
                DatasetDto.DatabaseSpec dbSpec = new DatasetDto.DatabaseSpec();
                dbSpec.setDatabaseName(dbName);
                dbSpec.setTables(new ArrayList<>()); // Empty - will use filter or all tables

                // Apply global filter if defined
                if (nonNull(config.getFilter())) {
                    DatasetDto.TableFilter filter = convertFilter(config.getFilter());
                    if (filter != null) {
                        dbSpec.setFilter(filter);
                    }
                }

                // Apply warehouse configuration if available
                if (nonNull(config.getTransfer()) && nonNull(config.getTransfer().getWarehouse())) {
                    dbSpec.setWarehouse(config.getTransfer().getWarehouse());
                }

                // Apply dbPrefix and dbRename if defined
                if (!isBlank(config.getDbPrefix())) {
                    dbSpec.setDbPrefix(config.getDbPrefix());
                }
                if (!isBlank(config.getDbRename())) {
                    dbSpec.setDbRename(config.getDbRename());
                }

                dto.getDatabases().add(dbSpec);
            }
        }

        // Check the Translator Warehouse Plans.
        config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().forEach((dbName, warehousePlan) -> {
            DatasetDto.DatabaseSpec dbSpec = dto.getDatabase(dbName);
            if (isNull(dbSpec)) {
                dbSpec = new DatasetDto.DatabaseSpec();
                dbSpec.setDatabaseName(dbName);
                dto.getDatabases().add(dbSpec);
            }
            dbSpec.getWarehouse().setExternalDirectory(warehousePlan.getExternalDirectory());
            dbSpec.getWarehouse().setManagedDirectory(warehousePlan.getManagedDirectory());
        });

        return dto;
    }

    /**
     * Convert HmsMirrorConfig's Filter to DatasetDto.TableFilter
     */
    private static DatasetDto.TableFilter convertFilter(com.cloudera.utils.hms.mirror.domain.core.Filter filter) {
        if (filter == null) {
            return null;
        }

        DatasetDto.TableFilter tableFilter = new DatasetDto.TableFilter();

        if (!isBlank(filter.getTblRegEx())) {
            tableFilter.setIncludeRegEx(filter.getTblRegEx());
        }

        if (!isBlank(filter.getTblExcludeRegEx())) {
            tableFilter.setExcludeRegEx(filter.getTblExcludeRegEx());
        }

        if (filter.getTblSizeLimit() != null && filter.getTblSizeLimit() > 0) {
            tableFilter.setMaxSizeMb(filter.getTblSizeLimit());
        }

        if (filter.getTblPartitionLimit() != null && filter.getTblPartitionLimit() > 0) {
            tableFilter.setMaxPartitions(filter.getTblPartitionLimit());
        }

        // Only return filter if at least one field is set
        if (!isBlank(tableFilter.getIncludeRegEx()) ||
            !isBlank(tableFilter.getExcludeRegEx()) ||
            tableFilter.getMaxSizeMb() > 0 ||
            tableFilter.getMaxPartitions() > 0) {
            return tableFilter;
        }

        return null;
    }

    /**
     * Convert Cluster to ConnectionDto
     */
    public static ConnectionDto toConnectionDto(HmsMirrorConfig config, Environment environment) {
        Cluster cluster = config.getCluster(environment);
        if (cluster == null) {
            log.warn("No cluster defined for environment: {}", environment);
            return null;
        }

        ConnectionDto dto = ConnectionDto.builder()
                .name(environment.name())
                .description("Connection for " + environment.name() + " cluster")
                .build();

        // Platform configuration
        if (nonNull(cluster.getPlatformType())) {
            dto.setPlatformType(cluster.getPlatformType());
        }

        // Core configuration
        // We need to overlay the hcfs value if the targetNameSpace was set.
        if (!isBlank(config.getTransfer().getTargetNamespace())) {
            dto.setHcfsNamespace(config.getTransfer().getTargetNamespace());
        } else {
            dto.setHcfsNamespace(cluster.getHcfsNamespace());
        }

        // HiveServer2 configuration
        if (nonNull(cluster.getHiveServer2())) {
            HiveServer2Config hs2 = cluster.getHiveServer2();
            dto.setHs2Uri(hs2.getUri());

            if (nonNull(hs2.getConnectionProperties())) {
                Map<String, String> props = new HashMap<>();
                hs2.getConnectionProperties().forEach((key, value) -> {
                    if (nonNull(value)) {
                        // Extract username and password if present
                        if ("user".equalsIgnoreCase(key.toString())) {
                            dto.setHs2Username(value.toString());
                        } else if ("password".equalsIgnoreCase(key.toString())) {
                            dto.setHs2Password(value.toString());
                        } else {
                            props.put(key.toString(), value.toString());
                        }
                    }
                });
                if (!props.isEmpty()) {
                    dto.setHs2ConnectionProperties(props);
                }
            }
        }

        // Metastore Direct configuration
        if (nonNull(cluster.getMetastoreDirect())) {
            DBStore metastore = cluster.getMetastoreDirect();
            dto.setMetastoreDirectEnabled(true);
            dto.setMetastoreDirectUri(metastore.getUri());

            // Convert DB_TYPE enum to string
            if (nonNull(metastore.getType())) {
                dto.setMetastoreDirectType(metastore.getType().name());
            }

            // Extract username and password from connectionProperties
            if (nonNull(metastore.getConnectionProperties())) {
                String username = metastore.getConnectionProperties().getProperty("user");
                String password = metastore.getConnectionProperties().getProperty("password");
                dto.setMetastoreDirectUsername(username);
                dto.setMetastoreDirectPassword(password);
            }

            if (nonNull(metastore.getConnectionProperties())) {
                String minStr = metastore.getConnectionProperties().getProperty("minConnections");
                String maxStr = metastore.getConnectionProperties().getProperty("maxConnections");
                if (!isBlank(minStr)) {
                    try {
                        dto.setMetastoreDirectMinConnections(Integer.parseInt(minStr));
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse minConnections: {}", minStr);
                    }
                }
                if (!isBlank(maxStr)) {
                    try {
                        dto.setMetastoreDirectMaxConnections(Integer.parseInt(maxStr));
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse maxConnections: {}", maxStr);
                    }
                }
            }
        } else {
            dto.setMetastoreDirectEnabled(false);
        }

        // Partition discovery settings
        if (nonNull(cluster.getPartitionDiscovery())) {
            dto.setPartitionDiscoveryAuto(cluster.getPartitionDiscovery().isAuto());
            dto.setPartitionDiscoveryInitMSCK(cluster.getPartitionDiscovery().isInitMSCK());
        }

        // Bucket limit - no direct mapping, skip for now

        // Cluster settings
        dto.setCreateIfNotExists(cluster.isCreateIfNotExists());
        dto.setEnableAutoTableStats(cluster.isEnableAutoTableStats());
        dto.setEnableAutoColumnStats(cluster.isEnableAutoColumnStats());

        // Partition Handling
        dto.setPartitionDiscoveryAuto(cluster.getPartitionDiscovery().isAuto());
        dto.setPartitionDiscoveryInitMSCK(cluster.getPartitionDiscovery().isInitMSCK());

        // System fields
        dto.setCreated(LocalDateTime.now());
        dto.setModified(LocalDateTime.now());
//        dto.setDefault(false);

        return dto;
    }

    /**
     * Convert HmsMirrorConfig to JobDto
     */
    public static JobDto toJobDto(HmsMirrorConfig config, String jobName) {
        JobDto dto = new JobDto();

        dto.setName(jobName);
        dto.setDescription("Job converted from HmsMirrorConfig");

        if (config.getTransfer() != null) {
            dto.setTargetNamespace(config.getTransfer().getTargetNamespace());
        }

        // Consolidate DB Create Statements
        dto.setConsolidateDBCreateStatements(config.isConsolidateDBCreateStatements());

        // Connection references
        dto.setLeftConnectionReference("LEFT");
        dto.setRightConnectionReference("RIGHT");

        // Strategy
        dto.setStrategy(config.getDataStrategy());

        // Intermediate Storage
        dto.setIntermediateStorage(config.getTransfer().getIntermediateStorage());

        // Consolidate DB Statements
        dto.setConsolidateDBCreateStatements(config.isConsolidateDBCreateStatements());

        // Target Namespace
        dto.setTargetNamespace(config.getTransfer().getTargetNamespace());

        // Hybrid configuration
        if (nonNull(config.getHybrid())) {
            dto.setHybrid(config.getHybrid());
        }

        // Database only flag
        dto.setDatabaseOnly(config.isDatabaseOnly());

        // Skip Link Check
        dto.setSkipLinkCheck(config.isSkipLinkCheck());

        // Disaster recovery and related flags
        boolean isReadOnly = config.isReadOnly();
        boolean isNoPurge = config.isNoPurge();

        // If readOnly or noPurge are set, treat as disaster recovery
        if (isReadOnly || isNoPurge) {
            dto.setDisasterRecovery(true);
        } else {
            dto.setDisasterRecovery(false);
        }

        // Sync flag
        dto.setSync(config.isSync());

        return dto;
    }

    /**

     */
    public static JobExecution toJobExecution(HmsMirrorConfig config) {
        JobExecution jExec = new JobExecution();

        jExec.setDryRun(!config.isExecute());

        return jExec;
    }

    /**
     * Helper method to extract createIfNotExists from cluster configuration
     * Tries RIGHT cluster first, then LEFT
     */
    private static boolean extractCreateIfNotExistsFromCluster(HmsMirrorConfig config) {
        Cluster right = config.getCluster(Environment.RIGHT);
        if (nonNull(right)) {
            return right.isCreateIfNotExists();
        }

        Cluster left = config.getCluster(Environment.LEFT);
        if (nonNull(left)) {
            return left.isCreateIfNotExists();
        }

        return false;
    }

    /**
     * Helper method to extract enableAutoTableStats from cluster configuration
     * Tries RIGHT cluster first, then LEFT
     */
    private static boolean extractEnableAutoTableStatsFromCluster(HmsMirrorConfig config) {
        Cluster right = config.getCluster(Environment.RIGHT);
        if (nonNull(right)) {
            return right.isEnableAutoTableStats();
        }

        Cluster left = config.getCluster(Environment.LEFT);
        if (nonNull(left)) {
            return left.isEnableAutoTableStats();
        }

        return false;
    }

    /**
     * Helper method to extract enableAutoColumnStats from cluster configuration
     * Tries RIGHT cluster first, then LEFT
     */
    private static boolean extractEnableAutoColumnStatsFromCluster(HmsMirrorConfig config) {
        Cluster right = config.getCluster(Environment.RIGHT);
        if (nonNull(right)) {
            return right.isEnableAutoColumnStats();
        }

        Cluster left = config.getCluster(Environment.LEFT);
        if (nonNull(left)) {
            return left.isEnableAutoColumnStats();
        }

        return false;
    }

    /**
     * Convert ConnectionDto back to Cluster object
     * This allows backward compatibility with code expecting Cluster objects
     */
    public static Cluster toCluster(ConnectionDto connectionDto) {
        if (connectionDto == null) {
            return null;
        }

        Cluster cluster = new Cluster();

        // Core configuration
        cluster.setHcfsNamespace(connectionDto.getHcfsNamespace());

        // Platform type
        if (connectionDto.getPlatformType() != null) {
            cluster.setPlatformType(connectionDto.getPlatformType());
        }

        // HiveServer2 configuration
        if (!isBlank(connectionDto.getHs2Uri())) {
            HiveServer2Config hs2 = new HiveServer2Config();
            hs2.setUri(connectionDto.getHs2Uri());

            // Build connection properties
            java.util.Properties connProps = new java.util.Properties();
            if (!isBlank(connectionDto.getHs2Username())) {
                connProps.put("user", connectionDto.getHs2Username());
            }
            if (!isBlank(connectionDto.getHs2Password())) {
                connProps.put("password", connectionDto.getHs2Password());
            }
            if (nonNull(connectionDto.getHs2ConnectionProperties())) {
                connProps.putAll(connectionDto.getHs2ConnectionProperties());
            }
            hs2.setConnectionProperties(connProps);

            cluster.setHiveServer2(hs2);
        }

        // Metastore Direct configuration
        if (connectionDto.isMetastoreDirectEnabled() && !isBlank(connectionDto.getMetastoreDirectUri())) {
            DBStore metastore = new DBStore();
            metastore.setUri(connectionDto.getMetastoreDirectUri());

            // Set type
            if (!isBlank(connectionDto.getMetastoreDirectType())) {
                metastore.setType(DBStore.DB_TYPE.valueOf(connectionDto.getMetastoreDirectType()));
            }

            // Build connection properties
            java.util.Properties metastoreProps = new java.util.Properties();
            if (!isBlank(connectionDto.getMetastoreDirectUsername())) {
                metastoreProps.put("user", connectionDto.getMetastoreDirectUsername());
            }
            if (!isBlank(connectionDto.getMetastoreDirectPassword())) {
                metastoreProps.put("password", connectionDto.getMetastoreDirectPassword());
            }
            if (nonNull(connectionDto.getMetastoreDirectMinConnections())) {
                metastoreProps.put("minConnections", connectionDto.getMetastoreDirectMinConnections().toString());
            }
            if (nonNull(connectionDto.getMetastoreDirectMaxConnections())) {
                metastoreProps.put("maxConnections", connectionDto.getMetastoreDirectMaxConnections().toString());
            }
            metastore.setConnectionProperties(metastoreProps);

            cluster.setMetastoreDirect(metastore);
        }

        // Partition discovery settings
        PartitionDiscovery pd = new PartitionDiscovery();
        pd.setAuto(connectionDto.isPartitionDiscoveryAuto());
        pd.setInitMSCK(connectionDto.isPartitionDiscoveryInitMSCK());
        cluster.setPartitionDiscovery(pd);

        // Cluster settings
        cluster.setCreateIfNotExists(connectionDto.isCreateIfNotExists());
        cluster.setEnableAutoTableStats(connectionDto.isEnableAutoTableStats());
        cluster.setEnableAutoColumnStats(connectionDto.isEnableAutoColumnStats());

        return cluster;
    }
}
