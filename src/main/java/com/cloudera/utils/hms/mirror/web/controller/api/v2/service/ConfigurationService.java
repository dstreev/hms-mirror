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


package com.cloudera.utils.hms.mirror.web.controller.api.v2.service;

import com.cloudera.utils.hive.config.ConnectionPool;
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.*; // Import all domain objects
import com.cloudera.utils.hms.mirror.feature.LegacyTranslations;
import com.cloudera.utils.hms.mirror.domain.support.Environment; // Import Environment enum
import com.cloudera.utils.hms.mirror.domain.support.SideType; // Import SideType enum
import com.cloudera.utils.hms.mirror.domain.support.TableType; // Import TableType enum
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap; // For Clusters Map
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service to manage the HmsMirror Configuration state.
 * This service loads the initial configuration and provides methods
 * for selective updates, ensuring null values in partial updates do not
 * override existing values. It now directly uses domain objects.
 */
@Service("ConfigurationServiceV2")
@Slf4j
public class ConfigurationService {

    // Now directly holds the HmsMirrorConfig domain object
    private final AtomicReference<HmsMirrorConfig> currentConfig = new AtomicReference<>();
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    /**
     * Initializes the configuration by loading the default config.yaml.
     * This method runs once after the service is constructed.
     */
    @PostConstruct
    public void init() {
        log.info("Initializing ConfigurationService: Attempting to load init-config.yaml.");
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("init-config.yaml")) {
            if (inputStream == null) {
                log.warn("init-config.yaml not found in classpath. Initializing with empty configuration.");
                currentConfig.set(new HmsMirrorConfig());
            } else {
                HmsMirrorConfig loadedConfig = yamlMapper.readValue(inputStream, HmsMirrorConfig.class);
                currentConfig.set(loadedConfig);
                log.info("Configuration loaded successfully from init-config.yaml.");
            }
        } catch (IOException e) {
            log.error("Error loading configuration from init-config.yaml: {}", e.getMessage(), e);
            currentConfig.set(new HmsMirrorConfig()); // Fallback to empty config on error
        }

        // Initialize lists, sets, and maps if they are null after loading,
        // to prevent NullPointerExceptions during updates.
        HmsMirrorConfig config = currentConfig.get();

        if (config.getAcceptance() == null) config.setAcceptance(new Acceptance());
        if (config.getFilter() == null) config.setFilter(new Filter());
        if (config.getFilter().getDbPropertySkipList() == null)
            config.getFilter().setDbPropertySkipList(new ArrayList<>());
        if (config.getDatabases() == null) config.setDatabases(new HashSet<>());
        if (config.getLegacyTranslations() == null) config.setLegacyTranslations(new LegacyTranslations());
        if (config.getLegacyTranslations().getRowSerde() == null)
            config.getLegacyTranslations().setRowSerde(new HashMap<>());
        if (config.getHybrid() == null) config.setHybrid(new HybridConfig());
        if (config.getIcebergConversion() == null) config.setIcebergConversion(new IcebergConversion());
        if (config.getIcebergConversion().getTableProperties() == null)
            config.getIcebergConversion().setTableProperties(new HashMap<>());
        if (config.getMigrateACID() == null) config.setMigrateACID(new MigrateACID());
        if (config.getMigrateVIEW() == null) config.setMigrateVIEW(new MigrateVIEW());
        if (config.getOptimization() == null) config.setOptimization(new Optimization());
        if (config.getOptimization().getOverrides() == null) config.getOptimization().setOverrides(new Overrides());
        if (config.getOptimization().getOverrides().getProperties() == null)
            config.getOptimization().getOverrides().setProperties(new TreeMap<>()); // TreeMap for Overrides properties
        if (config.getTransfer() == null) config.setTransfer(new TransferConfig());
        if (config.getTransfer().getStorageMigration() == null)
            config.getTransfer().setStorageMigration(new StorageMigration());
        if (config.getTransfer().getWarehouse() == null) config.getTransfer().setWarehouse(new Warehouse());
        if (config.getOwnershipTransfer() == null) config.setOwnershipTransfer(new TransferOwnership());
        if (config.getTranslator() == null) config.setTranslator(new Translator());
        if (config.getTranslator().getWarehouseMapBuilder() == null)
            config.getTranslator().setWarehouseMapBuilder(new WarehouseMapBuilder());
        if (config.getTranslator().getWarehouseMapBuilder().getSources() == null)
            config.getTranslator().getWarehouseMapBuilder().setSources(new HashMap<>());
        if (config.getTranslator().getWarehouseMapBuilder().getWarehousePlans() == null)
            config.getTranslator().getWarehouseMapBuilder().setWarehousePlans(new HashMap<>());

        // Clusters specific initialization (now directly accessing the Map<Environment, Cluster>)
        if (config.getClusters() == null) config.setClusters(new TreeMap<>()); // Initialize as TreeMap

        // Ensure LEFT and RIGHT clusters exist in the map and their nested objects are initialized
        Cluster leftCluster = config.getClusters().computeIfAbsent(Environment.LEFT, k -> {
            Cluster newCluster = new Cluster();
            newCluster.setEnvironment(Environment.LEFT);
            return newCluster;
        });
        if (leftCluster.getHiveServer2() == null) leftCluster.setHiveServer2(new HiveServer2Config());
        if (leftCluster.getHiveServer2().getConnectionProperties() == null)
            leftCluster.getHiveServer2().setConnectionProperties(new java.util.Properties());
        if (leftCluster.getPartitionDiscovery() == null) leftCluster.setPartitionDiscovery(new PartitionDiscovery());
        if (leftCluster.getMetastoreDirect() == null) leftCluster.setMetastoreDirect(new DBStore());
        if (leftCluster.getMetastoreDirect().getConnectionProperties() == null)
            leftCluster.getMetastoreDirect().setConnectionProperties(new java.util.Properties());
        if (leftCluster.getMetastoreDirect().getConnectionPool() == null)
            leftCluster.getMetastoreDirect().setConnectionPool(new ConnectionPool());

        Cluster rightCluster = config.getClusters().computeIfAbsent(Environment.RIGHT, k -> {
            Cluster newCluster = new Cluster();
            newCluster.setEnvironment(Environment.RIGHT);
            return newCluster;
        });
        if (rightCluster.getHiveServer2() == null) rightCluster.setHiveServer2(new HiveServer2Config());
        if (rightCluster.getHiveServer2().getConnectionProperties() == null)
            rightCluster.getHiveServer2().setConnectionProperties(new java.util.Properties());
        if (rightCluster.getPartitionDiscovery() == null) rightCluster.setPartitionDiscovery(new PartitionDiscovery());
        // MetastoreDirect on RIGHT can be null in the provided config.yaml, so don't force init if not present
        // If it becomes non-null through an update, the updateMetastoreDirect will handle its sub-components.

        log.info("ConfigurationService initialization complete.");
    }

    /**
     * Retrieves the current complete HmsMirror configuration.
     *
     * @return The current HmsMirrorConfig object.
     */
    public HmsMirrorConfig getConfiguration() {
        log.debug("Retrieving current HmsMirror configuration.");
        return currentConfig.get();
    }
    
    /**
     * Replaces the entire current configuration with the provided one.
     * Used for file uploads where we want to completely replace the config.
     *
     * @param newConfig The new configuration to set
     * @return The new configuration
     */
    public HmsMirrorConfig replaceConfiguration(HmsMirrorConfig newConfig) {
        log.info("Replacing entire HmsMirror configuration.");
        if (newConfig == null) {
            throw new IllegalArgumentException("Configuration cannot be null");
        }
        currentConfig.set(newConfig);
        log.info("Configuration replaced successfully.");
        return newConfig;
    }

    /**
     * Updates the main HmsMirrorConfig object selectively.
     * Null values in the provided partialConfig will not override existing values.
     *
     * @param partialConfig The HmsMirrorConfig object containing updates.
     * @return The updated HmsMirrorConfig.
     */
    public HmsMirrorConfig updateConfiguration(HmsMirrorConfig partialConfig) {
        log.info("Attempting to update main HmsMirror configuration.");
        HmsMirrorConfig existingConfig = currentConfig.get();

        if (partialConfig.getAcceptance() != null) {
            if (existingConfig.getAcceptance() == null) existingConfig.setAcceptance(new Acceptance());
            updateAcceptance(existingConfig.getAcceptance(), partialConfig.getAcceptance());
            log.debug("Updated 'acceptance' section.");
        }
        existingConfig.setBeta(partialConfig.isBeta());
        if (partialConfig.getClusters() != null) {
            if (existingConfig.getClusters() == null) existingConfig.setClusters(new TreeMap<>());
            updateClusters(existingConfig.getClusters(), partialConfig.getClusters());
            log.debug("Updated 'clusters' section.");
        }
        // Always set String fields, allowing nulls to overwrite
        existingConfig.setCommandLineOptions(partialConfig.getCommandLineOptions());
        log.debug("Updated 'commandLineOptions' to: {}", partialConfig.getCommandLineOptions());
        existingConfig.setComment(partialConfig.getComment());
        log.debug("Updated 'comment' to: {}", partialConfig.getComment());
        existingConfig.setCopyAvroSchemaUrls(partialConfig.isCopyAvroSchemaUrls());
        log.debug("Updated 'copyAvroSchemaUrls' to: {}", partialConfig.isCopyAvroSchemaUrls());
        existingConfig.setConnectionPoolLib(partialConfig.getConnectionPoolLib());
        log.debug("Updated 'connectionPoolLib' to: {}", partialConfig.getConnectionPoolLib());
        existingConfig.setDataStrategy(partialConfig.getDataStrategy());
        log.debug("Updated 'dataStrategy' to: {}", partialConfig.getDataStrategy());
        existingConfig.setDatabaseOnly(partialConfig.isDatabaseOnly());
        log.debug("Updated 'databaseOnly' to: {}", partialConfig.isDatabaseOnly());
        existingConfig.setDumpTestData(partialConfig.isDumpTestData());
        log.debug("Updated 'dumpTestData' to: {}", partialConfig.isDumpTestData());
        existingConfig.setLoadTestDataFile(partialConfig.getLoadTestDataFile());
        log.debug("Updated 'loadTestDataFile' to: {}", partialConfig.getLoadTestDataFile());
        if (partialConfig.getFilter() != null) {
            if (existingConfig.getFilter() == null) existingConfig.setFilter(new Filter());
            updateFilter(existingConfig.getFilter(), partialConfig.getFilter());
            log.debug("Updated 'filter' section.");
        }
        if (partialConfig.getLegacyTranslations() != null) {
            if (existingConfig.getLegacyTranslations() == null)
                existingConfig.setLegacyTranslations(new LegacyTranslations());
            updateLegacyTranslations(existingConfig.getLegacyTranslations(), partialConfig.getLegacyTranslations());
            log.debug("Updated 'legacyTranslations' section.");
        }
        existingConfig.setDbPrefix(partialConfig.getDbPrefix());
        log.debug("Updated 'dbPrefix' to: {}", partialConfig.getDbPrefix());
        existingConfig.setDbRename(partialConfig.getDbRename());
        log.debug("Updated 'dbRename' to: {}", partialConfig.getDbRename());
        existingConfig.setDumpSource(partialConfig.getDumpSource());
        log.debug("Updated 'dumpSource' to: {}", partialConfig.getDumpSource());
        existingConfig.setExecute(partialConfig.isExecute());
        log.debug("Updated 'execute' to: {}", partialConfig.isExecute());
        if (partialConfig.getHybrid() != null) {
            if (existingConfig.getHybrid() == null) existingConfig.setHybrid(new HybridConfig());
            updateHybridConfig(existingConfig.getHybrid(), partialConfig.getHybrid());
            log.debug("Updated 'hybrid' section.");
        }
        if (partialConfig.getIcebergConversion() != null) {
            if (existingConfig.getIcebergConversion() == null)
                existingConfig.setIcebergConversion(new IcebergConversion());
            updateIcebergConversion(existingConfig.getIcebergConversion(), partialConfig.getIcebergConversion());
            log.debug("Updated 'icebergConversion' section.");
        }
        if (partialConfig.getMigrateACID() != null) {
            if (existingConfig.getMigrateACID() == null) existingConfig.setMigrateACID(new MigrateACID());
            updateMigrateACID(existingConfig.getMigrateACID(), partialConfig.getMigrateACID());
            log.debug("Updated 'migrateACID' section.");
        }
        if (partialConfig.getMigrateVIEW() != null) {
            if (existingConfig.getMigrateVIEW() == null) existingConfig.setMigrateVIEW(new MigrateVIEW());
            updateMigrateVIEW(existingConfig.getMigrateVIEW(), partialConfig.getMigrateVIEW());
            log.debug("Updated 'migrateVIEW' section.");
        }
        existingConfig.setMigrateNonNative(partialConfig.isMigrateNonNative());
        log.debug("Updated 'migrateNonNative' to: {}", partialConfig.isMigrateNonNative());
        if (partialConfig.getOptimization() != null) {
            if (existingConfig.getOptimization() == null) existingConfig.setOptimization(new Optimization());
            updateOptimization(existingConfig.getOptimization(), partialConfig.getOptimization());
            log.debug("Updated 'optimization' section.");
        }
        existingConfig.setEncryptedPasswords(partialConfig.isEncryptedPasswords());
        log.debug("Updated 'encryptedPasswords' to: {}", partialConfig.isEncryptedPasswords());
        existingConfig.setQuiet(partialConfig.isQuiet());
        log.debug("Updated 'quiet' to: {}", partialConfig.isQuiet());
        existingConfig.setReadOnly(partialConfig.isReadOnly());
        log.debug("Updated 'readOnly' to: {}", partialConfig.isReadOnly());
        existingConfig.setNoPurge(partialConfig.isNoPurge());
        log.debug("Updated 'noPurge' to: {}", partialConfig.isNoPurge());
        existingConfig.setReplace(partialConfig.isReplace());
        log.debug("Updated 'replace' to: {}", partialConfig.isReplace());
        existingConfig.setResetRight(partialConfig.isResetRight());
        log.debug("Updated 'resetRight' to: {}", partialConfig.isResetRight());
        existingConfig.setSaveWorkingTables(partialConfig.isSaveWorkingTables());
        log.debug("Updated 'saveWorkingTables' to: {}", partialConfig.isSaveWorkingTables());
        existingConfig.setSkipFeatures(partialConfig.isSkipFeatures());
        log.debug("Updated 'skipFeatures' to: {}", partialConfig.isSkipFeatures());
        existingConfig.setSkipLegacyTranslation(partialConfig.isSkipLegacyTranslation());
        log.debug("Updated 'skipLegacyTranslation' to: {}", partialConfig.isSkipLegacyTranslation());
        existingConfig.setSqlOutput(partialConfig.isSqlOutput());
        log.debug("Updated 'sqlOutput' to: {}", partialConfig.isSqlOutput());
        existingConfig.setSuppressCliWarnings(partialConfig.isSuppressCliWarnings());
        log.debug("Updated 'suppressCliWarnings' to: {}", partialConfig.isSuppressCliWarnings());
        existingConfig.setSync(partialConfig.isSync());
        existingConfig.setSkipLinkCheck(partialConfig.isSkipLinkCheck());
        log.debug("Updated 'sync' to: {}", partialConfig.isSync());
        if (partialConfig.getTransfer() != null) {
            if (existingConfig.getTransfer() == null) existingConfig.setTransfer(new TransferConfig());
            updateTransferConfig(existingConfig.getTransfer(), partialConfig.getTransfer());
            log.debug("Updated 'transfer' section.");
        }
        if (partialConfig.getOwnershipTransfer() != null) {
            if (existingConfig.getOwnershipTransfer() == null)
                existingConfig.setOwnershipTransfer(new TransferOwnership());
            updateTransferOwnership(existingConfig.getOwnershipTransfer(), partialConfig.getOwnershipTransfer());
            log.debug("Updated 'ownershipTransfer' section.");
        }
        if (partialConfig.getTranslator() != null) {
            if (existingConfig.getTranslator() == null) existingConfig.setTranslator(new Translator());
            updateTranslator(existingConfig.getTranslator(), partialConfig.getTranslator());
            log.debug("Updated 'translator' section.");
        }
        log.info("Main HmsMirror configuration updated successfully.");
        return existingConfig;
    }

    /*
     * Helper methods for updating nested DTOs (now using domain objects directly).
     * These methods apply the "don't override nulls" logic.
     */

    public void updateAcceptance(Acceptance existing, Acceptance partial) {
        log.debug("Updating Acceptance config.");
        // Direct assignment for primitive booleans
        existing.setSilentOverride(partial.isSilentOverride());
        existing.setBackedUpHDFS(partial.isBackedUpHDFS());
        existing.setBackedUpMetastore(partial.isBackedUpMetastore());
        existing.setTrashConfigured(partial.isTrashConfigured());
        existing.setPotentialDataLoss(partial.isPotentialDataLoss());
    }

    // This now handles the actual Map<Environment, Cluster> from HmsMirrorConfig
    public void updateClusters(Map<Environment, Cluster> existing, Map<Environment, Cluster> partial) {
        log.debug("Updating Clusters map.");
        if (partial.containsKey(Environment.LEFT) && partial.get(Environment.LEFT) != null) {
            Cluster leftPartial = partial.get(Environment.LEFT);
            Cluster existingLeft = existing.computeIfAbsent(Environment.LEFT, k -> new Cluster());
            updateCluster(existingLeft, leftPartial);
            log.debug("Updated 'LEFT' cluster config.");
        }
        if (partial.containsKey(Environment.RIGHT) && partial.get(Environment.RIGHT) != null) {
            Cluster rightPartial = partial.get(Environment.RIGHT);
            Cluster existingRight = existing.computeIfAbsent(Environment.RIGHT, k -> new Cluster());
            updateCluster(existingRight, rightPartial);
            log.debug("Updated 'RIGHT' cluster config.");
        }
    }

    /**
     * Updates a specific Cluster object within the configuration.
     * This method handles null-safe updates for its nested properties.
     *
     * @param existing The existing Cluster object to update.
     * @param partial  The partial Cluster object containing updates.
     */
    public void updateCluster(Cluster existing, Cluster partial) {
        log.debug("Updating Cluster config ({}).", partial.getEnvironment() != null ? partial.getEnvironment().toString() : "unknown");
        if (partial.getEnvironment() != null) existing.setEnvironment(partial.getEnvironment());
        // Direct assignment for primitive booleans
        existing.setCreateIfNotExists(partial.isCreateIfNotExists());
        if (partial.getPlatformType() != null) existing.setPlatformType(partial.getPlatformType());
        if (partial.getHcfsNamespace() != null) existing.setHcfsNamespace(partial.getHcfsNamespace());

        if (partial.getHiveServer2() != null) {
            if (existing.getHiveServer2() == null) existing.setHiveServer2(new HiveServer2Config());
            updateHiveServer2Config(existing.getHiveServer2(), partial.getHiveServer2());
            log.debug("Updated 'hiveServer2' for cluster.");
        }
        if (partial.getPartitionDiscovery() != null) {
            if (existing.getPartitionDiscovery() == null) existing.setPartitionDiscovery(new PartitionDiscovery());
            updatePartitionDiscovery(existing.getPartitionDiscovery(), partial.getPartitionDiscovery());
        }
        // Direct assignment for primitive booleans
        existing.setEnableAutoTableStats(partial.isEnableAutoTableStats());
        existing.setEnableAutoColumnStats(partial.isEnableAutoColumnStats());

        if (partial.getMetastoreDirect() != null) {
            if (existing.getMetastoreDirect() == null) existing.setMetastoreDirect(new DBStore());
            updateMetastoreDirect(existing.getMetastoreDirect(), partial.getMetastoreDirect());
            log.debug("Updated 'metastoreDirect' for cluster.");
        }
        // These are deprecated fields in the domain object, so we still ensure they are updated if provided.
        existing.setLegacyHive(partial.isLegacyHive());
        existing.setHdpHive3(partial.isHdpHive3());
    }

    /**
     * Retrieves a specific Cluster object by its environment.
     *
     * @param environment The environment (LEFT or RIGHT) of the cluster to retrieve.
     * @return The Cluster object, or null if it does not exist.
     */
    public Cluster getCluster(Environment environment) {
        log.debug("Retrieving cluster for environment: {}", environment);
        // Ensure the clusters map is not null before attempting to get from it
        if (currentConfig.get().getClusters() == null) {
            log.warn("Clusters map is null in configuration. Returning null for environment: {}", environment);
            return null;
        }
        return currentConfig.get().getClusters().get(environment);
    }

    /**
     * Updates a specific Cluster object in the configuration.
     * If the cluster for the given environment doesn't exist, it will be created.
     *
     * @param environment    The environment (LEFT or RIGHT) of the cluster to update.
     * @param partialCluster The partial Cluster object containing updates.
     */
    public void updateSpecificCluster(Environment environment, Cluster partialCluster) {
        log.info("Updating specific cluster for environment: {} with data: {}", environment, partialCluster);
        // Ensure the clusters map exists
        if (currentConfig.get().getClusters() == null) {
            currentConfig.get().setClusters(new TreeMap<>());
            log.debug("Clusters map was null, initialized for specific cluster update.");
        }

        Cluster existingCluster = currentConfig.get().getClusters().computeIfAbsent(environment, k -> {
            Cluster newCluster = new Cluster();
            newCluster.setEnvironment(environment);
            return newCluster;
        });

        updateCluster(existingCluster, partialCluster);
        log.info("Specific cluster for environment {} updated successfully.", environment);
    }


    public void updateHiveServer2Config(HiveServer2Config existing, HiveServer2Config partial) {
        log.debug("Updating HiveServer2Config.");
        if (partial.getUri() != null) existing.setUri(partial.getUri());
        // Direct assignment for primitive booleans
        existing.setDisconnected(partial.isDisconnected());
        // Handle Properties
        if (partial.getConnectionProperties() != null) {
            if (existing.getConnectionProperties() == null)
                existing.setConnectionProperties(new java.util.Properties());
            partial.getConnectionProperties().forEach((k, v) -> {
                // Properties map values could be null, don't update if null
                if (v != null) existing.getConnectionProperties().setProperty(k.toString(), v.toString());
                else log.debug("Skipping null value for hiveServer2 connection property key '{}'.", k);
            });
        }
        if (partial.getDriverClassName() != null) existing.setDriverClassName(partial.getDriverClassName());
        if (partial.getJarFile() != null) existing.setJarFile(partial.getJarFile());
        if (partial.getVersion() != null) existing.setVersion(partial.getVersion());
    }

    public void updateMetastoreDirect(DBStore existing, DBStore partial) {
        log.debug("Updating MetastoreDirect (DBStore).");
        if (partial.getUri() != null) existing.setUri(partial.getUri());
        if (partial.getType() != null) existing.setType(partial.getType());
        if (partial.getInitSql() != null) existing.setInitSql(partial.getInitSql());
        if (partial.getResource() != null) existing.setResource(partial.getResource());
        if (partial.getVersion() != null) existing.setVersion(partial.getVersion());
        // Handle Properties
        if (partial.getConnectionProperties() != null) {
            if (existing.getConnectionProperties() == null)
                existing.setConnectionProperties(new java.util.Properties());
            partial.getConnectionProperties().forEach((k, v) -> {
                // Properties map values could be null, don't update if null
                if (v != null) existing.getConnectionProperties().setProperty(k.toString(), v.toString());
                else log.debug("Skipping null value for metastoreDirect connection property key '{}'.", k);
            });
        }
        if (partial.getConnectionPool() != null) {
            if (existing.getConnectionPool() == null) existing.setConnectionPool(new ConnectionPool());
            updateConnectionPool(existing.getConnectionPool(), partial.getConnectionPool());
        }
    }

    public void updateConnectionPool(ConnectionPool existing, ConnectionPool partial) {
        log.debug("Updating ConnectionPool.");
        existing.setMin(partial.getMin());
        existing.setMax(partial.getMax());
        existing.setTimeout(partial.getTimeout());
    }


    public void updatePartitionDiscovery(PartitionDiscovery existing, PartitionDiscovery partial) {
        log.debug("Updating PartitionDiscovery config.");
        // Direct assignment for primitive booleans
        existing.setAuto(partial.isAuto());
        existing.setInitMSCK(partial.isInitMSCK());
    }

    public void updateFilter(Filter existing, Filter partial) {
        log.debug("Updating Filter config.");
        if (partial.getDbRegEx() != null) existing.setDbRegEx(partial.getDbRegEx());
        if (partial.getTblExcludeRegEx() != null) existing.setTblExcludeRegEx(partial.getTblExcludeRegEx());
        if (partial.getTblRegEx() != null) existing.setTblRegEx(partial.getTblRegEx());
        if (partial.getTblSizeLimit() != null) existing.setTblSizeLimit(partial.getTblSizeLimit());
        if (partial.getTblPartitionLimit() != null) existing.setTblPartitionLimit(partial.getTblPartitionLimit());
        // dbPropertySkipList is managed by separate list methods
        // Direct assignment for primitive booleans
    }

    public void updateLegacyTranslations(LegacyTranslations existing, LegacyTranslations partial) {
        log.debug("Updating LegacyTranslations config.");
        if (partial.getRowSerde() != null) {
            if (existing.getRowSerde() == null) existing.setRowSerde(new HashMap<>());
            partial.getRowSerde().forEach((k, v) -> {
                if (v != null) existing.getRowSerde().put(k, v);
                else log.debug("Skipping null value for rowSerde key '{}'.", k);
            });
        }
    }

    public void updateHybridConfig(HybridConfig existing, HybridConfig partial) {
        log.debug("Updating HybridConfig.");
        existing.setExportImportPartitionLimit(partial.getExportImportPartitionLimit());
        existing.setSqlPartitionLimit(partial.getSqlPartitionLimit());
        existing.setSqlSizeLimit(partial.getSqlSizeLimit());
    }

    public void updateIcebergConversion(IcebergConversion existing, IcebergConversion partial) {
        log.debug("Updating IcebergConversion config.");
        // Direct assignment for primitive booleans
        existing.setEnable(partial.isEnable());
        if (partial.getFileTypeTranslation() != null) existing.setFileTypeTranslation(partial.getFileTypeTranslation());
        existing.setVersion(partial.getVersion());
        if (partial.getTableProperties() != null) {
            if (existing.getTableProperties() == null) existing.setTableProperties(new HashMap<>());
            partial.getTableProperties().forEach((k, v) -> {
                if (v != null) existing.getTableProperties().put(k, v);
                else log.debug("Skipping null value for tableProperties key '{}'.", k);
            });
        }
        // Direct assignment for primitive booleans
        existing.setInplace(partial.isInplace());
    }

    public void updateMigrateACID(MigrateACID existing, MigrateACID partial) {
        log.debug("Updating MigrateACID config.");
        // Direct assignment for primitive booleans
        existing.setOn(partial.isOn());
        existing.setOnly(partial.isOnly());
        if (partial.getArtificialBucketThreshold() != null)
            existing.setArtificialBucketThreshold(partial.getArtificialBucketThreshold());
        if (partial.getPartitionLimit() != null) existing.setPartitionLimit(partial.getPartitionLimit());
        // Direct assignment for primitive booleans
        existing.setDowngrade(partial.isDowngrade());
        existing.setInplace(partial.isInplace());
    }

    public void updateMigrateVIEW(MigrateVIEW existing, MigrateVIEW partial) {
        log.debug("Updating MigrateVIEW config.");
        // Direct assignment for primitive booleans
        existing.setOn(partial.isOn());
    }

    public void updateOptimization(Optimization existing, Optimization partial) {
        log.debug("Updating Optimization config.");
        // Direct assignment for primitive booleans
        existing.setSortDynamicPartitionInserts(partial.isSortDynamicPartitionInserts());
        existing.setSkip(partial.isSkip());
        existing.setAutoTune(partial.isAutoTune());
        existing.setCompressTextOutput(partial.isCompressTextOutput());
        existing.setSkipStatsCollection(partial.isSkipStatsCollection());
        if (partial.getOverrides() != null) {
            if (existing.getOverrides() == null) existing.setOverrides(new Overrides());
            updateOverrides(existing.getOverrides(), partial.getOverrides());
        }
        // Direct assignment for primitive booleans
        existing.setBuildShadowStatistics(partial.isBuildShadowStatistics());
    }

    public void updateOverrides(Overrides existing, Overrides partial) {
        log.debug("Updating Overrides config.");
        if (partial.getProperties() != null) {
            if (existing.getProperties() == null) existing.setProperties(new TreeMap<>());
            // Overrides properties is Map<String, Map<SideType, String>>. Needs deep merge.
            partial.getProperties().forEach((key, sideMap) -> {
                if (sideMap != null) {
                    Map<SideType, String> existingSideMap = existing.getProperties().computeIfAbsent(key, k -> new TreeMap<>());
                    sideMap.forEach((sideType, value) -> {
                        if (value != null) existingSideMap.put(sideType, value);
                        else if (existingSideMap.containsKey(sideType))
                            existingSideMap.remove(sideType); // Remove if value is null
                    });
                    if (existingSideMap.isEmpty())
                        existing.getProperties().remove(key); // Remove outer key if inner map is empty
                } else if (existing.getProperties().containsKey(key)) {
                    existing.getProperties().remove(key); // Remove key entirely if partial has null for it
                }
            });
        }
    }

    public void updateTransferConfig(TransferConfig existing, TransferConfig partial) {
        log.debug("Updating TransferConfig.");
        if (partial.getTransferPrefix() != null) existing.setTransferPrefix(partial.getTransferPrefix());
        if (partial.getShadowPrefix() != null) existing.setShadowPrefix(partial.getShadowPrefix());
        if (partial.getStorageMigrationPostfix() != null)
            existing.setStorageMigrationPostfix(partial.getStorageMigrationPostfix());
        if (partial.getExportBaseDirPrefix() != null) existing.setExportBaseDirPrefix(partial.getExportBaseDirPrefix());
        if (partial.getRemoteWorkingDirectory() != null)
            existing.setRemoteWorkingDirectory(partial.getRemoteWorkingDirectory());
        if (partial.getIntermediateStorage() != null) existing.setIntermediateStorage(partial.getIntermediateStorage());
        if (partial.getTargetNamespace() != null) existing.setTargetNamespace(partial.getTargetNamespace());
        if (partial.getStorageMigration() != null) {
            if (existing.getStorageMigration() == null) existing.setStorageMigration(new StorageMigration());
            updateStorageMigration(existing.getStorageMigration(), partial.getStorageMigration());
        }
        if (partial.getWarehouse() != null) {
            if (existing.getWarehouse() == null) existing.setWarehouse(new Warehouse());
            updateWarehouse(existing.getWarehouse(), partial.getWarehouse());
        }
    }

    public void updateStorageMigration(StorageMigration existing, StorageMigration partial) {
        log.debug("Updating StorageMigration config.");
        if (partial.getTranslationType() != null) existing.setTranslationType(partial.getTranslationType());
        if (partial.getDataMovementStrategy() != null)
            existing.setDataMovementStrategy(partial.getDataMovementStrategy());
        if (partial.getDataFlow() != null) existing.setDataFlow(partial.getDataFlow());
        // Direct assignment for primitive booleans
        existing.setSkipDatabaseLocationAdjustments(partial.isSkipDatabaseLocationAdjustments());
        existing.setCreateArchive(partial.isCreateArchive());
        existing.setConsolidateTablesForDistcp(partial.isConsolidateTablesForDistcp());
        existing.setStrict(partial.isStrict());
    }

    public void updateWarehouse(Warehouse existing, Warehouse partial) {
        log.debug("Updating Warehouse config.");
        if (partial.getSource() != null) existing.setSource(partial.getSource());
        if (partial.getExternalDirectory() != null) existing.setExternalDirectory(partial.getExternalDirectory());
        if (partial.getManagedDirectory() != null) existing.setManagedDirectory(partial.getManagedDirectory());
    }

    public void updateTransferOwnership(TransferOwnership existing, TransferOwnership partial) {
        log.debug("Updating TransferOwnership config.");
        // Direct assignment for primitive booleans
        existing.setDatabase(partial.isDatabase());
        existing.setTable(partial.isTable());
    }

    public void updateTranslator(Translator existing, Translator partial) {
        log.debug("Updating Translator config.");
        // Direct assignment for primitive booleans
        existing.setForceExternalLocation(partial.isForceExternalLocation());
        if (partial.getAutoGlobalLocationMap() != null)
            existing.setAutoGlobalLocationMap(partial.getAutoGlobalLocationMap());
        if (partial.getUserGlobalLocationMap() != null) {
            if (existing.getUserGlobalLocationMap() == null) existing.setUserGlobalLocationMap(new HashMap<>());
            partial.getUserGlobalLocationMap().forEach((key, tableTypeMap) -> {
                if (tableTypeMap != null) {
                    Map<TableType, String> existingTableTypeMap = existing.getUserGlobalLocationMap().computeIfAbsent(key, k -> new HashMap<>());
                    tableTypeMap.forEach((tableType, location) -> {
                        if (location != null) existingTableTypeMap.put(tableType, location);
                        else if (existingTableTypeMap.containsKey(tableType)) existingTableTypeMap.remove(tableType);
                    });
                    if (existingTableTypeMap.isEmpty()) existing.getUserGlobalLocationMap().remove(key);
                } else if (existing.getUserGlobalLocationMap().containsKey(key)) {
                    existing.getUserGlobalLocationMap().remove(key);
                }
            });
            existing.rebuildOrderedGlobalLocationMap();
        }
        if (partial.getWarehouseMapBuilder() != null) {
            if (existing.getWarehouseMapBuilder() == null) existing.setWarehouseMapBuilder(new WarehouseMapBuilder());
            updateWarehouseMapBuilder(existing.getWarehouseMapBuilder(), partial.getWarehouseMapBuilder());
        }
    }

    public void updateWarehouseMapBuilder(WarehouseMapBuilder existing, WarehouseMapBuilder partial) {
        log.debug("Updating WarehouseMapBuilder config.");
        if (partial.getSources() != null) {
            if (existing.getSources() == null) existing.setSources(new HashMap<>());
            partial.getSources().forEach((dbName, sourceLocationMap) -> {
                if (sourceLocationMap != null) {
                    SourceLocationMap existingSourceLocationMap = existing.getSources().computeIfAbsent(dbName, k -> new SourceLocationMap());
                    if (sourceLocationMap.getLocations() != null) {
                        if (existingSourceLocationMap.getLocations() == null)
                            existingSourceLocationMap.setLocations(new HashMap<>());
                        sourceLocationMap.getLocations().forEach((tableType, locationMap) -> {
                            if (locationMap != null) {
                                Map<String, Set<String>> existingLocationMap = existingSourceLocationMap.getLocations().computeIfAbsent(tableType, k -> new HashMap<>());
                                locationMap.forEach((location, tables) -> {
                                    if (tables != null) {
                                        Set<String> existingTables = existingLocationMap.computeIfAbsent(location, k -> new HashSet<>());
                                        existingTables.addAll(tables);
                                    } else {
                                        existingLocationMap.remove(location);
                                    }
                                });
                                if (existingLocationMap.isEmpty())
                                    existingSourceLocationMap.getLocations().remove(tableType);
                            } else if (existingSourceLocationMap.getLocations().containsKey(tableType)) {
                                existingSourceLocationMap.getLocations().remove(tableType);
                            }
                        });
                    }
                } else if (existing.getSources().containsKey(dbName)) {
                    existing.getSources().remove(dbName);
                }
            });
        }
        // Direct assignment for primitive booleans
        existing.setInSync(partial.isInSync());
        if (partial.getWarehousePlans() != null) {
            if (existing.getWarehousePlans() == null) existing.setWarehousePlans(new HashMap<>());
            partial.getWarehousePlans().forEach((k, v) -> {
                if (v != null) existing.getWarehousePlans().put(k, v);
                else log.debug("Skipping null value for warehouseMapBuilder plan key '{}'.", k);
            });
        }
    }

    /*
     * List/Set management methods for 'databases'
     */

    public Set<String> getDatabases() {
        log.debug("Retrieving 'databases' set.");
        return new HashSet<>(currentConfig.get().getDatabases());
    }

    public void clearDatabases() {
        log.info("Clearing all entries from 'databases' set.");
        currentConfig.get().getDatabases().clear();
    }

    public boolean addDatabase(String databaseName) {
        log.info("Attempting to add database: {}", databaseName);
        boolean added = currentConfig.get().getDatabases().add(databaseName);
        if (added) {
            log.info("Database '{}' added successfully: {}", databaseName, added);
        } else {
            log.warn("Database '{}' already exists in the set. Not added.", databaseName);
        }
        return added;
    }

    public boolean deleteDatabase(String databaseName) {
        log.info("Attempting to delete database: {}", databaseName);
        boolean removed = currentConfig.get().getDatabases().remove(databaseName);
        if (removed) {
            log.info("Database '{}' deleted successfully.", databaseName);
        } else {
            log.warn("Database '{}' not found in the set. No deletion performed.", databaseName);
        }
        return removed;
    }

    /*
     * List management methods for 'filter.dbPropertySkipList'
     */

    public List<String> getDbPropertySkipList() {
        log.debug("Retrieving 'filter.dbPropertySkipList'.");
        return new ArrayList<>(currentConfig.get().getFilter().getDbPropertySkipList());
    }

    public void clearDbPropertySkipList() {
        log.info("Clearing all entries from 'filter.dbPropertySkipList'.");
        currentConfig.get().getFilter().getDbPropertySkipList().clear();
    }

    public boolean addDbPropertySkipListItem(String item) {
        log.info("Attempting to add item to 'filter.dbPropertySkipList': {}", item);
        if (!currentConfig.get().getFilter().getDbPropertySkipList().contains(item)) {
            boolean added = currentConfig.get().getFilter().getDbPropertySkipList().add(item);
            log.info("Item '{}' added to 'filter.dbPropertySkipList': {}", item, added);
            return added;
        }
        log.warn("Item '{}' already exists in 'filter.dbPropertySkipList'. Not added.", item);
        return false;
    }

    public boolean deleteDbPropertySkipListItem(String item) {
        log.info("Attempting to delete item from 'filter.dbPropertySkipList': {}", item);
        boolean removed = currentConfig.get().getFilter().getDbPropertySkipList().remove(item);
        if (removed) {
            log.info("Item '{}' deleted from 'filter.dbPropertySkipList'.", item);
        } else {
            log.warn("Item '{}' not found in 'filter.dbPropertySkipList'. No deletion performed.", item);
        }
        return removed;
    }

    /*
     * Map management methods for 'legacyTranslations.rowSerde'
     */
    public Map<String, String> getRowSerdeTranslations() {
        log.debug("Retrieving 'legacyTranslations.rowSerde' map.");
        return new HashMap<>(currentConfig.get().getLegacyTranslations().getRowSerde());
    }

    public void addRowSerdeTranslation(String key, String value) {
        log.info("Adding/Updating rowSerde translation: key='{}', value='{}'", key, value);
        currentConfig.get().getLegacyTranslations().getRowSerde().put(key, value);
    }

    public void deleteRowSerdeTranslation(String key) {
        log.info("Attempting to delete rowSerde translation for key: '{}'", key);
        if (currentConfig.get().getLegacyTranslations().getRowSerde().remove(key) != null) {
            log.info("RowSerde translation for key '{}' deleted.", key);
        } else {
            log.warn("RowSerde translation for key '{}' not found. No deletion performed.", key);
        }
    }

    public void clearRowSerdeTranslations() {
        log.info("Clearing all entries from 'legacyTranslations.rowSerde'.");
        currentConfig.get().getLegacyTranslations().getRowSerde().clear();
    }

    /*
     * Map management methods for 'icebergConversion.tableProperties'
     */
    public Map<String, String> getIcebergTableProperties() {
        log.debug("Retrieving 'icebergConversion.tableProperties' map.");
        return new HashMap<>(currentConfig.get().getIcebergConversion().getTableProperties());
    }

    public void addIcebergTableProperty(String key, String value) {
        log.info("Adding/Updating Iceberg table property: key='{}', value='{}'", key, value);
        currentConfig.get().getIcebergConversion().getTableProperties().put(key, value);
    }

    public void deleteIcebergTableProperty(String key) {
        log.info("Attempting to delete Iceberg table property for key: '{}'", key);
        if (currentConfig.get().getIcebergConversion().getTableProperties().remove(key) != null) {
            log.info("Iceberg table property for key '{}' deleted.", key);
        } else {
            log.warn("Iceberg table property for key '{}' not found. No deletion performed.", key);
        }
    }

    public void clearIcebergTableProperties() {
        log.info("Clearing all entries from 'icebergConversion.tableProperties'.");
        currentConfig.get().getIcebergConversion().getTableProperties().clear();
    }

    /*
     * Map management methods for 'optimization.overrides.properties'
     * This is a Map<String, Map<SideType, String>>
     */
    public Map<String, Map<SideType, String>> getOptimizationOverrideProperties() {
        log.debug("Retrieving 'optimization.overrides.properties' map.");
        // Return a deep copy
        Map<String, Map<SideType, String>> deepCopy = new HashMap<>();
        currentConfig.get().getOptimization().getOverrides().getProperties().forEach((key, innerMap) -> {
            deepCopy.put(key, new HashMap<>(innerMap));
        });
        return deepCopy;
    }

    public void addOptimizationOverrideProperty(String key, SideType sideType, String value) {
        log.info("Adding/Updating optimization override property: key='{}', side='{}', value='{}'", key, sideType, value);
        Map<String, Map<SideType, String>> properties = currentConfig.get().getOptimization().getOverrides().getProperties();
        properties.computeIfAbsent(key, k -> new TreeMap<>()).put(sideType, value); // Use TreeMap for consistent ordering
    }

    public void deleteOptimizationOverrideProperty(String key, SideType sideType) {
        log.info("Attempting to delete optimization override property for key: '{}', side: '{}'", key, sideType);
        Map<String, Map<SideType, String>> properties = currentConfig.get().getOptimization().getOverrides().getProperties();
        if (properties.containsKey(key)) {
            Map<SideType, String> innerMap = properties.get(key);
            if (innerMap.remove(sideType) != null) {
                log.info("Optimization override property for key '{}', side '{}' deleted.", key, sideType);
                if (innerMap.isEmpty()) {
                    properties.remove(key); // Remove outer key if inner map is empty
                }
            } else {
                log.warn("Optimization override property for key '{}', side '{}' not found. No deletion performed.", key, sideType);
            }
        } else {
            log.warn("Optimization override property key '{}' not found. No deletion performed.", key);
        }
    }

    public void clearOptimizationOverrideProperties() {
        log.info("Clearing all entries from 'optimization.overrides.properties'.");
        currentConfig.get().getOptimization().getOverrides().getProperties().clear();
    }


    /*
     * Map management methods for 'translator.warehouseMapBuilder.sources'
     */
    // This is a Map<String, SourceLocationMap>
    public Map<String, SourceLocationMap> getTranslatorWarehouseSources() {
        log.debug("Retrieving 'translator.warehouseMapBuilder.sources' map.");
        // Return a deep copy
        Map<String, SourceLocationMap> deepCopy = new HashMap<>();
        currentConfig.get().getTranslator().getWarehouseMapBuilder().getSources().forEach((key, sourceMap) -> {
            try {
                deepCopy.put(key, (SourceLocationMap) sourceMap.clone());
            } catch (CloneNotSupportedException e) {
                log.error("Error cloning SourceLocationMap for key {}: {}", key, e.getMessage());
                // Fallback to shallow copy or re-throw as unchecked
                deepCopy.put(key, sourceMap);
            }
        });
        return deepCopy;
    }

    // This method needs to handle deep updates or replacement
    public void addTranslatorWarehouseSource(String dbName, SourceLocationMap sourceLocationMap) {
        log.info("Adding/Updating translator warehouse source for database: {}", dbName);
        currentConfig.get().getTranslator().getWarehouseMapBuilder().getSources().put(dbName, sourceLocationMap);
    }

    public void deleteTranslatorWarehouseSource(String dbName) {
        log.info("Attempting to delete translator warehouse source for database: '{}'", dbName);
        if (currentConfig.get().getTranslator().getWarehouseMapBuilder().getSources().remove(dbName) != null) {
            log.info("Translator warehouse source for database '{}' deleted.", dbName);
        } else {
            log.warn("Translator warehouse source for database '{}' not found. No deletion performed.", dbName);
        }
    }

    public void clearTranslatorWarehouseSources() {
        log.info("Clearing all entries from 'translator.warehouseMapBuilder.sources'.");
        currentConfig.get().getTranslator().getWarehouseMapBuilder().getSources().clear();
    }

    /*
     * Map management methods for 'translator.warehouseMapBuilder.warehousePlans'
     */
    // This is Map<String, Warehouse>
    public Map<String, Warehouse> getTranslatorWarehousePlans() {
        log.debug("Retrieving 'translator.warehouseMapBuilder.warehousePlans' map.");
        // Return a deep copy
        Map<String, Warehouse> deepCopy = new HashMap<>();
        currentConfig.get().getTranslator().getWarehouseMapBuilder().getWarehousePlans().forEach((key, warehouse) -> {
            try {
                deepCopy.put(key, (Warehouse) warehouse.clone());
            } catch (CloneNotSupportedException e) {
                log.error("Error cloning Warehouse object for key {}: {}", key, e.getMessage());
                // Fallback to shallow copy or re-throw as unchecked
                deepCopy.put(key, warehouse);
            }
        });
        return deepCopy;
    }

    public void addTranslatorWarehousePlan(String key, Warehouse value) {
        log.info("Adding/Updating translator warehouse plan: key='{}', value='{}'", key, value);
        currentConfig.get().getTranslator().getWarehouseMapBuilder().getWarehousePlans().put(key, value);
    }

    public void deleteTranslatorWarehousePlan(String key) {
        log.info("Attempting to delete translator warehouse plan for key: '{}'", key);
        if (currentConfig.get().getTranslator().getWarehouseMapBuilder().getWarehousePlans().remove(key) != null) {
            log.info("Translator warehouse plan for key '{}' deleted.", key);
        } else {
            log.warn("Translator warehouse plan for key '{}' not found. No deletion performed.", key);
        }
    }

    public void clearTranslatorWarehousePlans() {
        log.info("Clearing all entries from 'translator.warehouseMapBuilder.warehousePlans'.");
        currentConfig.get().getTranslator().getWarehouseMapBuilder().getWarehousePlans().clear();
    }

    // New methods for HiveServer2 connection properties (LEFT/RIGHT)
    public java.util.Properties getHiveServer2ConnectionProperties(String clusterName) {
        log.debug("Retrieving HiveServer2 connection properties for cluster: {}", clusterName);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getHiveServer2() != null && cluster.getHiveServer2().getConnectionProperties() != null) {
            return (java.util.Properties) cluster.getHiveServer2().getConnectionProperties().clone();
        }
        log.warn("HiveServer2 connection properties not found for cluster: {}", clusterName);
        return new java.util.Properties();
    }

    public void addHiveServer2ConnectionProperty(String clusterName, String key, String value) {
        log.info("Adding/Updating HiveServer2 connection property for cluster '{}': key='{}', value='{}'", clusterName, key, value);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getHiveServer2() != null) {
            if (cluster.getHiveServer2().getConnectionProperties() == null) {
                cluster.getHiveServer2().setConnectionProperties(new java.util.Properties());
            }
            cluster.getHiveServer2().getConnectionProperties().setProperty(key, value);
        } else {
            log.warn("Failed to add HiveServer2 connection property. Cluster '{}' or its HiveServer2 configuration not found.", clusterName);
        }
    }

    public void deleteHiveServer2ConnectionProperty(String clusterName, String key) {
        log.info("Attempting to delete HiveServer2 connection property for cluster '{}', key: '{}'", clusterName, key);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getHiveServer2() != null && cluster.getHiveServer2().getConnectionProperties() != null) {
            if (cluster.getHiveServer2().getConnectionProperties().remove(key) != null) {
                log.info("HiveServer2 connection property for cluster '{}', key '{}' deleted.", clusterName, key);
            } else {
                log.warn("HiveServer2 connection property for cluster '{}', key '{}' not found. No deletion performed.", clusterName, key);
            }
        } else {
            log.warn("HiveServer2 connection properties not found for cluster: {}. No deletion performed.", clusterName);
        }
    }

    public void clearHiveServer2ConnectionProperties(String clusterName) {
        log.info("Clearing all HiveServer2 connection properties for cluster: {}", clusterName);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getHiveServer2() != null && cluster.getHiveServer2().getConnectionProperties() != null) {
            cluster.getHiveServer2().getConnectionProperties().clear();
        } else {
            log.warn("Failed to clear HiveServer2 connection properties. Cluster '{}' or its HiveServer2 configuration not found.", clusterName);
        }
    }

    // New methods for MetastoreDirect connection properties (LEFT only based on config.yaml)
    public java.util.Properties getMetastoreDirectConnectionProperties(String clusterName) {
        log.debug("Retrieving MetastoreDirect connection properties for cluster: {}", clusterName);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getMetastoreDirect() != null && cluster.getMetastoreDirect().getConnectionProperties() != null) {
            return (java.util.Properties) cluster.getMetastoreDirect().getConnectionProperties().clone();
        }
        log.warn("MetastoreDirect connection properties not found for cluster: {}", clusterName);
        return new java.util.Properties();
    }

    public void addMetastoreDirectConnectionProperty(String clusterName, String key, String value) {
        log.info("Adding/Updating MetastoreDirect connection property for cluster '{}': key='{}', value='{}'", clusterName, key, value);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getMetastoreDirect() != null) {
            if (cluster.getMetastoreDirect().getConnectionProperties() == null) {
                cluster.getMetastoreDirect().setConnectionProperties(new java.util.Properties());
            }
            cluster.getMetastoreDirect().getConnectionProperties().setProperty(key, value);
        } else {
            log.warn("Failed to add MetastoreDirect connection property. Cluster '{}' or its MetastoreDirect configuration not found.", clusterName);
        }
    }

    public void deleteMetastoreDirectConnectionProperty(String clusterName, String key) {
        log.info("Attempting to delete MetastoreDirect connection property for cluster '{}', key: '{}'", clusterName, key);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getMetastoreDirect() != null && cluster.getMetastoreDirect().getConnectionProperties() != null) {
            if (cluster.getMetastoreDirect().getConnectionProperties().remove(key) != null) {
                log.info("MetastoreDirect connection property for cluster '{}', key '{}' deleted.", clusterName, key);
            } else {
                log.warn("MetastoreDirect connection property for cluster '{}', key '{}' not found. No deletion performed.", clusterName, key);
            }
        } else {
            log.warn("MetastoreDirect connection properties not found for cluster: {}. No deletion performed.", clusterName);
        }
    }

    public void clearMetastoreDirectConnectionProperties(String clusterName) {
        log.info("Clearing all MetastoreDirect connection properties for cluster: {}", clusterName);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getMetastoreDirect() != null && cluster.getMetastoreDirect().getConnectionProperties() != null) {
            cluster.getMetastoreDirect().getConnectionProperties().clear();
        } else {
            log.warn("Failed to clear MetastoreDirect connection properties. Cluster '{}' or its MetastoreDirect configuration not found.", clusterName);
        }
    }

    /**
     * Helper to retrieve a cluster by name (LEFT or RIGHT).
     *
     * @param clusterName The name of the cluster (e.g., "LEFT", "RIGHT").
     * @return The Cluster object, or null if not found.
     */
    private Cluster getClusterByName(String clusterName) {
        if (currentConfig.get().getClusters() == null) {
            return null;
        }
        try {
            Environment env = Environment.valueOf(clusterName.toUpperCase());
            return currentConfig.get().getClusters().get(env);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid cluster name provided: {}", clusterName);
            return null;
        }
    }

    /**
     * Retrieves the PartitionDiscovery configuration for a specific cluster.
     *
     * @param clusterName The name of the cluster (e.g., "LEFT", "RIGHT").
     * @return The PartitionDiscovery object, or null if not found.
     */
    public PartitionDiscovery getPartitionDiscovery(String clusterName) {
        log.debug("Retrieving PartitionDiscovery for cluster: {}", clusterName);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null && cluster.getPartitionDiscovery() != null) {
            return cluster.getPartitionDiscovery();
        }
        log.warn("PartitionDiscovery config not found for cluster: {}", clusterName);
        return null;
    }

    /**
     * Updates the PartitionDiscovery configuration for a specific cluster.
     *
     * @param clusterName               The name of the cluster (e.g., "LEFT", "RIGHT").
     * @param partialPartitionDiscovery The partial PartitionDiscovery object containing updates.
     */
    public void updatePartitionDiscoveryForCluster(String clusterName, PartitionDiscovery partialPartitionDiscovery) {
        log.info("Updating PartitionDiscovery for cluster: {} with data: {}", clusterName, partialPartitionDiscovery);
        Cluster cluster = getClusterByName(clusterName);
        if (cluster != null) {
            if (cluster.getPartitionDiscovery() == null) {
                cluster.setPartitionDiscovery(new PartitionDiscovery());
                log.debug("PartitionDiscovery object was null for cluster {}, initialized a new one.", clusterName);
            }
            updatePartitionDiscovery(cluster.getPartitionDiscovery(), partialPartitionDiscovery);
            log.info("PartitionDiscovery for cluster {} updated successfully.", clusterName);
        } else {
            log.warn("Cluster '{}' not found for PartitionDiscovery update.", clusterName);
        }
    }

    /**
     * Retrieves the userGlobalLocationMap from Translator.
     *
     * @return A map of source paths to TableType to target location mappings.
     */
    public Map<String, Map<TableType, String>> getTranslatorUserGlobalLocationMap() {
        log.debug("Retrieving Translator's userGlobalLocationMap.");
        if (currentConfig.get().getTranslator() != null && currentConfig.get().getTranslator().getUserGlobalLocationMap() != null) {
            // Return a deep copy to prevent direct modification of the internal map
            Map<String, Map<TableType, String>> deepCopy = new HashMap<>();
            currentConfig.get().getTranslator().getUserGlobalLocationMap().forEach((key, innerMap) -> {
                deepCopy.put(key, new HashMap<>(innerMap));
            });
            return deepCopy;
        }
        log.warn("Translator or userGlobalLocationMap not found. Returning empty map.");
        return new HashMap<>();
    }

    /**
     * Adds or updates an entry in the userGlobalLocationMap.
     *
     * @param from      The source path (key).
     * @param tableType The table type.
     * @param to        The target location (value).
     */
    public void addTranslatorUserGlobalLocationMapEntry(String from, TableType tableType, String to) {
        log.info("Adding/Updating Translator userGlobalLocationMap entry: from='{}', tableType='{}', to='{}'", from, tableType, to);
        if (currentConfig.get().getTranslator() != null) {
            if (currentConfig.get().getTranslator().getUserGlobalLocationMap() == null) {
                currentConfig.get().getTranslator().setUserGlobalLocationMap(new HashMap<>());
            }
            currentConfig.get().getTranslator().addUserGlobalLocationMap(tableType, from, to);
            currentConfig.get().getTranslator().rebuildOrderedGlobalLocationMap(); // Rebuild ordered map
            log.info("Translator userGlobalLocationMap entry added/updated successfully.");
        } else {
            log.warn("Translator object not found. Cannot add/update userGlobalLocationMap entry.");
        }
    }

    /**
     * Deletes an entry (or a specific TableType mapping within an entry) from the userGlobalLocationMap.
     *
     * @param from      The source path (key).
     * @param tableType Optional: The specific table type to remove. If null, removes all mappings for 'from'.
     */
    public void deleteTranslatorUserGlobalLocationMapEntry(String from, TableType tableType) {
        log.info("Attempting to delete Translator userGlobalLocationMap entry: from='{}', tableType='{}'", from, tableType);
        if (currentConfig.get().getTranslator() != null && currentConfig.get().getTranslator().getUserGlobalLocationMap() != null) {
            Map<String, Map<TableType, String>> userMap = currentConfig.get().getTranslator().getUserGlobalLocationMap();
            if (userMap.containsKey(from)) {
                if (tableType == null) {
                    userMap.remove(from);
                    log.info("Translator userGlobalLocationMap entry for '{}' and all table types deleted.", from);
                } else {
                    Map<TableType, String> innerMap = userMap.get(from);
                    if (innerMap != null && innerMap.remove(tableType) != null) {
                        log.info("Translator userGlobalLocationMap entry for '{}', tableType '{}' deleted.", from, tableType);
                        if (innerMap.isEmpty()) {
                            userMap.remove(from); // Remove outer key if inner map is empty
                        }
                    } else {
                        log.warn("Translator userGlobalLocationMap entry for '{}', tableType '{}' not found. No deletion performed.", from, tableType);
                    }
                }
                currentConfig.get().getTranslator().rebuildOrderedGlobalLocationMap(); // Rebuild ordered map
            } else {
                log.warn("Translator userGlobalLocationMap entry for '{}' not found. No deletion performed.", from);
            }
        } else {
            log.warn("Translator object or userGlobalLocationMap not found. No deletion performed.");
        }
    }

    /**
     * Clears all entries from the userGlobalLocationMap.
     */
    public void clearTranslatorUserGlobalLocationMap() {
        log.info("Clearing all entries from Translator's userGlobalLocationMap.");
        if (currentConfig.get().getTranslator() != null) {
            if (currentConfig.get().getTranslator().getUserGlobalLocationMap() != null) {
                currentConfig.get().getTranslator().getUserGlobalLocationMap().clear();
                currentConfig.get().getTranslator().rebuildOrderedGlobalLocationMap(); // Rebuild ordered map
                log.info("Translator userGlobalLocationMap cleared successfully.");
            } else {
                log.warn("Translator userGlobalLocationMap is already null. No action needed.");
            }
        } else {
            log.warn("Translator object not found. Cannot clear userGlobalLocationMap.");
        }
    }

    /**
     * Retrieves the autoGlobalLocationMap from Translator (read-only).
     *
     * @return A map of source paths to TableType to target location mappings.
     */
    public Map<String, Map<TableType, String>> getTranslatorAutoGlobalLocationMap() {
        log.debug("Retrieving Translator's autoGlobalLocationMap.");
        if (currentConfig.get().getTranslator() != null && currentConfig.get().getTranslator().getAutoGlobalLocationMap() != null) {
            // Return a deep copy to prevent direct modification
            Map<String, Map<TableType, String>> deepCopy = new HashMap<>();
            currentConfig.get().getTranslator().getAutoGlobalLocationMap().forEach((key, innerMap) -> {
                deepCopy.put(key, new HashMap<>(innerMap));
            });
            return deepCopy;
        }
        log.warn("Translator or autoGlobalLocationMap not found. Returning empty map.");
        return new HashMap<>();
    }
}
