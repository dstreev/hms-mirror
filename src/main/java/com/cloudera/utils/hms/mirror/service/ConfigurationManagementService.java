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

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service for managing HMS Mirror Configuration persistence and retrieval.
 * This service acts as a dedicated layer for configuration CRUD operations,
 * abstracting the underlying storage mechanism (RocksDB) from the web controllers.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class ConfigurationManagementService {

    private final RocksDB rocksDB;
    private final ColumnFamilyHandle configurationsColumnFamily;

    @Autowired
    public ConfigurationManagementService(RocksDB rocksDB,
                                        @Qualifier("configurationsColumnFamily") ColumnFamilyHandle configurationsColumnFamily) {
        this.rocksDB = rocksDB;
        this.configurationsColumnFamily = configurationsColumnFamily;
    }

    /**
     * Lists all configurations grouped by data strategy.
     *
     * @return Map containing configuration listing results
     */
    public Map<String, Object> listConfigurations() {
        log.debug("Listing all configurations");
        try {
            Map<String, Object> result = new HashMap<>();
            Map<String, List<Map<String, Object>>> strategiesMap = new HashMap<>();
            
            // Iterate through all keys in the configurations column family
            try (RocksIterator iterator = rocksDB.newIterator(configurationsColumnFamily)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    String configName = new String(iterator.key());
                    byte[] configValue = iterator.value();
                    
                    try {
                        // Parse the YAML to extract data strategy
                        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> configData = yamlMapper.readValue(configValue, Map.class);
                        String strategy = (String) configData.getOrDefault("dataStrategy", "UNKNOWN");
                        
                        // Create configuration metadata object
                        Map<String, Object> configInfo = new HashMap<>();
                        configInfo.put("name", configName);
                        configInfo.put("yamlConfig", new String(configValue)); // Include the YAML content
                        configInfo.put("createdDate", java.time.LocalDate.now().toString());
                        configInfo.put("modifiedDate", java.time.LocalDate.now().toString());
                        configInfo.put("description", configData.get("comment")); // Add description from comment field

                        strategiesMap.computeIfAbsent(strategy, k -> new ArrayList<>()).add(configInfo);
                    } catch (Exception e) {
                        log.warn("Failed to parse configuration {}, skipping", configName, e);
                    }
                    iterator.next();
                }
            }
            
            result.put("status", "success");
            result.put("data", strategiesMap);
            result.put("strategies", strategiesMap);
            return result;
            
        } catch (Exception e) {
            log.error("Error listing configurations", e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "error");
            errorResult.put("message", "Failed to list configurations: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Loads a specific configuration by data strategy and name.
     *
     * @param configName   The configuration name
     * @return Map containing the configuration load results
     */
    public Map<String, Object> loadConfiguration(String configName) {
        log.debug("Loading configuration: {}", configName);
        try {
            byte[] value = rocksDB.get(configurationsColumnFamily, configName.getBytes());
            
            Map<String, Object> result = new HashMap<>();
            if (value != null) {
                String configData = new String(value);
                
                // Parse the YAML/JSON configuration data
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> configMap = mapper.readValue(configData, Map.class);
                    result.put("status", "SUCCESS");
                    result.put("configuration", configMap);
                } catch (Exception parseException) {
                    // Fallback to treating as plain string
                    result.put("status", "SUCCESS");
                    result.put("configuration", configData);
                }
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Configuration not found: " + configName);
            }
            
            return result;
            
        } catch (RocksDBException e) {
            log.error("Error loading configuration {}", configName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "error");
            errorResult.put("message", "Failed to load configuration: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Saves a configuration using the ConfigLiteDto format.
     * This preserves the lightweight structure without converting to full config objects.
     *
     * @param dataStrategy The data strategy name
     * @param configName   The configuration name
     * @param configDto    The configuration DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> saveConfiguration(String configName, ConfigLiteDto configDto) {
        log.debug("Saving configuration: {}", configName);
        try {
            // Convert ConfigLiteDto to YAML for storage
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            String yamlConfig = yamlMapper.writeValueAsString(configDto);
            
            rocksDB.put(configurationsColumnFamily, configName.getBytes(), yamlConfig.getBytes());
            
            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Configuration saved successfully");
            result.put("key", configName);
            return result;
            
        } catch (Exception e) {
            log.error("Error saving configuration {}", configName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save configuration: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Deletes a configuration by data strategy and name.
     *
     * @param configName   The configuration name
     * @return Map containing the delete operation results
     */
    public Map<String, Object> deleteConfiguration(String configName) {
        log.debug("Deleting configuration: {}", configName);
        try {
            // Check if the configuration exists first
            byte[] existingValue = rocksDB.get(configurationsColumnFamily, configName.getBytes());
            Map<String, Object> result = new HashMap<>();
            
            if (existingValue != null) {
                // Delete the configuration
                rocksDB.delete(configurationsColumnFamily, configName.getBytes());
                result.put("status", "SUCCESS");
                result.put("message", "Configuration deleted successfully");
                result.put("key", configName);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Configuration not found: " + configName);
            }
            
            return result;
            
        } catch (RocksDBException e) {
            log.error("Error deleting configuration {}", configName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to delete configuration: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Checks if a configuration exists.
     *
     * @param dataStrategy The data strategy name
     * @param configName   The configuration name
     * @return true if the configuration exists, false otherwise
     */
    public boolean configurationExists(String configName) {
        log.debug("Checking if configuration exists: {}", configName);
        try {
            Map<String, Object> result = loadConfiguration(configName);
            return "SUCCESS".equals(result.get("status"));
        } catch (Exception e) {
            log.warn("Error checking configuration existence {}", configName, e);
            return false;
        }
    }

    /**
     * Gets available data strategies for configuration creation.
     *
     * @return List of available data strategies with labels
     */
    public List<Map<String, String>> getAvailableDataStrategies() {
        log.debug("Getting available data strategies");
        return List.of(
            Map.of("value", "HYBRID", "label", "HYBRID - Recommended for most migrations"),
            Map.of("value", "SQL", "label", "SQL - SQL-based migration"),
            Map.of("value", "EXPORT_IMPORT", "label", "EXPORT_IMPORT - Export/Import approach"),
            Map.of("value", "SCHEMA_ONLY", "label", "SCHEMA_ONLY - Schema migration only"),
            Map.of("value", "STORAGE_MIGRATION", "label", "STORAGE_MIGRATION - In-place storage format migrations"),
            Map.of("value", "LINKED", "label", "LINKED - Creates external tables pointing to original data")
        );
    }

    /**
     * Validates configuration data before saving.
     *
     * @param configDto The configuration DTO to validate
     * @return Map containing validation results
     */
    public Map<String, Object> validateConfiguration(ConfigLiteDto configDto) {
        log.debug("Validating configuration: {}", configDto.getName());
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Basic validation
            if (configDto.getName() == null || configDto.getName().trim().isEmpty()) {
                result.put("status", "error");
                result.put("message", "Configuration name is required");
                return result;
            }
            
            // Additional validation logic can be added here
            result.put("status", "success");
            result.put("message", "Configuration is valid");
            return result;
            
        } catch (Exception e) {
            log.error("Error validating configuration", e);
            result.put("status", "error");
            result.put("message", "Validation failed: " + e.getMessage());
            return result;
        }
    }

    /**
     * Saves a configuration using HmsMirrorConfig objects (full configuration support).
     * This method is used by the RocksDBManagementController for YAML-based configuration storage.
     *
     * @param dataStrategy The data strategy name
     * @param configName   The configuration name
     * @param configDto    The full HmsMirrorConfig object to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> saveConfiguration(String dataStrategy, String configName, HmsMirrorConfig configDto) {
        log.debug("Saving full configuration: {}", configName);
        try {
            // Use RocksDB to store the configuration as YAML
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            String yamlConfig = yamlMapper.writeValueAsString(configDto);
            
            rocksDB.put(configurationsColumnFamily, configName.getBytes(), yamlConfig.getBytes());
            
            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Configuration saved successfully");
            result.put("key", configName);
            return result;
            
        } catch (Exception e) {
            log.error("Error saving full configuration {}", configName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save configuration: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Converts a map-based configuration to HmsMirrorConfig DTO.
     * This is used when configurations are provided as generic maps.
     *
     * @param configData   The configuration data as a map
     * @param dataStrategy The data strategy name
     * @param configName   The configuration name
     * @return HmsMirrorConfig object
     */
    public HmsMirrorConfig convertMapToDto(Map<String, Object> configData, String dataStrategy, String configName) {
        log.debug("Converting map to DTO for configuration: {}", configName);
        try {
            ObjectMapper mapper = new ObjectMapper();
            
            // Convert the map to JSON and then to HmsMirrorConfig
            String jsonString = mapper.writeValueAsString(configData);
            HmsMirrorConfig config = mapper.readValue(jsonString, HmsMirrorConfig.class);
            
            // Ensure data strategy is set correctly if not already present
            if (config.getDataStrategy() == null) {
                config.setDataStrategy(DataStrategyEnum.valueOf(dataStrategy));
            }
            
            return config;
            
        } catch (Exception e) {
            log.error("Error converting map to DTO for {}", configName, e);
            // Return a minimal config object if conversion fails
            HmsMirrorConfig config = new HmsMirrorConfig();
            config.setDataStrategy(DataStrategyEnum.valueOf(dataStrategy));
            return config;
        }
    }

    /**
     * Lists configurations grouped by strategy - used by RocksDBManagementController.
     * This method uses the existing listConfigurations but formats it for the old API.
     *
     * @return Map containing configuration listing results
     */
    public Map<String, Object> listConfigurationsByStrategy() {
        log.debug("Listing configurations by strategy (legacy API)");
        return listConfigurations();
    }
}