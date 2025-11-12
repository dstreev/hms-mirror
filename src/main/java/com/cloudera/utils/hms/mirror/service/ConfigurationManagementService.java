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
import com.cloudera.utils.hms.mirror.repository.ConfigurationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service for managing HMS Mirror Configuration persistence and retrieval.
 * This service acts as a dedicated layer for configuration CRUD operations,
 * delegating persistence to the ConfigurationRepository.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class ConfigurationManagementService {

    private final ConfigurationRepository configurationRepository;

    /**
     * Lists all configurations grouped by data strategy.
     *
     * @return Map containing configuration listing results
     */
    public Map<String, Object> list() {
        log.debug("Listing all configurations");
        try {
            List<ConfigLiteDto> configurations = configurationRepository.findAllSortedByName();

            Map<String, Object> result = new HashMap<>();
            Map<String, List<Map<String, Object>>> dataByStrategy = new HashMap<>();

            // Group configurations by type (System Defaults vs Custom)
            for (ConfigLiteDto config : configurations) {
                String strategy = config.getName().startsWith("(System)")
                    ? "System Defaults"
                    : "Custom";

                Map<String, Object> configInfo = new HashMap<>();
                configInfo.put("name", config.getName());
                configInfo.put("createdDate", config.getCreated());
                configInfo.put("modifiedDate", config.getModified());
                configInfo.put("description", config.getDescription());
                configInfo.put("yamlConfig", ""); // Empty for now, can be populated if needed

                dataByStrategy.computeIfAbsent(strategy, k -> new ArrayList<>()).add(configInfo);
            }

            // Build strategies list
            List<String> strategies = new ArrayList<>(dataByStrategy.keySet());

            result.put("status", "success");
            result.put("data", dataByStrategy);
            result.put("totalConfigurations", configurations.size());
            result.put("strategies", strategies);
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
    public Map<String, Object> load(String configName) {
        log.debug("Loading configuration: {}", configName);
        try {
            Optional<ConfigLiteDto> configOpt = configurationRepository.findByKey(configName);

            Map<String, Object> result = new HashMap<>();
            if (configOpt.isPresent()) {
                result.put("status", "SUCCESS");
                result.put("configuration", configOpt.get());
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Configuration not found: " + configName);
            }

            return result;

        } catch (Exception e) {
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
     * @param configName   The configuration name
     * @param configDto    The configuration DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> save(ConfigLiteDto configDto) {
        log.debug("Saving configuration: {}", configDto.getKey());
        try {
            // Save using repository (timestamps and name are handled by repository layer)
            configurationRepository.save(configDto);

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Configuration saved successfully");
            result.put("key", configDto.getKey());
            return result;

        } catch (Exception e) {
            log.error("Error saving configuration {}", configDto.getKey(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save configuration: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Creates a new configuration. Fails if configuration already exists.
     *
     * @param configDto The configuration DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> create(ConfigLiteDto configDto) {
        log.debug("Creating new configuration: {}", configDto.getKey());
        try {
            // Check if a configuration with this name already exists
            if (configurationRepository.existsById(configDto.getKey())) {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "CONFLICT");
                result.put("message", "Configuration with name '" + configDto.getName() + "' already exists");
                return result;
            }

            // Save using repository (timestamps and name are handled by repository layer)
            configurationRepository.save(configDto);

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Configuration created successfully");
            result.put("key", configDto.getKey());
            return result;

        } catch (Exception e) {
            log.error("Error creating configuration {}", configDto.getKey(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to create configuration: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Deletes a configuration by data strategy and name.
     *
     * @param configName   The configuration name
     * @return Map containing the delete operation results
     */
    public Map<String, Object> delete(String configName) {
        log.debug("Deleting configuration: {}", configName);
        try {
            Map<String, Object> result = new HashMap<>();

            if (configurationRepository.existsById(configName)) {
                // Delete the configuration
                configurationRepository.deleteById(configName);
                result.put("status", "SUCCESS");
                result.put("message", "Configuration deleted successfully");
                result.put("key", configName);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Configuration not found: " + configName);
            }

            return result;

        } catch (Exception e) {
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
     * @param configName   The configuration name
     * @return true if the configuration exists, false otherwise
     */
    public boolean exists(String configName) {
        log.debug("Checking if configuration exists: {}", configName);
        try {
            return configurationRepository.existsById(configName);
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
    public Map<String, Object> validate(ConfigLiteDto configDto) {
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
     * Lists configurations grouped by strategy - used by RocksDBManagementController.
     * This method uses the existing listConfigurations but formats it for the old API.
     *
     * @return Map containing configuration listing results
     */
    public Map<String, Object> listByStrategy() {
        log.debug("Listing configurations by strategy (legacy API)");
        return list();
    }
}