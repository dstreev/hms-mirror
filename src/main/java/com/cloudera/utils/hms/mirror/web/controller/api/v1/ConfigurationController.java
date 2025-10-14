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

package com.cloudera.utils.hms.mirror.web.controller.api.v1;

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.ConfigurationManagementService;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for HMS Mirror Configuration Management.
 * Provides standardized CRUD operations for configuration storage and retrieval.
 * Follows the same patterns as ConnectionController for consistency.
 */
@RestController
@RequestMapping("/api/v1/config")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Configuration Management", description = "HMS Mirror configuration CRUD operations")
@Slf4j
public class ConfigurationController {

    private final ConfigurationManagementService configurationManagementService;

    @Autowired
    public ConfigurationController(ConfigurationManagementService configurationManagementService) {
        this.configurationManagementService = configurationManagementService;
    }

    @GetMapping(produces = "application/json")
    @Operation(summary = "List all configurations", 
               description = "Returns all HMS Mirror configurations grouped by data strategy")
    @ApiResponse(responseCode = "200", description = "Configurations retrieved successfully",
                 content = @Content(schema = @Schema(implementation = Map.class)))
    public ResponseEntity<Map<String, Object>> getConfigurations() {
        log.info("ConfigurationController.getConfigurations() called");
        
        try {
            Map<String, Object> result = configurationManagementService.listConfigurations();
            
            if ("success".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else {
                log.error("Failed to list configurations: {}", result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error listing configurations", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve configurations: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping(value = "/{configName}", produces = "application/json")
    @Operation(summary = "Get configuration by strategy and name", 
               description = "Retrieves a specific HMS Mirror configuration by data strategy and name")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Configuration retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Configuration not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve configuration")
    })
    public ResponseEntity<Map<String, Object>> getConfiguration(
            @Parameter(description = "Configuration name", required = true)
            @PathVariable String configName) {
        
        log.info("ConfigurationController.getConfiguration() called - name: {}", configName);
        
        try {
            Map<String, Object> result = configurationManagementService.loadConfiguration(configName);
            
            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to load configuration {}: {}", configName, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error retrieving configuration {}", configName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve configuration: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(consumes = "application/json", produces = "application/json")
    @Operation(summary = "Create or update configuration", 
               description = "Creates a new HMS Mirror configuration or updates an existing one")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Configuration saved successfully"),
        @ApiResponse(responseCode = "201", description = "Configuration created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid configuration data"),
        @ApiResponse(responseCode = "500", description = "Failed to save configuration")
    })
    public ResponseEntity<Map<String, Object>> saveConfiguration(
            @Parameter(description = "Configuration data", required = true)
            @RequestBody ConfigLiteDto configDto) {
        
        log.info("ConfigurationController.saveConfiguration() called - name: {}}",
                configDto.getName());
        
        try {
            // Validate required fields
            if (configDto.getName() == null || configDto.getName().trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Configuration name is required");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Validate configuration first
            Map<String, Object> validationResult = configurationManagementService.validateConfiguration(configDto);
            if (!"success".equals(validationResult.get("status"))) {
                return ResponseEntity.badRequest().body(validationResult);
            }
            
            // Check if configuration already exists
            boolean isUpdate = configurationManagementService.configurationExists(configDto.getName());
            
            // Save the configuration using the DTO version to preserve lite structure
            Map<String, Object> result = configurationManagementService.saveConfiguration(
                configDto.getName(),
                configDto);
            
            if ("SUCCESS".equals(result.get("status"))) {
                HttpStatus status = isUpdate ? HttpStatus.OK : HttpStatus.CREATED;
                result.put("operation", isUpdate ? "updated" : "created");
                return ResponseEntity.status(status).body(result);
            } else {
                log.error("Failed to save configuration {}: {}",
                        configDto.getName(), result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error saving configuration {}", configDto.getName(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to save configuration: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PutMapping(value = "/{configName}", consumes = "application/json", produces = "application/json")
    @Operation(summary = "Update existing configuration", 
               description = "Updates an existing HMS Mirror configuration")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Configuration updated successfully"),
        @ApiResponse(responseCode = "404", description = "Configuration not found"),
        @ApiResponse(responseCode = "400", description = "Invalid configuration data"),
        @ApiResponse(responseCode = "500", description = "Failed to update configuration")
    })
    public ResponseEntity<Map<String, Object>> updateConfiguration(
            @Parameter(description = "Configuration name", required = true)
            @PathVariable String configName,
            @Parameter(description = "Updated configuration data", required = true)
            @RequestBody ConfigLiteDto configDto) {
        
        log.info("ConfigurationController.updateConfiguration() called - name: {}", configName);

        try {
            // Check if configuration exists
            boolean configExists = configurationManagementService.configurationExists(configName);
            
            if (!configExists) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Configuration not found");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            }
            
            // Ensure the DTO has the correct name and strategy from the path
            configDto.setName(configName);

            // Update the configuration using the DTO version to preserve lite structure
            Map<String, Object> result = configurationManagementService.saveConfiguration(
                configName, configDto);
            
            if ("SUCCESS".equals(result.get("status"))) {
                result.put("operation", "updated");
                return ResponseEntity.ok(result);
            } else {
                log.error("Failed to update configuration {}: {}", configName, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error updating configuration {}", configName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to update configuration: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping(value = "/{configName}", produces = "application/json")
    @Operation(summary = "Delete configuration", 
               description = "Deletes a specific HMS Mirror configuration")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Configuration deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Configuration not found"),
        @ApiResponse(responseCode = "500", description = "Failed to delete configuration")
    })
    public ResponseEntity<Map<String, Object>> deleteConfiguration(
            @Parameter(description = "Configuration name", required = true)
            @PathVariable String configName) {
        
        log.info("ConfigurationController.deleteConfiguration() called - name: {}", configName);
        
        try {
            Map<String, Object> result = configurationManagementService.deleteConfiguration(
                configName);
            
            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to delete configuration {}: {}",configName, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error deleting configuration {}", configName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to delete configuration: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(value = "/copy/{sourceConfigName}", produces = "application/json")
    @Operation(summary = "Copy configuration", 
               description = "Creates a copy of an existing configuration with a new name and/or strategy")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Configuration copied successfully"),
        @ApiResponse(responseCode = "404", description = "Source configuration not found"),
        @ApiResponse(responseCode = "409", description = "Target configuration already exists"),
        @ApiResponse(responseCode = "500", description = "Failed to copy configuration")
    })
    public ResponseEntity<Map<String, Object>> copyConfiguration(
            @Parameter(description = "Source configuration name", required = true)
            @PathVariable String sourceConfigName,
            @Parameter(description = "Target configuration details", required = true)
            @RequestBody Map<String, String> copyRequest) {
        
        String targetConfigName = copyRequest.get("targetConfigName");
        
        log.info("ConfigurationController.copyConfiguration() called - source: {}, target: {}",
                sourceConfigName, targetConfigName);
        
        try {
            if (targetConfigName == null) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "targetDataStrategy and targetConfigName are required");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Load source configuration
            Map<String, Object> sourceResult = configurationManagementService.loadConfiguration(
                sourceConfigName);
            
            if (!"SUCCESS".equals(sourceResult.get("status"))) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Source configuration not found");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            }
            
            // Check if target already exists
            boolean targetExists = configurationManagementService.configurationExists(
                targetConfigName);
            
            if (targetExists) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Target configuration already exists");
                return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);
            }
            
            // Get the configuration object and modify for copy
            Object configObject = sourceResult.get("configuration");
            if (configObject instanceof HmsMirrorConfig) {
                // Convert to DTO for copying
                HmsMirrorConfig sourceConfig =
                    (HmsMirrorConfig) configObject;
                
                ConfigLiteDto targetConfigDto = new ConfigLiteDto();
                targetConfigDto.setName(targetConfigName);

                // Copy relevant fields from source (basic implementation)
                if (sourceConfig.getComment() != null) {
                    targetConfigDto.setComment("Copy of " + sourceConfig.getComment());
                }
                
                // Copy basic boolean flags
                targetConfigDto.setDatabaseOnly(sourceConfig.isDatabaseOnly());
                targetConfigDto.setMigrateNonNative(sourceConfig.isMigrateNonNative());
                targetConfigDto.setReadOnly(sourceConfig.isReadOnly());
                
                // Save the copied configuration
                Map<String, Object> saveResult = configurationManagementService.saveConfiguration(
                    targetConfigName, targetConfigDto);
                
                if ("SUCCESS".equals(saveResult.get("status"))) {
                    saveResult.put("operation", "copied");
                    saveResult.put("source", sourceConfigName);
                    return ResponseEntity.status(HttpStatus.CREATED).body(saveResult);
                } else {
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(saveResult);
                }
            } else {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Invalid source configuration format");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
            }
            
        } catch (Exception e) {
            log.error("Error copying configuration {} to {}",
                     sourceConfigName, targetConfigName, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to copy configuration: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping(value = "/strategies", produces = "application/json")
    @Operation(summary = "List available data strategies", 
               description = "Returns all available data strategy options")
    @ApiResponse(responseCode = "200", description = "Data strategies retrieved successfully")
    public ResponseEntity<Map<String, Object>> getDataStrategies() {
        log.info("ConfigurationController.getDataStrategies() called");
        
        try {
            Map<String, Object> result = new HashMap<>();
            result.put("status", "success");
            result.put("strategies", configurationManagementService.getAvailableDataStrategies());
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("Error retrieving data strategies", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve data strategies: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

}