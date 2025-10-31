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

import com.cloudera.utils.hms.mirror.service.DatasetManagementService;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
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
 * REST Controller for HMS Mirror Dataset Management.
 * Provides standardized CRUD operations for dataset storage and retrieval.
 * Follows the same patterns as ConfigurationController for consistency.
 */
@RestController
@RequestMapping("/api/v1/datasets")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Dataset Management", description = "HMS Mirror dataset CRUD operations")
@Slf4j
public class DatasetController {

    private final DatasetManagementService datasetManagementService;

    @Autowired
    public DatasetController(DatasetManagementService datasetManagementService) {
        this.datasetManagementService = datasetManagementService;
    }

    @GetMapping(produces = "application/json")
    @Operation(summary = "List all datasets", 
               description = "Returns all HMS Mirror datasets with metadata")
    @ApiResponse(responseCode = "200", description = "Datasets retrieved successfully",
                 content = @Content(schema = @Schema(implementation = Map.class)))
    public ResponseEntity<Map<String, Object>> getDatasets() {
        log.info("DatasetController.getDatasets() called");
        
        try {
            Map<String, Object> result = datasetManagementService.list();
            
            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else {
                log.error("Failed to list datasets: {}", result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error listing datasets", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve datasets: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping(value = "/{key}", produces = "application/json")
    @Operation(summary = "Get dataset by key",
               description = "Retrieves a specific HMS Mirror dataset by key")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Dataset retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Dataset not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve dataset")
    })
    public ResponseEntity<Map<String, Object>> getDataset(
            @Parameter(description = "Dataset key", required = true)
            @PathVariable String key) {

        log.info("DatasetController.getDataset() called - key: {}", key);

        try {
            Map<String, Object> result = datasetManagementService.load(key);

            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to load dataset {}: {}", key, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error retrieving dataset {}", key, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve dataset: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(consumes = "application/json", produces = "application/json")
    @Operation(summary = "Create or update dataset", 
               description = "Creates a new HMS Mirror dataset or updates an existing one")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Dataset saved successfully"),
        @ApiResponse(responseCode = "201", description = "Dataset created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid dataset data"),
        @ApiResponse(responseCode = "500", description = "Failed to save dataset")
    })
    public ResponseEntity<Map<String, Object>> saveDataset(
            @Parameter(description = "Dataset data", required = true)
            @RequestBody DatasetDto datasetDto) {
        
        log.info("DatasetController.saveDataset() called - name: {}",
                datasetDto.getName());
        
        try {
            // Validate required fields
            if (datasetDto.getName() == null || datasetDto.getName().trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Dataset name is required");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Validate dataset first
            Map<String, Object> validationResult = datasetManagementService.validate(datasetDto);
            if (!"success".equals(validationResult.get("status"))) {
                return ResponseEntity.badRequest().body(validationResult);
            }

            // Check if dataset already exists
            boolean isUpdate = datasetManagementService.exists(datasetDto.getKey());
            
            // Save the dataset
            Map<String, Object> result = datasetManagementService.save(datasetDto);
            
            if ("SUCCESS".equals(result.get("status"))) {
                HttpStatus status = isUpdate ? HttpStatus.OK : HttpStatus.CREATED;
                result.put("operation", isUpdate ? "updated" : "created");
                return ResponseEntity.status(status).body(result);
            } else {
                log.error("Failed to save dataset {}: {}",
                        datasetDto.getName(), result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error saving dataset {}", datasetDto.getName(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to save dataset: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PutMapping(value = "/{key}", consumes = "application/json", produces = "application/json")
    @Operation(summary = "Update existing dataset",
               description = "Updates an existing HMS Mirror dataset")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Dataset updated successfully"),
        @ApiResponse(responseCode = "404", description = "Dataset not found"),
        @ApiResponse(responseCode = "400", description = "Invalid dataset data"),
        @ApiResponse(responseCode = "500", description = "Failed to update dataset")
    })
    public ResponseEntity<Map<String, Object>> updateDataset(
            @Parameter(description = "Dataset key", required = true)
            @PathVariable String key,
            @Parameter(description = "Updated dataset data", required = true)
            @RequestBody DatasetDto datasetDto) {

        log.info("DatasetController.updateDataset() called - key: {}", key);

        try {
            // Validate dataset first
            Map<String, Object> validationResult = datasetManagementService.validate(datasetDto);
            if (!"success".equals(validationResult.get("status"))) {
                return ResponseEntity.badRequest().body(validationResult);
            }

            // Ensure the DTO has the correct key from the path
            datasetDto.setKey(key);

            // Update the dataset
            Map<String, Object> result = datasetManagementService.update(datasetDto);

            if ("SUCCESS".equals(result.get("status"))) {
                result.put("operation", "updated");
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to update dataset {}: {}", key, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error updating dataset {}", key, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to update dataset: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping(value = "/{key}", produces = "application/json")
    @Operation(summary = "Delete dataset",
               description = "Deletes a specific HMS Mirror dataset")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Dataset deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Dataset not found"),
        @ApiResponse(responseCode = "500", description = "Failed to delete dataset")
    })
    public ResponseEntity<Map<String, Object>> deleteDataset(
            @Parameter(description = "Dataset key", required = true)
            @PathVariable String key) {

        log.info("DatasetController.deleteDataset() called - key: {}", key);

        try {
            Map<String, Object> result = datasetManagementService.delete(key);

            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to delete dataset {}: {}", key, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error deleting dataset {}", key, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to delete dataset: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(value = "/validate", consumes = "application/json", produces = "application/json")
    @Operation(summary = "Validate dataset", 
               description = "Validates a dataset configuration without saving it")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Dataset is valid"),
        @ApiResponse(responseCode = "400", description = "Dataset validation failed"),
        @ApiResponse(responseCode = "500", description = "Failed to validate dataset")
    })
    public ResponseEntity<Map<String, Object>> validateDataset(
            @Parameter(description = "Dataset data to validate", required = true)
            @RequestBody DatasetDto datasetDto) {
        
        log.info("DatasetController.validateDataset() called - name: {}", datasetDto.getName());
        
        try {
            Map<String, Object> result = datasetManagementService.validate(datasetDto);
            
            if ("success".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.badRequest().body(result);
            }
            
        } catch (Exception e) {
            log.error("Error validating dataset", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to validate dataset: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}