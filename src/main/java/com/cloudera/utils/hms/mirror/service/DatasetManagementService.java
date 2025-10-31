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

import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.repository.DatasetRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service for managing HMS Mirror Dataset persistence and retrieval.
 * This service acts as a dedicated layer for dataset CRUD operations,
 * delegating persistence to the DatasetRepository.
 *
 * Datasets define collections of databases and their associated tables/filters
 * for processing by HMS Mirror.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class DatasetManagementService {

    private final DatasetRepository datasetRepository;

    /**
     * Lists all datasets with their metadata.
     *
     * @return Map containing dataset listing results
     */
    public Map<String, Object> list() {
        log.debug("Listing all datasets");
        try {
            List<DatasetDto> datasets = datasetRepository.findAllSortedByName();

            // Convert to the format expected by frontend: {name: DatasetDto}
            Map<String, DatasetDto> dataMap = new HashMap<>();
            for (DatasetDto datasetDto : datasets) {
                dataMap.put(datasetDto.getName(), datasetDto);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("data", dataMap);
            return result;

        } catch (Exception e) {
            log.error("Error listing datasets", e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "error");
            errorResult.put("message", "Failed to list datasets: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Loads a specific dataset by key.
     *
     * @param key The dataset key
     * @return Map containing the dataset load results
     */
    public Map<String, Object> load(String key) {
        log.debug("Loading dataset: {}", key);
        try {
            Optional<DatasetDto> datasetOpt = datasetRepository.findById(key);

            Map<String, Object> result = new HashMap<>();
            if (datasetOpt.isPresent()) {
                result.put("status", "SUCCESS");
                result.put("data", datasetOpt.get());
                result.put("key", key);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Dataset not found: " + key);
            }

            return result;

        } catch (Exception e) {
            log.error("Error loading dataset {}", key, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to load dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Saves a dataset using the DatasetDto format.
     *
     * @param datasetDto  The dataset DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> save(DatasetDto datasetDto) {
        log.debug("Saving dataset: {}", datasetDto.getName());
        try {
            // Save using repository (timestamps and name are handled by repository layer)
            datasetRepository.save(datasetDto);

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Dataset saved successfully");
            result.put("name", datasetDto.getName());
            return result;

        } catch (Exception e) {
            log.error("Error saving dataset {}", datasetDto.getName(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Updates an existing dataset.
     *
     * @param datasetDto  The updated dataset DTO
     * @return Map containing the update operation results
     */
    public Map<String, Object> update(DatasetDto datasetDto) {
        log.debug("Updating dataset: {}", datasetDto.getKey());
        try {
            // Check if dataset exists first
            Optional<DatasetDto> existingDatasetOpt = datasetRepository.findById(datasetDto.getKey());
            Map<String, Object> result = new HashMap<>();

            if (existingDatasetOpt.isPresent()) {
                // Preserve original creation date if it exists
                DatasetDto existingDataset = existingDatasetOpt.get();
                if (existingDataset.getCreated() != null) {
                    datasetDto.setCreated(existingDataset.getCreated());
                }

                // Save the updated dataset
                return save(datasetDto);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Dataset not found: " + datasetDto.getKey());
                return result;
            }

        } catch (Exception e) {
            log.error("Error updating dataset {}", datasetDto.getKey(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to update dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Deletes a dataset by key.
     *
     * @param key The dataset key
     * @return Map containing the delete operation results
     */
    public Map<String, Object> delete(String key) {
        log.debug("Deleting dataset: {}", key);
        try {
            // Check if the dataset exists first
            Map<String, Object> result = new HashMap<>();

            if (datasetRepository.existsById(key)) {
                // Delete the dataset
                datasetRepository.deleteById(key);
                result.put("status", "SUCCESS");
                result.put("message", "Dataset deleted successfully");
                result.put("key", key);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Dataset not found: " + key);
            }

            return result;

        } catch (Exception e) {
            log.error("Error deleting dataset {}", key, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to delete dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Copies a dataset with a new name.
     *
     * @param sourceDatasetName The source dataset name
     * @param targetDatasetName The target dataset name
     * @return Map containing the copy operation results
     */
    public Map<String, Object> copy(String sourceDatasetName, String targetDatasetName) {
        log.debug("Copying dataset from {} to {}", sourceDatasetName, targetDatasetName);
        try {
            // Load source dataset
            Map<String, Object> loadResult = load(sourceDatasetName);
            
            if (!"SUCCESS".equals(loadResult.get("status"))) {
                return loadResult; // Return the error from loading
            }
            
            // Get the source dataset and create a copy
            DatasetDto sourceDataset = (DatasetDto) loadResult.get("data");

            // Create a deep copy of the dataset
            DatasetDto targetDataset = new DatasetDto();
            targetDataset.setName(targetDatasetName);
            targetDataset.setDescription(
                (sourceDataset.getDescription() != null ? sourceDataset.getDescription() : "") +
                " (Copy of " + sourceDatasetName + ")"
            );
            targetDataset.setDatabases(sourceDataset.getDatabases());
            targetDataset.setCreated(null); // Will be set by repository
            targetDataset.setModified(null); // Will be set by repository
            
            // Save the copied dataset
            return save(targetDataset);
            
        } catch (Exception e) {
            log.error("Error copying dataset from {} to {}", sourceDatasetName, targetDatasetName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to copy dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Checks if a dataset exists.
     *
     * @param key The dataset key
     * @return true if the dataset exists, false otherwise
     */
    public boolean exists(String key) {
        log.debug("Checking if dataset exists: {}", key);
        try {
            return datasetRepository.existsById(key);
        } catch (Exception e) {
            log.warn("Error checking dataset existence {}", key, e);
            return false;
        }
    }

    /**
     * Validates dataset data before saving.
     *
     * @param datasetDto The dataset DTO to validate
     * @return Map containing validation results
     */
    public Map<String, Object> validate(DatasetDto datasetDto) {
        log.debug("Validating dataset: {}", datasetDto.getName());
        Map<String, Object> result = new HashMap<>();
        List<String> errors = new ArrayList<>();
        
        try {
            // Basic validation
            if (datasetDto.getName() == null || datasetDto.getName().trim().isEmpty()) {
                errors.add("Dataset name is required");
            }
            
            if (datasetDto.getDatabases() == null || datasetDto.getDatabases().isEmpty()) {
                errors.add("At least one database must be specified");
            } else {
                // Validate each database specification
                for (int i = 0; i < datasetDto.getDatabases().size(); i++) {
                    DatasetDto.DatabaseSpec dbSpec = datasetDto.getDatabases().get(i);
                    String prefix = "Database " + (i + 1) + ": ";
                    
                    if (dbSpec.getDatabaseName() == null || dbSpec.getDatabaseName().trim().isEmpty()) {
                        errors.add(prefix + "Database name is required");
                    }

                    // Check that database has either tables OR filter, but not both
                    // NOTE: A database can have warehouse config or location mappings without tables/filters
                    boolean hasTables = dbSpec.getTables() != null && !dbSpec.getTables().isEmpty();
                    boolean hasFilter = dbSpec.getFilter() != null;
                    boolean hasWarehouseConfig = dbSpec.getWarehouse() != null &&
                        (dbSpec.getWarehouse().getManagedDirectory() != null ||
                         dbSpec.getWarehouse().getExternalDirectory() != null);
                    boolean hasLocationMappings = dbSpec.getUserGlobalLocationMap() != null &&
                        !dbSpec.getUserGlobalLocationMap().isEmpty();
                    boolean hasNamingConfig = dbSpec.getDbPrefix() != null || dbSpec.getDbRename() != null;

                    // Database must have at least one configuration type
                    if (!hasTables && !hasFilter && !hasWarehouseConfig && !hasLocationMappings && !hasNamingConfig) {
                        errors.add(prefix + "Must specify at least one configuration (tables, filter, warehouse, location mappings, or naming options)");
                    } else if (hasTables && hasFilter) {
                        errors.add(prefix + "Cannot specify both table list and filter criteria (they are mutually exclusive)");
                    }

                    // Validate that dbPrefix and dbRename are mutually exclusive
                    if (dbSpec.getDbPrefix() != null && !dbSpec.getDbPrefix().trim().isEmpty() &&
                        dbSpec.getDbRename() != null && !dbSpec.getDbRename().trim().isEmpty()) {
                        errors.add(prefix + "Cannot specify both dbPrefix and dbRename (they are mutually exclusive)");
                    }
                    
                    // Validate filter if present
                    if (hasFilter) {
                        DatasetDto.TableFilter filter = dbSpec.getFilter();
                        boolean hasIncludePattern = filter.getIncludeRegEx() != null && !filter.getIncludeRegEx().trim().isEmpty();
                        boolean hasExcludePattern = filter.getExcludeRegEx() != null && !filter.getExcludeRegEx().trim().isEmpty();
                        boolean hasTableTypes = filter.getTableTypes() != null && !filter.getTableTypes().isEmpty();
                        boolean hasSizeConstraints = (filter.getMinSizeMb() > 0) || (filter.getMaxSizeMb() > 0);
                        boolean hasPartitionConstraints = (filter.getMinPartitions() > 0) || (filter.getMaxPartitions() > 0);

                        if (!hasIncludePattern && !hasExcludePattern && !hasTableTypes &&
                            !hasSizeConstraints && !hasPartitionConstraints) {
                            errors.add(prefix + "Filter must specify at least one criteria (include/exclude pattern, table types, size constraints, or partition constraints)");
                        }
                    }
                }
            }
            
            if (errors.isEmpty()) {
                result.put("status", "success");
                result.put("message", "Dataset is valid");
            } else {
                result.put("status", "error");
                result.put("message", "Dataset validation failed");
                result.put("errors", errors);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Error validating dataset", e);
            result.put("status", "error");
            result.put("message", "Validation failed: " + e.getMessage());
            return result;
        }
    }
}