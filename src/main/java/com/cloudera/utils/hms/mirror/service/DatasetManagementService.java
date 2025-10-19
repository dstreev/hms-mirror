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
import org.rocksdb.RocksDBException;
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
    public Map<String, Object> listDatasets() {
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
     * Loads a specific dataset by name.
     *
     * @param datasetName The dataset name
     * @return Map containing the dataset load results
     */
    public Map<String, Object> loadDataset(String datasetName) {
        log.debug("Loading dataset: {}", datasetName);
        try {
            Optional<DatasetDto> datasetOpt = datasetRepository.findById(datasetName);

            Map<String, Object> result = new HashMap<>();
            if (datasetOpt.isPresent()) {
                result.put("status", "SUCCESS");
                result.put("data", datasetOpt.get());
                result.put("name", datasetName);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Dataset not found: " + datasetName);
            }

            return result;

        } catch (Exception e) {
            log.error("Error loading dataset {}", datasetName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to load dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Saves a dataset using the DatasetDto format.
     *
     * @param datasetName The dataset name
     * @param datasetDto  The dataset DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> saveDataset(String datasetName, DatasetDto datasetDto) {
        log.debug("Saving dataset: {}", datasetName);
        try {
            // Save using repository (timestamps and name are handled by repository layer)
            datasetRepository.save(datasetName, datasetDto);

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Dataset saved successfully");
            result.put("name", datasetName);
            return result;

        } catch (Exception e) {
            log.error("Error saving dataset {}", datasetName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Updates an existing dataset.
     *
     * @param datasetName The dataset name
     * @param datasetDto  The updated dataset DTO
     * @return Map containing the update operation results
     */
    public Map<String, Object> updateDataset(String datasetName, DatasetDto datasetDto) {
        log.debug("Updating dataset: {}", datasetName);
        try {
            // Check if dataset exists first
            Optional<DatasetDto> existingDatasetOpt = datasetRepository.findById(datasetName);
            Map<String, Object> result = new HashMap<>();

            if (existingDatasetOpt.isPresent()) {
                // Preserve original creation date if it exists
                DatasetDto existingDataset = existingDatasetOpt.get();
                if (existingDataset.getCreatedDate() != null) {
                    datasetDto.setCreatedDate(existingDataset.getCreatedDate());
                }

                // Save the updated dataset
                return saveDataset(datasetName, datasetDto);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Dataset not found: " + datasetName);
                return result;
            }

        } catch (Exception e) {
            log.error("Error updating dataset {}", datasetName, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to update dataset: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Deletes a dataset by name.
     *
     * @param datasetName The dataset name
     * @return Map containing the delete operation results
     */
    public Map<String, Object> deleteDataset(String datasetName) {
        log.debug("Deleting dataset: {}", datasetName);
        try {
            // Check if the dataset exists first
            Map<String, Object> result = new HashMap<>();

            if (datasetRepository.existsById(datasetName)) {
                // Delete the dataset
                datasetRepository.deleteById(datasetName);
                result.put("status", "SUCCESS");
                result.put("message", "Dataset deleted successfully");
                result.put("name", datasetName);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Dataset not found: " + datasetName);
            }

            return result;

        } catch (Exception e) {
            log.error("Error deleting dataset {}", datasetName, e);
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
    public Map<String, Object> copyDataset(String sourceDatasetName, String targetDatasetName) {
        log.debug("Copying dataset from {} to {}", sourceDatasetName, targetDatasetName);
        try {
            // Load source dataset
            Map<String, Object> loadResult = loadDataset(sourceDatasetName);
            
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
            targetDataset.setCreatedDate(null); // Will be set by repository
            targetDataset.setModifiedDate(null); // Will be set by repository
            
            // Save the copied dataset
            return saveDataset(targetDatasetName, targetDataset);
            
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
     * @param datasetName The dataset name
     * @return true if the dataset exists, false otherwise
     */
    public boolean datasetExists(String datasetName) {
        log.debug("Checking if dataset exists: {}", datasetName);
        try {
            return datasetRepository.existsById(datasetName);
        } catch (Exception e) {
            log.warn("Error checking dataset existence {}", datasetName, e);
            return false;
        }
    }

    /**
     * Validates dataset data before saving.
     *
     * @param datasetDto The dataset DTO to validate
     * @return Map containing validation results
     */
    public Map<String, Object> validateDataset(DatasetDto datasetDto) {
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
                    boolean hasTables = dbSpec.getTables() != null && !dbSpec.getTables().isEmpty();
                    boolean hasFilter = dbSpec.getFilter() != null;
                    
                    if (!hasTables && !hasFilter) {
                        errors.add(prefix + "Must specify either a table list or filter criteria");
                    } else if (hasTables && hasFilter) {
                        errors.add(prefix + "Cannot specify both table list and filter criteria");
                    }
                    
                    // Validate filter if present
                    if (hasFilter) {
                        DatasetDto.TableFilter filter = dbSpec.getFilter();
                        if ((filter.getIncludePattern() == null || filter.getIncludePattern().trim().isEmpty()) &&
                            (filter.getExcludePattern() == null || filter.getExcludePattern().trim().isEmpty()) &&
                            (filter.getTableTypes() == null || filter.getTableTypes().isEmpty())) {
                            errors.add(prefix + "Filter must specify at least one criteria (include pattern, exclude pattern, or table types)");
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