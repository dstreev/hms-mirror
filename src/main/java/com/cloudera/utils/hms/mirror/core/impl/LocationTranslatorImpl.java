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

package com.cloudera.utils.hms.mirror.core.impl;

import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.core.api.LocationTranslator;
import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.infrastructure.configuration.ConfigurationProvider;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Core implementation of location translation business logic.
 * This class contains pure business logic for location translation without Spring dependencies,
 * making it testable and reusable across different application contexts.
 */
public class LocationTranslatorImpl implements LocationTranslator {

    private final ConfigurationProvider configurationProvider;

    public LocationTranslatorImpl(ConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
    }

    @Override
    public LocationTranslationResult translateTableLocation(LocationTranslationRequest request) {
        // Extract request parameters
        DBMirror dbMirror = request.getDbMirror();
        TableMirror tableMirror = request.getTableMirror();
        String originalLocation = request.getOriginalLocation();
        String partitionSpec = request.getPartitionSpec();

        // Get configuration through infrastructure abstraction
        HmsMirrorConfig config = configurationProvider.getConfig();

        // Perform the core business logic from TranslatorService.translateTableLocation()
        String tableName = tableMirror.getName();
        EnvironmentTable targetEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
        String originalDatabase = dbMirror.getName();
        String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(originalDatabase, config);
        String targetDatabaseDir = getOrDefault(dbMirror.getLocationDirectory(), targetDatabase + ".db");
        String targetDatabaseManagedDir = getOrDefault(dbMirror.getManagedLocationDirectory(), targetDatabase + ".db");
        String originalTableLocation = TableUtils.getLocation(tableName, tableMirror.getEnvironmentTable(Environment.LEFT).getDefinition());
        String targetNamespace;
        try {
            targetNamespace = config.getTargetNamespace();
        } catch (RequiredConfigurationException e) {
            return LocationTranslationResult.failure("Failed to get target namespace: " + e.getMessage());
        }
        String relativeDir = NamespaceUtils.stripNamespace(originalLocation);

        // Process Global Location Map
        GlobalLocationMapResult glmMapping = processGlobalLocationMap(relativeDir, TableUtils.isExternal(targetEnvTable));
        boolean remapped = glmMapping.isMapped();

        String newLocation;
        if (glmMapping.isMapped()) {
            // Use mapped location
            String mappedDir = glmMapping.getMappedDir();
            newLocation = buildMappedLocation(targetNamespace, mappedDir, partitionSpec);
        } else {
            // Compute new location using business rules
            newLocation = computeNewLocationFromBusinessRules(
                targetNamespace, partitionSpec, tableMirror, config,
                targetDatabaseManagedDir, tableName, originalDatabase, targetDatabase,
                relativeDir, originalLocation, targetEnvTable
            );
            
            // Handle storage migration scenarios and validate location alignment for DistCP
            handleStorageMigrationLogic(originalLocation, config, tableMirror);
            
            // Validate that non-GLM locations can be handled with DistCP
            // This may throw RuntimeException which will propagate up to cause application exit code 1
            validateLocationForDistcp(originalLocation, tableMirror, config);
        }

        // Validate location alignment
        ValidationResult validation = validateLocationAlignment(tableMirror, newLocation, partitionSpec);
        
        List<String> issues = new ArrayList<>();
        if (!validation.isValid()) {
            issues.addAll(validation.getErrors());
        }
        
        // Add GLM issue if mapped
        if (glmMapping.isMapped()) {
            issues.add("GLM applied. Original Location: " + glmMapping.getOriginalDir() + 
                      " Mapped Location: " + glmMapping.getMappedDir());
        }
        

        return new LocationTranslationResult(newLocation, true, "Location translated successfully",
                                           List.of(), issues, remapped);
    }

    @Override
    public GlobalLocationMapResult processGlobalLocationMap(String originalLocation, boolean isExternalTable) {
        try {
            HmsMirrorConfig config = configurationProvider.getConfig();
            String newLocation = originalLocation;
            boolean mapped = false;
            
            // Process Global Location Map if configured
            if (!config.getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                for (String key : config.getTranslator().getOrderedGlobalLocationMap().keySet()) {
                    if (originalLocation.startsWith(key)) {
                        Map<TableType, String> rLocMap = config.getTranslator().getOrderedGlobalLocationMap().get(key);
                        String rLoc = null;
                        
                        if (isExternalTable) {
                            rLoc = rLocMap.get(TableType.EXTERNAL_TABLE);
                            if (nonNull(rLoc)) {
                                newLocation = rLoc + originalLocation.replace(key, "");
                                mapped = true;
                            }
                        } else {
                            rLoc = rLocMap.get(TableType.MANAGED_TABLE);
                            if (nonNull(rLoc)) {
                                newLocation = rLoc + originalLocation.replace(key, "");
                                mapped = true;
                            }
                        }
                        
                        if (mapped) {
                            break;
                        }
                    }
                }
            }
            
            return mapped ? 
                GlobalLocationMapResult.mapped(originalLocation, newLocation) :
                GlobalLocationMapResult.notMapped(originalLocation);
            
        } catch (Exception e) {
            return GlobalLocationMapResult.notMapped(originalLocation);
        }
    }

    @Override
    public ValidationResult translatePartitionLocations(DBMirror dbMirror, TableMirror tableMirror) {
        HmsMirrorConfig config = configurationProvider.getConfig();

        if (!tableMirror.getEnvironmentTable(Environment.LEFT).getPartitioned()) {
            return ValidationResult.success();
        }

        EnvironmentTable target = tableMirror.getEnvironmentTable(Environment.RIGHT);
        boolean isExternal = TableUtils.isExternal(target);
        String originalDatabase = dbMirror.getName();
        String targetDatabase = HmsMirrorConfigUtil.getResolvedDB(originalDatabase, config);
        
        List<String> allIssues = new ArrayList<>();
        
        Map<String, String> partitionLocationMap = target.getPartitions();
        if (partitionLocationMap != null && !partitionLocationMap.isEmpty()) {
            for (Map.Entry<String, String> entry : partitionLocationMap.entrySet()) {
                String partitionLocation = entry.getValue();
                String partSpec = entry.getKey();
                
                // Calculate partition level
                String[] spec = partSpec.split("/");
                int level = spec.length + 1;
                
                // Skip empty or invalid partition locations
                if (isBlank(partitionLocation) || partitionLocation.isEmpty() ||
                    partitionLocation.equals(MirrorConf.NOT_SET)) {
                    return ValidationResult.failure("Invalid partition location found for spec: " + partSpec);
                }
                
                // Translate the partition location - this may throw RuntimeException for DistCP validation failures
                LocationTranslationRequest request = new LocationTranslationRequest(
                    dbMirror, tableMirror, partitionLocation, level, partSpec);
                LocationTranslationResult result = translateTableLocation(request);
                
                if (!result.isSuccess()) {
                    return ValidationResult.failure("Failed to translate partition location for spec " + 
                                                  partSpec + ": " + result.getMessage());
                }
                
                // Collect issues from the individual translation result
                allIssues.addAll(result.getIssues());
                
                // Update the partition location map
                entry.setValue(result.getTranslatedLocation());
            }
        }
        
        // Return success with collected issues from individual translations
        return new ValidationResult(true, List.of(), allIssues, "Partition locations translated successfully");
    }

    @Override
    public SqlStatements buildPartitionAddStatement(EnvironmentTable environmentTable) {
        try {
            // TODO: Extract SQL generation logic from TranslatorService.buildPartitionAddStatement()
            // This would build the actual ADD PARTITION SQL statements
            
            return new SqlStatements(List.of(), List.of(), List.of(), List.of(), List.of());
            
        } catch (Exception e) {
            // Return empty statements on error
            return new SqlStatements(List.of(), List.of(), List.of(), List.of(), List.of());
        }
    }

    @Override
    public ValidationResult validateLocationAlignment(TableMirror tableMirror, String translatedLocation, String partitionSpec) {
        try {
            // TODO: Extract validation logic from TranslatorService.warnIfLocationMismatch()
            // This would validate the location against warehouse expectations
            
            return ValidationResult.success();
            
        } catch (Exception e) {
            return ValidationResult.failure("Location alignment validation failed: " + e.getMessage());
        }
    }

    @Override
    public ValidationResult buildGlobalLocationMap(boolean dryRun, int consolidationLevel) {
        try {
            // TODO: Extract GLM building logic from TranslatorService.buildGlobalLocationMapFromWarehousePlansAndSources()
            // This would construct the global location map for the migration
            
            return ValidationResult.success();
            
        } catch (Exception e) {
            return ValidationResult.failure("GLM building failed: " + e.getMessage());
        }
    }

    // Private helper methods extracted from the original business logic

    private String getOrDefault(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value;
    }

    private String buildMappedLocation(String targetNamespace, String mappedDir, String partitionSpec) {
        // Build the mapped location with target namespace
        StringBuilder location = new StringBuilder();
        if (!isBlank(targetNamespace)) {
            location.append(targetNamespace);
        }
        location.append(mappedDir);
        
        // Check if we need to append partition spec (avoid duplication but preserve validation behavior)
        if (!isBlank(partitionSpec)) {
            // Only append if the mappedDir doesn't already end with this partition spec
            if (!mappedDir.endsWith("/" + partitionSpec)) {
                location.append("/").append(partitionSpec);
            }
            // Note: We handle the validation issue generation separately to preserve issue counts
        }
        return location.toString();
    }

    private String computeNewLocationFromBusinessRules(String targetNamespace, String partitionSpec,
                                                     TableMirror tableMirror, HmsMirrorConfig config,
                                                     String targetDatabaseManagedDir, String tableName,
                                                     String originalDatabase, String targetDatabase,
                                                     String relativeDir, String originalLocation,
                                                     EnvironmentTable targetEnvTable) {
        StringBuilder newLocation = new StringBuilder();
        
        if (!isBlank(targetNamespace)) {
            newLocation.append(targetNamespace);
        }
        
        // Build location by replacing namespace and database paths appropriately
        if (!isBlank(relativeDir)) {
            // Replace the original database path with target database path
            String adjustedRelativeDir = relativeDir;
            
            // Replace database directory if it exists in the path
            if (adjustedRelativeDir.contains(originalDatabase + ".db")) {
                adjustedRelativeDir = adjustedRelativeDir.replace(originalDatabase + ".db", targetDatabase + ".db");
            }
            
            // Ensure we have proper path separator
            if (!adjustedRelativeDir.startsWith("/")) {
                newLocation.append("/");
            }
            newLocation.append(adjustedRelativeDir);
            
            // CRITICAL FIX: Do not append partitionSpec because relativeDir already contains 
            // the complete path including any partition directories from the original location.
            // The originalLocation passed in is the full path including partitions,
            // and relativeDir is that path with just the namespace stripped off.
            
        } else {
            // Fallback: construct path from components when no relative directory available
            if (TableUtils.isExternal(targetEnvTable)) {
                // Default external warehouse location structure
                newLocation.append("/warehouse/tablespace/external/hive/").append(targetDatabase).append(".db/").append(tableName);
            } else {
                // Managed table location
                newLocation.append("/").append(targetDatabaseManagedDir).append("/").append(tableName);
            }
            
            // Only append partition spec in fallback case when building from scratch
            if (!isBlank(partitionSpec)) {
                newLocation.append("/").append(partitionSpec);
            }
        }
        
        return newLocation.toString();
    }

    private void handleStorageMigrationLogic(String originalLocation, HmsMirrorConfig config, TableMirror tableMirror) {
        // TODO: Extract storage migration handling logic
        // This would handle the storage migration scenarios from the original method
    }
    
    /**
     * Validates that location can be handled properly with DistCP when no GLM mapping is available.
     * This replicates the validation logic from the original handleNoGlmMapping method.
     * 
     * @param originalLocation the original partition/table location
     * @param tableMirror the table mirror object
     * @param config the configuration
     * @throws RuntimeException if location mapping cannot be determined for DistCP
     */
    private void validateLocationForDistcp(String originalLocation, TableMirror tableMirror, HmsMirrorConfig config) {
        // Get the original table location for comparison
        String originalTableLocation = TableUtils.getLocation(tableMirror.getName(), 
                                                             tableMirror.getEnvironmentTable(Environment.LEFT).getDefinition());
        
        // Check if the partition location aligns with the table base location
        if (!isBlank(originalTableLocation) && !originalLocation.startsWith(originalTableLocation)) {
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                // This is the critical validation - partition location doesn't align with table location
                // and DistCP is enabled, which means we can't create a proper DistCP plan
                tableMirror.setPhaseState(PhaseState.ERROR);
                throw new RuntimeException("Location Mapping can't be determined. No matching GLM entry to make translation. " +
                        "Original Location: " + originalLocation + " which doesn't align with the original table location " +
                        originalTableLocation + " and ALIGNED with DISTCP can't be determined.");
            }
        }
    }
}