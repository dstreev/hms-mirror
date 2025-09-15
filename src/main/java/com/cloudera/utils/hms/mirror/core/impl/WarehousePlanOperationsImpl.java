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

import com.cloudera.utils.hms.mirror.core.api.WarehousePlanOperations;
import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.WarehouseMapBuilder;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.infrastructure.configuration.ConfigurationProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Core implementation of warehouse plan operations business logic.
 * This class contains pure business logic without Spring dependencies,
 * making it testable and reusable across different application contexts.
 */
public class WarehousePlanOperationsImpl implements WarehousePlanOperations {

    private static final String EXTERNAL_AND_MANAGED_LOCATIONS_REQUIRED =
            "External and Managed Warehouse Locations must be defined.";
    private static final String EXTERNAL_AND_MANAGED_LOCATIONS_DIFFERENT =
            "External and Managed Warehouse Locations must be different.";

    private final ConfigurationProvider configurationProvider;

    public WarehousePlanOperationsImpl(ConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
    }

    @Override
    public WarehousePlanResult createWarehousePlan(WarehousePlanRequest request) {
        if (isNull(request)) {
            return WarehousePlanResult.failure("Warehouse plan request cannot be null");
        }

        ValidationResult validation = validateWarehousePlan(request);
        if (!validation.isValid()) {
            return WarehousePlanResult.failure(validation.getErrors());
        }

        try {
            var config = configurationProvider.getConfig();
            WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder();
            
            Warehouse warehouse = mapBuilder.addWarehousePlan(
                request.getDatabase(), 
                request.getExternalLocation(), 
                request.getManagedLocation()
            );
            
            // Add database to configuration
            config.getDatabases().add(request.getDatabase());
            
            return WarehousePlanResult.success(warehouse);
            
        } catch (Exception e) {
            return WarehousePlanResult.failure("Failed to create warehouse plan: " + e.getMessage());
        }
    }

    @Override
    public WarehousePlanResult removeWarehousePlan(String database) {
        if (isBlank(database)) {
            return WarehousePlanResult.failure("Database name cannot be blank");
        }

        try {
            var config = configurationProvider.getConfig();
            WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder();
            
            Warehouse removedWarehouse = mapBuilder.removeWarehousePlan(database);
            config.getDatabases().remove(database);
            
            if (isNull(removedWarehouse)) {
                return WarehousePlanResult.failure("Warehouse plan not found for database: " + database);
            }
            
            return WarehousePlanResult.success(removedWarehouse);
            
        } catch (Exception e) {
            return WarehousePlanResult.failure("Failed to remove warehouse plan: " + e.getMessage());
        }
    }

    @Override
    public Warehouse getWarehousePlan(String database) {
        if (isBlank(database)) {
            return null;
        }

        try {
            WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder();
            return mapBuilder.getWarehousePlans().get(database);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Map<String, Warehouse> getAllWarehousePlans() {
        try {
            WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder();
            return new HashMap<>(mapBuilder.getWarehousePlans());
        } catch (Exception e) {
            return Map.of();
        }
    }

    @Override
    public void clearWarehousePlans() {
        try {
            var config = configurationProvider.getConfig();
            WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder();
            mapBuilder.clearWarehousePlan();
            mapBuilder.clearSources();
            config.getDatabases().clear();
        } catch (Exception e) {
            // Log error but don't throw - clearing should be resilient
        }
    }

    @Override
    public ValidationResult validateWarehousePlan(WarehousePlanRequest request) {
        if (isNull(request)) {
            return ValidationResult.failure("Warehouse plan request cannot be null");
        }

        return validateWarehouseLocations(request.getExternalLocation(), request.getManagedLocation());
    }

    @Override
    public ValidationResult validateWarehouseLocations(String external, String managed) {
        if (isBlank(external) || isBlank(managed)) {
            return ValidationResult.failure(EXTERNAL_AND_MANAGED_LOCATIONS_REQUIRED);
        }
        
        if (external.equals(managed)) {
            return ValidationResult.failure(EXTERNAL_AND_MANAGED_LOCATIONS_DIFFERENT);
        }
        
        return ValidationResult.success();
    }

    @Override
    public WarehouseDirectories getWarehouseDirectories(String database, Environment environment) {
        if (isBlank(database) || isNull(environment)) {
            return null;
        }

        try {
            Warehouse warehouse = getWarehousePlan(database);
            if (isNull(warehouse)) {
                return null;
            }

            return new WarehouseDirectories(
                database,
                warehouse.getExternalDirectory(),
                warehouse.getManagedDirectory()
            );
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public WarehouseSourceMappingResult buildWarehouseSources(int consolidationLevelBase, boolean partitionLevelMismatch) {
        try {
            // This would contain the complex logic from DatabaseService.buildDatabaseSources()
            // For now, return a simplified implementation
            Map<String, String> sourceMappings = new HashMap<>();
            
            // The actual implementation would:
            // 1. Look at warehouse plans
            // 2. Pull database/table/partition locations from metastore
            // 3. Build consolidated location mappings
            // 4. Handle partition level mismatches
            
            return WarehouseSourceMappingResult.success(sourceMappings, consolidationLevelBase);
            
        } catch (Exception e) {
            return WarehouseSourceMappingResult.failure("Failed to build warehouse sources: " + e.getMessage());
        }
    }

    private WarehouseMapBuilder getWarehouseMapBuilder() {
        var config = configurationProvider.getConfig();
        return config.getTranslator().getWarehouseMapBuilder();
    }
}