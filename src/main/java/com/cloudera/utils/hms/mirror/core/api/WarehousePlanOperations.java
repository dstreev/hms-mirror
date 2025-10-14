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

package com.cloudera.utils.hms.mirror.core.api;

import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import java.util.Map;

/**
 * Core business interface for warehouse plan operations.
 * This interface defines pure business logic for managing warehouse plans
 * and location mappings without Spring dependencies.
 */
public interface WarehousePlanOperations {

    /**
     * Creates a new warehouse plan for a database.
     */
    WarehousePlanResult createWarehousePlan(WarehousePlanRequest request);

    /**
     * Removes an existing warehouse plan for a database.
     */
    WarehousePlanResult removeWarehousePlan(String database);

    /**
     * Retrieves a warehouse plan for a specific database.
     */
    Warehouse getWarehousePlan(String database);

    /**
     * Retrieves all warehouse plans.
     */
    Map<String, Warehouse> getAllWarehousePlans();

    /**
     * Clears all warehouse plans.
     */
    void clearWarehousePlans();

    /**
     * Validates a warehouse plan configuration.
     */
    ValidationResult validateWarehousePlan(WarehousePlanRequest request);

    /**
     * Gets warehouse directories for a specific environment and database.
     */
    WarehouseDirectories getWarehouseDirectories(String database, Environment environment);

    /**
     * Builds warehouse source mappings based on configuration.
     */
    WarehouseSourceMappingResult buildWarehouseSources(int consolidationLevelBase, boolean partitionLevelMismatch);

    /**
     * Validates that external and managed locations are properly configured.
     */
    ValidationResult validateWarehouseLocations(String external, String managed);
}