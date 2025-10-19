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
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;

/**
 * Core business interface for location translation operations.
 * This interface defines pure business logic for translating table and partition locations
 * during migration operations, without Spring dependencies.
 */
public interface LocationTranslator {

    /**
     * Translates a table location from source to target environment.
     * This is the main location translation algorithm that handles warehouse mappings,
     * global location maps, and various migration strategies.
     */
    LocationTranslationResult translateTableLocation(LocationTranslationRequest request);

    /**
     * Translates all partition locations for a table.
     * Returns validation result indicating success or failure with reasons.
     */
    ValidationResult translatePartitionLocations(DBMirror dbMirror, TableMirror tableMirror);

    /**
     * Processes a location through the Global Location Map (GLM).
     * Returns mapping result indicating if and how the location was mapped.
     */
    GlobalLocationMapResult processGlobalLocationMap(String originalLocation, boolean isExternalTable);

    /**
     * Builds SQL statement for adding partitions to a table.
     * Generates the partition addition SQL based on partition specifications and locations.
     */
    SqlStatements buildPartitionAddStatement(EnvironmentTable environmentTable);

    /**
     * Validates that a translated location matches warehouse expectations.
     * Checks if the location aligns with external/managed warehouse directories.
     */
    ValidationResult validateLocationAlignment(TableMirror tableMirror, String translatedLocation, 
                                             String partitionSpec);

    /**
     * Builds global location map from warehouse plans and sources.
     * This is used for constructing the GLM during migration planning.
     */
    ValidationResult buildGlobalLocationMap(boolean dryRun, int consolidationLevel);
}