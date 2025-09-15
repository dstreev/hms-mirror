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
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;

import java.util.List;

/**
 * Core business interface for data strategy selection and management.
 * This interface defines pure business logic for selecting and managing
 * data migration strategies without Spring dependencies.
 */
public interface DataStrategySelector {

    /**
     * Selects the appropriate data strategy based on migration requirements.
     */
    DataStrategySelectionResult selectStrategy(DataStrategyRequest request);

    /**
     * Gets the default data strategy for a given configuration.
     */
    DataStrategy getDefaultStrategy();

    /**
     * Gets a specific data strategy by type.
     */
    DataStrategy getStrategy(DataStrategyEnum strategyType);

    /**
     * Lists all available data strategies.
     */
    List<DataStrategyInfo> getAvailableStrategies();

    /**
     * Validates that a data strategy is compatible with the migration requirements.
     */
    ValidationResult validateStrategyCompatibility(DataStrategyEnum strategyType, 
                                                 DataStrategyRequest request);

    /**
     * Gets strategy recommendations based on source and target environments.
     */
    StrategyRecommendationResult getStrategyRecommendations(DataStrategyRequest request);

    /**
     * Validates data strategy configuration.
     */
    ValidationResult validateStrategyConfiguration(DataStrategyEnum strategyType, 
                                                 MigrationConfiguration config);

    /**
     * Determines if a strategy requires specific prerequisites.
     */
    PrerequisiteCheckResult checkPrerequisites(DataStrategyEnum strategyType, 
                                             MigrationConfiguration config);
}