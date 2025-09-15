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
import com.cloudera.utils.hms.mirror.domain.support.SerdeType;

import java.util.Map;

/**
 * Core business interface for statistics calculation and optimization.
 * This interface defines pure business logic for calculating statistics,
 * optimization settings, and performance tuning without Spring dependencies.
 */
public interface StatisticsCalculator {

    /**
     * Calculates the maximum Tez grouping size for a table.
     */
    TezGroupingCalculationResult calculateTezMaxGrouping(TableStatistics tableStats);

    /**
     * Determines the SerDe type from table statistics.
     */
    SerdeTypeResult determineSerdeType(Map<String, Object> statistics);

    /**
     * Generates distributed partition elements for optimization.
     */
    DistributedPartitionResult generateDistributedPartitionElements(TableStatistics tableStats, 
                                                                   OptimizationConfiguration optimizationConfig);

    /**
     * Calculates partition distribution ratio for file distribution.
     */
    PartitionDistributionResult calculatePartitionDistributionRatio(TableStatistics tableStats);

    /**
     * Generates session optimization settings.
     */
    SessionOptimizationResult generateSessionOptimizations(TableStatistics controlStats, 
                                                          OptimizationConfiguration config);

    /**
     * Validates optimization configuration.
     */
    ValidationResult validateOptimizationConfiguration(OptimizationConfiguration config);

    /**
     * Calculates compression settings based on file format and size.
     */
    CompressionRecommendationResult calculateCompressionRecommendations(TableStatistics tableStats);

    /**
     * Determines auto-stats collection settings.
     */
    AutoStatsResult calculateAutoStatsSettings(TableStatistics tableStats, 
                                              OptimizationConfiguration config);

    /**
     * Calculates reducer count recommendations.
     */
    ReducerRecommendationResult calculateReducerRecommendations(TableStatistics tableStats);
}