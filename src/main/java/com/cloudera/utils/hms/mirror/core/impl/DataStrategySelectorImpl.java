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

import com.cloudera.utils.hms.mirror.core.api.DataStrategySelector;
import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;

import java.util.*;

import static java.util.Objects.isNull;

/**
 * Core implementation of data strategy selection business logic.
 * This class contains pure strategy selection logic without Spring dependencies,
 * making it testable and reusable across different application contexts.
 */
public class DataStrategySelectorImpl implements DataStrategySelector {

    private final Map<DataStrategyEnum, DataStrategy> strategies;
    private final DataStrategy defaultStrategy;

    public DataStrategySelectorImpl(Map<DataStrategyEnum, DataStrategy> strategies, 
                                  DataStrategy defaultStrategy) {
        this.strategies = new EnumMap<>(strategies);
        this.defaultStrategy = defaultStrategy;
    }

    @Override
    public DataStrategySelectionResult selectStrategy(DataStrategyRequest request) {
        if (request == null) {
            return DataStrategySelectionResult.failure("Data strategy request cannot be null");
        }

        try {
            DataStrategyEnum preferredStrategy = request.getPreferredStrategy();
            
            // If no preferred strategy, recommend one
            if (isNull(preferredStrategy)) {
                StrategyRecommendationResult recommendation = getStrategyRecommendations(request);
                if (!recommendation.getRecommendedStrategies().isEmpty()) {
                    preferredStrategy = recommendation.getPrimaryRecommendation();
                } else {
                    preferredStrategy = DataStrategyEnum.SCHEMA_ONLY; // Safe fallback
                }
            }

            // Validate strategy compatibility
            ValidationResult validation = validateStrategyCompatibility(preferredStrategy, request);
            if (!validation.isValid()) {
                return DataStrategySelectionResult.failure(
                    "Strategy " + preferredStrategy + " is not compatible: " + validation.getMessage()
                );
            }

            DataStrategy selectedStrategy = strategies.getOrDefault(preferredStrategy, defaultStrategy);
            
            return DataStrategySelectionResult.success(selectedStrategy, preferredStrategy,
                "Strategy selected based on request parameters");
                
        } catch (Exception e) {
            return DataStrategySelectionResult.failure("Failed to select strategy: " + e.getMessage());
        }
    }

    @Override
    public DataStrategy getDefaultStrategy() {
        return defaultStrategy;
    }

    @Override
    public DataStrategy getStrategy(DataStrategyEnum strategyType) {
        if (strategyType == null) {
            return defaultStrategy;
        }
        return strategies.getOrDefault(strategyType, defaultStrategy);
    }

    @Override
    public List<DataStrategyInfo> getAvailableStrategies() {
        List<DataStrategyInfo> strategyInfos = new ArrayList<>();
        
        for (DataStrategyEnum strategy : DataStrategyEnum.values()) {
            if (strategies.containsKey(strategy)) {
                strategyInfos.add(createStrategyInfo(strategy));
            }
        }
        
        return strategyInfos;
    }

    @Override
    public ValidationResult validateStrategyCompatibility(DataStrategyEnum strategyType, 
                                                        DataStrategyRequest request) {
        if (strategyType == null || request == null) {
            return ValidationResult.failure("Strategy type and request cannot be null");
        }

        List<String> errors = new ArrayList<>();
        
        // Check ACID table compatibility
        if (request.isAcidTablesPresent()) {
            Set<DataStrategyEnum> acidCompatible = Set.of(
                DataStrategyEnum.ACID,
                DataStrategyEnum.HYBRID_ACID_DOWNGRADE_INPLACE,
                DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE,
                DataStrategyEnum.EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE
            );
            
            if (!acidCompatible.contains(strategyType)) {
                errors.add("Strategy " + strategyType + " does not support ACID tables");
            }
        }

        // Check data inclusion compatibility
        if (request.isIncludeData()) {
            Set<DataStrategyEnum> dataCompatible = Set.of(
                DataStrategyEnum.HYBRID,
                DataStrategyEnum.SQL,
                DataStrategyEnum.EXPORT_IMPORT,
                DataStrategyEnum.STORAGE_MIGRATION,
                DataStrategyEnum.LINKED,
                DataStrategyEnum.INTERMEDIATE,
                DataStrategyEnum.ACID
            );
            
            if (!dataCompatible.contains(strategyType) && strategyType != DataStrategyEnum.DUMP) {
                errors.add("Strategy " + strategyType + " does not support data migration");
            }
        }

        // Check view compatibility
        if (request.isViewsPresent() && strategyType == DataStrategyEnum.STORAGE_MIGRATION) {
            errors.add("Storage migration strategy does not support views");
        }

        if (errors.isEmpty()) {
            return ValidationResult.success();
        } else {
            return ValidationResult.failure(errors);
        }
    }

    @Override
    public StrategyRecommendationResult getStrategyRecommendations(DataStrategyRequest request) {
        if (request == null) {
            return StrategyRecommendationResult.recommendation(
                List.of(DataStrategyEnum.SCHEMA_ONLY),
                DataStrategyEnum.SCHEMA_ONLY,
                "Default recommendation due to null request",
                List.of()
            );
        }

        List<DataStrategyEnum> recommendations = new ArrayList<>();
        List<String> considerations = new ArrayList<>();

        // Schema-only is always safe
        recommendations.add(DataStrategyEnum.SCHEMA_ONLY);

        // If data is needed
        if (request.isIncludeData()) {
            if (request.getEstimatedDataSize() < 1024 * 1024 * 1024) { // < 1GB
                recommendations.add(0, DataStrategyEnum.EXPORT_IMPORT);
                considerations.add("Export/Import recommended for small datasets");
            } else {
                recommendations.add(0, DataStrategyEnum.HYBRID);
                considerations.add("Hybrid strategy recommended for large datasets");
            }
        }

        // ACID table considerations
        if (request.isAcidTablesPresent()) {
            recommendations.add(0, DataStrategyEnum.ACID);
            considerations.add("ACID strategy required for transactional tables");
        }

        // View considerations
        if (request.isViewsPresent()) {
            considerations.add("Views require careful handling with most strategies");
        }

        DataStrategyEnum primary = recommendations.isEmpty() ? 
            DataStrategyEnum.SCHEMA_ONLY : recommendations.get(0);

        return StrategyRecommendationResult.recommendation(
            recommendations,
            primary,
            "Recommendations based on data characteristics",
            considerations
        );
    }

    @Override
    public ValidationResult validateStrategyConfiguration(DataStrategyEnum strategyType, 
                                                        MigrationConfiguration config) {
        if (strategyType == null || config == null) {
            return ValidationResult.failure("Strategy type and configuration cannot be null");
        }

        List<String> errors = new ArrayList<>();

        // Validate parallelism
        if (config.getParallelism() < 1) {
            errors.add("Parallelism must be at least 1");
        }

        // Strategy-specific validations
        switch (strategyType) {
            case STORAGE_MIGRATION:
                if (!config.isEnableDistCp()) {
                    errors.add("Storage migration requires DistCP to be enabled");
                }
                break;
            case EXPORT_IMPORT:
                if (config.getParallelism() > 1 && !config.isDryRun()) {
                    errors.add("Export/Import with parallelism > 1 requires careful coordination");
                }
                break;
        }

        if (errors.isEmpty()) {
            return ValidationResult.success();
        } else {
            return ValidationResult.failure(errors);
        }
    }

    @Override
    public PrerequisiteCheckResult checkPrerequisites(DataStrategyEnum strategyType, 
                                                     MigrationConfiguration config) {
        if (strategyType == null || config == null) {
            return PrerequisiteCheckResult.partiallyMet(
                List.of("Strategy type and configuration required"),
                List.of()
            );
        }

        List<String> missing = new ArrayList<>();
        List<String> satisfied = new ArrayList<>();

        // Check strategy-specific prerequisites
        switch (strategyType) {
            case STORAGE_MIGRATION:
                if (config.isEnableDistCp()) {
                    satisfied.add("DistCP enabled");
                } else {
                    missing.add("DistCP must be enabled for storage migration");
                }
                break;
                
            case HYBRID:
            case SQL:
                satisfied.add("SQL execution capability");
                break;
                
            case EXPORT_IMPORT:
                satisfied.add("Export/Import tools available");
                break;
                
            default:
                satisfied.add("Basic prerequisites met");
                break;
        }

        if (missing.isEmpty()) {
            return PrerequisiteCheckResult.allSatisfied(satisfied);
        } else {
            return PrerequisiteCheckResult.partiallyMet(missing, satisfied);
        }
    }

    private DataStrategyInfo createStrategyInfo(DataStrategyEnum strategy) {
        switch (strategy) {
            case SCHEMA_ONLY:
                return new DataStrategyInfo(strategy, "Schema Only", 
                    "Migrates only table schemas without data",
                    false, false, true, List.of());
                    
            case HYBRID:
                return new DataStrategyInfo(strategy, "Hybrid", 
                    "Combines SQL and DistCP for optimal performance",
                    true, false, true, List.of("SQL execution", "DistCP"));
                    
            case SQL:
                return new DataStrategyInfo(strategy, "SQL", 
                    "Uses SQL INSERT statements for data transfer",
                    true, false, true, List.of("SQL execution"));
                    
            case EXPORT_IMPORT:
                return new DataStrategyInfo(strategy, "Export/Import", 
                    "Uses Hive export/import functionality",
                    true, false, false, List.of("Export/Import tools"));
                    
            case STORAGE_MIGRATION:
                return new DataStrategyInfo(strategy, "Storage Migration", 
                    "Migrates data using DistCP",
                    true, false, false, List.of("DistCP", "Storage access"));
                    
            case ACID:
                return new DataStrategyInfo(strategy, "ACID", 
                    "Handles transactional ACID tables",
                    true, true, true, List.of("ACID support"));
                    
            case LINKED:
                return new DataStrategyInfo(strategy, "Linked", 
                    "Creates linked tables pointing to original data",
                    true, false, false, List.of("Cross-cluster access"));
                    
            case DUMP:
                return new DataStrategyInfo(strategy, "Dump", 
                    "Exports metadata and data for analysis",
                    true, false, true, List.of());
                    
            default:
                return new DataStrategyInfo(strategy, strategy.name(), 
                    "Strategy: " + strategy.name(),
                    false, false, false, List.of());
        }
    }
}