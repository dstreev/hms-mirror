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
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import java.util.List;

/**
 * Core business interface for DistCp plan generation and management.
 * This interface defines pure business logic for creating distributed copy
 * plans and scripts without Spring dependencies.
 */
public interface DistCpPlanGenerator {

    /**
     * Generates a complete DistCp plan for a database migration.
     */
    DistCpPlanResult generateDistCpPlan(DistCpPlanRequest request);

    /**
     * Builds DistCp source file lists for a database.
     */
    DistCpSourceListResult buildDistCpSourceList(String database, Environment environment,
                                                int consolidationLevel, boolean consolidateTables);

    /**
     * Generates executable DistCp scripts.
     */
    DistCpScriptResult generateDistCpScripts(DistCpPlanRequest request);

    /**
     * Creates DistCp workbook documentation.
     */
    DistCpWorkbookResult generateDistCpWorkbook(DistCpPlanRequest request);

    /**
     * Validates DistCp plan configuration.
     */
    ValidationResult validateDistCpPlan(DistCpPlanRequest request);

    /**
     * Calculates DistCp execution requirements and recommendations.
     */
    DistCpExecutionRecommendationResult calculateExecutionRecommendations(DistCpPlanRequest request);

    /**
     * Analyzes path alignment for DistCp compatibility.
     */
    PathAlignmentResult analyzePathAlignment(List<String> sourcePaths, String targetBasePath);

    /**
     * Optimizes DistCp plans for better performance.
     */
    DistCpOptimizationResult optimizeDistCpPlan(DistCpPlan originalPlan, 
                                               DistCpOptimizationConfiguration config);

    /**
     * Estimates DistCp execution time and resources.
     */
    DistCpEstimationResult estimateExecution(DistCpPlan plan);
}