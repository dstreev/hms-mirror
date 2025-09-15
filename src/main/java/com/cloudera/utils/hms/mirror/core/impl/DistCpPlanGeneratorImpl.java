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

import com.cloudera.utils.hms.mirror.core.api.DistCpPlanGenerator;
import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.infrastructure.configuration.ConfigurationProvider;

import java.util.*;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Core implementation of DistCp plan generation business logic.
 * This class contains pure business logic for creating distributed copy
 * plans and scripts without Spring dependencies.
 */
public class DistCpPlanGeneratorImpl implements DistCpPlanGenerator {

    private final ConfigurationProvider configurationProvider;

    public DistCpPlanGeneratorImpl(ConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
    }

    @Override
    public DistCpPlanResult generateDistCpPlan(DistCpPlanRequest request) {
        if (request == null) {
            return DistCpPlanResult.failure("DistCp plan request cannot be null", List.of("Null request"));
        }

        try {
            ValidationResult validation = validateDistCpPlan(request);
            if (!validation.isValid()) {
                return DistCpPlanResult.failure("Plan validation failed", validation.getErrors());
            }

            // Build source lists
            DistCpSourceListResult sourceListResult = buildDistCpSourceList(
                request.getDatabase(),
                request.getSourceEnvironment(),
                request.getConsolidationLevel(),
                request.isConsolidateTables()
            );

            if (!sourceListResult.isSuccess()) {
                return DistCpPlanResult.failure("Failed to build source lists", List.of(sourceListResult.getMessage()));
            }

            // Create job definitions
            List<DistCpJobDefinition> jobDefinitions = createJobDefinitions(request, sourceListResult);

            // Build target path mappings
            Map<String, String> targetPathMappings = buildTargetPathMappings(request);

            DistCpPlan plan = new DistCpPlan(
                request.getDatabase(),
                sourceListResult.getSourceLists(),
                targetPathMappings,
                jobDefinitions,
                sourceListResult.getEstimatedDataSize(),
                sourceListResult.getTotalPaths()
            );

            return DistCpPlanResult.success(plan, "DistCp plan generated successfully");

        } catch (Exception e) {
            return DistCpPlanResult.failure("Failed to generate DistCp plan: " + e.getMessage(), 
                                          List.of(e.getMessage()));
        }
    }

    @Override
    public DistCpSourceListResult buildDistCpSourceList(String database, Environment environment,
                                                       int consolidationLevel, boolean consolidateTables) {
        if (isBlank(database) || isNull(environment)) {
            return DistCpSourceListResult.failure("Database name and environment cannot be null");
        }

        try {
            // This is a simplified implementation
            // In the real implementation, this would:
            // 1. Query the metastore for table locations
            // 2. Analyze partition structures
            // 3. Consolidate paths based on consolidation level
            // 4. Group tables if consolidation is enabled

            Map<String, List<String>> sourceLists = new HashMap<>();
            
            // Mock source paths for demonstration
            if (consolidateTables) {
                List<String> consolidatedPaths = Arrays.asList(
                    "/warehouse/" + database + "/table1/",
                    "/warehouse/" + database + "/table2/",
                    "/warehouse/" + database + "/table3/"
                );
                sourceLists.put("consolidated", consolidatedPaths);
            } else {
                sourceLists.put("table1", List.of("/warehouse/" + database + "/table1/"));
                sourceLists.put("table2", List.of("/warehouse/" + database + "/table2/"));
                sourceLists.put("table3", List.of("/warehouse/" + database + "/table3/"));
            }

            return DistCpSourceListResult.success(sourceLists, sourceLists.size(), 1024L * 1024 * 1024); // 1GB
            
        } catch (Exception e) {
            return DistCpSourceListResult.failure("Failed to build source list: " + e.getMessage());
        }
    }

    @Override
    public DistCpScriptResult generateDistCpScripts(DistCpPlanRequest request) {
        if (request == null || !request.isGenerateScripts()) {
            return DistCpScriptResult.failure("Script generation not requested or invalid request");
        }

        try {
            DistCpPlanResult planResult = generateDistCpPlan(request);
            if (!planResult.isSuccess()) {
                return DistCpScriptResult.failure("Cannot generate scripts without valid plan");
            }

            StringBuilder scriptBuilder = new StringBuilder();
            scriptBuilder.append("#!/usr/bin/env sh\n");
            scriptBuilder.append("\n");
            scriptBuilder.append("# DistCP Migration Script for Database: ").append(request.getDatabase()).append("\n");
            scriptBuilder.append("# Generated by HMS-Mirror Core API\n");
            scriptBuilder.append("\n");

            // Add environment variable setup
            scriptBuilder.append("# Environment Setup\n");
            scriptBuilder.append("if [ -z ${HCFS_BASE_DIR+x} ]; then\n");
            scriptBuilder.append("  echo \"HCFS_BASE_DIR is unset\"\n");
            scriptBuilder.append("  exit 1\n");
            scriptBuilder.append("fi\n");
            scriptBuilder.append("\n");

            // Add job commands
            DistCpPlan plan = planResult.getPlan();
            for (DistCpJobDefinition job : plan.getJobDefinitions()) {
                scriptBuilder.append("# Job: ").append(job.getJobName()).append("\n");
                scriptBuilder.append("hadoop distcp");
                
                // Add options
                for (Map.Entry<String, String> option : job.getDistCpOptions().entrySet()) {
                    scriptBuilder.append(" ").append(option.getKey()).append("=").append(option.getValue());
                }
                
                scriptBuilder.append(" -f $HCFS_BASE_DIR/").append(job.getSourceListFile());
                scriptBuilder.append(" ").append(job.getTargetDirectory()).append("\n");
                scriptBuilder.append("\n");
            }

            List<String> scriptFiles = List.of(request.getOutputDirectory() + "/" + request.getDatabase() + "_distcp.sh");

            return DistCpScriptResult.success(scriptBuilder.toString(), scriptFiles);

        } catch (Exception e) {
            return DistCpScriptResult.failure("Failed to generate scripts: " + e.getMessage());
        }
    }

    @Override
    public DistCpWorkbookResult generateDistCpWorkbook(DistCpPlanRequest request) {
        if (request == null || !request.isGenerateWorkbook()) {
            return DistCpWorkbookResult.failure("Workbook generation not requested or invalid request");
        }

        try {
            DistCpPlanResult planResult = generateDistCpPlan(request);
            if (!planResult.isSuccess()) {
                return DistCpWorkbookResult.failure("Cannot generate workbook without valid plan");
            }

            StringBuilder workbookBuilder = new StringBuilder();
            workbookBuilder.append("# DistCP Migration Workbook\n");
            workbookBuilder.append("\n");
            workbookBuilder.append("## Database: ").append(request.getDatabase()).append("\n");
            workbookBuilder.append("- **Source Environment**: ").append(request.getSourceEnvironment()).append("\n");
            workbookBuilder.append("- **Target Environment**: ").append(request.getTargetEnvironment()).append("\n");
            workbookBuilder.append("- **Data Strategy**: ").append(request.getDataStrategy()).append("\n");
            workbookBuilder.append("\n");

            workbookBuilder.append("## Migration Plan Summary\n");
            DistCpPlan plan = planResult.getPlan();
            workbookBuilder.append("- **Total Jobs**: ").append(plan.getJobDefinitions().size()).append("\n");
            workbookBuilder.append("- **Estimated Data Size**: ").append(plan.getEstimatedDataSize() / (1024 * 1024)).append(" MB\n");
            workbookBuilder.append("- **Estimated File Count**: ").append(plan.getEstimatedFileCount()).append("\n");
            workbookBuilder.append("\n");

            workbookBuilder.append("## Job Details\n");
            workbookBuilder.append("| Job Name | Source Paths | Target Directory | Priority |\n");
            workbookBuilder.append("|:---|:---|:---|:---|\n");

            for (DistCpJobDefinition job : plan.getJobDefinitions()) {
                workbookBuilder.append("| ").append(job.getJobName());
                workbookBuilder.append(" | ").append(job.getSourcePaths().size()).append(" paths");
                workbookBuilder.append(" | ").append(job.getTargetDirectory());
                workbookBuilder.append(" | ").append(job.getPriority()).append(" |\n");
            }

            String workbookFile = request.getOutputDirectory() + "/" + request.getDatabase() + "_distcp_workbook.md";

            return DistCpWorkbookResult.success(workbookBuilder.toString(), workbookFile);

        } catch (Exception e) {
            return DistCpWorkbookResult.failure("Failed to generate workbook: " + e.getMessage());
        }
    }

    @Override
    public ValidationResult validateDistCpPlan(DistCpPlanRequest request) {
        if (request == null) {
            return ValidationResult.failure("DistCp plan request cannot be null");
        }

        List<String> errors = new ArrayList<>();

        if (isBlank(request.getDatabase())) {
            errors.add("Database name cannot be blank");
        }

        if (request.getSourceEnvironment() == null) {
            errors.add("Source environment cannot be null");
        }

        if (request.getTargetEnvironment() == null) {
            errors.add("Target environment cannot be null");
        }

        if (request.getConsolidationLevel() < 0) {
            errors.add("Consolidation level cannot be negative");
        }

        if (isBlank(request.getOutputDirectory())) {
            errors.add("Output directory cannot be blank");
        }

        // Strategy-specific validations
        if (request.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY) {
            errors.add("DistCP is not applicable for schema-only migrations");
        }

        if (errors.isEmpty()) {
            return ValidationResult.success();
        } else {
            return ValidationResult.failure(errors);
        }
    }

    @Override
    public DistCpExecutionRecommendationResult calculateExecutionRecommendations(DistCpPlanRequest request) {
        try {
            DistCpPlanResult planResult = generateDistCpPlan(request);
            if (!planResult.isSuccess()) {
                return new DistCpExecutionRecommendationResult(Map.of(), List.of("Cannot calculate without valid plan"), 1, 2048, "1MB");
            }

            DistCpPlan plan = planResult.getPlan();
            
            // Calculate recommendations based on data size
            long dataSizeBytes = plan.getEstimatedDataSize();
            int fileCount = plan.getEstimatedFileCount();
            
            // Recommend mappers (1 mapper per 256MB or 1000 files, whichever is greater)
            int recommendedMappers = Math.max(
                (int) Math.ceil(dataSizeBytes / (256.0 * 1024 * 1024)),
                Math.max(1, fileCount / 1000)
            );
            recommendedMappers = Math.min(recommendedMappers, 100); // Cap at 100

            // Recommend memory (2GB base + extra for large jobs)
            long recommendedMemoryMb = 2048;
            if (dataSizeBytes > 10L * 1024 * 1024 * 1024) { // > 10GB
                recommendedMemoryMb = 4096;
            }

            Map<String, String> options = new HashMap<>();
            options.put("-m", String.valueOf(recommendedMappers));
            options.put("-bandwidth", dataSizeBytes > 1024 * 1024 * 1024 ? "100" : "50"); // MB/s

            List<String> recommendations = Arrays.asList(
                "Use " + recommendedMappers + " mappers for optimal performance",
                "Allocate " + recommendedMemoryMb + "MB memory per mapper",
                "Consider running during off-peak hours for large transfers"
            );

            return new DistCpExecutionRecommendationResult(options, recommendations, 
                                                         recommendedMappers, recommendedMemoryMb, "100MB");
                                                         
        } catch (Exception e) {
            return new DistCpExecutionRecommendationResult(Map.of(), 
                List.of("Error calculating recommendations: " + e.getMessage()), 1, 2048, "1MB");
        }
    }

    @Override
    public PathAlignmentResult analyzePathAlignment(List<String> sourcePaths, String targetBasePath) {
        if (sourcePaths == null || sourcePaths.isEmpty()) {
            return PathAlignmentResult.misaligned(List.of(), List.of(), "No source paths provided");
        }

        try {
            // Find common base path
            String commonBase = findCommonBasePath(sourcePaths);
            
            List<String> alignedPaths = new ArrayList<>();
            List<String> misalignedPaths = new ArrayList<>();

            for (String path : sourcePaths) {
                if (path.startsWith(commonBase)) {
                    alignedPaths.add(path);
                } else {
                    misalignedPaths.add(path);
                }
            }

            if (misalignedPaths.isEmpty()) {
                return PathAlignmentResult.aligned(alignedPaths, commonBase);
            } else {
                return PathAlignmentResult.misaligned(alignedPaths, misalignedPaths, 
                    "Some paths do not share common base path");
            }
            
        } catch (Exception e) {
            return PathAlignmentResult.misaligned(List.of(), sourcePaths, 
                "Error analyzing paths: " + e.getMessage());
        }
    }

    @Override
    public DistCpOptimizationResult optimizeDistCpPlan(DistCpPlan originalPlan, 
                                                      DistCpOptimizationConfiguration config) {
        if (originalPlan == null || config == null) {
            return DistCpOptimizationResult.noOptimization(originalPlan, "Invalid input parameters");
        }

        try {
            List<String> optimizations = new ArrayList<>();
            
            // For now, return the original plan with potential optimization notes
            if (config.isEnableConsolidation()) {
                optimizations.add("Consolidation enabled - jobs may be merged for efficiency");
            }
            
            if (originalPlan.getJobDefinitions().size() > config.getMaxJobsPerBatch()) {
                optimizations.add("Plan may be split into multiple batches");
            }

            if (optimizations.isEmpty()) {
                return DistCpOptimizationResult.noOptimization(originalPlan, "No optimizations needed");
            } else {
                return DistCpOptimizationResult.optimized(originalPlan, optimizations);
            }
            
        } catch (Exception e) {
            return DistCpOptimizationResult.noOptimization(originalPlan, 
                "Error during optimization: " + e.getMessage());
        }
    }

    @Override
    public DistCpEstimationResult estimateExecution(DistCpPlan plan) {
        if (plan == null) {
            return new DistCpEstimationResult(0, 2048, 1, 0.0, "No plan provided");
        }

        try {
            long dataSizeBytes = plan.getEstimatedDataSize();
            int fileCount = plan.getEstimatedFileCount();
            
            // Estimate throughput (MB/s) - conservative estimate
            double throughputMbps = 50.0; // 50 MB/s average
            
            // Estimate duration
            long dataSizeMb = dataSizeBytes / (1024 * 1024);
            long estimatedDurationMinutes = (long) Math.ceil(dataSizeMb / (throughputMbps * 60));
            
            // Recommend mappers and memory
            int recommendedMappers = Math.max(1, Math.min(100, fileCount / 1000));
            long recommendedMemoryMb = recommendedMappers > 20 ? 4096 : 2048;
            
            String summary = String.format("Estimated %d minutes for %d MB with %d mappers", 
                                         estimatedDurationMinutes, dataSizeMb, recommendedMappers);

            return new DistCpEstimationResult(estimatedDurationMinutes, recommendedMemoryMb, 
                                            recommendedMappers, throughputMbps, summary);
                                            
        } catch (Exception e) {
            return new DistCpEstimationResult(0, 2048, 1, 0.0, "Error estimating: " + e.getMessage());
        }
    }

    // Helper methods

    private List<DistCpJobDefinition> createJobDefinitions(DistCpPlanRequest request, 
                                                         DistCpSourceListResult sourceListResult) {
        List<DistCpJobDefinition> jobDefinitions = new ArrayList<>();
        
        Map<String, String> defaultOptions = new HashMap<>();
        defaultOptions.put("-overwrite", "");
        defaultOptions.put("-update", "");
        
        int priority = 1;
        for (Map.Entry<String, List<String>> entry : sourceListResult.getSourceLists().entrySet()) {
            String jobName = request.getDatabase() + "_" + entry.getKey();
            String sourceListFile = jobName + "_sources.txt";
            String targetDirectory = "/target/warehouse/" + request.getDatabase() + "/" + entry.getKey();
            
            DistCpJobDefinition job = new DistCpJobDefinition(
                jobName,
                sourceListFile,
                targetDirectory,
                entry.getValue(),
                defaultOptions,
                priority++,
                30 // Estimated 30 minutes
            );
            
            jobDefinitions.add(job);
        }
        
        return jobDefinitions;
    }
    
    private Map<String, String> buildTargetPathMappings(DistCpPlanRequest request) {
        Map<String, String> targetMappings = new HashMap<>();
        
        if (request.getTables() != null) {
            for (String table : request.getTables()) {
                targetMappings.put(table, "/target/warehouse/" + request.getDatabase() + "/" + table);
            }
        } else {
            // Default mapping
            targetMappings.put(request.getDatabase(), "/target/warehouse/" + request.getDatabase());
        }
        
        return targetMappings;
    }
    
    private String findCommonBasePath(List<String> paths) {
        if (paths.isEmpty()) {
            return "";
        }
        
        if (paths.size() == 1) {
            String path = paths.get(0);
            int lastSlash = path.lastIndexOf('/');
            return lastSlash > 0 ? path.substring(0, lastSlash) : path;
        }
        
        String commonPath = paths.get(0);
        for (int i = 1; i < paths.size(); i++) {
            commonPath = findCommonPrefix(commonPath, paths.get(i));
        }
        
        // Ensure we end at a directory boundary
        int lastSlash = commonPath.lastIndexOf('/');
        return lastSlash > 0 ? commonPath.substring(0, lastSlash) : commonPath;
    }
    
    private String findCommonPrefix(String str1, String str2) {
        int minLength = Math.min(str1.length(), str2.length());
        for (int i = 0; i < minLength; i++) {
            if (str1.charAt(i) != str2.charAt(i)) {
                return str1.substring(0, i);
            }
        }
        return str1.substring(0, minLength);
    }
}