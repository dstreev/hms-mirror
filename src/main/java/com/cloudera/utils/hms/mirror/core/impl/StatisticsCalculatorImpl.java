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
import com.cloudera.utils.hms.mirror.core.api.StatisticsCalculator;
import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.domain.support.SerdeType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Core implementation of statistics calculation business logic.
 * This class contains pure calculation logic without Spring dependencies,
 * making it testable and reusable across different application contexts.
 */
public class StatisticsCalculatorImpl implements StatisticsCalculator {

    @Override
    public TezGroupingCalculationResult calculateTezMaxGrouping(TableStatistics tableStats) {
        if (tableStats == null) {
            return TezGroupingCalculationResult.failure("Table statistics cannot be null");
        }

        try {
            SerdeTypeResult serdeResult = determineSerdeType(tableStats.getStatistics());
            SerdeType serdeType = serdeResult.getSerdeType();
            
            Long maxGrouping = serdeType.getTargetSize() * 2L;
            
            if (tableStats.isPartitioned() && nonNull(tableStats.getAvgFileSize())) {
                Double avgFileSize = tableStats.getAvgFileSize();
                // If not 50% of target size, adjust grouping
                if (avgFileSize < serdeType.getTargetSize() * 0.5) {
                    maxGrouping = (long) (serdeType.getTargetSize() / 2);
                }
            }
            
            return TezGroupingCalculationResult.success(maxGrouping, serdeType);
            
        } catch (Exception e) {
            return TezGroupingCalculationResult.failure("Failed to calculate Tez grouping: " + e.getMessage());
        }
    }

    @Override
    public SerdeTypeResult determineSerdeType(Map<String, Object> statistics) {
        if (statistics == null) {
            return SerdeTypeResult.unknown("", "Statistics map is null");
        }

        String fileFormat = statistics.getOrDefault(MirrorConf.FILE_FORMAT, "UNKNOWN").toString();
        
        if (isNull(fileFormat) || "UNKNOWN".equals(fileFormat)) {
            return SerdeTypeResult.unknown(fileFormat, "File format not specified or unknown");
        }
        
        try {
            SerdeType serdeType = SerdeType.valueOf(fileFormat);
            return SerdeTypeResult.detected(serdeType, fileFormat);
        } catch (IllegalArgumentException e) {
            return SerdeTypeResult.unknown(fileFormat, "Unable to determine type for file format: " + fileFormat);
        }
    }

    @Override
    public DistributedPartitionResult generateDistributedPartitionElements(TableStatistics tableStats,
                                                                          OptimizationConfiguration optimizationConfig) {
        if (tableStats == null || optimizationConfig == null) {
            return DistributedPartitionResult.failure("Table statistics and optimization config cannot be null");
        }

        try {
            StringBuilder sb = new StringBuilder();
            
            if (tableStats.isPartitioned() && 
                optimizationConfig.isAutoTune() && 
                !optimizationConfig.isSkipStatsCollection()) {
                
                if (nonNull(tableStats.getDataSize())) {
                    PartitionDistributionResult ratioResult = calculatePartitionDistributionRatio(tableStats);
                    
                    if (ratioResult.isSuccess() && ratioResult.getDistributionRatio() >= 1) {
                        sb.append("ROUND((rand() * 1000) % ")
                          .append(ratioResult.getDistributionRatio())
                          .append(")");
                    }
                }
            }
            
            return DistributedPartitionResult.success(sb.toString(), 
                                                    tableStats.isPartitioned() ? 
                                                    (long) tableStats.getPartitionCount() : 0L);
                                                    
        } catch (Exception e) {
            return DistributedPartitionResult.failure("Failed to generate partition elements: " + e.getMessage());
        }
    }

    @Override
    public PartitionDistributionResult calculatePartitionDistributionRatio(TableStatistics tableStats) {
        if (tableStats == null) {
            return PartitionDistributionResult.failure("Table statistics cannot be null");
        }

        try {
            if (tableStats.getPartitionCount() == 0 || tableStats.getDataSize() == null) {
                return PartitionDistributionResult.success(0L, 0L);
            }
            
            SerdeTypeResult serdeResult = determineSerdeType(tableStats.getStatistics());
            SerdeType serdeType = serdeResult.getSerdeType();
            
            Long dataSize = tableStats.getDataSize();
            Long avgPartSize = Math.floorDiv(dataSize, (long) tableStats.getPartitionCount());
            Long ratio = Math.floorDiv(avgPartSize, (long) serdeType.getTargetSize()) - 1;
            
            return PartitionDistributionResult.success(Math.max(0L, ratio), avgPartSize);
            
        } catch (Exception e) {
            return PartitionDistributionResult.failure("Failed to calculate partition distribution ratio: " + e.getMessage());
        }
    }

    @Override
    public SessionOptimizationResult generateSessionOptimizations(TableStatistics controlStats,
                                                                OptimizationConfiguration config) {
        if (controlStats == null || config == null) {
            return SessionOptimizationResult.failure("Control statistics and configuration cannot be null");
        }

        try {
            Map<String, String> sessionSettings = new HashMap<>();
            List<String> recommendations = new java.util.ArrayList<>();

            if (config.isSkipStatsCollection()) {
                return SessionOptimizationResult.success(sessionSettings, 
                    List.of("Stats collection is disabled"));
            }

            // Small file optimization
            SerdeTypeResult serdeResult = determineSerdeType(controlStats.getStatistics());
            SerdeType serdeType = serdeResult.getSerdeType();
            
            if (nonNull(controlStats.getAvgFileSize())) {
                Double avgFileSize = controlStats.getAvgFileSize();
                if (avgFileSize < serdeType.getTargetSize() * 0.5) {
                    TezGroupingCalculationResult tezResult = calculateTezMaxGrouping(controlStats);
                    if (tezResult.isSuccess()) {
                        sessionSettings.put("tez.grouping.max-size", String.valueOf(tezResult.getMaxGroupingSize()));
                        recommendations.add("Setting TEZ max grouping to account for small files");
                    }
                }
            }

            // Compression recommendations
            if (config.isEnableCompression()) {
                CompressionRecommendationResult compressionResult = calculateCompressionRecommendations(controlStats);
                if (compressionResult.isRecommended()) {
                    sessionSettings.put("hive.exec.compress.output", "true");
                    sessionSettings.put("mapred.output.compression.codec", compressionResult.getCompressionType());
                    recommendations.add("Enabling compression: " + compressionResult.getReason());
                }
            }

            return SessionOptimizationResult.success(sessionSettings, recommendations);
            
        } catch (Exception e) {
            return SessionOptimizationResult.failure("Failed to generate session optimizations: " + e.getMessage());
        }
    }

    @Override
    public ValidationResult validateOptimizationConfiguration(OptimizationConfiguration config) {
        if (config == null) {
            return ValidationResult.failure("Optimization configuration cannot be null");
        }

        if (config.getDefaultReducerCount() < 1) {
            return ValidationResult.failure("Default reducer count must be at least 1");
        }

        if (config.getDefaultTargetFileSize() <= 0) {
            return ValidationResult.failure("Default target file size must be greater than 0");
        }

        return ValidationResult.success();
    }

    @Override
    public CompressionRecommendationResult calculateCompressionRecommendations(TableStatistics tableStats) {
        if (tableStats == null) {
            return CompressionRecommendationResult.notRecommended("Table statistics cannot be null");
        }

        try {
            SerdeTypeResult serdeResult = determineSerdeType(tableStats.getStatistics());
            SerdeType serdeType = serdeResult.getSerdeType();
            
            // Recommend compression for text formats with large data sizes
            if (serdeType == SerdeType.TEXT && tableStats.getDataSize() != null && tableStats.getDataSize() > 1024 * 1024 * 100) {
                return CompressionRecommendationResult.recommended(
                    "org.apache.hadoop.io.compress.GzipCodec",
                    "Large text files benefit from compression",
                    0.3 // Expected 30% of original size
                );
            }
            
            // ORC and Parquet already have built-in compression
            if (serdeType == SerdeType.ORC || serdeType == SerdeType.PARQUET) {
                return CompressionRecommendationResult.notRecommended(
                    "Format already includes efficient compression"
                );
            }
            
            return CompressionRecommendationResult.notRecommended(
                "No specific compression recommendation for this format and size"
            );
            
        } catch (Exception e) {
            return CompressionRecommendationResult.notRecommended("Error calculating compression: " + e.getMessage());
        }
    }

    @Override
    public AutoStatsResult calculateAutoStatsSettings(TableStatistics tableStats, OptimizationConfiguration config) {
        if (tableStats == null || config == null) {
            return AutoStatsResult.disabled("Table statistics or configuration is null");
        }

        if (config.isSkipStatsCollection()) {
            return AutoStatsResult.disabled("Stats collection is disabled in configuration");
        }

        // Enable table stats for all tables, column stats only for smaller tables
        boolean enableColumnStats = tableStats.getDataSize() != null && tableStats.getDataSize() < 1024 * 1024 * 1024; // < 1GB
        
        return AutoStatsResult.enabled(true, enableColumnStats, 
            enableColumnStats ? "Table and column stats enabled" : "Only table stats enabled for large table");
    }

    @Override
    public ReducerRecommendationResult calculateReducerRecommendations(TableStatistics tableStats) {
        if (tableStats == null) {
            return ReducerRecommendationResult.calculated(1, "default", "Table statistics not available");
        }

        try {
            if (tableStats.getDataSize() == null) {
                return ReducerRecommendationResult.calculated(1, "default", "Data size not available");
            }
            
            // Calculate reducers based on data size (1 reducer per 256MB)
            long dataSizeBytes = tableStats.getDataSize();
            int calculatedReducers = (int) Math.max(1, Math.ceil(dataSizeBytes / (256.0 * 1024 * 1024)));
            
            // Cap at reasonable maximum
            int recommendedReducers = Math.min(calculatedReducers, 100);
            
            return ReducerRecommendationResult.calculated(recommendedReducers, "data_size_based", 
                "Calculated " + recommendedReducers + " reducers for " + dataSizeBytes + " bytes of data");
                
        } catch (Exception e) {
            return ReducerRecommendationResult.calculated(1, "fallback", "Error calculating: " + e.getMessage());
        }
    }
}