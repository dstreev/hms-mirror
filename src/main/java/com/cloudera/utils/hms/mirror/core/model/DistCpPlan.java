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

package com.cloudera.utils.hms.mirror.core.model;

import java.util.List;
import java.util.Map;

/**
 * Represents a complete DistCp execution plan.
 */
public class DistCpPlan {
    private final String database;
    private final Map<String, List<String>> sourcePathMappings;
    private final Map<String, String> targetPathMappings;
    private final List<DistCpJobDefinition> jobDefinitions;
    private final long estimatedDataSize;
    private final int estimatedFileCount;

    public DistCpPlan(String database,
                     Map<String, List<String>> sourcePathMappings,
                     Map<String, String> targetPathMappings,
                     List<DistCpJobDefinition> jobDefinitions,
                     long estimatedDataSize,
                     int estimatedFileCount) {
        this.database = database;
        this.sourcePathMappings = sourcePathMappings;
        this.targetPathMappings = targetPathMappings;
        this.jobDefinitions = jobDefinitions;
        this.estimatedDataSize = estimatedDataSize;
        this.estimatedFileCount = estimatedFileCount;
    }

    public String getDatabase() { return database; }
    public Map<String, List<String>> getSourcePathMappings() { return sourcePathMappings; }
    public Map<String, String> getTargetPathMappings() { return targetPathMappings; }
    public List<DistCpJobDefinition> getJobDefinitions() { return jobDefinitions; }
    public long getEstimatedDataSize() { return estimatedDataSize; }
    public int getEstimatedFileCount() { return estimatedFileCount; }
}