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
 * Represents a single DistCp job definition.
 */
public class DistCpJobDefinition {
    private final String jobName;
    private final String sourceListFile;
    private final String targetDirectory;
    private final List<String> sourcePaths;
    private final Map<String, String> distCpOptions;
    private final int priority;
    private final long estimatedDuration;

    public DistCpJobDefinition(String jobName, String sourceListFile, String targetDirectory,
                              List<String> sourcePaths, Map<String, String> distCpOptions,
                              int priority, long estimatedDuration) {
        this.jobName = jobName;
        this.sourceListFile = sourceListFile;
        this.targetDirectory = targetDirectory;
        this.sourcePaths = sourcePaths;
        this.distCpOptions = distCpOptions;
        this.priority = priority;
        this.estimatedDuration = estimatedDuration;
    }

    public String getJobName() { return jobName; }
    public String getSourceListFile() { return sourceListFile; }
    public String getTargetDirectory() { return targetDirectory; }
    public List<String> getSourcePaths() { return sourcePaths; }
    public Map<String, String> getDistCpOptions() { return distCpOptions; }
    public int getPriority() { return priority; }
    public long getEstimatedDuration() { return estimatedDuration; }
}