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

/**
 * Result of DistCp execution estimation.
 */
public class DistCpEstimationResult {
    private final long estimatedDurationMinutes;
    private final long estimatedMemoryMb;
    private final int recommendedMappers;
    private final double estimatedThroughputMbps;
    private final String summary;

    public DistCpEstimationResult(long estimatedDurationMinutes, long estimatedMemoryMb,
                                 int recommendedMappers, double estimatedThroughputMbps,
                                 String summary) {
        this.estimatedDurationMinutes = estimatedDurationMinutes;
        this.estimatedMemoryMb = estimatedMemoryMb;
        this.recommendedMappers = recommendedMappers;
        this.estimatedThroughputMbps = estimatedThroughputMbps;
        this.summary = summary;
    }

    public long getEstimatedDurationMinutes() { return estimatedDurationMinutes; }
    public long getEstimatedMemoryMb() { return estimatedMemoryMb; }
    public int getRecommendedMappers() { return recommendedMappers; }
    public double getEstimatedThroughputMbps() { return estimatedThroughputMbps; }
    public String getSummary() { return summary; }
}