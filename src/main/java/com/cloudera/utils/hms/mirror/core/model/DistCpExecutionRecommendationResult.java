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
 * Result of DistCp execution recommendations.
 */
public class DistCpExecutionRecommendationResult {
    private final Map<String, String> recommendedOptions;
    private final List<String> recommendations;
    private final int recommendedMappers;
    private final long recommendedMemoryMb;
    private final String bandwidthLimit;

    public DistCpExecutionRecommendationResult(Map<String, String> recommendedOptions,
                                             List<String> recommendations,
                                             int recommendedMappers,
                                             long recommendedMemoryMb,
                                             String bandwidthLimit) {
        this.recommendedOptions = recommendedOptions;
        this.recommendations = recommendations;
        this.recommendedMappers = recommendedMappers;
        this.recommendedMemoryMb = recommendedMemoryMb;
        this.bandwidthLimit = bandwidthLimit;
    }

    public Map<String, String> getRecommendedOptions() { return recommendedOptions; }
    public List<String> getRecommendations() { return recommendations; }
    public int getRecommendedMappers() { return recommendedMappers; }
    public long getRecommendedMemoryMb() { return recommendedMemoryMb; }
    public String getBandwidthLimit() { return bandwidthLimit; }
}