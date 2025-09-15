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

import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;

import java.util.List;

/**
 * Result of strategy recommendation analysis.
 */
public class StrategyRecommendationResult {
    private final List<DataStrategyEnum> recommendedStrategies;
    private final DataStrategyEnum primaryRecommendation;
    private final String reasoning;
    private final List<String> considerations;

    public StrategyRecommendationResult(List<DataStrategyEnum> recommendedStrategies,
                                      DataStrategyEnum primaryRecommendation,
                                      String reasoning,
                                      List<String> considerations) {
        this.recommendedStrategies = recommendedStrategies;
        this.primaryRecommendation = primaryRecommendation;
        this.reasoning = reasoning;
        this.considerations = considerations;
    }

    public static StrategyRecommendationResult recommendation(List<DataStrategyEnum> strategies,
                                                            DataStrategyEnum primary,
                                                            String reasoning,
                                                            List<String> considerations) {
        return new StrategyRecommendationResult(strategies, primary, reasoning, considerations);
    }

    public List<DataStrategyEnum> getRecommendedStrategies() { return recommendedStrategies; }
    public DataStrategyEnum getPrimaryRecommendation() { return primaryRecommendation; }
    public String getReasoning() { return reasoning; }
    public List<String> getConsiderations() { return considerations; }
}