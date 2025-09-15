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

/**
 * Result of DistCp plan optimization.
 */
public class DistCpOptimizationResult {
    private final boolean optimized;
    private final DistCpPlan optimizedPlan;
    private final List<String> optimizations;
    private final String message;

    public DistCpOptimizationResult(boolean optimized, DistCpPlan optimizedPlan,
                                   List<String> optimizations, String message) {
        this.optimized = optimized;
        this.optimizedPlan = optimizedPlan;
        this.optimizations = optimizations;
        this.message = message;
    }

    public static DistCpOptimizationResult optimized(DistCpPlan plan, List<String> optimizations) {
        return new DistCpOptimizationResult(true, plan, optimizations, "Plan optimized successfully");
    }

    public static DistCpOptimizationResult noOptimization(DistCpPlan plan, String reason) {
        return new DistCpOptimizationResult(false, plan, List.of(), reason);
    }

    public boolean isOptimized() { return optimized; }
    public DistCpPlan getOptimizedPlan() { return optimizedPlan; }
    public List<String> getOptimizations() { return optimizations; }
    public String getMessage() { return message; }
}