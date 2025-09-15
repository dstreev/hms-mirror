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
 * Result of session optimization calculations.
 */
public class SessionOptimizationResult {
    private final boolean success;
    private final Map<String, String> sessionSettings;
    private final List<String> recommendations;
    private final List<String> warnings;
    private final String message;

    public SessionOptimizationResult(boolean success, Map<String, String> sessionSettings,
                                   List<String> recommendations, List<String> warnings, 
                                   String message) {
        this.success = success;
        this.sessionSettings = sessionSettings;
        this.recommendations = recommendations;
        this.warnings = warnings;
        this.message = message;
    }

    public static SessionOptimizationResult success(Map<String, String> sessionSettings,
                                                   List<String> recommendations) {
        return new SessionOptimizationResult(true, sessionSettings, recommendations, List.of(),
                                           "Session optimizations generated successfully");
    }

    public static SessionOptimizationResult failure(String message) {
        return new SessionOptimizationResult(false, Map.of(), List.of(), List.of(), message);
    }

    public boolean isSuccess() { return success; }
    public Map<String, String> getSessionSettings() { return sessionSettings; }
    public List<String> getRecommendations() { return recommendations; }
    public List<String> getWarnings() { return warnings; }
    public String getMessage() { return message; }
}