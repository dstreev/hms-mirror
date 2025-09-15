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
 * Result of DistCp plan generation.
 */
public class DistCpPlanResult {
    private final boolean success;
    private final DistCpPlan plan;
    private final String message;
    private final List<String> warnings;
    private final List<String> errors;

    public DistCpPlanResult(boolean success, DistCpPlan plan, String message,
                           List<String> warnings, List<String> errors) {
        this.success = success;
        this.plan = plan;
        this.message = message;
        this.warnings = warnings;
        this.errors = errors;
    }

    public static DistCpPlanResult success(DistCpPlan plan, String message) {
        return new DistCpPlanResult(true, plan, message, List.of(), List.of());
    }

    public static DistCpPlanResult success(DistCpPlan plan, String message, List<String> warnings) {
        return new DistCpPlanResult(true, plan, message, warnings, List.of());
    }

    public static DistCpPlanResult failure(String message, List<String> errors) {
        return new DistCpPlanResult(false, null, message, List.of(), errors);
    }

    public boolean isSuccess() { return success; }
    public DistCpPlan getPlan() { return plan; }
    public String getMessage() { return message; }
    public List<String> getWarnings() { return warnings; }
    public List<String> getErrors() { return errors; }
}