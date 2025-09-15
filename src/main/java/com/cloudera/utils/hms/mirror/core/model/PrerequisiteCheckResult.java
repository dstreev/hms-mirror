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
 * Result of prerequisite checks for a data strategy.
 */
public class PrerequisiteCheckResult {
    private final boolean allPrerequisitesMet;
    private final List<String> missingPrerequisites;
    private final List<String> satisfiedPrerequisites;
    private final String summary;

    public PrerequisiteCheckResult(boolean allPrerequisitesMet,
                                 List<String> missingPrerequisites,
                                 List<String> satisfiedPrerequisites,
                                 String summary) {
        this.allPrerequisitesMet = allPrerequisitesMet;
        this.missingPrerequisites = missingPrerequisites;
        this.satisfiedPrerequisites = satisfiedPrerequisites;
        this.summary = summary;
    }

    public static PrerequisiteCheckResult allSatisfied(List<String> satisfied) {
        return new PrerequisiteCheckResult(true, List.of(), satisfied, "All prerequisites satisfied");
    }

    public static PrerequisiteCheckResult partiallyMet(List<String> missing, List<String> satisfied) {
        return new PrerequisiteCheckResult(false, missing, satisfied, 
                                         missing.size() + " prerequisites not met");
    }

    public boolean isAllPrerequisitesMet() { return allPrerequisitesMet; }
    public List<String> getMissingPrerequisites() { return missingPrerequisites; }
    public List<String> getSatisfiedPrerequisites() { return satisfiedPrerequisites; }
    public String getSummary() { return summary; }
}