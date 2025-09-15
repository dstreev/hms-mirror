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
 * Result object for location translation operations.
 * Contains the translated location and any issues or warnings encountered.
 */
public class LocationTranslationResult {
    private final String translatedLocation;
    private final boolean success;
    private final String message;
    private final List<String> warnings;
    private final List<String> issues;
    private final boolean remapped;

    public LocationTranslationResult(String translatedLocation, boolean success, String message,
                                   List<String> warnings, List<String> issues, boolean remapped) {
        this.translatedLocation = translatedLocation;
        this.success = success;
        this.message = message;
        this.warnings = warnings;
        this.issues = issues;
        this.remapped = remapped;
    }

    public static LocationTranslationResult success(String translatedLocation, boolean remapped) {
        return new LocationTranslationResult(translatedLocation, true, "Translation successful", 
                                           List.of(), List.of(), remapped);
    }

    public static LocationTranslationResult failure(String message) {
        return new LocationTranslationResult(null, false, message, List.of(), List.of(), false);
    }

    public String getTranslatedLocation() { return translatedLocation; }
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<String> getWarnings() { return warnings; }
    public List<String> getIssues() { return issues; }
    public boolean isRemapped() { return remapped; }
}