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
 * Result object for validation operations.
 * Contains validation status and any issues found.
 */
public class ValidationResult {
    private final boolean valid;
    private final List<String> errors;
    private final List<String> warnings;
    private final String message;

    public ValidationResult(boolean valid, List<String> errors, List<String> warnings, String message) {
        this.valid = valid;
        this.errors = errors;
        this.warnings = warnings;
        this.message = message;
    }

    public static ValidationResult success() {
        return new ValidationResult(true, List.of(), List.of(), "Validation successful");
    }

    public static ValidationResult failure(String message) {
        return new ValidationResult(false, List.of(message), List.of(), message);
    }

    public static ValidationResult failure(List<String> errors) {
        return new ValidationResult(false, errors, List.of(), "Validation failed");
    }

    public boolean isValid() { return valid; }
    public List<String> getErrors() { return errors; }
    public List<String> getWarnings() { return warnings; }
    public String getMessage() { return message; }
}