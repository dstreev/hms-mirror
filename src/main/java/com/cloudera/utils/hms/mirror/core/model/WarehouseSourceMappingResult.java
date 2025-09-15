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
 * Result object for warehouse source mapping operations.
 */
public class WarehouseSourceMappingResult {
    private final boolean success;
    private final String message;
    private final Map<String, String> sourceMappings;
    private final List<String> errors;
    private final int consolidationLevel;

    public WarehouseSourceMappingResult(boolean success, String message, 
                                      Map<String, String> sourceMappings,
                                      int consolidationLevel, List<String> errors) {
        this.success = success;
        this.message = message;
        this.sourceMappings = sourceMappings;
        this.consolidationLevel = consolidationLevel;
        this.errors = errors;
    }

    public static WarehouseSourceMappingResult success(Map<String, String> mappings, int consolidationLevel) {
        return new WarehouseSourceMappingResult(true, "Source mappings built successfully", 
                                              mappings, consolidationLevel, List.of());
    }

    public static WarehouseSourceMappingResult failure(String message) {
        return new WarehouseSourceMappingResult(false, message, Map.of(), 0, List.of(message));
    }

    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public Map<String, String> getSourceMappings() { return sourceMappings; }
    public int getConsolidationLevel() { return consolidationLevel; }
    public List<String> getErrors() { return errors; }
}