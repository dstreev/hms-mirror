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
 * Result object for global location mapping operations.
 * Represents the outcome of processing a location through the global location map.
 */
public class GlobalLocationMapResult {
    private final boolean mapped;
    private final String originalDir;
    private final String mappedDir;
    private final String message;

    public GlobalLocationMapResult(boolean mapped, String originalDir, String mappedDir, String message) {
        this.mapped = mapped;
        this.originalDir = originalDir;
        this.mappedDir = mappedDir;
        this.message = message;
    }

    public static GlobalLocationMapResult mapped(String originalDir, String mappedDir) {
        return new GlobalLocationMapResult(true, originalDir, mappedDir, "Location mapped successfully");
    }

    public static GlobalLocationMapResult notMapped(String originalDir) {
        return new GlobalLocationMapResult(false, originalDir, null, "No mapping found");
    }

    public boolean isMapped() { return mapped; }
    public String getOriginalDir() { return originalDir; }
    public String getMappedDir() { return mappedDir; }
    public String getMessage() { return message; }
}