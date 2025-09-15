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
 * Result of DistCp source list building.
 */
public class DistCpSourceListResult {
    private final boolean success;
    private final Map<String, List<String>> sourceLists;
    private final String message;
    private final int totalPaths;
    private final long estimatedDataSize;

    public DistCpSourceListResult(boolean success, Map<String, List<String>> sourceLists,
                                 String message, int totalPaths, long estimatedDataSize) {
        this.success = success;
        this.sourceLists = sourceLists;
        this.message = message;
        this.totalPaths = totalPaths;
        this.estimatedDataSize = estimatedDataSize;
    }

    public static DistCpSourceListResult success(Map<String, List<String>> sourceLists,
                                               int totalPaths, long estimatedDataSize) {
        return new DistCpSourceListResult(true, sourceLists, "Source lists generated successfully",
                                        totalPaths, estimatedDataSize);
    }

    public static DistCpSourceListResult failure(String message) {
        return new DistCpSourceListResult(false, Map.of(), message, 0, 0);
    }

    public boolean isSuccess() { return success; }
    public Map<String, List<String>> getSourceLists() { return sourceLists; }
    public String getMessage() { return message; }
    public int getTotalPaths() { return totalPaths; }
    public long getEstimatedDataSize() { return estimatedDataSize; }
}