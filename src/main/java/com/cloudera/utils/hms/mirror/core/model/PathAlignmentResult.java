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
 * Result of path alignment analysis for DistCp compatibility.
 */
public class PathAlignmentResult {
    private final boolean aligned;
    private final List<String> alignedPaths;
    private final List<String> misalignedPaths;
    private final String commonBasePath;
    private final String reason;

    public PathAlignmentResult(boolean aligned, List<String> alignedPaths, List<String> misalignedPaths,
                              String commonBasePath, String reason) {
        this.aligned = aligned;
        this.alignedPaths = alignedPaths;
        this.misalignedPaths = misalignedPaths;
        this.commonBasePath = commonBasePath;
        this.reason = reason;
    }

    public static PathAlignmentResult aligned(List<String> paths, String commonBasePath) {
        return new PathAlignmentResult(true, paths, List.of(), commonBasePath, "All paths are aligned");
    }

    public static PathAlignmentResult misaligned(List<String> aligned, List<String> misaligned, String reason) {
        return new PathAlignmentResult(false, aligned, misaligned, "", reason);
    }

    public boolean isAligned() { return aligned; }
    public List<String> getAlignedPaths() { return alignedPaths; }
    public List<String> getMisalignedPaths() { return misalignedPaths; }
    public String getCommonBasePath() { return commonBasePath; }
    public String getReason() { return reason; }
}