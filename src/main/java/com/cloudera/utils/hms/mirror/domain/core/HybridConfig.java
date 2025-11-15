/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain.core;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Hybrid migration configuration that combines EXPORT_IMPORT and SQL data movement strategies " +
        "based on partition count and data size thresholds")
public class HybridConfig implements Cloneable {
    @Schema(description = "Maximum number of partitions for a table to use EXPORT_IMPORT strategy. " +
            "Tables with more partitions will use SQL strategy instead",
            defaultValue = "100",
            example = "100")
    private int exportImportPartitionLimit = 100;

    @Schema(description = "Maximum number of partitions for a table to use SQL strategy. " +
            "Tables with more partitions will be skipped or use alternative strategy",
            defaultValue = "500",
            example = "500")
    private int sqlPartitionLimit = 500;

    @Schema(description = "Maximum table size in bytes for SQL-based data movement. " +
            "Larger tables will use EXPORT_IMPORT or alternative strategy",
            defaultValue = "1073741824",
            example = "1073741824")
    private long sqlSizeLimit = (1024 * 1024 * 1024); // 1Gb

    @Override
    public HybridConfig clone() {
        try {
            HybridConfig clone = (HybridConfig) super.clone();
            // All fields are primitives (int, long), which are copied by value automatically.
            // No additional deep copying needed.
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
