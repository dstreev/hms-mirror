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
@Schema(description = "Query and data transfer optimization settings for migration operations")
public class Optimization implements Cloneable {

    @Schema(description = "Control whether to set 'hive.optimize.sort.dynamic.partition' to true. " +
            "If false, uses a PRESCRIPTIVE approach with DISTRIBUTE BY clause for partitioned tables",
            defaultValue = "false")
    private boolean sortDynamicPartitionInserts = Boolean.FALSE;

    @Schema(description = "Skip all optimizations by disabling 'hive.optimize.sort.dynamic.partition' " +
            "and not using DISTRIBUTE BY. User-specified overrides will still be applied",
            defaultValue = "false")
    private boolean skip = Boolean.FALSE;

    @Schema(description = "Automatically tune query parameters based on cluster resources and table characteristics",
            defaultValue = "false")
    private boolean autoTune = Boolean.FALSE;

    @Schema(description = "Compress text output during data transfer operations",
            defaultValue = "false")
    private boolean compressTextOutput = Boolean.FALSE;

    @Schema(description = "Skip statistics collection after table creation and data loading",
            defaultValue = "false")
    private boolean skipStatsCollection = Boolean.FALSE;

    @Schema(description = "Custom Hive configuration property overrides to apply during migration")
    private Overrides overrides = new Overrides();

    @Schema(description = "Build statistics for shadow/transfer tables used during migration",
            defaultValue = "false")
    private boolean buildShadowStatistics = Boolean.FALSE;

    @Override
    public Optimization clone() {
        try {
            Optimization clone = (Optimization) super.clone();
            // Deep copy mutable Overrides object to ensure clone independence
            if (overrides != null) {
                clone.overrides = overrides.clone();
            }
            // All other fields are boolean primitives, copied by value automatically
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
