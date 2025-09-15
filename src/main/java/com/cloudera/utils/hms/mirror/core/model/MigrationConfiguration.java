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

import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;

/**
 * Configuration for migration operations.
 */
public class MigrationConfiguration {
    private final DataStrategyEnum dataStrategy;
    private final boolean includeData;
    private final boolean includeViews;
    private final boolean includeAcidTables;
    private final boolean enableDistCp;
    private final boolean dryRun;
    private final int parallelism;

    public MigrationConfiguration(DataStrategyEnum dataStrategy, boolean includeData,
                                boolean includeViews, boolean includeAcidTables,
                                boolean enableDistCp, boolean dryRun, int parallelism) {
        this.dataStrategy = dataStrategy;
        this.includeData = includeData;
        this.includeViews = includeViews;
        this.includeAcidTables = includeAcidTables;
        this.enableDistCp = enableDistCp;
        this.dryRun = dryRun;
        this.parallelism = parallelism;
    }

    public DataStrategyEnum getDataStrategy() { return dataStrategy; }
    public boolean isIncludeData() { return includeData; }
    public boolean isIncludeViews() { return includeViews; }
    public boolean isIncludeAcidTables() { return includeAcidTables; }
    public boolean isEnableDistCp() { return enableDistCp; }
    public boolean isDryRun() { return dryRun; }
    public int getParallelism() { return parallelism; }
}