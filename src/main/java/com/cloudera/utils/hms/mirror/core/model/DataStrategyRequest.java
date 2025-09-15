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
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import java.util.List;

/**
 * Request object for data strategy selection.
 */
public class DataStrategyRequest {
    private final DataStrategyEnum preferredStrategy;
    private final Environment sourceEnvironment;
    private final Environment targetEnvironment;
    private final List<String> sourceTables;
    private final boolean includeData;
    private final boolean acidTablesPresent;
    private final boolean viewsPresent;
    private final boolean partitionedTablesPresent;
    private final long estimatedDataSize;

    public DataStrategyRequest(DataStrategyEnum preferredStrategy,
                              Environment sourceEnvironment,
                              Environment targetEnvironment,
                              List<String> sourceTables,
                              boolean includeData,
                              boolean acidTablesPresent,
                              boolean viewsPresent,
                              boolean partitionedTablesPresent,
                              long estimatedDataSize) {
        this.preferredStrategy = preferredStrategy;
        this.sourceEnvironment = sourceEnvironment;
        this.targetEnvironment = targetEnvironment;
        this.sourceTables = sourceTables;
        this.includeData = includeData;
        this.acidTablesPresent = acidTablesPresent;
        this.viewsPresent = viewsPresent;
        this.partitionedTablesPresent = partitionedTablesPresent;
        this.estimatedDataSize = estimatedDataSize;
    }

    public DataStrategyEnum getPreferredStrategy() { return preferredStrategy; }
    public Environment getSourceEnvironment() { return sourceEnvironment; }
    public Environment getTargetEnvironment() { return targetEnvironment; }
    public List<String> getSourceTables() { return sourceTables; }
    public boolean isIncludeData() { return includeData; }
    public boolean isAcidTablesPresent() { return acidTablesPresent; }
    public boolean isViewsPresent() { return viewsPresent; }
    public boolean isPartitionedTablesPresent() { return partitionedTablesPresent; }
    public long getEstimatedDataSize() { return estimatedDataSize; }
}