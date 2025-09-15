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

import java.util.Map;

/**
 * Represents comprehensive statistics for a table.
 */
public class TableStatistics {
    private final String tableName;
    private final boolean isPartitioned;
    private final int partitionCount;
    private final Map<String, Object> statistics;
    private final Long dataSize;
    private final Double avgFileSize;
    private final String fileFormat;

    public TableStatistics(String tableName, boolean isPartitioned, int partitionCount,
                          Map<String, Object> statistics, Long dataSize, 
                          Double avgFileSize, String fileFormat) {
        this.tableName = tableName;
        this.isPartitioned = isPartitioned;
        this.partitionCount = partitionCount;
        this.statistics = statistics;
        this.dataSize = dataSize;
        this.avgFileSize = avgFileSize;
        this.fileFormat = fileFormat;
    }

    public String getTableName() { return tableName; }
    public boolean isPartitioned() { return isPartitioned; }
    public int getPartitionCount() { return partitionCount; }
    public Map<String, Object> getStatistics() { return statistics; }
    public Long getDataSize() { return dataSize; }
    public Double getAvgFileSize() { return avgFileSize; }
    public String getFileFormat() { return fileFormat; }
}