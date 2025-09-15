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
 * Represents table metadata extracted from a Hive table.
 */
public class TableMetadata {
    private final String database;
    private final String tableName;
    private final String tableType;
    private final String location;
    private final String inputFormat;
    private final String outputFormat;
    private final String serdeLib;
    private final Map<String, String> parameters;
    private final boolean partitioned;
    private final long recordCount;

    public TableMetadata(String database, String tableName, String tableType,
                        String location, String inputFormat, String outputFormat,
                        String serdeLib, Map<String, String> parameters,
                        boolean partitioned, long recordCount) {
        this.database = database;
        this.tableName = tableName;
        this.tableType = tableType;
        this.location = location;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.serdeLib = serdeLib;
        this.parameters = parameters;
        this.partitioned = partitioned;
        this.recordCount = recordCount;
    }

    public String getDatabase() { return database; }
    public String getTableName() { return tableName; }
    public String getTableType() { return tableType; }
    public String getLocation() { return location; }
    public String getInputFormat() { return inputFormat; }
    public String getOutputFormat() { return outputFormat; }
    public String getSerdeLib() { return serdeLib; }
    public Map<String, String> getParameters() { return parameters; }
    public boolean isPartitioned() { return partitioned; }
    public long getRecordCount() { return recordCount; }
}