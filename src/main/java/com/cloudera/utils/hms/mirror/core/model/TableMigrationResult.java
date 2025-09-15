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

import java.time.LocalDateTime;
import java.util.List;

/**
 * Result object for table migration operations.
 * Contains status, metrics, and any error information from the migration.
 */
public class TableMigrationResult {
    private final String tableName;
    private final boolean success;
    private final String message;
    private final List<String> warnings;
    private final List<String> errors;
    private final LocalDateTime startTime;
    private final LocalDateTime endTime;
    private final long recordsProcessed;

    public TableMigrationResult(String tableName, boolean success, String message,
                              List<String> warnings, List<String> errors,
                              LocalDateTime startTime, LocalDateTime endTime,
                              long recordsProcessed) {
        this.tableName = tableName;
        this.success = success;
        this.message = message;
        this.warnings = warnings;
        this.errors = errors;
        this.startTime = startTime;
        this.endTime = endTime;
        this.recordsProcessed = recordsProcessed;
    }

    public String getTableName() { return tableName; }
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<String> getWarnings() { return warnings; }
    public List<String> getErrors() { return errors; }
    public LocalDateTime getStartTime() { return startTime; }
    public LocalDateTime getEndTime() { return endTime; }
    public long getRecordsProcessed() { return recordsProcessed; }
}