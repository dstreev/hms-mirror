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

/**
 * Request object for table migration operations.
 * Contains all necessary information to perform a table migration.
 */
public class TableMigrationRequest {
    private final String sourceDatabaseName;
    private final String sourceTableName;
    private final String targetDatabaseName;
    private final String targetTableName;
    private final DataStrategyEnum migrationStrategy;
    private final Environment sourceEnvironment;
    private final Environment targetEnvironment;
    private final boolean dryRun;

    public TableMigrationRequest(String sourceDatabaseName, String sourceTableName,
                               String targetDatabaseName, String targetTableName,
                               DataStrategyEnum migrationStrategy,
                               Environment sourceEnvironment, Environment targetEnvironment,
                               boolean dryRun) {
        this.sourceDatabaseName = sourceDatabaseName;
        this.sourceTableName = sourceTableName;
        this.targetDatabaseName = targetDatabaseName;
        this.targetTableName = targetTableName;
        this.migrationStrategy = migrationStrategy;
        this.sourceEnvironment = sourceEnvironment;
        this.targetEnvironment = targetEnvironment;
        this.dryRun = dryRun;
    }

    public String getSourceDatabaseName() { return sourceDatabaseName; }
    public String getSourceTableName() { return sourceTableName; }
    public String getTargetDatabaseName() { return targetDatabaseName; }
    public String getTargetTableName() { return targetTableName; }
    public DataStrategyEnum getMigrationStrategy() { return migrationStrategy; }
    public Environment getSourceEnvironment() { return sourceEnvironment; }
    public Environment getTargetEnvironment() { return targetEnvironment; }
    public boolean isDryRun() { return dryRun; }
}