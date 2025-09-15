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

package com.cloudera.utils.hms.mirror.core.api;

import com.cloudera.utils.hms.mirror.core.model.*;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

/**
 * Core business interface for table operations.
 * This interface defines pure business logic without Spring dependencies,
 * making it reusable across different application contexts.
 */
public interface TableOperations {

    /**
     * Migrates a table from source to target environment.
     */
    TableMigrationResult migrateTable(TableMigrationRequest request);

    /**
     * Validates that a table can be migrated with the given configuration.
     */
    ValidationResult validateTableMigration(TableMigrationRequest request);

    /**
     * Validates table filter rules against a table.
     */
    ValidationResult validateTableFilter(TableMirror tableMirror, Environment environment);

    /**
     * Extracts metadata from a table for analysis.
     */
    TableMetadata extractTableMetadata(String database, String table, Environment environment);

    /**
     * Checks if a table exists in the specified environment.
     */
    boolean tableExists(String database, String table, Environment environment);

    /**
     * Validates table compatibility between source and target environments.
     */
    ValidationResult validateTableCompatibility(TableMirror tableMirror);

    /**
     * Generates SQL statements for table migration.
     */
    SqlStatements generateMigrationSql(TableMigrationRequest request);
}