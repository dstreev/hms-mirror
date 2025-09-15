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
 * Container for SQL statements generated during migration operations.
 */
public class SqlStatements {
    private final List<String> preExecutionStatements;
    private final List<String> tableCreationStatements;
    private final List<String> dataMovementStatements;
    private final List<String> postExecutionStatements;
    private final List<String> cleanupStatements;

    public SqlStatements(List<String> preExecutionStatements,
                        List<String> tableCreationStatements,
                        List<String> dataMovementStatements,
                        List<String> postExecutionStatements,
                        List<String> cleanupStatements) {
        this.preExecutionStatements = preExecutionStatements;
        this.tableCreationStatements = tableCreationStatements;
        this.dataMovementStatements = dataMovementStatements;
        this.postExecutionStatements = postExecutionStatements;
        this.cleanupStatements = cleanupStatements;
    }

    public List<String> getPreExecutionStatements() { return preExecutionStatements; }
    public List<String> getTableCreationStatements() { return tableCreationStatements; }
    public List<String> getDataMovementStatements() { return dataMovementStatements; }
    public List<String> getPostExecutionStatements() { return postExecutionStatements; }
    public List<String> getCleanupStatements() { return cleanupStatements; }

    /**
     * Returns all statements in execution order.
     */
    public List<String> getAllStatements() {
        return List.of(
            preExecutionStatements,
            tableCreationStatements,
            dataMovementStatements,
            postExecutionStatements,
            cleanupStatements
        ).stream()
        .flatMap(List::stream)
        .toList();
    }
}