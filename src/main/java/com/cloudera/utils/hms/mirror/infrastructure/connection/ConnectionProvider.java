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

package com.cloudera.utils.hms.mirror.infrastructure.connection;

import com.cloudera.utils.hms.mirror.domain.support.Environment;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Infrastructure interface for providing database connections.
 * Abstracts the underlying connection pool implementation from business logic.
 */
public interface ConnectionProvider {
    
    /**
     * Gets a connection for the specified environment.
     */
    Connection getConnection(Environment environment) throws SQLException;
    
    /**
     * Gets a connection for direct metastore access.
     */
    Connection getMetastoreConnection(Environment environment) throws SQLException;
    
    /**
     * Validates that a connection can be established to the environment.
     */
    boolean validateConnection(Environment environment);
    
    /**
     * Closes all connections and cleans up resources.
     */
    void close();
}