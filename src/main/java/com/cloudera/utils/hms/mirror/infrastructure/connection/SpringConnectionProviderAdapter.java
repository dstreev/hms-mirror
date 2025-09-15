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
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Adapter that bridges the existing Spring-based ConnectionPoolService
 * with the new infrastructure interface. This allows gradual migration
 * without breaking existing functionality.
 * 
 * Note: This is configured as a bean in a configuration class, not auto-detected
 * via component scanning to avoid test configuration issues.
 */
public class SpringConnectionProviderAdapter implements ConnectionProvider {

    private final ConnectionPoolService connectionPoolService;

    public SpringConnectionProviderAdapter(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Override
    public Connection getConnection(Environment environment) throws SQLException {
        return connectionPoolService.getHS2EnvironmentConnection(environment);
    }

    @Override
    public Connection getMetastoreConnection(Environment environment) throws SQLException {
        return connectionPoolService.getMetastoreDirectEnvironmentConnection(environment);
    }

    @Override
    public boolean validateConnection(Environment environment) {
        try {
            Connection conn = getConnection(environment);
            boolean isValid = conn != null && !conn.isClosed();
            if (conn != null) {
                conn.close();
            }
            return isValid;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void close() {
        // Delegate to the existing service cleanup if available
        // Note: ConnectionPoolService doesn't have a public close method in current implementation
    }
}