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

package com.cloudera.utils.hms.mirror.connections;

import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.DriverUtilsService;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.PoolingDataSource;

import javax.sql.DataSource;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Getter
@Setter
@Slf4j
public abstract class ConnectionPoolsBase implements ConnectionPools {

    protected final DriverUtilsService driverUtilsService;
    protected final ConversionResult conversionResult;
    protected final PasswordService passwordService;
    protected final ConnectionPoolService connectionPoolService;

    private ConnectionState connectionState = ConnectionState.DISCONNECTED;

    protected final Map<Environment, DataSource> hs2DataSources = new TreeMap<>();
    protected final Map<Environment, Driver> hs2Drivers = new TreeMap<>();
    protected final Map<Environment, DataSource> metastoreDirectDataSources = new TreeMap<>();

    public ConnectionPoolsBase(DriverUtilsService driverUtilsService, ConversionResult conversionResult,
                               PasswordService passwordService, ConnectionPoolService connectionPoolService) {
        this.driverUtilsService = driverUtilsService;
        this.conversionResult = conversionResult;
        this.passwordService = passwordService;
        this.connectionPoolService = connectionPoolService;
    }

    public void close() {
        try {
            if (hs2DataSources.get(Environment.LEFT) != null) {
                if (hs2DataSources.get(Environment.LEFT) instanceof PoolingDataSource) {
                    ((PoolingDataSource<?>) hs2DataSources.get(Environment.LEFT)).close();
                } else if (hs2DataSources.get(Environment.LEFT) instanceof HikariDataSource) {
                    ((HikariDataSource) hs2DataSources.get(Environment.LEFT)).close();
                }
            }
        } catch (SQLException throwables) {
            //
        }
        try {
            if (hs2DataSources.get(Environment.RIGHT) != null) {
                if (hs2DataSources.get(Environment.RIGHT) instanceof PoolingDataSource) {
                    ((PoolingDataSource<?>) hs2DataSources.get(Environment.RIGHT)).close();
                } else if (hs2DataSources.get(Environment.RIGHT) instanceof HikariDataSource) {
                    ((HikariDataSource) hs2DataSources.get(Environment.RIGHT)).close();
                }
            }
        } catch (SQLException throwables) {
            //
        }
        // Clear the DataSources
        hs2DataSources.clear();

        if (metastoreDirectDataSources.get(Environment.LEFT) != null)
            if (metastoreDirectDataSources.get(Environment.LEFT) instanceof PoolingDataSource) {
                try {
                    ((PoolingDataSource<?>) metastoreDirectDataSources.get(Environment.LEFT)).close();
                } catch (SQLException e) {
                    log.error("Issue closing connection for: {}", "LEFT Metastore", e);
                    throw new RuntimeException(e);
                }
            } else if (metastoreDirectDataSources.get(Environment.LEFT) instanceof HikariDataSource)
                ((HikariDataSource) metastoreDirectDataSources.get(Environment.LEFT)).close();

        if (metastoreDirectDataSources.get(Environment.RIGHT) != null)
            if (metastoreDirectDataSources.get(Environment.RIGHT) instanceof PoolingDataSource) {
                try {
                    ((PoolingDataSource<?>) metastoreDirectDataSources.get(Environment.RIGHT)).close();
                } catch (SQLException e) {
                    log.error("Issue closing connection for: {}", "RIGHT Metastore Direct", e);
                    throw new RuntimeException(e);
                }
            } else if (metastoreDirectDataSources.get(Environment.RIGHT) instanceof HikariDataSource)
                ((HikariDataSource) metastoreDirectDataSources.get(Environment.RIGHT)).close();
        // Clear the DataSources.
        metastoreDirectDataSources.clear();
    }


    public synchronized Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Driver lclDriver = getHS2EnvironmentDriver(environment);
        Connection conn = null;
        if (lclDriver != null) {
            DriverManager.registerDriver(lclDriver);
            try {
                DataSource ds = getHS2EnvironmentDataSource(environment);
                if (ds != null) {
                    conn = ds.getConnection();
                }
            } catch (Throwable se) {
                log.error(se.getMessage(), se);
                throw new RuntimeException(se);
            } finally {
                DriverManager.deregisterDriver(lclDriver);
            }
        }
        return conn;
    }

    protected DataSource getHS2EnvironmentDataSource(Environment environment) {
        return hs2DataSources.get(environment);
    }

    protected synchronized Driver getHS2EnvironmentDriver(Environment environment) {
        return hs2Drivers.get(environment);
    }

    public synchronized Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = null;
        DataSource ds = getMetastoreDirectEnvironmentDataSource(environment);
        if (ds != null)
            conn = ds.getConnection();
        return conn;
    }

    protected DataSource getMetastoreDirectEnvironmentDataSource(Environment environment) {
        return metastoreDirectDataSources.get(environment);
    }

    protected void initHS2Drivers() {
        Set<Environment> environments = new HashSet<>();
        environments.add(Environment.LEFT);
        environments.add(Environment.RIGHT);

        conversionResult.getConnections().forEach(this::accept);
    }

    private void accept(Environment environment, ConnectionDto connection) {
        Driver driver = driverUtilsService.getHs2Driver(connection, environment);
        try {
            DriverManager.deregisterDriver(driver);
        } catch (SQLException se) {
            log.error(se.getMessage(), se);
            throw new RuntimeException("SQL issue getting driver for " + environment + " connection: ", se);
        }
        hs2Drivers.put(environment, driver);
    }


    protected abstract void initHS2PooledDataSources();

    protected void initMetastoreDataSources() {
        // Metastore Direct
        conversionResult.getConnections().forEach((environment, connection) -> {
            if (nonNull(connection) && !isBlank(connection.getMetastoreDirectUri())) {
                // Make a copy.
                Properties connProperties = new Properties();
                connProperties.putAll(connection.getMetastoreDirectConnectionProperties());
                // Add Username and Password to Properties.
                connProperties.put("user", connection.getMetastoreDirectUsername());
                connProperties.put("password", connection.getMetastoreDirectPassword());

                // TODO: Fix for encrypted passwords.

                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(connection.getMetastoreDirectUri());
                config.setDataSourceProperties(connProperties);
                // Attempt to get the Driver Version for the Metastore Direct Connection.
                try {
                    HikariDataSource poolingDatasource = new HikariDataSource(config);
                    metastoreDirectDataSources.put(environment, poolingDatasource);

                    DataSource ds = getMetastoreDirectEnvironmentDataSource(environment);
                    Class driverClass = DriverManager.getDriver(ds.getConnection().getMetaData().getURL()).getClass();
                    String jarFile = DriverUtilsService.byGetProtectionDomain(driverClass);
                    // These should be in the path.
                    // metastoreDirectConfig.setResource(jarFile);
                    log.info("{} - Metastore Direct JDBC JarFile: {}", environment, jarFile);
                    String version = ds.getConnection().getMetaData().getDriverVersion();
                    connection.setMetastoreDirectVersion(version);
                    log.info("{} - Metastore Direct JDBC Driver Version: {}", environment, version);
                } catch (SQLException | URISyntaxException e) {
                    log.error("Issue getting Metastore Direct JDBC JarFile details", e);
                    throw new RuntimeException("Issue initializing the Metastore Datasource for " + environment, e);
                }

                // Test Connection.
                Connection conn = null;
                try {
                    conn = getMetastoreDirectEnvironmentConnection(environment);
                } catch (Throwable t) {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            log.error("Issue closing Metastore Direct connection for the {}", environment, e);
                            throw new RuntimeException(e);
                        }
                    } else {
                        throw new RuntimeException("Issue getting metastore connection for " + environment, t);
                    }
                }
            }
        });

    }

    public synchronized void init() {
        if (!conversionResult.isMockTestDataset()) {
            switch (connectionState) {
                case DISCONNECTED:
                case ERROR:
                    connectionState = ConnectionState.INITIALIZING;
                    // Attempt to re-init.
                    initHS2Drivers();
                    initHS2PooledDataSources();
                    initMetastoreDataSources();
                    break;
                case INITIALIZING:
                case INITIALIZED:
                case CONNECTED:
                    // Do Nothing
                    break;
            }
            connectionState = ConnectionState.CONNECTED;
        }
    }

}
