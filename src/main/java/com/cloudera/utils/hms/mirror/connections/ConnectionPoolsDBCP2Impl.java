/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.HiveDriverEnum;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.mirror.service.DriverUtilsService;
import com.cloudera.utils.hms.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
public class ConnectionPoolsDBCP2Impl extends ConnectionPoolsBase implements ConnectionPools {


    public ConnectionPoolsDBCP2Impl(DriverUtilsService driverUtilsService,
                                    ConversionResult conversionResult, PasswordService passwordService,
                                    ConnectionPoolService connectionPoolService) {
        super(driverUtilsService, conversionResult, passwordService, connectionPoolService);
    }

    protected void initHS2PooledDataSources() {
        conversionResult.getConnections().forEach((environment, connection) -> {
            if (!connection.isHs2Connected()) {
                Driver lclDriver = getHS2EnvironmentDriver(environment);
                if (nonNull(lclDriver)) {
                    try {
                        DriverManager.registerDriver(lclDriver);
                        try {
                            Properties props = new Properties();
                            // Add the HikariCP properties established in the configs and add them to the connection properties.
                            props.putAll(connectionPoolService.getHikariProperties().toProperties());
                            // Add the User name and Password to the connection properties.
                            if (!isBlank(connection.getHs2Username())) {
                                props.put("user", connection.getHs2Username());
                            }
                            if (!isBlank(connection.getHs2Password())) {
                                props.put("password", connection.getHs2Password());
                            }

                            // If the ExecuteSession has the 'passwordKey' set, resolve Encrypted PasswordApp first.
                            // TODO: Fix for encrypted passwords.

                            // Make a copy.
                            Properties connProperties = new Properties();
                            // Trim properties to include only those supported by the driver.
                            connProperties.putAll(HiveDriverEnum.getDriverEnum(connection.getHs2DriverType().getDriverClass())
                                    .reconcileForDriver(props));

                            // We need to review any property overrides for the environment to see
                            //   if they're trying to set the queue. EG tez.queue.name or mapred.job.queue.name
                            String queueOverride = ConfigUtils.getQueuePropertyOverride(environment,
                                    conversionResult.getConfig().getOptimization().getOverrides());
                            if (queueOverride != null) {
                                connProperties.put("connectionInitSqls", queueOverride);
                            }

                            // Get the DBCP2 properties established in the configs and add them to the connection properties.
                            connProperties.putAll(connectionPoolService.getDbcp2Properties().toProperties());

                            log.info("{} - HS2 DBCP2 Connection Properties: {}", environment, connProperties);
                            ConnectionFactory connectionFactory =
                                    new DriverManagerConnectionFactory(connection.getHs2Uri(), connProperties);

                            PoolableConnectionFactory poolableConnectionFactory =
                                    new PoolableConnectionFactory(connectionFactory, null);

                            ObjectPool<PoolableConnection> connectionPool =
                                    new GenericObjectPool<>(poolableConnectionFactory);

                            poolableConnectionFactory.setPool(connectionPool);

                            PoolingDataSource<PoolableConnection> poolingDatasource = new PoolingDataSource<>(connectionPool);

                            hs2DataSources.put(environment, poolingDatasource);
                        } catch (Throwable se) {
                            log.error(se.getMessage(), se);
                            throw new RuntimeException(se);
                        } finally {
                            DriverManager.deregisterDriver(lclDriver);
                        }
                    } catch (SQLException e) {
                        log.error(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                    Connection conn = null;
                    try {
                        conn = getHS2EnvironmentConnection(environment);
                    } catch (Throwable t) {
                        if (conn != null) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                log.error("Issue closing HS2 connection for the {}", environment, e);
                            }
                        }
                    }

                }
            }
        });
    }

    @Override
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

                // Attempt to get the Driver Version for the Metastore Direct Connection.
                try {
                    ConnectionFactory msconnectionFactory =
                            new DriverManagerConnectionFactory(connection.getMetastoreDirectUri(), connProperties);

                    PoolableConnectionFactory mspoolableConnectionFactory =
                            new PoolableConnectionFactory(msconnectionFactory, null);

                    ObjectPool<PoolableConnection> msconnectionPool =
                            new GenericObjectPool<>(mspoolableConnectionFactory);

                    mspoolableConnectionFactory.setPool(msconnectionPool);
                    metastoreDirectDataSources.put(environment, new PoolingDataSource<>(msconnectionPool));

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
                    // TODO: Need to figure out what to do here.
//                    throw e;
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
                        throw new RuntimeException(t);
                    }
                }
            }
        });
    }

}
