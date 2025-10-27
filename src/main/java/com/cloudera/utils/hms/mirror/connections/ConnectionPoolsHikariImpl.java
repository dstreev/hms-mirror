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

import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.HiveDriverEnum;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.DriverUtilsService;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.util.ConfigUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
public class ConnectionPoolsHikariImpl extends ConnectionPoolsBase implements ConnectionPools {

    public ConnectionPoolsHikariImpl(DriverUtilsService driverUtilsService,
                                     ConversionResult conversionResult, PasswordService passwordService,
                                     ConnectionPoolService connectionPoolService) {
        super(driverUtilsService, conversionResult, passwordService, connectionPoolService);
    }

    protected void initHS2PooledDataSources() throws SessionException, EncryptionException {
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
                            /*
                            if (executeSession.getConfig().isEncryptedPasswords()) {
                                if (nonNull(executeSession.getConfig().getPasswordKey()) && !executeSession.getConfig().getPasswordKey().isEmpty()) {
                                    String encryptedPassword = connProperties.getProperty("password");
                                    String decryptedPassword = passwordService.decryptPassword(executeSession.getConfig().getPasswordKey(), encryptedPassword);
                                    connProperties.setProperty("password", decryptedPassword);
                                } else {
                                    throw new SessionException("Passwords encrypted, but no password key present.");
                                }
                            }
                             */

                            // Make a copy.
                            Properties connProperties = new Properties();
                            // Trim properties to include only those supported by the driver.
                            connProperties.putAll(HiveDriverEnum.getDriverEnum(connection.getHs2DriverType().getDriverClass())
                                    .reconcileForDriver(props));

                            // We need to review any property overrides for the environment to see
                            //   if they're trying to set the queue. EG tez.queue.name or mapred.job.queue.name
                            String queueOverride = ConfigUtils.getQueuePropertyOverride(environment,
                                    conversionResult.getConfigLite().getOptimization().getOverrides());
                            if (queueOverride != null) {
                                connProperties.put("connectionInitSql", queueOverride);
                            }

                            // Add the HikariCP properties established in the configs and add them to the connection properties.
                            connProperties.putAll(connectionPoolService.getHikariProperties().toProperties());

                            log.info("{} - HS2 Hikari Connection Properties: {}", environment, props);
                            HikariConfig config = new HikariConfig(props);
                            config.setJdbcUrl(connection.getHs2Uri());
                            // Set the Hikari Connection Pool DataSource Properties with the cleaned up properties.
                            config.setDataSourceProperties(connProperties);
                            HikariDataSource poolingDatasource = new HikariDataSource(config);

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


}
