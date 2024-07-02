/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsDBCP2Impl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHikariImpl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHybridImpl;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_CONNECTION_ISSUE;
import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_DISCONNECTED;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Component
@Getter
@Setter
@Slf4j
public class ConnectionPoolService implements ConnectionPools {

    private boolean connected = false;

    private HmsMirrorConfig hmsMirrorConfig;
    private ExecuteSession executeSession;
    private ConnectionPools connectionPools = null;
    private EnvironmentService environmentService;

    private PasswordService passwordService;


    @Autowired
    public void setEnvironmentService(EnvironmentService environmentService) {
        this.environmentService = environmentService;
    }

    @Autowired
    public void setPasswordService(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    public Boolean checkConnections() {
        boolean rtn = Boolean.FALSE;
        if (hmsMirrorConfig == null) {
            throw new RuntimeException("Configuration not set.  Connections can't be established.");
        }

        Set<Environment> envs = new HashSet<>();
        if (!(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP ||
                hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.ICEBERG_CONVERSION)) {
            envs.add(Environment.LEFT);
            envs.add(Environment.RIGHT);
        } else {
            envs.add(Environment.LEFT);
        }

        for (Environment env : envs) {
            Cluster cluster = hmsMirrorConfig.getCluster(env);
            if (cluster != null
                    && cluster.getHiveServer2() != null
                    && cluster.getHiveServer2().isValidUri()
                    && !cluster.getHiveServer2().isDisconnected()) {
                Connection conn = null;
                try {
                    conn = getConnectionPools().getHS2EnvironmentConnection(env);
                    //cluster.getConnection();
                    // May not be set for DUMP strategy (RIGHT cluster)
                    log.debug("{}:: Checking Hive Connection", env);
                    if (conn != null) {
//                        Statement stmt = null;
//                        ResultSet resultSet = null;
//                        try {
//                            stmt = conn.createStatement();
//                            resultSet = stmt.executeQuery("SHOW DATABASES");
//                            resultSet = stmt.executeQuery("SELECT 'HIVE CONNECTION TEST PASSED' AS STATUS");
                        log.debug("{}:: Hive Connection Successful", env);
                        rtn = Boolean.TRUE;
//                        } catch (SQLException sql) {
                        // DB Doesn't Exists.
//                            log.error(env + ": Hive Connection check failed.", sql);
//                            rtn = Boolean.FALSE;
//                        } finally {
//                            if (resultSet != null) {
//                                try {
//                                    resultSet.close();
//                                } catch (SQLException sqlException) {
//                                     ignore
//                                }
//                            }
//                            if (stmt != null) {
//                                try {
//                                    stmt.close();
//                                } catch (SQLException sqlException) {
                        // ignore
//                                }
//                            }
//                        }
                    } else {
                        log.error("{}: Hive Connection check failed.  Connection is null.", env);
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    rtn = Boolean.FALSE;
                    log.error("{}: Hive Connection check failed.", env, se);
                } finally {
                    if (conn != null) {
                        try {
                            log.info("{}: Closing Connection", env);
                            conn.close();
                        } catch (Throwable throwables) {
                            log.error("{}: Error closing connection.", env, throwables);
                        }
                    }
                }
            }
        }
        return rtn;
    }

    @Override
    public void addHiveServer2(Environment environment, HiveServer2Config hiveServer2) {
        getConnectionPools().addHiveServer2(environment, hiveServer2);
    }

    @Override
    public void addMetastoreDirect(Environment environment, DBStore dbStore) {
        getConnectionPools().addMetastoreDirect(environment, dbStore);
    }

    @Override
    public void close() {
        if (connectionPools != null) {
            // Set State of Connection.
            connected = false;
            getConnectionPools().close();
        }
    }

    public ConnectionPools getConnectionPools() {
        if (connectionPools == null) {
            try {
                connectionPools = getConnectionPoolsImpl();
            } catch (SQLException e) {
                log.error("Error creating connection pools", e);
                throw new RuntimeException(e);
            }
        }
        return connectionPools;
    }

    private ConnectionPools getConnectionPoolsImpl() throws SQLException {
        ConnectionPools rtn = null;

        if (hmsMirrorConfig == null) {
            throw new RuntimeException("Configuration not set.  Connections can't be established.");
        }
        ConnectionPoolType cpt = hmsMirrorConfig.getConnectionPoolLib();
        if (cpt == null) {
            // Need to calculate the connectio pool type:
            // When both clusters are defined:
                // Use DBCP2 when both clusters are non-legacy
                // Use HYBRID when one cluster is legacy and the other is not
                // Use HIKARICP when both clusters are non-legacy.
            // When only the left cluster is defined:
                // Use DBCP2 when the left cluster is legacy
                // Use HIKARICP when the left cluster is non-legacy
            if (isNull(hmsMirrorConfig.getCluster(Environment.RIGHT))) {
                if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                    cpt = ConnectionPoolType.DBCP2;
                } else {
                    cpt = ConnectionPoolType.HIKARICP;
                }
            } else {
                if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                        && hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                    cpt = ConnectionPoolType.DBCP2;
                } else if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()
                        || hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                    cpt = ConnectionPoolType.HYBRID;
                } else {
                    cpt = ConnectionPoolType.HIKARICP;
                }
            }
        } else {
            log.info("Connection Pool Type explicitly set to: {}", cpt);
        }
        switch (cpt) {
            case DBCP2:
                log.info("Using DBCP2 Connection Pooling Libraries");
                rtn = new ConnectionPoolsDBCP2Impl(executeSession, passwordService);
                break;
            case HIKARICP:
                log.info("Using HIKARICP Connection Pooling Libraries");
                rtn = new ConnectionPoolsHikariImpl(executeSession, passwordService);
                break;
            case HYBRID:
                log.info("Using HYBRID Connection Pooling Libraries");
                rtn = new ConnectionPoolsHybridImpl(executeSession, passwordService);
                break;
        }
        // Initialize the connection pools
        rtn.init();
        return rtn;
    }

    @Override
    public Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getHS2EnvironmentConnection(environment);
        return conn;
    }

    @Override
    public Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getMetastoreDirectEnvironmentConnection(environment);
        return conn;
    }

    @Override
    public void init() throws SQLException {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        ExecuteSession executeSession = executeSessionService.getActiveSession();
        RunStatus runStatus = executeSession.getRunStatus();

        if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP) {
            hmsMirrorConfig.setExecute(Boolean.FALSE); // No Actions.
            hmsMirrorConfig.setSync(Boolean.FALSE);
        }

        // Make adjustments to the config clusters based on settings.
        // Buildout the right connection pool details.
        Set<Environment> hs2Envs = new HashSet<Environment>();
        switch (hmsMirrorConfig.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                if (hmsMirrorConfig.getDumpSource() == Environment.RIGHT) {
                    // switch LEFT and RIGHT
                    hmsMirrorConfig.getClusters().remove(Environment.LEFT);
                    hmsMirrorConfig.getClusters().put(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.RIGHT));
                    hmsMirrorConfig.getCluster(Environment.LEFT).setEnvironment(Environment.LEFT);
                    hmsMirrorConfig.getClusters().remove(Environment.RIGHT);
                }
            case STORAGE_MIGRATION:
                // Get Pool
                getConnectionPools().addHiveServer2(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2());
                hs2Envs.add(Environment.LEFT);
                break;
            case SQL:
            case SCHEMA_ONLY:
            case EXPORT_IMPORT:
            case HYBRID:
                // When doing inplace downgrade of ACID tables, we're only dealing with the LEFT cluster.
                if (!hmsMirrorConfig.getMigrateACID().isInplace() && null != hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2()) {
                    getConnectionPools().addHiveServer2(Environment.RIGHT, hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2());
                    hs2Envs.add(Environment.RIGHT);
                }
            default:
                getConnectionPools().addHiveServer2(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2());
                hs2Envs.add(Environment.LEFT);
                break;
        }
        if (hmsMirrorConfig.loadPartitionMetadata()) {
            if (nonNull(hmsMirrorConfig.getCluster(Environment.LEFT)) &&
                    nonNull(hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect())) {
                getConnectionPools().addMetastoreDirect(Environment.LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect());
            }
            if (nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT)) &&
                    nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                getConnectionPools().addMetastoreDirect(Environment.RIGHT, hmsMirrorConfig.getCluster(Environment.RIGHT).getMetastoreDirect());
            }
        }
        try {
            // TODO: Should we try to close first to clean up any existing connections?
            getConnectionPools().init();
            for (Environment target : hs2Envs) {
                Connection conn = null;
                Statement stmt = null;
                try {
                    conn = getConnectionPools().getHS2EnvironmentConnection(target);
                    if (conn == null) {
                        if (target == Environment.RIGHT && hmsMirrorConfig.getCluster(target).getHiveServer2().isDisconnected()) {
                            // Skip error.  Set Warning that we're disconnected.
                            runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                        } else if (!hmsMirrorConfig.isLoadingTestData()) {
                            runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                        }
                    } else {
                        // Exercise the connection.
                        stmt = conn.createStatement();
                        stmt.execute("SELECT 1");
                    }
                } catch (SQLException se) {
                    if (target == Environment.RIGHT && hmsMirrorConfig.getCluster(target).getHiveServer2().isDisconnected()) {
                        // Set warning that RIGHT is disconnected.
                        runStatus.addWarning(ENVIRONMENT_DISCONNECTED, target);
                    } else {
                        log.error(se.getMessage(), se);
                        runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                    runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        } catch (SQLException cnfe) {
            log.error("Issue initializing connections.  Check driver locations", cnfe);
            throw new RuntimeException(cnfe);
        }
//            hmsMirrorConfig.getCluster(Environment.LEFT).setPools(connectionPoolService.getConnectionPools());
        switch (hmsMirrorConfig.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                break;
            default:
                // Don't set the Pools when Disconnected.
                if (nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT))
                        && nonNull(hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2())
                        && !hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
//                        hmsMirrorConfig.getCluster(Environment.RIGHT).setPools(connectionPoolService.getConnectionPools());
                }
        }

        if (hmsMirrorConfig.isConnectionKerberized()) {
            log.debug("Detected a Kerberized JDBC Connection.  Attempting to setup/initialize GSS.");
            environmentService.setupGSS();
        }
        log.debug("Checking Hive Connections");
        if (!hmsMirrorConfig.isLoadingTestData() && !checkConnections()) {
            log.error("Check Hive Connections Failed.");
            if (hmsMirrorConfig.isConnectionKerberized()) {
                log.error("Check Kerberos configuration if GSS issues are encountered.  See the running.md docs for details.");
            }
            throw new RuntimeException("Check Hive Connections Failed.  Check Logs.");
        }
        // Set state of connection.
        connected = true;
    }

}
