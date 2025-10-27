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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsDBCP2Impl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHikariImpl;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolsHybridImpl;
import com.cloudera.utils.hms.mirror.domain.core.DBCP2Properties;
import com.cloudera.utils.hms.mirror.domain.core.HikariProperties;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.util.ConfigUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Env;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_CONNECTION_ISSUE;
import static com.cloudera.utils.hms.mirror.MessageCode.ENVIRONMENT_DISCONNECTED;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Getter
@Setter
@Slf4j
@RequiredArgsConstructor
public class ConnectionPoolService {

    private boolean connected = false;

    // Replace with ExecutionContext and ConversionResult.
//    private ExecuteSession executeSession;
    // This will be in the ConversionResult now.
//    private ConnectionPools connectionPools = null;
    @NonNull
    private final ExecutionContextService context;
    @NonNull
    private final EnvironmentService environmentService;
    @NonNull
    private final CliEnvironment cliEnvironment;
    @NonNull
    private final DriverUtilsService driverUtilsService;
    @NonNull
    private final ConfigService configService;
    @NonNull
    private final PasswordService passwordService;
    @NonNull
    private final HikariProperties hikariProperties;
    @NonNull
    private final DBCP2Properties dbcp2Properties;


    public ConversionResult getConversionResult() {
        return context.getConversionResult();
    }

    public void close() {
        getConversionResult().getConnectionPools().close();
    }

    public ConnectionPools getConnectionPools() {
        if (getConversionResult().getConnectionPools() == null) {
            getConversionResult().setConnectionPools(getConnectionPoolsImpl());
        }
        return getConversionResult().getConnectionPools();
    }

    private ConnectionPools getConnectionPoolsImpl() {
        ConnectionPools rtn = null;
        ConversionResult conversionResult = getConversionResult();

        if (isNull(conversionResult)) {
            log.error("Configuration not set.  Connections can't be established.");
            return null;
//            throw new RuntimeException("Configuration not set.  Connections can't be established.");
        }

        ConnectionPoolType cpt = conversionResult.getConnectionPoolLib();
        if (isNull(cpt)) {
            // Need to calculate the connectio pool type:
            // When both clusters are defined:
            // Use DBCP2 when both clusters are non-legacy
            // Use HYBRID when one cluster is legacy and the other is not
            // Use HIKARICP when both clusters are non-legacy.
            // When only the left cluster is defined:
            // Use DBCP2 when the left cluster is legacy
            // Use HIKARICP when the left cluster is non-legacy
            if (isNull(conversionResult.getConnection(Environment.RIGHT))) {
                if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                    cpt = ConnectionPoolType.DBCP2;
                } else {
                    cpt = ConnectionPoolType.HIKARICP;
                }
            } else {
                if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()
                        && conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
                    cpt = ConnectionPoolType.DBCP2;
                } else if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()
                        || conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
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
                rtn = new ConnectionPoolsDBCP2Impl(driverUtilsService, conversionResult, passwordService, this);
                break;
            case HIKARICP:
                log.info("Using HIKARICP Connection Pooling Libraries");
                rtn = new ConnectionPoolsHikariImpl(driverUtilsService, conversionResult, passwordService, this);
                break;
            case HYBRID:
                log.info("Using HYBRID Connection Pooling Libraries");
                rtn = new ConnectionPoolsHybridImpl(driverUtilsService, conversionResult, passwordService, this);
                break;
        }
        // Initialize the connections pools
//        rtn.init();
        return rtn;
    }

    //    @Override
    public Connection getHS2EnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getHS2EnvironmentConnection(environment);
        return conn;
    }

    //    @Override
    public Connection getMetastoreDirectEnvironmentConnection(Environment environment) throws SQLException {
        Connection conn = getConnectionPools().getMetastoreDirectEnvironmentConnection(environment);
        return conn;
    }

//    public boolean reset() throws SQLException, EncryptionException, SessionException {

    /// /        close();
//        boolean rtn = init();
//        return rtn;
//    }
    public boolean init() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        // Set Session Connections.
        boolean rtn = Boolean.TRUE;
        ConversionResult conversionResult = getConversionResult();

        // TODO: Fix.
        /*
        if (conversionResult.isConnected()) {
            log.info("Connections already established.  Skipping Connection Setup/Validation.");
            return Boolean.TRUE;
        }
         */

        RunStatus runStatus = conversionResult.getRunStatus();
        runStatus.setProgress(ProgressEnum.STARTED);
        this.close();

        initConnectionsSetup();

        // Initialize the drivers
//        getConnectionPools().init();

        rtn = initHS2();
        boolean msRtn = initMetastoreDirect();
        boolean nsRtn = initHcfsNamespaces();

        if (rtn && msRtn && nsRtn) {
//             TODO: Fix
//            conversionResult.setConnected(Boolean.TRUE);
        } else {
            runStatus.setProgress(ProgressEnum.FAILED);
        }

        // Ensure they werefdfasd
        return rtn && msRtn && nsRtn;
    }

    protected boolean initHcfsNamespaces() throws SessionException {
        AtomicBoolean rtn = new AtomicBoolean(Boolean.TRUE);
        ConversionResult conversionResult = getConversionResult();
//        ExecuteSession session = getConversionResult();
//        Connections connections = getConversionResult().getConnections();
        // TODO: Make sure connections has been populated.

//        HmsMirrorConfig config = session.getConfig();

        conversionResult.getConnections().forEach((k, v) -> {
            // checked..
            if (!isBlank(v.getHcfsNamespace())) {
//                connections.getNamespaces().get(k).setEndpoint(v.getHcfsNamespace());
                try {
                    log.info("Testing HCFS Connection for {}", k);
                    // Concat 'ls' with v.getHcfsNamespace() to a checkConnectionStr
                    String checkConnectionStr = "ls " + v.getHcfsNamespace();
                    CommandReturn cr = cliEnvironment.processInput(checkConnectionStr);
                    if (cr.isError()) {
                        log.error("HCFS Connection Failed for {} with: {}", k, cr.getError());
                        v.setHcfsStatus(ConnectionStatus.FAILED);
                        v.setHcfsStatusMessage(cr.getError());
                        rtn.set(Boolean.FALSE);
                    } else {
                        log.info("HCFS Connection Successful for {}", k);
                        v.setHcfsStatus(ConnectionStatus.SUCCESS);
                    }
                } catch (DisabledException e) {
                    log.info("HCFS Connection Disabled for {}", k);
                    v.setHcfsStatus(ConnectionStatus.DISABLED);
//                        throw new RuntimeException(e);
                }
            } else {
                log.info("HCFS Connection Not Configured for {}", k);
                v.setHcfsStatus(ConnectionStatus.NOT_CONFIGURED);
            }
        });
        return rtn.get();
    }

    public boolean initMetastoreDirectOnly() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        initConnectionsSetup();
        boolean rtn = initMetastoreDirect();
        return rtn;
    }

    protected boolean initMetastoreDirect() throws SQLException, SessionException, EncryptionException {
        AtomicBoolean rtn = new AtomicBoolean(Boolean.TRUE);
        ConversionResult conversionResult = getConversionResult();

        // TODO: Make sure connections has been populated.
        conversionResult.getConnections().forEach((k, v) -> {
            if (!isNull(v) && !isBlank(v.getMetastoreDirectUri())) { // && !finalConfigErrors) {
                if (configService.isMetastoreDirectConfigured(v)) {
                    try {
                        log.info("Testing Metastore Direct Connection for {}", k);
                        // TODO: Need to check that the connection has been setup..  I don't think it has yet.
                        Connection conn = getConnectionPools().getMetastoreDirectEnvironmentConnection(k);
                        if (conn != null) {
                            log.info("Metastore Direct Connection Successful for {}", k);
                        } else {
                            log.error("Metastore Direct Connection Failed for {}", k);
                            rtn.set(Boolean.FALSE);
                        }
                        v.setMetastoreDirectStatus(ConnectionStatus.SUCCESS);
                    } catch (SQLException se) {
                        log.error("Metastore Direct Connection Failed for {}", k, se);
                        v.setMetastoreDirectStatus(ConnectionStatus.FAILED);
                        v.setMetastoreDirectStatusMessage(se.getMessage());
                    }
                }

            } else if (!isNull(v)) {
                log.info("Metastore Direct Connection Check Configuration for {}", k);
                v.setMetastoreDirectStatus(ConnectionStatus.CHECK_CONFIGURATION);
            } else {
                log.info("Metastore Direct Connection Not Configured for {}", k);
                v.setMetastoreDirectStatus(ConnectionStatus.NOT_CONFIGURED);
            }
        });
        return rtn.get();
    }

    protected void initConnectionsSetup() throws SQLException, SessionException, EncryptionException, URISyntaxException {
        // Close and reset the connections.

        ConversionResult conversionResult = getConversionResult();
//        Connections connections = conversionResult.getConnections();
        JobDto job = conversionResult.getJob();
        JobExecution jobExecution = conversionResult.getJobExecution();
        // TODO: Review.  Use the ConnectionDtos
//        connections.reset();

        ConfigLiteDto config = conversionResult.getConfigLite();

//        RunStatus runStatus = executeSession.getRunStatus();

        if (job.getStrategy() == DataStrategyEnum.DUMP) {
            jobExecution.setDryRun(Boolean.TRUE); // No Actions.
            job.setSync(Boolean.FALSE);
        }

        // Make adjustments to the config clusters based on settings.
        // Buildout the right connections pool details.
        switch (job.getStrategy()) {
            case DUMP:
            case STORAGE_MIGRATION:
                // Don't load the datasource for the right with DUMP strategy.
                conversionResult.getConnections().remove(Environment.RIGHT);
                break;
        }

        // Should've been called already and HMSMirrorAppService.
        environmentService.setupGSS();

        // Initialize the drivers
        getConnectionPools().init();

    }

    //    @Override
    protected boolean initHS2() throws SQLException, SessionException, EncryptionException {
        AtomicBoolean rtn = new AtomicBoolean(Boolean.TRUE);
        ConversionResult conversionResult = getConversionResult();
//        Connections connections = conversionResult.getConnections();

        RunStatus runStatus = conversionResult.getRunStatus();

//            Environment[] hs2Envs = {Environment.LEFT, Environment.RIGHT};
            conversionResult.getConnections().forEach((environment, connection) -> {

//            for (Environment target : hs2Envs) {
                Connection conn = null;
                Statement stmt = null;
                try {
                    log.info("Testing HiveServer2 Connection for {}", environment);
                    conn = getConnectionPools().getHS2EnvironmentConnection(environment);
                    if (isNull(conn)) {
                        if (isNull(conversionResult.getConnection(environment))
                                || isNull(conversionResult.getConnection(environment).getHs2Uri())) {
                            runStatus.addWarning(ENVIRONMENT_DISCONNECTED, environment);
                        // TODO: Review if we need this.
//                        } else if (environment == Environment.RIGHT
//                                && conversionResult.getConnection(environment).isHs2Disconnected()) {
//                            // Skip error.  Set Warning that we're disconnected.
//                            runStatus.addWarning(ENVIRONMENT_DISCONNECTED, environment);
                        // TODO: Fix when we have test data fixed.
//                        } else if (!config.isLoadingTestData()) {
//                            runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, target);
//                            connections.getHiveServer2Connections().get(target).setStatus(ConnectionStatus.FAILED);
//                            connections.getHiveServer2Connections().get(target).setMessage("Connection is null.");
//                            rtn = Boolean.FALSE;
                        }
                    } else {
                        // Exercise the connections.
                        log.info("HS2 Connection Successful for {}", environment);
                        stmt = conn.createStatement();

                        // Run these first to ensure we preset the queue, if being set.
                        // Property Overrides from 'config.optimization.overrides'
                        // TODO: Review these property overrides.  I think these are NOT for the
                        //       connection properties, but for the initSqls to run on the session.
                        List<String> overrides = ConfigUtils.getPropertyOverridesFor(environment, conversionResult.getConfigLite().getOptimization().getOverrides());
                        connection.getHs2EnvSets().addAll(overrides);
                        // Run the overrides;
                        for (String o : overrides) {
                            log.info("Running Override: {} on {} connection", o, environment);
                            stmt.execute(o);
                        }

                        // Create an array of strings with various settings to run.
                        // Only set this when we need to know we need compute and we need to establish a session.
                        String[] sessionSets = conversionResult.getJobExecution().isExecute()?new String[]{
                                "SET hive.query.results.cache.enabled=false",
                                "SET hive.fetch.task.conversion = none"}:new String[0];

                        for (String s : sessionSets) {
                            connection.getHs2EnvSets().add(s);
                            log.info("Running session check: {} on {} connection", s, environment);
                            stmt.execute(s);
                        }

                        log.info("HS2 Connection validated (resources) for {}", environment);
                        connection.setHs2Status(ConnectionStatus.SUCCESS);
                        connection.setHs2StatusMessage("Connection Successful and Validated.");
                    }
                } catch (SQLException se) {
                    if (environment == Environment.RIGHT && !conversionResult.getConnection(environment).isHs2Connected()) {
                        // Set warning that RIGHT is disconnected.
                        runStatus.addWarning(ENVIRONMENT_DISCONNECTED, environment);
                    } else {
                        log.error(se.getMessage(), se);
                        runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, environment, se.getMessage());
                        connection.setHs2Status(ConnectionStatus.FAILED);
                        connection.setHs2StatusMessage(se.getMessage());
                        rtn.set(Boolean.FALSE);
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                    runStatus.addError(ENVIRONMENT_CONNECTION_ISSUE, environment, t.getMessage());
                    connection.setHs2Status(ConnectionStatus.FAILED);
                    connection.setHs2StatusMessage(t.getMessage());
                    rtn.set(Boolean.FALSE);
                } finally {
                    if (nonNull(stmt)) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    if (nonNull(conn)) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            });

        // Set state of connections.
        connected = rtn.get();
        return rtn.get();
    }

}
