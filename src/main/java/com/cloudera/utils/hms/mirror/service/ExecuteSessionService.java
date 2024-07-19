/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class ExecuteSessionService {

    public static final String DEFAULT = "default.yaml";

    private CliEnvironment cliEnvironment;
    private ConfigService configService;
    private ConnectionPoolService connectionPoolService;

//    /*
//    This should be an immutable 'running' or 'ran' version of a session.
//     */
//    @Setter
////    private ExecuteSession activeSession;

    /*
    This is the current session that can be modified (but not running yet).  This is where
    configurations can be changed before running.  Once the session is kicked off, the object
    should be cloned and the original session moved to the 'activeSession' field and added
    to the 'executeSessionQueue'.
     */
    @Setter
    private ExecuteSession loadedSession;

    private ExecuteSession activeSession;

    private String reportOutputDirectory;

    private boolean amendSessionIdToReportDir = Boolean.TRUE;

    /*
    Used to limit the number of sessions that are retained in memory.
     */
//    private int maxRetainedSessions = 5;
    private final Map<String, ExecuteSession> executeSessionMap = new TreeMap<>();
    private final Map<String, ExecuteSession> sessions = new HashMap<>();

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    public void setReportOutputDirectory(String reportOutputDirectory, boolean amendSessionIdToReportDir) {
        this.amendSessionIdToReportDir = amendSessionIdToReportDir;
        this.reportOutputDirectory = reportOutputDirectory;
    }

    public ExecuteSession createSession(String sessionId, HmsMirrorConfig hmsMirrorConfig) {
        String sessionName = !isBlank(sessionId) ? sessionId : DEFAULT;

        ExecuteSession session;
        if (sessions.containsKey(sessionName)) {
            session = sessions.get(sessionName);
            session.setConfig(hmsMirrorConfig);
        } else {
            session = new ExecuteSession();
            session.setSessionId(sessionName);
            session.setConfig(hmsMirrorConfig);
            sessions.put(sessionName, session);
        }
        return session;
    }

    public void clearLoadedSession() throws SessionException {
        clearActiveSession();
        loadedSession = null;
    }

    public void clearActiveSession() throws SessionException {
        if (nonNull(activeSession)) {
            RunStatus runStatus = activeSession.getRunStatus();
            switch (runStatus.getProgress()) {
                case IN_PROGRESS:
                    throw new SessionException("Session is still running.  You can't change the session while it is running.");
                case CANCEL_FAILED:
                    throw new SessionException("Session has failed to cancel.  You can't change the session after it has failed to cancel.");
                case CANCELLED:
                case COMPLETED:
                case FAILED:
                default:
                    break;
            }
            activeSession = null;
            // Close the connections pools, so they can be reset.
            connectionPoolService.close();
        }
    }

    public ExecuteSession getSession() {
        if (activeSession != null)
            return activeSession;
        else
            return loadedSession;
    }

//    public ExecuteSession getSession() {
//        if (activeSession == null) {
//            log.warn("No active session.  Transitioning loaded session to active.");
////            try {
////                transitionLoadedSessionToActive(null);
////            } catch (SessionException e) {
////                //throw new RuntimeException(e);
////            }
////            throw new RuntimeException("No active session. Try configuring a session first.");
//        }
//        return activeSession;
//    }

    /*
    If sessionId is null, then pull the 'current' session.
    If sessionId is NOT null, look for it in the session map and return it.
    When not found, throw exception.
     */
    public ExecuteSession getSession(String sessionId) {
        if (isBlank(sessionId)) {
            if (isNull(loadedSession)) {
                throw new RuntimeException("No session loaded.");
            }
            return loadedSession;
        } else {
            if (sessions.containsKey(sessionId)) {
                return sessions.get(sessionId);
            } else {
                throw new RuntimeException("Session not found: " + sessionId);
            }
        }
    }

    /*
      Look at the 'activeSession' and if it is not null, check that it is not running.
        If it is not running, then clone the currentSession and add it to the 'executeSessionQueue'.
        Set the 'activeSession' to null.  The 'getActiveSession' will then return the last session
        placed in the queue and set 'activeSession' to that session.

        This allow us to keep the current and active sessions separate.  The active session is the
        one that will be referenced during the run.
     */
    public Boolean transitionLoadedSessionToActive(Integer concurrency, boolean connectionCheckOnly) throws SessionException {
        Boolean rtn = Boolean.TRUE;

        if (!isNull(activeSession) && activeSession.isRunning()) {
            throw new SessionException("Session is still running.  Cannot transition to active.");
        }

        // This should get the loaded session and clone it.
        if (isNull(loadedSession)) {
            throw new SessionException("No session loaded.");
        }

        ExecuteSession session = getSession();
        // If it's connected (Active Session), don't go through all this again.
        if (isNull(session) || !session.isConnected()) {
            log.debug("Configure and setup Session");
            HmsMirrorConfig config = loadedSession.getConfig();

            // Setup connections concurrency
            // We need to pass on a few scale parameters to the hs2 configs so the connections pools can handle the scale requested.
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2()) && nonNull(concurrency)) {
                Cluster cluster = config.getCluster(Environment.LEFT);
                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(concurrency / 2));
                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(concurrency / 2));
                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(concurrency));
                    if (isNull(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis")) ||
                            isBlank(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis"))) {
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "5000");
                    }
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(concurrency));
                }
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2()) && nonNull(concurrency)) {
                Cluster cluster = config.getCluster(Environment.RIGHT);
                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(concurrency / 2));
                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(concurrency / 2));
                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(concurrency));
                    if (isNull(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis")) ||
                            isBlank(cluster.getHiveServer2().getConnectionProperties().getProperty("maxWaitMillis"))) {
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "5000");
                    }
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(concurrency));
                }
            }

            // TODO: Set Metastore Direct Concurrency.

            session = loadedSession.clone();

            // Connection Service should be set to the resolved config.
            connectionPoolService.close();
//        connectionPoolService.setHmsMirrorConfig(session.getConfig());
            connectionPoolService.setExecuteSession(session);

//        try {
//            connectionPoolService.init();
//        } catch (Exception e) {
//            log.error("Error initializing connections pool.", e);
//            throw new RuntimeException("Error initializing connections pool.", e);
//        }

            if (isNull(session.getSessionId())) {
                // Set the active session id to the current date and time.
                DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
                session.setSessionId(dtf.format(new Date()));
            }

            String sessionReportDir = null;
            if (amendSessionIdToReportDir) {
                sessionReportDir = reportOutputDirectory + File.separator + session.getSessionId();
            } else {
                sessionReportDir = reportOutputDirectory;
            }
            session.getConfig().setOutputDirectory(sessionReportDir);

            // Create the RunStatus and Conversion objects.
            RunStatus runStatus = new RunStatus();
            runStatus.setConcurrency(concurrency);
            // Link the RunStatus to the session so users know what session details to retrieve.
            runStatus.setSessionId(session.getSessionId());
            session.setRunStatus(runStatus);
            session.setConversion(new Conversion());
            // Clear all previous run states from sessions to keep memory usage down.
            for (Map.Entry<String, ExecuteSession> entry : executeSessionMap.entrySet()) {
                entry.getValue().setConversion(null);
                entry.getValue().setRunStatus(null);
            }
            activeSession = session;

            // Validate the session.
            rtn = configService.validate(activeSession, getCliEnvironment(), connectionCheckOnly);

            // Set whether the config has been validated.
            activeSession.getConfig().setValidated(rtn);

            executeSessionMap.put(session.getSessionId(), session);
            // Set connected flag.
//            activeSession.setConnected(Boolean.TRUE);
        } else {
            log.debug("Session connected already");
        }
        return rtn;
    }

}
