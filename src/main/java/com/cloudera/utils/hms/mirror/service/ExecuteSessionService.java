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
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
@Slf4j
@Getter
public class ExecuteSessionService {

    public static final String DEFAULT = "default.yaml";

    private CliEnvironment cliEnvironment;

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

    /*
    Used to limit the number of sessions that are retained in memory.
     */
    private int maxRetainedSessions = 5;
    private final Deque<ExecuteSession> executeSessionQueue = new ConcurrentLinkedDeque<>();
    private final Map<String, ExecuteSession> sessions = new HashMap<>();

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Bean
    CommandLineRunner setMaxRetainedSessions(@Value("${hms-mirror.session-retention.max}") int maxSessions) {
        return args -> {
            this.maxRetainedSessions = maxSessions;
        };
    }

    public ExecuteSession createSession(String sessionId, HmsMirrorConfig hmsMirrorConfig) {
        String sessionName = sessionId != null? sessionId : DEFAULT;

        ExecuteSession session;
        if (sessions.containsKey(sessionName)) {
            session = sessions.get(sessionName);
            session.setOrigConfig(hmsMirrorConfig);
        } else {
            session = new ExecuteSession();
            session.setSessionId(sessionName);
            session.setOrigConfig(hmsMirrorConfig);
            // These aren't cloned and should only be created before the session is run.
//            session.setRunStatus(new RunStatus());
//            session.setConversion(new Conversion());
            sessions.put(sessionName, session);
        }
        return session;
    }

    /*
    If sessionId is null, then pull the 'current' session.
    If sessionId is NOT null, look for it in the session map and return it.
    When not found, throw exception.
     */
    public ExecuteSession getSession(String sessionId) {
        if (sessionId == null) {
            if (loadedSession == null) {
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

    public ExecuteSession getActiveSession() {
        ExecuteSession activeSession = executeSessionQueue.peekFirst();//getSession(DEFAULT);
        if (activeSession == null) {
            throw new RuntimeException("No active session found.");
        }
        return activeSession;
    }

    /*
      Look at the 'activeSession' and if it is not null, check that it is not running.
        If it is not running, then clone the currentSession and add it to the 'executeSessionQueue'.
        Set the 'activeSession' to null.  The 'getActiveSession' will then return the last session
        placed in the queue and set 'activeSession' to that session.

        This allow us to keep the current and active sessions separate.  The active session is the
        one that will be referenced during the run.
     */
    public ExecuteSession transitionSessionToActive(String sessionId) {
        ExecuteSession activeSession = null;
        try {
            activeSession = getActiveSession();
        } catch (RuntimeException e) {
            // This is expected if there is no active session.
        }
        if (activeSession != null && activeSession.getRunning().get()) {
            throw new RuntimeException("Session is still running.  Cannot transition to active.");
        }

        ExecuteSession loadedSession = getSession(sessionId);
        ExecuteSession session = loadedSession.clone();
        // Set the active session id to the current date and time.
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        session.setSessionId(dtf.format(new Date()));
        // Create the RunStatus and Conversion objects.
        session.setRunStatus(new RunStatus());
        session.setConversion(new Conversion());
        executeSessionQueue.addFirst(session);
        // Maintain the queue size by remove the last session if the queue size exceeds the max.
        while (executeSessionQueue.size() > maxRetainedSessions) {
            executeSessionQueue.removeLast();
        }
        return session;
    }

}
