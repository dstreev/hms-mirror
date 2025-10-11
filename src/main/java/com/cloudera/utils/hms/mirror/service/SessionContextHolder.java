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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@Slf4j
public class SessionContextHolder {
    
    private static final ThreadLocal<ExecuteSession> sessionContext = new ThreadLocal<>();
    
    public static void setSession(ExecuteSession session) {
        sessionContext.set(session);
        log.debug("Set session in context: {}", session != null ? session.getSessionId() : "null");
    }
    
    public static ExecuteSession getSession() {
        ExecuteSession session = sessionContext.get();
        if (session == null) {
            session = createDefaultSession();
            sessionContext.set(session);
            log.debug("Auto-created session in getSession(): {}", session.getSessionId());
        }
        return session;
    }
    
    public static ExecuteSession getOrCreateSession() {
        return getSession(); // Now simply delegates to getSession() since it auto-creates
    }
    
    public static ExecuteSession getOrCreateSession(String sessionId) {
        ExecuteSession session = sessionContext.get();
        if (session == null) {
            session = createSessionWithId(sessionId);
            sessionContext.set(session);
            log.debug("Created and set new session in context with ID: {}", session.getSessionId());
        } else if (sessionId != null && !sessionId.equals(session.getSessionId())) {
            log.debug("Existing session {} does not match requested ID {}, creating new session", 
                     session.getSessionId(), sessionId);
            session = createSessionWithId(sessionId);
            sessionContext.set(session);
        }
        return session;
    }
    
    private static ExecuteSession createDefaultSession() {
        ExecuteSession session = new ExecuteSession();
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        String sessionId = "session-" + dtf.format(new Date());
        session.setSessionId(sessionId);
        session.setConfig(new HmsMirrorConfig());
        log.debug("Created default ExecuteSession with ID: {} and default config", sessionId);
        return session;
    }
    
    private static ExecuteSession createSessionWithId(String sessionId) {
        ExecuteSession session = new ExecuteSession();
        String actualSessionId = (sessionId != null && !sessionId.trim().isEmpty()) ? sessionId : generateSessionId();
        session.setSessionId(actualSessionId);
        session.setConfig(new HmsMirrorConfig());
        log.debug("Created ExecuteSession with ID: {} and default config", actualSessionId);
        return session;
    }
    
    private static String generateSessionId() {
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        return "session-" + dtf.format(new Date()) + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    public static void clearSession() {
        ExecuteSession session = sessionContext.get();
        sessionContext.remove();
        log.debug("Cleared session from context: {}", session != null ? session.getSessionId() : "null");
    }
    
    public static boolean hasSession() {
        return sessionContext.get() != null;
    }
}