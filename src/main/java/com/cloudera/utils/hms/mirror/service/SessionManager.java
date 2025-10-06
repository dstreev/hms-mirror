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
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
public class SessionManager {

    public static final String DEFAULT_SESSION_ID = "default";
    public static final String HTTP_SESSION_EXECUTE_SESSION_KEY = "executeSession";
    
    private final ExecuteSessionService executeSessionService;
    
    private final Map<String, ExecuteSession> sessions = new ConcurrentHashMap<>();
    
    private ExecuteSession defaultSession;

    @Autowired
    public SessionManager(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
        log.debug("SessionManager initialized");
    }

    public ExecuteSession getCurrentSession() {
        // First try to get from thread-local context (set by interceptor)
        ExecuteSession contextSession = SessionContextHolder.getSession();
        if (contextSession != null) {
            log.trace("Returning session from thread context: {}", contextSession.getSessionId());
            return contextSession;
        }
        
        // Fall back to HTTP session
        ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        
        if (attr != null) {
            HttpSession httpSession = attr.getRequest().getSession(false);
            if (httpSession != null) {
                ExecuteSession session = (ExecuteSession) httpSession.getAttribute(HTTP_SESSION_EXECUTE_SESSION_KEY);
                if (session != null) {
                    log.trace("Returning session from HTTP session: {}", session.getSessionId());
                    return session;
                }
            }
        }
        
        log.debug("No session found in context or HTTP session, returning default session");
        return getDefaultSession();
    }

    public ExecuteSession getCurrentSession(String sessionId) {
        if (isBlank(sessionId) || DEFAULT_SESSION_ID.equals(sessionId)) {
            return getCurrentSession();
        }
        
        return sessions.get(sessionId);
    }

    public ExecuteSession getDefaultSession() {
        if (defaultSession == null) {
            synchronized (this) {
                if (defaultSession == null) {
                    defaultSession = executeSessionService.getOrCreateDefaultSession();
                    if (defaultSession == null) {
                        try {
                            defaultSession = createSession(DEFAULT_SESSION_ID, null);
                        } catch (SessionException e) {
                            log.error("Failed to create default session", e);
                            throw new RuntimeException("Failed to create default session", e);
                        }
                    }
                }
            }
        }
        return defaultSession;
    }

    public ExecuteSession createSession(String sessionId, HmsMirrorConfig config) throws SessionException {
        String actualSessionId = isBlank(sessionId) ? DEFAULT_SESSION_ID : sessionId;
        
        ExecuteSession session = executeSessionService.createSession(actualSessionId, config);
        
        if (DEFAULT_SESSION_ID.equals(actualSessionId)) {
            defaultSession = session;
            executeSessionService.setSession(session);
        } else {
            sessions.put(actualSessionId, session);
        }
        
        associateWithHttpSession(session);
        
        log.debug("Created session: {}", actualSessionId);
        return session;
    }

    public void associateWithHttpSession(ExecuteSession session) {
        if (session == null) {
            log.debug("Cannot associate null session with HTTP session");
            return;
        }
        
        ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        
        if (attr != null) {
            HttpSession httpSession = attr.getRequest().getSession(true);
            httpSession.setAttribute(HTTP_SESSION_EXECUTE_SESSION_KEY, session);
            log.debug("Associated ExecuteSession {} with HTTP session {}", 
                     session.getSessionId(), httpSession.getId());
        }
    }

    public boolean save(HmsMirrorConfig config, int maxThreads) throws SessionException {
        ExecuteSession session = getCurrentSession();
        return executeSessionService.save(config, maxThreads);
    }

    public boolean save(String sessionId, HmsMirrorConfig config, int maxThreads) throws SessionException {
        ExecuteSession session = getCurrentSession(sessionId);
        if (session == null) {
            throw new SessionException("Session not found: " + sessionId);
        }
        
        ExecuteSession originalSession = executeSessionService.getSession();
        try {
            executeSessionService.setSession(session);
            return executeSessionService.save(config, maxThreads);
        } finally {
            executeSessionService.setSession(originalSession);
        }
    }

    public Boolean startSession(Integer concurrency) throws SessionException {
        ExecuteSession session = getCurrentSession();
        
        ExecuteSession originalSession = executeSessionService.getSession();
        try {
            executeSessionService.setSession(session);
            return executeSessionService.startSession(concurrency);
        } finally {
            executeSessionService.setSession(originalSession);
        }
    }

    public Boolean startSession(String sessionId, Integer concurrency) throws SessionException {
        ExecuteSession session = getCurrentSession(sessionId);
        if (session == null) {
            throw new SessionException("Session not found: " + sessionId);
        }
        
        ExecuteSession originalSession = executeSessionService.getSession();
        try {
            executeSessionService.setSession(session);
            return executeSessionService.startSession(concurrency);
        } finally {
            executeSessionService.setSession(originalSession);
        }
    }

    public void closeSession(String sessionId) throws SessionException {
        if (isBlank(sessionId) || DEFAULT_SESSION_ID.equals(sessionId)) {
            return;
        }
        
        ExecuteSession session = sessions.remove(sessionId);
        if (session != null) {
            session.close();
            log.debug("Closed session: {}", sessionId);
        }
    }

    public void cleanupHttpSession() {
        ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        
        if (attr != null) {
            HttpSession httpSession = attr.getRequest().getSession(false);
            if (httpSession != null) {
                httpSession.removeAttribute(HTTP_SESSION_EXECUTE_SESSION_KEY);
            }
        }
    }

    public Map<String, ExecuteSession> getAllSessions() {
        Map<String, ExecuteSession> allSessions = new ConcurrentHashMap<>(sessions);
        if (defaultSession != null) {
            allSessions.put(DEFAULT_SESSION_ID, defaultSession);
        }
        return allSessions;
    }

    public boolean isSessionExists(String sessionId) {
        if (isBlank(sessionId) || DEFAULT_SESSION_ID.equals(sessionId)) {
            return defaultSession != null;
        }
        return sessions.containsKey(sessionId);
    }
}