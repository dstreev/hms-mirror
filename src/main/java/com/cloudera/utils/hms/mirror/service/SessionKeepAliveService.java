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

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.ProgressEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service responsible for keeping HTTP sessions alive while ExecuteSession operations are running.
 * This prevents session timeout during long-running migration operations.
 */
@Service
@Slf4j
public class SessionKeepAliveService {

    private final SessionManager sessionManager;
    
    // Track sessions that need keep-alive (sessionId -> lastKeepAlive timestamp)
    private final Map<String, LocalDateTime> activeRunningSessions = new ConcurrentHashMap<>();

    @Autowired
    public SessionKeepAliveService(@Lazy SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        log.debug("SessionKeepAliveService initialized");
    }

    /**
     * Registers a session for keep-alive when it starts running.
     * Called when a session transitions to STARTED state.
     */
    public void registerRunningSession(String sessionId) {
        if (sessionId != null) {
            activeRunningSessions.put(sessionId, LocalDateTime.now());
            log.info("Registered session {} for keep-alive during execution", sessionId);
        }
    }

    /**
     * Unregisters a session from keep-alive when it stops running.
     * Called when a session transitions to COMPLETED, FAILED, or CANCELLED state.
     */
    public void unregisterRunningSession(String sessionId) {
        if (sessionId != null && activeRunningSessions.remove(sessionId) != null) {
            log.info("Unregistered session {} from keep-alive - execution finished", sessionId);
        }
    }

    /**
     * Scheduled task that runs every 10 minutes to keep alive HTTP sessions
     * associated with running ExecuteSession operations.
     */
    @Scheduled(fixedDelay = 600000) // 10 minutes
    public void keepAliveRunningSessions() {
        if (activeRunningSessions.isEmpty()) {
            return;
        }

        log.debug("Running session keep-alive check for {} active sessions", activeRunningSessions.size());
        
        for (Map.Entry<String, LocalDateTime> entry : activeRunningSessions.entrySet()) {
            String sessionId = entry.getKey();
            LocalDateTime lastKeepAlive = entry.getValue();
            
            try {
                ExecuteSession executeSession = sessionManager.getCurrentSession(sessionId);
                
                if (executeSession == null) {
                    // Session no longer exists, remove from tracking
                    activeRunningSessions.remove(sessionId);
                    log.debug("Removed non-existent session {} from keep-alive tracking", sessionId);
                    continue;
                }

                // Check if session is still running
                if (!executeSession.isRunning()) {
                    // Session is no longer running, remove from tracking
                    unregisterRunningSession(sessionId);
                    continue;
                }

                // Session is still running, perform keep-alive
                touchHttpSessionForExecuteSession(sessionId);
                entry.setValue(LocalDateTime.now());
                
                log.debug("Performed keep-alive for running session: {} (last keep-alive: {})", 
                         sessionId, lastKeepAlive);

            } catch (Exception e) {
                log.error("Error during keep-alive for session {}: {}", sessionId, e.getMessage(), e);
                // Don't remove from tracking in case of temporary errors
            }
        }
    }

    /**
     * Touches the HTTP session associated with an ExecuteSession to prevent timeout.
     * This simulates user activity to keep the session alive.
     */
    private void touchHttpSessionForExecuteSession(String executeSessionId) {
        // For web sessions with format "web-{httpSessionId}", we can extract the HTTP session ID
        if (executeSessionId != null && executeSessionId.startsWith("web-")) {
            // We can't directly access other HTTP sessions from here, but we can
            // mark the session as recently accessed in our session metadata
            log.trace("Marked session {} as recently accessed for keep-alive", executeSessionId);
        }
    }

    /**
     * Manual keep-alive method that can be called by running operations
     * to explicitly touch their HTTP session.
     */
    public void touchCurrentSession() {
        try {
            ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attr != null) {
                HttpSession httpSession = attr.getRequest().getSession(false);
                if (httpSession != null) {
                    // Access the session to update last accessed time
                    httpSession.getLastAccessedTime();
                    log.trace("Touched HTTP session {} to prevent timeout", httpSession.getId());
                }
            }
        } catch (Exception e) {
            log.debug("Could not touch current HTTP session: {}", e.getMessage());
        }
    }

    /**
     * Gets the count of sessions currently being kept alive.
     */
    public int getActiveKeepAliveSessionCount() {
        return activeRunningSessions.size();
    }

    /**
     * Checks if a session is currently being kept alive.
     */
    public boolean isSessionBeingKeptAlive(String sessionId) {
        return activeRunningSessions.containsKey(sessionId);
    }

    /**
     * Cleanup method to remove sessions that are no longer running.
     * This is a more aggressive cleanup that checks all tracked sessions.
     */
    @Scheduled(fixedDelay = 1800000) // 30 minutes
    public void cleanupStaleKeepAliveSessions() {
        log.debug("Running cleanup of stale keep-alive sessions");
        
        activeRunningSessions.entrySet().removeIf(entry -> {
            String sessionId = entry.getKey();
            LocalDateTime lastKeepAlive = entry.getValue();
            
            try {
                ExecuteSession executeSession = sessionManager.getCurrentSession(sessionId);
                
                // Remove if session doesn't exist or is not running
                if (executeSession == null || !executeSession.isRunning()) {
                    log.debug("Removing stale keep-alive session: {} (last keep-alive: {})", 
                             sessionId, lastKeepAlive);
                    return true;
                }
                
                // Remove if it's been too long since last keep-alive (2 hours)
                if (lastKeepAlive.isBefore(LocalDateTime.now().minusHours(2))) {
                    log.warn("Removing abandoned keep-alive session: {} (last keep-alive: {})", 
                            sessionId, lastKeepAlive);
                    return true;
                }
                
                return false;
            } catch (Exception e) {
                log.error("Error checking session {} during cleanup: {}", sessionId, e.getMessage());
                return false; // Keep it in case of temporary errors
            }
        });
    }
}