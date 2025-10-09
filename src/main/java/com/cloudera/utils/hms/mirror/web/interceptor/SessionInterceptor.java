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

package com.cloudera.utils.hms.mirror.web.interceptor;

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.SessionContextHolder;
import com.cloudera.utils.hms.mirror.service.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import static com.cloudera.utils.hms.mirror.service.SessionManager.HTTP_SESSION_EXECUTE_SESSION_KEY;

@Component
@Slf4j
public class SessionInterceptor implements HandlerInterceptor {

    @Autowired
    private SessionManager sessionManager;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String requestUri = request.getRequestURI();
        log.debug("SessionInterceptor handling request: {}", requestUri);
        
        // Always create HTTP session if it doesn't exist for web requests
        HttpSession httpSession = request.getSession(true);
        ExecuteSession executeSession = (ExecuteSession) httpSession.getAttribute(HTTP_SESSION_EXECUTE_SESSION_KEY);
        
        log.debug("HTTP Session ID: {}, ExecuteSession from session: {}", 
                 httpSession.getId(), executeSession != null ? executeSession.getSessionId() : "null");
        
        if (executeSession == null) {
            // Create a unique session ID based on HTTP session
            String sessionId = "web-" + httpSession.getId().substring(0, 8);
            log.debug("Creating new ExecuteSession with ID: {}", sessionId);
            
            try {
                // Use SessionContextHolder for standardized session creation
                // Since getOrCreateSession(sessionId) handles the specific sessionId case
                executeSession = SessionContextHolder.getOrCreateSession(sessionId);
                httpSession.setAttribute(HTTP_SESSION_EXECUTE_SESSION_KEY, executeSession);
                log.info("Created new ExecuteSession {} for HTTP session {}", executeSession.getSessionId(), httpSession.getId());
            } catch (Exception e) {
                log.warn("Failed to create ExecuteSession via SessionContextHolder, falling back to default: {}", e.getMessage());
                executeSession = sessionManager.getDefaultSession();
                httpSession.setAttribute(HTTP_SESSION_EXECUTE_SESSION_KEY, executeSession);
            }
        }
        
        SessionContextHolder.setSession(executeSession);
        log.debug("Set session context for request {}: {}", requestUri, 
                 executeSession != null ? executeSession.getSessionId() : "null");
        
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        SessionContextHolder.clearSession();
        log.trace("Cleared session context after request completion");
    }
}