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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.service.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

@Controller
@Order(1) // High priority to override other controllers
@Slf4j
public class ReactSpaController {
    
    @Autowired
    private SessionManager sessionManager;

    /**
     * Serves the React SPA for the root path.
     */
    @GetMapping(value = {"", "/"}, produces = "text/html")
    public String index(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Serving React SPA for root: {}", requestUri);
        ensureSessionExists(request);
        return "forward:/react/index.html";
    }

    /**
     * Serves the React SPA for client-side routes.
     */
    @GetMapping({"/dashboard", "/config", "/config/**", "/connections", "/connections/**", "/databases", "/execution", "/reports", "/reports/**"})
    public String spa(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Serving React SPA for client route: {}", requestUri);
        ensureSessionExists(request);
        return "forward:/react/index.html";
    }
    
    /**
     * Ensures that an ExecuteSession exists for the current HTTP session.
     * This is needed because static resource forwards don't go through the SessionInterceptor.
     */
    private void ensureSessionExists(HttpServletRequest request) {
        HttpSession httpSession = request.getSession(true);
        ExecuteSession executeSession = (ExecuteSession) httpSession.getAttribute("executeSession");
        
        if (executeSession == null) {
            String sessionId = "web-" + httpSession.getId().substring(0, 8);
            log.debug("Creating ExecuteSession {} for HTTP session {}", sessionId, httpSession.getId());
            
//            try {
                executeSession = sessionManager.createSession(sessionId);
                if (executeSession != null) {
                    httpSession.setAttribute("executeSession", executeSession);
                    log.info("Created ExecuteSession {} for React UI", sessionId);
                } else {
                    log.warn("Failed to create ExecuteSession, using default");
                    executeSession = sessionManager.getDefaultSession();
                    httpSession.setAttribute("executeSession", executeSession);
                }
//            } catch (SessionException e) {
//                log.warn("Failed to create ExecuteSession: {}, using default", e.getMessage());
//                executeSession = sessionManager.getDefaultSession();
//                httpSession.setAttribute(SessionManager.HTTP_SESSION_EXECUTE_SESSION_KEY, executeSession);
//            }
        } else {
            log.debug("ExecuteSession {} already exists for HTTP session {}", 
                     executeSession.getSessionId(), httpSession.getId());
        }
    }

    /**
     * Redirect legacy Thymeleaf paths to the React SPA.
     */
    @GetMapping({"/legacy/**", "/thymeleaf/**"})
    public String redirectLegacy(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        log.info("Redirecting legacy path {} to React SPA", requestUri);
        return "redirect:/";
    }
}