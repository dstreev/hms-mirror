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

package com.cloudera.utils.hms.mirror.web.controller.api.v2;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.SessionContextHolder;
import com.cloudera.utils.hms.mirror.service.SessionKeepAliveService;
import com.cloudera.utils.hms.mirror.service.SessionManager;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;

@CrossOrigin
@RestController
@Slf4j
@RequestMapping(path = "/api/v2/session")
@Tag(name = "Session Management", description = "Multi-session management operations")
public class SessionController {

    @Autowired
    private SessionManager sessionManager;
    
    @Autowired(required = false)
    private SessionKeepAliveService sessionKeepAliveService;

    @Operation(summary = "Get current session information")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Current session retrieved successfully")
    })
    @GetMapping("/current")
    public ResponseEntity<ExecuteSession> getCurrentSession() {
        ExecuteSession session = sessionManager.getCurrentSession();
        return ResponseEntity.ok(session);
    }

    @Operation(summary = "Get current session basic info")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Current session info retrieved successfully")
    })
    @GetMapping("/debug")
    public ResponseEntity<Map<String, Object>> debugSessionInfo(HttpServletRequest request) {
        Map<String, Object> debugInfo = new HashMap<>();
        
        try {
            // Check thread context
            ExecuteSession contextSession = SessionContextHolder.getSession();
            debugInfo.put("threadContextSession", contextSession != null ? contextSession.getSessionId() : "null");
            
            // Check HTTP session
            HttpSession httpSession = request.getSession(false);
            if (httpSession != null) {
                debugInfo.put("httpSessionId", httpSession.getId());
                ExecuteSession sessionFromHttp = (ExecuteSession) httpSession.getAttribute(SessionManager.HTTP_SESSION_EXECUTE_SESSION_KEY);
                debugInfo.put("executeSessionFromHttp", sessionFromHttp != null ? sessionFromHttp.getSessionId() : "null");
            } else {
                debugInfo.put("httpSessionId", "null");
                debugInfo.put("executeSessionFromHttp", "no_http_session");
            }
            
            // Check via SessionManager
            ExecuteSession managerSession = sessionManager.getCurrentSession();
            debugInfo.put("sessionManagerSession", managerSession != null ? managerSession.getSessionId() : "null");
            
            // Check if RequestAttributes available
            ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            debugInfo.put("requestAttributesAvailable", attr != null);
            
            return ResponseEntity.ok(debugInfo);
        } catch (Exception e) {
            debugInfo.put("error", e.getMessage());
            return ResponseEntity.ok(debugInfo);
        }
    }

    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getCurrentSessionInfo() {
        try {
            ExecuteSession session = sessionManager.getCurrentSession();
            Map<String, Object> sessionInfo = new HashMap<>();
            
            if (session != null && session.getSessionId() != null) {
                sessionInfo.put("sessionId", session.getSessionId());
                sessionInfo.put("connected", session.isConnected());
                sessionInfo.put("running", session.isRunning());
                sessionInfo.put("concurrency", session.getConcurrency());
                log.debug("Returning session info for session: {}", session.getSessionId());
            } else {
                log.warn("Session is null or has null sessionId");
                sessionInfo.put("sessionId", "unknown");
                sessionInfo.put("connected", false);
                sessionInfo.put("running", false);
                sessionInfo.put("concurrency", 0);
            }
            
            return ResponseEntity.ok(sessionInfo);
        } catch (Exception e) {
            log.error("Error getting session info", e);
            Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("sessionId", "error");
            errorInfo.put("connected", false);
            errorInfo.put("running", false);
            errorInfo.put("concurrency", 0);
            return ResponseEntity.ok(errorInfo);
        }
    }

    @Operation(summary = "Get specific session by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Session retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Session not found")
    })
    @GetMapping("/{sessionId}")
    public ResponseEntity<ExecuteSession> getSession(
            @Parameter(description = "Session ID") @PathVariable String sessionId) {
        
        ExecuteSession session = sessionManager.getCurrentSession(sessionId);
        if (session == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(session);
    }

    @Operation(summary = "Create a new session")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Session created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid session configuration")
    })
    @PostMapping("/{sessionId}")
    public ResponseEntity<ExecuteSession> createSession(
            @Parameter(description = "Session ID") @PathVariable String sessionId,
            @RequestBody(required = false) HmsMirrorConfig config) {
        
        try {
            ExecuteSession session = sessionManager.createSession(sessionId, config);
            return ResponseEntity.status(201).body(session);
        } catch (SessionException e) {
            log.error("Failed to create session: {}", sessionId, e);
            return ResponseEntity.badRequest().build();
        }
    }

    @Operation(summary = "Start a session")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Session started successfully"),
            @ApiResponse(responseCode = "400", description = "Session cannot be started"),
            @ApiResponse(responseCode = "404", description = "Session not found")
    })
    @PostMapping("/{sessionId}/start")
    public ResponseEntity<RunStatus> startSession(
            @Parameter(description = "Session ID") @PathVariable String sessionId,
            @Parameter(description = "Concurrency level") @RequestParam(defaultValue = "10") Integer concurrency) {
        
        try {
            if (!sessionManager.isSessionExists(sessionId)) {
                return ResponseEntity.notFound().build();
            }
            
            Boolean started = sessionManager.startSession(sessionId, concurrency);
            if (started) {
                ExecuteSession session = sessionManager.getCurrentSession(sessionId);
                return ResponseEntity.ok(session.getRunStatus());
            } else {
                return ResponseEntity.badRequest().build();
            }
        } catch (SessionException e) {
            log.error("Failed to start session: {}", sessionId, e);
            return ResponseEntity.badRequest().build();
        }
    }

    @Operation(summary = "Save session configuration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Configuration saved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid configuration"),
            @ApiResponse(responseCode = "404", description = "Session not found")
    })
    @PutMapping("/{sessionId}/config")
    public ResponseEntity<Boolean> saveSessionConfig(
            @Parameter(description = "Session ID") @PathVariable String sessionId,
            @RequestBody HmsMirrorConfig config,
            @Parameter(description = "Max threads") @RequestParam(defaultValue = "10") Integer maxThreads) {
        
        try {
            if (!sessionManager.isSessionExists(sessionId)) {
                return ResponseEntity.notFound().build();
            }
            
            Boolean saved = sessionManager.save(sessionId, config, maxThreads);
            return ResponseEntity.ok(saved);
        } catch (SessionException e) {
            log.error("Failed to save session config: {}", sessionId, e);
            return ResponseEntity.badRequest().build();
        }
    }

    @Operation(summary = "Delete a session")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Session deleted successfully"),
            @ApiResponse(responseCode = "400", description = "Session cannot be deleted")
    })
    @DeleteMapping("/{sessionId}")
    public ResponseEntity<Void> deleteSession(
            @Parameter(description = "Session ID") @PathVariable String sessionId) {
        
        try {
            sessionManager.closeSession(sessionId);
            return ResponseEntity.noContent().build();
        } catch (SessionException e) {
            log.error("Failed to delete session: {}", sessionId, e);
            return ResponseEntity.badRequest().build();
        }
    }

    @Operation(summary = "List all sessions")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Sessions listed successfully")
    })
    @GetMapping("/list")
    public ResponseEntity<Map<String, ExecuteSession>> listSessions() {
        Map<String, ExecuteSession> sessions = sessionManager.getAllSessions();
        return ResponseEntity.ok(sessions);
    }

    @Operation(summary = "Keep current session alive")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Session keep-alive performed successfully")
    })
    @PostMapping("/keep-alive")
    public ResponseEntity<Map<String, Object>> keepAlive() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            if (sessionKeepAliveService != null) {
                sessionKeepAliveService.touchCurrentSession();
                
                ExecuteSession session = sessionManager.getCurrentSession();
                if (session != null) {
                    response.put("sessionId", session.getSessionId());
                    response.put("isRunning", session.isRunning());
                    response.put("isBeingKeptAlive", sessionKeepAliveService.isSessionBeingKeptAlive(session.getSessionId()));
                    response.put("status", "success");
                } else {
                    response.put("status", "no_session");
                }
            } else {
                response.put("status", "service_unavailable");
            }
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error performing keep-alive", e);
            response.put("status", "error");
            response.put("error", e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    @Operation(summary = "Get session keep-alive status")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Keep-alive status retrieved successfully")
    })
    @GetMapping("/keep-alive/status")
    public ResponseEntity<Map<String, Object>> getKeepAliveStatus() {
        Map<String, Object> response = new HashMap<>();
        
        if (sessionKeepAliveService != null) {
            response.put("serviceEnabled", true);
            response.put("activeKeepAliveSessions", sessionKeepAliveService.getActiveKeepAliveSessionCount());
            
            ExecuteSession session = sessionManager.getCurrentSession();
            if (session != null) {
                response.put("currentSessionId", session.getSessionId());
                response.put("currentSessionRunning", session.isRunning());
                response.put("currentSessionBeingKeptAlive", sessionKeepAliveService.isSessionBeingKeptAlive(session.getSessionId()));
            }
        } else {
            response.put("serviceEnabled", false);
        }
        
        return ResponseEntity.ok(response);
    }
}