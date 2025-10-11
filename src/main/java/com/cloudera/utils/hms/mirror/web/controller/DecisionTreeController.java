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
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.SessionManager;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeResponse;
import com.cloudera.utils.hms.mirror.web.model.UserSelection;
import com.cloudera.utils.hms.mirror.web.service.DecisionTreeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/decision-tree")
@Slf4j
public class DecisionTreeController {

    private final DecisionTreeService decisionTreeService;
    private final SessionManager sessionManager;

    @Autowired
    public DecisionTreeController(DecisionTreeService decisionTreeService, SessionManager sessionManager) {
        this.decisionTreeService = decisionTreeService;
        this.sessionManager = sessionManager;
    }

    @GetMapping("/start/{strategy}")
    public ResponseEntity<DecisionTreeResponse> startDecisionTree(@PathVariable("strategy") String strategy) {
        try {
            ExecuteSession session = sessionManager.getCurrentSession();
            HmsMirrorConfig config = session.getConfig();
            
            // Initialize decision tree for the selected strategy
            DecisionTreeNode rootNode = decisionTreeService.initializeDecisionTree(strategy, config);
            
            DecisionTreeResponse response = DecisionTreeResponse.builder()
                .currentNode(rootNode)
                .sessionId(session.getSessionId())
                .strategy(strategy)
                .build();
                
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error starting decision tree for strategy: {}", strategy, e);
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/navigate")
    public ResponseEntity<DecisionTreeResponse> navigateTree(@RequestBody UserSelection userSelection) {
        try {
            ExecuteSession session;
            if (userSelection.getSessionId() != null && !userSelection.getSessionId().isEmpty()) {
                session = sessionManager.getCurrentSession(userSelection.getSessionId());
                if (session == null) {
                    log.error("Session not found: {}", userSelection.getSessionId());
                    return ResponseEntity.badRequest().build();
                }
            } else {
                session = sessionManager.getCurrentSession();
            }
            HmsMirrorConfig config = session.getConfig();
            
            // Apply user selection to config
            decisionTreeService.applyUserSelection(userSelection, config);
            
            // Get next node based on current selection
            DecisionTreeNode nextNode = decisionTreeService.getNextNode(
                userSelection.getCurrentNodeId(), 
                userSelection.getSelectedOption(),
                config
            );
            
            DecisionTreeResponse response = DecisionTreeResponse.builder()
                .currentNode(nextNode)
                .sessionId(session.getSessionId())
                .strategy(userSelection.getStrategy())
                .isComplete(nextNode == null)
                .build();
                
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error navigating decision tree", e);
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/current-config")
    public ResponseEntity<Map<String, Object>> getCurrentConfig(@RequestParam(value = "sessionId", required = false) String sessionId) {
        try {
            ExecuteSession session;
            if (sessionId != null && !sessionId.isEmpty()) {
                session = sessionManager.getCurrentSession(sessionId);
                if (session == null) {
                    log.error("Session not found: {}", sessionId);
                    return ResponseEntity.badRequest().build();
                }
            } else {
                session = sessionManager.getCurrentSession();
            }
            HmsMirrorConfig config = session.getConfig();
            
            Map<String, Object> configData = decisionTreeService.extractConfigData(config);
            return ResponseEntity.ok(configData);
        } catch (Exception e) {
            log.error("Error getting current config", e);
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/reset")
    public ResponseEntity<Void> resetDecisionTree(@RequestParam(value = "sessionId", required = false) String sessionId) {
        try {
            ExecuteSession session;
            if (sessionId != null && !sessionId.isEmpty()) {
                session = sessionManager.getCurrentSession(sessionId);
                if (session == null) {
                    log.error("Session not found: {}", sessionId);
                    return ResponseEntity.badRequest().build();
                }
            } else {
                session = sessionManager.getCurrentSession();
            }
            decisionTreeService.resetConfig(session.getConfig());
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Error resetting decision tree", e);
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/validate")
    public ResponseEntity<Map<String, Object>> validateCurrentConfig(@RequestParam(value = "sessionId", required = false) String sessionId) {
        try {
            ExecuteSession session;
            if (sessionId != null && !sessionId.isEmpty()) {
                session = sessionManager.getCurrentSession(sessionId);
                if (session == null) {
                    log.error("Session not found: {}", sessionId);
                    return ResponseEntity.badRequest().build();
                }
            } else {
                session = sessionManager.getCurrentSession();
            }
            HmsMirrorConfig config = session.getConfig();
            
            Map<String, Object> validation = decisionTreeService.validateConfig(config);
            return ResponseEntity.ok(validation);
        } catch (Exception e) {
            log.error("Error validating config", e);
            return ResponseEntity.badRequest().build();
        }
    }
}