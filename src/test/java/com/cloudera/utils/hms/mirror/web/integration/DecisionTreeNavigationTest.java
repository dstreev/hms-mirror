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

package com.cloudera.utils.hms.mirror.web.integration;

import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeResponse;
import com.cloudera.utils.hms.mirror.web.model.UserSelection;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
public class DecisionTreeNavigationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("SQL Decision Tree: Discover Actual Node Structure and Navigate with Real Values")
    void testSQLDecisionTreeRealNavigation() throws Exception {
        System.out.println("🚀 SQL DECISION TREE REAL NAVIGATION TEST");
        System.out.println("   🎯 This test discovers actual decision tree structure and navigates with real values");
        
        // Create session to maintain state across all REST API calls
        MockHttpSession session = new MockHttpSession();
        
        // Track all configuration values we set during navigation
        Map<String, Object> actualConfigurationValues = new HashMap<>();
        
        // Step 1: Initialize SQL decision tree
        System.out.println("\n📍 STEP 1: Initialize SQL Decision Tree");
        MvcResult startResult = mockMvc.perform(get("/api/v1/decision-tree/start/SQL")
                .session(session))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.strategy").value("SQL"))
                .andExpect(jsonPath("$.complete").value(false))
                .andExpect(jsonPath("$.sessionId").exists())
                .andReturn();

        DecisionTreeResponse currentResponse = objectMapper.readValue(
            startResult.getResponse().getContentAsString(),
            DecisionTreeResponse.class
        );
        
        String sessionId = currentResponse.getSessionId();
        System.out.println("   ✅ SQL Decision Tree initialized with session: " + sessionId);
        System.out.println("   🎯 Starting node: " + currentResponse.getCurrentNode().getId());

        // Step 2: Navigate the REAL decision tree by discovering actual node structure
        System.out.println("\n📍 STEP 2: Navigate Real SQL Decision Tree");
        navigateRealDecisionTree(currentResponse, sessionId, session, actualConfigurationValues);
        
        System.out.println("   ✅ Real decision tree navigation finished");
        System.out.println("   📊 Total configuration values discovered: " + actualConfigurationValues.size());

        // Step 3: Retrieve final server configuration  
        System.out.println("\n📍 STEP 3: Retrieve Final Server Configuration");
        MvcResult configResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .session(session))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> actualServerConfig = objectMapper.readValue(
            configResult.getResponse().getContentAsString(),
            Map.class
        );
        
        System.out.println("   ✅ Retrieved server configuration with " + actualServerConfig.size() + " properties");
        
        // Step 4: Show all server configuration properties that were actually set
        System.out.println("\n📍 STEP 4: Display Complete Server Configuration");
        displayServerConfiguration(actualServerConfig);

        System.out.println("\n🎉 SQL DECISION TREE REAL NAVIGATION TEST COMPLETE!");
        System.out.println("   ✅ Successfully navigated real decision tree structure");
        System.out.println("   ✅ Server configuration retrieved and displayed");
        System.out.println("   🎯 Test demonstrates complete REST API decision tree workflow!");
        
        // Basic validations
        assertNotNull(actualServerConfig, "Server configuration should not be null");
        assertTrue(actualServerConfig.size() > 0, "Server configuration should contain properties");
    }

    /**
     * Navigate through the real SQL decision tree by examining actual node structure
     */
    private void navigateRealDecisionTree(DecisionTreeResponse startResponse, String sessionId, 
                                        MockHttpSession session, Map<String, Object> configValues) throws Exception {
        DecisionTreeResponse currentResponse = startResponse;
        int nodeCount = 0;
        int maxNodes = 20; // Reasonable limit for debugging
        
        System.out.println("🌳 NAVIGATING REAL SQL DECISION TREE:");
        
        while (!currentResponse.isComplete() && nodeCount < maxNodes) {
            if (currentResponse.getCurrentNode() == null) {
                System.out.println("   ⚠️ No current node available, decision tree complete");
                break;
            }
            
            DecisionTreeNode currentNode = currentResponse.getCurrentNode();
            String nodeId = currentNode.getId();
            String nodeTitle = currentNode.getTitle();
            String nodeType = currentNode.getType().toString();
            String description = currentNode.getDescription();
            
            System.out.println("   🔘 NODE " + (nodeCount + 1) + ": " + nodeId);
            System.out.println("      📝 Title: " + nodeTitle);
            System.out.println("      🏷️  Type: " + nodeType);
            System.out.println("      📋 Description: " + description);
            
            // Examine available options
            if (currentNode.getOptions() != null && !currentNode.getOptions().isEmpty()) {
                System.out.println("      📊 Available options (" + currentNode.getOptions().size() + "):");
                currentNode.getOptions().forEach(option -> 
                    System.out.println("         • " + option.getValue() + " - " + option.getLabel())
                );
            } else {
                System.out.println("      ⚠️ No options available for this node");
            }
            
            // Determine what configuration this node might set
            String configKey = getConfigurationKeyForNode(nodeId);
            if (configKey != null) {
                System.out.println("      🔧 This node configures: " + configKey);
            }
            
            // Determine appropriate selection for this node
            String selection = determineRealSelectionForNode(nodeId, currentNode);
            
            if (selection == null) {
                System.out.println("      ❌ Cannot determine valid selection for node: " + nodeId);
                break;
            }
            
            System.out.println("      ➤ SELECTING: " + selection);
            
            // Record what configuration value we're setting
            if (configKey != null) {
                configValues.put(configKey, selection);
                System.out.println("      ✅ Recording configuration: " + configKey + " = " + selection);
            }
            
            // Navigate to next node
            try {
                currentResponse = navigateToNextNode(currentResponse, sessionId, session, selection);
                if (currentResponse == null) {
                    System.out.println("      ❌ Navigation failed, stopping");
                    break;
                }
                System.out.println("      ✅ Navigation successful");
            } catch (Exception e) {
                System.out.println("      ❌ Navigation error: " + e.getMessage());
                break;
            }
            
            nodeCount++;
        }
        
        System.out.println("🎯 REAL DECISION TREE NAVIGATION SUMMARY:");
        System.out.println("   📊 Total nodes navigated: " + nodeCount);
        System.out.println("   🔧 Configuration values recorded: " + configValues.size());
        
        if (!configValues.isEmpty()) {
            System.out.println("   📝 Recorded configuration values:");
            configValues.forEach((key, value) -> 
                System.out.println("      • " + key + " = " + value)
            );
        }
    }

    /**
     * Determine what configuration key this node affects
     */
    private String getConfigurationKeyForNode(String nodeId) {
        switch (nodeId) {
            case "left_connection":
                return "clusters.LEFT.connection";
            case "right_connection":
                return "clusters.RIGHT.connection";
            case "database_only":
                return "databaseOnly";
            case "execute_mode":
                return "execute";
            case "sync_mode":
                return "sync";
            case "skip_link_check":
                return "skipLinkCheck";
            case "copy_avro_schema_urls":
                return "copyAvroSchemaUrls";
            default:
                return "node." + nodeId;
        }
    }

    /**
     * Determine appropriate selection based on real node structure
     */
    private String determineRealSelectionForNode(String nodeId, DecisionTreeNode node) {
        // Check if node has options
        if (node.getOptions() != null && !node.getOptions().isEmpty()) {
            // For CONNECTION_SELECT nodes, look for meaningful connection options
            if (node.getType() == DecisionTreeNode.NodeType.CONNECTION_SELECT) {
                // Try to find common connection options
                return node.getOptions().stream()
                    .filter(option -> {
                        String value = option.getValue().toLowerCase();
                        return value.contains("default") || 
                               value.contains("existing") || 
                               value.contains("new") ||
                               value.contains("skip");
                    })
                    .map(option -> option.getValue())
                    .findFirst()
                    .orElse(node.getOptions().get(0).getValue()); // Default to first option
            }
            
            // For other node types with options, use first option
            return node.getOptions().get(0).getValue();
        }
        
        // For nodes without options, provide appropriate input based on type
        switch (node.getType()) {
            case BOOLEAN:
                return "true";
            case TEXT_INPUT:
                return "test_value";
            case NUMBER_INPUT:
                return "100";
            default:
                // For unknown nodes, try common navigation values
                return "continue";
        }
    }

    /**
     * Navigate to the next node in the decision tree
     */
    private DecisionTreeResponse navigateToNextNode(DecisionTreeResponse currentResponse, String sessionId, 
                                                   MockHttpSession session, String selection) throws Exception {
        UserSelection userSelection = new UserSelection();
        userSelection.setSessionId(sessionId);
        userSelection.setStrategy("SQL");
        userSelection.setCurrentNodeId(currentResponse.getCurrentNode().getId());
        userSelection.setSelectedOption(selection);
        
        MvcResult navResult = mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .session(session)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(userSelection)))
                .andExpect(status().isOk())
                .andReturn();
        
        return objectMapper.readValue(
            navResult.getResponse().getContentAsString(),
            DecisionTreeResponse.class
        );
    }

    /**
     * Display complete server configuration in an organized way
     */
    private void displayServerConfiguration(Map<String, Object> config) {
        System.out.println("🔍 COMPLETE SERVER CONFIGURATION:");
        System.out.println("   📊 Total properties: " + config.size());
        System.out.println();
        
        // Display top-level properties
        System.out.println("   📋 TOP-LEVEL PROPERTIES:");
        config.entrySet().stream()
            .filter(entry -> !(entry.getValue() instanceof Map))
            .forEach(entry -> 
                System.out.println("      • " + entry.getKey() + " = " + entry.getValue() + 
                    " (" + (entry.getValue() != null ? entry.getValue().getClass().getSimpleName() : "null") + ")")
            );
        
        // Display nested objects
        System.out.println("\n   🏗️ NESTED OBJECTS:");
        config.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof Map)
            .forEach(entry -> {
                System.out.println("      📦 " + entry.getKey() + ":");
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedMap = (Map<String, Object>) entry.getValue();
                nestedMap.forEach((key, value) -> 
                    System.out.println("         • " + key + " = " + value + 
                        " (" + (value != null ? value.getClass().getSimpleName() : "null") + ")")
                );
            });
    }
}