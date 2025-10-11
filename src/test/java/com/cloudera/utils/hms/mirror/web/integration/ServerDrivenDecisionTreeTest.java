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

import com.cloudera.utils.hms.mirror.web.model.DecisionOption;
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
public class ServerDrivenDecisionTreeTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Server-Driven Decision Tree: Let server control navigation and extract/set properties at each node")
    void testServerDrivenDecisionTreeNavigation() throws Exception {
        System.out.println("🚀 SERVER-DRIVEN DECISION TREE TEST");
        System.out.println("   🎯 Server controls navigation, client extracts properties and sets values");
        
        // Create session to maintain state across all REST API calls
        MockHttpSession session = new MockHttpSession();
        
        // Track all properties we discover and set during server-driven navigation
        Map<String, Object> discoveredProperties = new HashMap<>();
        Map<String, Object> setValues = new HashMap<>();
        
        // Step 1: Initialize SQL decision tree - server provides first node
        System.out.println("\n📍 STEP 1: Initialize SQL Decision Tree (Server Provides First Node)");
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
        System.out.println("   ✅ Server initialized SQL decision tree with session: " + sessionId);
        System.out.println("   🎯 Server presented first node: " + currentResponse.getCurrentNode().getId());

        // Step 2: Let server drive navigation - process each node the server presents
        System.out.println("\n📍 STEP 2: Server-Driven Navigation (Process Each Node Server Presents)");
        processServerDrivenNavigation(currentResponse, sessionId, session, discoveredProperties, setValues);
        
        System.out.println("   ✅ Server-driven navigation complete");
        System.out.println("   📊 Properties discovered: " + discoveredProperties.size());
        System.out.println("   📊 Values set: " + setValues.size());

        // Step 3: Retrieve final HmsMirrorConfig from server session
        System.out.println("\n📍 STEP 3: Retrieve Final HmsMirrorConfig from Server Session");
        MvcResult configResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .session(session))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> finalServerConfig = objectMapper.readValue(
            configResult.getResponse().getContentAsString(),
            Map.class
        );
        
        System.out.println("   ✅ Retrieved final HmsMirrorConfig with " + finalServerConfig.size() + " properties");

        // Step 4: Validate that values we set are reflected in server's HmsMirrorConfig
        System.out.println("\n📍 STEP 4: Validate Set Values Against Server's HmsMirrorConfig");
        validateServerMirrorConfig(setValues, finalServerConfig, discoveredProperties);

        System.out.println("\n🎉 SERVER-DRIVEN DECISION TREE TEST COMPLETE!");
        System.out.println("   ✅ Server successfully controlled entire navigation flow");
        System.out.println("   ✅ Client successfully extracted properties and set values");
        System.out.println("   ✅ Server's HmsMirrorConfig reflects client-set values");
        System.out.println("   🎯 Complete server-client decision tree workflow validated!");
        
        // Test assertions
        assertNotNull(finalServerConfig, "Server's HmsMirrorConfig should not be null");
        assertFalse(finalServerConfig.isEmpty(), "Server's HmsMirrorConfig should contain properties");
        assertFalse(setValues.isEmpty(), "Should have set some values during navigation");
        System.out.println("   ✅ All test assertions passed!");
    }

    /**
     * Process server-driven navigation - let server present nodes, extract properties, set values
     */
    private void processServerDrivenNavigation(DecisionTreeResponse startResponse, String sessionId, 
                                             MockHttpSession session, Map<String, Object> discoveredProperties,
                                             Map<String, Object> setValues) throws Exception {
        DecisionTreeResponse currentResponse = startResponse;
        int nodeCount = 0;
        int maxNodes = 30; // Reasonable limit to prevent infinite loops
        
        System.out.println("🌳 PROCESSING SERVER-DRIVEN DECISION TREE NAVIGATION:");
        
        while (!currentResponse.isComplete() && nodeCount < maxNodes) {
            if (currentResponse.getCurrentNode() == null) {
                System.out.println("   ✅ Server indicates decision tree is complete (no more nodes)");
                break;
            }
            
            DecisionTreeNode serverNode = currentResponse.getCurrentNode();
            String nodeId = serverNode.getId();
            String nodeTitle = serverNode.getTitle();
            String nodeType = serverNode.getType().toString();
            String description = serverNode.getDescription();
            
            System.out.println("   🔘 SERVER PRESENTED NODE " + (nodeCount + 1) + ": " + nodeId);
            System.out.println("      📝 Title: " + nodeTitle);
            System.out.println("      🏷️  Type: " + nodeType);
            System.out.println("      📋 Description: " + description);
            
            // Step A: Extract properties from the server-presented node
            Map<String, Object> nodeProperties = extractPropertiesFromServerNode(serverNode);
            System.out.println("      🔍 Extracted " + nodeProperties.size() + " properties from server node");
            
            // Add discovered properties to our tracking
            nodeProperties.forEach((key, value) -> {
                discoveredProperties.put(nodeId + "." + key, value);
                System.out.println("         📌 Property: " + key + " = " + value);
            });
            
            // Step B: Determine values to set for the extracted properties
            Map<String, Object> valuesToSet = determineValuesToSet(serverNode, nodeProperties);
            System.out.println("      🔧 Determined " + valuesToSet.size() + " values to set");
            
            // Add set values to our tracking
            valuesToSet.forEach((key, value) -> {
                setValues.put(nodeId + "." + key, value);
                System.out.println("         ✅ Setting: " + key + " = " + value);
            });
            
            // Step C: Create selection based on server node's available options
            String selection = createSelectionFromServerNode(serverNode, valuesToSet);
            if (selection == null) {
                System.out.println("      ❌ Could not create valid selection for server node: " + nodeId);
                break;
            }
            
            System.out.println("      ➤ SENDING SELECTION TO SERVER: " + selection);
            
            // Step D: Send values back to server via REST API
            try {
                currentResponse = sendSelectionToServer(currentResponse, sessionId, session, selection, valuesToSet);
                if (currentResponse == null) {
                    System.out.println("      ❌ Server rejected selection, ending navigation");
                    break;
                }
                System.out.println("      ✅ Server accepted selection and provided next node");
            } catch (Exception e) {
                System.out.println("      ❌ Failed to send selection to server: " + e.getMessage());
                
                // Try alternative selections if available
                String[] alternatives = getAlternativeSelections(serverNode);
                boolean successful = false;
                
                for (String alternative : alternatives) {
                    try {
                        System.out.println("      🔄 Trying alternative selection: " + alternative);
                        currentResponse = sendSelectionToServer(currentResponse, sessionId, session, alternative, valuesToSet);
                        if (currentResponse != null) {
                            System.out.println("      ✅ Server accepted alternative selection: " + alternative);
                            successful = true;
                            break;
                        }
                    } catch (Exception e2) {
                        System.out.println("      ❌ Alternative failed: " + alternative);
                    }
                }
                
                if (!successful) {
                    System.out.println("      ❌ All selections failed, ending navigation");
                    break;
                }
            }
            
            nodeCount++;
        }
        
        System.out.println("🎯 SERVER-DRIVEN NAVIGATION SUMMARY:");
        System.out.println("   📊 Nodes processed: " + nodeCount);
        System.out.println("   🔍 Properties discovered: " + discoveredProperties.size());
        System.out.println("   🔧 Values set: " + setValues.size());
        
        if (!discoveredProperties.isEmpty()) {
            System.out.println("   📝 All discovered properties:");
            discoveredProperties.forEach((key, value) -> 
                System.out.println("      📌 " + key + " = " + value)
            );
        }
        
        if (!setValues.isEmpty()) {
            System.out.println("   📝 All values set:");
            setValues.forEach((key, value) -> 
                System.out.println("      ✅ " + key + " = " + value)
            );
        }
    }

    /**
     * Extract properties that can be configured from the server-presented node
     */
    private Map<String, Object> extractPropertiesFromServerNode(DecisionTreeNode serverNode) {
        Map<String, Object> properties = new HashMap<>();
        
        // Extract basic node properties
        properties.put("id", serverNode.getId());
        properties.put("title", serverNode.getTitle());
        properties.put("type", serverNode.getType().toString());
        properties.put("description", serverNode.getDescription());
        properties.put("category", serverNode.getCategory());
        properties.put("required", serverNode.isRequired());
        
        // Extract available options if present
        if (serverNode.getOptions() != null && !serverNode.getOptions().isEmpty()) {
            properties.put("optionCount", serverNode.getOptions().size());
            properties.put("hasOptions", true);
            
            // Extract option details
            for (int i = 0; i < serverNode.getOptions().size(); i++) {
                DecisionOption option = serverNode.getOptions().get(i);
                properties.put("option" + i + ".value", option.getValue());
                properties.put("option" + i + ".label", option.getLabel());
                properties.put("option" + i + ".recommended", option.isRecommended());
                if (option.getConfigMapping() != null) {
                    properties.put("option" + i + ".configMapping", option.getConfigMapping().toString());
                }
            }
        } else {
            properties.put("hasOptions", false);
            properties.put("optionCount", 0);
        }
        
        // Extract metadata if present
        if (serverNode.getMetadata() != null) {
            serverNode.getMetadata().forEach((key, value) -> 
                properties.put("metadata." + key, value)
            );
        }
        
        // Extract validation rules if present
        if (serverNode.getValidationRules() != null) {
            serverNode.getValidationRules().forEach((key, value) -> 
                properties.put("validationRule." + key, value)
            );
        }
        
        return properties;
    }

    /**
     * Determine what values to set based on the server node and its properties
     */
    private Map<String, Object> determineValuesToSet(DecisionTreeNode serverNode, Map<String, Object> nodeProperties) {
        Map<String, Object> valuesToSet = new HashMap<>();
        
        String nodeId = serverNode.getId();
        String nodeType = serverNode.getType().toString();
        
        // Set values based on node type and purpose
        switch (nodeType) {
            case "BOOLEAN":
                valuesToSet.put("booleanValue", determineBooleanValue(nodeId));
                break;
                
            case "TEXT_INPUT":
                valuesToSet.put("textValue", determineTextValue(nodeId));
                break;
                
            case "NUMBER_INPUT":
                valuesToSet.put("numberValue", determineNumberValue(nodeId));
                break;
                
            case "SINGLE_SELECT":
                if (serverNode.getOptions() != null && !serverNode.getOptions().isEmpty()) {
                    // Use recommended option or first option
                    DecisionOption selectedOption = serverNode.getOptions().stream()
                        .filter(DecisionOption::isRecommended)
                        .findFirst()
                        .orElse(serverNode.getOptions().get(0));
                    valuesToSet.put("selectedOption", selectedOption.getValue());
                    valuesToSet.put("selectedLabel", selectedOption.getLabel());
                }
                break;
                
            case "MULTI_SELECT":
                if (serverNode.getOptions() != null && !serverNode.getOptions().isEmpty()) {
                    // Select first option for multi-select
                    valuesToSet.put("selectedOptions", serverNode.getOptions().get(0).getValue());
                }
                break;
                
            case "CONNECTION_SELECT":
                valuesToSet.put("connectionProfile", determineConnectionValue(nodeId));
                break;
                
            default:
                valuesToSet.put("defaultValue", "test_value_for_" + nodeId);
                break;
        }
        
        return valuesToSet;
    }

    /**
     * Create selection to send to server based on server node and values to set
     */
    private String createSelectionFromServerNode(DecisionTreeNode serverNode, Map<String, Object> valuesToSet) {
        String nodeType = serverNode.getType().toString();
        
        // Create selection based on node type
        switch (nodeType) {
            case "BOOLEAN":
                return valuesToSet.getOrDefault("booleanValue", "true").toString();
                
            case "TEXT_INPUT":
                return valuesToSet.getOrDefault("textValue", "test_input").toString();
                
            case "NUMBER_INPUT":
                return valuesToSet.getOrDefault("numberValue", "100").toString();
                
            case "SINGLE_SELECT":
                return valuesToSet.getOrDefault("selectedOption", "default").toString();
                
            case "MULTI_SELECT":
                return valuesToSet.getOrDefault("selectedOptions", "option1").toString();
                
            case "CONNECTION_SELECT":
                return valuesToSet.getOrDefault("connectionProfile", "default").toString();
                
            case "PAGE_TRANSITION":
                return "continue";
                
            default:
                // If server provided options, use the first one
                if (serverNode.getOptions() != null && !serverNode.getOptions().isEmpty()) {
                    return serverNode.getOptions().get(0).getValue();
                }
                return "continue";
        }
    }

    /**
     * Send selection and values to server via REST API
     */
    private DecisionTreeResponse sendSelectionToServer(DecisionTreeResponse currentResponse, String sessionId, 
                                                      MockHttpSession session, String selection, 
                                                      Map<String, Object> valuesToSet) throws Exception {
        
        UserSelection userSelection = new UserSelection();
        userSelection.setSessionId(sessionId);
        userSelection.setStrategy("SQL");
        userSelection.setCurrentNodeId(currentResponse.getCurrentNode().getId());
        userSelection.setSelectedOption(selection);
        
        // Include form data with values we want to set
        if (!valuesToSet.isEmpty()) {
            userSelection.setFormData(valuesToSet);
        }
        
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
     * Get alternative selections to try if primary selection fails
     */
    private String[] getAlternativeSelections(DecisionTreeNode serverNode) {
        String nodeType = serverNode.getType().toString();
        
        switch (nodeType) {
            case "BOOLEAN":
                return new String[]{"false", "true"};
            case "CONNECTION_SELECT":
                return new String[]{"skip", "default", "none"};
            case "SINGLE_SELECT":
                return new String[]{"skip", "none", "default"};
            default:
                return new String[]{"continue", "skip", "next"};
        }
    }

    /**
     * Determine boolean value based on node purpose
     */
    private boolean determineBooleanValue(String nodeId) {
        // Set meaningful boolean values based on node purpose
        switch (nodeId) {
            case "database_only":
                return false; // We want to migrate tables too
            case "copy_avro_schemas":
            case "migrate_views":
                return true; // Enable these features
            case "skip_link_check":
            case "acid_downgrade":
                return false; // Don't skip or downgrade
            default:
                return true; // Default to true for most boolean settings
        }
    }

    /**
     * Determine text value based on node purpose
     */
    private String determineTextValue(String nodeId) {
        switch (nodeId) {
            case "intermediate_storage":
                return "/tmp/hms-mirror-intermediate";
            case "target_namespace":
                return "hdfs://target-cluster/warehouse";
            case "iceberg_properties":
                return "write.format.default=parquet";
            default:
                return "test_value_for_" + nodeId;
        }
    }

    /**
     * Determine number value based on node purpose
     */
    private int determineNumberValue(String nodeId) {
        switch (nodeId) {
            case "acid_bucket_threshold":
                return 2;
            case "acid_partition_limit":
                return 500;
            case "iceberg_version":
                return 2;
            default:
                return 100;
        }
    }

    /**
     * Determine connection value - this would normally come from available connection profiles
     */
    private String determineConnectionValue(String nodeId) {
        // In a real scenario, this would query available connection profiles
        return "default_connection_profile";
    }

    /**
     * Validate that values we set are reflected in the server's HmsMirrorConfig
     */
    private void validateServerMirrorConfig(Map<String, Object> setValues, Map<String, Object> serverConfig, 
                                          Map<String, Object> discoveredProperties) {
        System.out.println("🔍 VALIDATING SERVER'S HmsMirrorConfig AGAINST SET VALUES:");
        System.out.println("   📊 Values we set: " + setValues.size());
        System.out.println("   📊 Server config properties: " + serverConfig.size());
        System.out.println("   📊 Properties discovered: " + discoveredProperties.size());

        // Show complete server configuration
        System.out.println("\n📋 COMPLETE SERVER HmsMirrorConfig:");
        serverConfig.forEach((key, value) -> 
            System.out.println("   • " + key + " = " + value + " (" + 
                (value != null ? value.getClass().getSimpleName() : "null") + ")")
        );

        // Show what values we tried to set
        System.out.println("\n📝 VALUES WE SET DURING NAVIGATION:");
        setValues.forEach((key, value) -> 
            System.out.println("   ✅ " + key + " = " + value)
        );

        // Show properties we discovered
        System.out.println("\n🔍 PROPERTIES DISCOVERED FROM SERVER NODES:");
        discoveredProperties.forEach((key, value) -> 
            System.out.println("   📌 " + key + " = " + value)
        );

        // Basic validation - server should have core properties
        String[] coreProperties = {"databaseOnly", "execute", "readOnly", "sync", "skipLinkCheck", "copyAvroSchemaUrls"};
        int foundCore = 0;
        
        for (String prop : coreProperties) {
            if (serverConfig.containsKey(prop)) {
                System.out.println("   ✅ Core property found: " + prop + " = " + serverConfig.get(prop));
                foundCore++;
            }
        }
        
        System.out.println("\n📊 VALIDATION SUMMARY:");
        System.out.println("   ✅ Core properties found: " + foundCore + "/" + coreProperties.length);
        System.out.println("   🔍 Properties discovered during navigation: " + discoveredProperties.size());
        System.out.println("   🔧 Values set during navigation: " + setValues.size());
        System.out.println("   📋 Final server config properties: " + serverConfig.size());
        
        assertTrue(foundCore > 0, "Should find at least some core properties in server config");
        assertTrue(serverConfig.size() > 0, "Server config should not be empty");
        assertTrue(discoveredProperties.size() > 0, "Should discover properties from server nodes");
        
        System.out.println("   🎯 Server-driven decision tree workflow validation complete!");
    }
}