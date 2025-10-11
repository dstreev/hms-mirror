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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.service.SessionManager;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode.NodeType;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeResponse;
import com.cloudera.utils.hms.mirror.web.model.DecisionOption;
import com.cloudera.utils.hms.mirror.web.model.UserSelection;
import com.cloudera.utils.hms.mirror.web.service.DecisionTreeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(DecisionTreeController.class)
@DisplayName("Decision Tree Controller Tests")
class DecisionTreeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DecisionTreeService decisionTreeService;

    @MockBean
    private SessionManager sessionManager;

    @Autowired
    private ObjectMapper objectMapper;

    private ExecuteSession mockSession;
    private HmsMirrorConfig mockConfig;
    private DecisionTreeNode mockNode;

    @BeforeEach
    void setUp() {
        mockSession = new ExecuteSession();
        mockSession.setSessionId("test-session-12345");
        
        mockConfig = new HmsMirrorConfig();
        mockConfig.setReadOnly(false);
        mockConfig.setSync(false);
        mockConfig.setDatabaseOnly(false);
        mockSession.setConfig(mockConfig);

        mockNode = createMockDecisionTreeNode();
    }

    @Test
    @DisplayName("Test Case 1: Initialize Decision Tree for SQL Strategy")
    void testStartDecisionTree_SqlStrategy_Success() throws Exception {
        // Arrange
        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.initializeDecisionTree("SQL", mockConfig)).thenReturn(mockNode);

        // Act & Assert
        MvcResult result = mockMvc.perform(get("/api/v1/decision-tree/start/SQL"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.sessionId").value("test-session-12345"))
                .andExpect(jsonPath("$.strategy").value("SQL"))
                .andExpect(jsonPath("$.complete").value(false))
                .andExpect(jsonPath("$.currentNode").exists())
                .andExpect(jsonPath("$.currentNode.id").value("connectionSelection"))
                .andExpect(jsonPath("$.currentNode.type").value("CONNECTION_SELECT"))
                .andReturn();

        // Verify service interactions
        verify(sessionManager).getCurrentSession();
        verify(decisionTreeService).initializeDecisionTree("SQL", mockConfig);

        // Parse response and validate structure
        String responseContent = result.getResponse().getContentAsString();
        DecisionTreeResponse response = objectMapper.readValue(responseContent, DecisionTreeResponse.class);
        
        assertNotNull(response.getCurrentNode());
        assertEquals("connectionSelection", response.getCurrentNode().getId());
        assertFalse(response.isComplete());
    }

    @Test
    @DisplayName("Test Case 2: Navigate Decision Tree - Connection Selection")
    void testNavigateDecisionTree_ConnectionSelection_Success() throws Exception {
        // Arrange
        UserSelection userSelection = new UserSelection();
        userSelection.setStrategy("SQL");
        userSelection.setCurrentNodeId("connectionSelection");
        userSelection.setSelectedOption("existing");
        userSelection.setSessionId("test-session-12345");

        DecisionTreeNode nextNode = createGeneralBehaviorNode();
        
        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.getNextNode("connectionSelection", "existing", mockConfig))
                .thenReturn(nextNode);

        // Act & Assert
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(userSelection)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionId").value("test-session-12345"))
                .andExpect(jsonPath("$.strategy").value("SQL"))
                .andExpect(jsonPath("$.complete").value(false))
                .andExpect(jsonPath("$.currentNode.id").value("generalBehavior"))
                .andExpect(jsonPath("$.currentNode.type").value("MULTI_SELECT"))
                .andExpect(jsonPath("$.currentNode.options").isArray())
                .andExpect(jsonPath("$.currentNode.options[0].id").value("databaseOnly"));

        // Verify service interactions
        verify(sessionManager).getCurrentSession();
        verify(decisionTreeService).applyUserSelection(any(UserSelection.class), eq(mockConfig));
        verify(decisionTreeService).getNextNode("connectionSelection", "existing", mockConfig);
    }

    @Test
    @DisplayName("Test Case 3: Navigate Decision Tree - General Behavior Multi-Select")
    void testNavigateDecisionTree_GeneralBehavior_MultiSelect() throws Exception {
        // Arrange
        UserSelection userSelection = new UserSelection();
        userSelection.setStrategy("SQL");
        userSelection.setCurrentNodeId("generalBehavior");
        userSelection.setSelectedOptions(Arrays.asList("readOnly", "sync"));
        userSelection.setSessionId("test-session-12345");

        DecisionTreeNode nextNode = createMigrationTypesNode();
        
        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.getNextNode(eq("generalBehavior"), isNull(), eq(mockConfig)))
                .thenReturn(nextNode);

        // Act & Assert
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(userSelection)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.currentNode.id").value("migrationTypes"))
                .andExpect(jsonPath("$.currentNode.type").value("SINGLE_SELECT"));

        // Verify that multi-select options were processed
        verify(decisionTreeService).applyUserSelection(argThat(selection -> 
            selection.getSelectedOptions().contains("readOnly") && 
            selection.getSelectedOptions().contains("sync")), eq(mockConfig));
    }

    @Test
    @DisplayName("Test Case 4: Complete Decision Tree Flow")
    void testNavigateDecisionTree_CompletedFlow() throws Exception {
        // Arrange
        UserSelection userSelection = new UserSelection();
        userSelection.setStrategy("SQL");
        userSelection.setCurrentNodeId("migrationTypes");
        userSelection.setSelectedOption("distcp");
        userSelection.setSessionId("test-session-12345");

        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.getNextNode("migrationTypes", "distcp", mockConfig))
                .thenReturn(null); // null indicates completion

        // Act & Assert
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(userSelection)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.complete").value(true))
                .andExpect(jsonPath("$.currentNode").doesNotExist());
    }

    @Test
    @DisplayName("Test Case 5: Get Current Configuration")
    void testGetCurrentConfig_Success() throws Exception {
        // Arrange
        Map<String, Object> configData = new HashMap<>();
        configData.put("skipLinkCheck", false);
        configData.put("databaseOnly", false);
        configData.put("readOnly", true);
        configData.put("sync", true);
        configData.put("execute", false);
        configData.put("migrationType", "distcp");
        configData.put("connectionType", "existing");

        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.extractConfigData(mockConfig)).thenReturn(configData);

        // Act & Assert
        mockMvc.perform(get("/api/v1/decision-tree/current-config"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.skipLinkCheck").value(false))
                .andExpect(jsonPath("$.databaseOnly").value(false))
                .andExpect(jsonPath("$.readOnly").value(true))
                .andExpect(jsonPath("$.sync").value(true))
                .andExpect(jsonPath("$.migrationType").value("distcp"))
                .andExpect(jsonPath("$.connectionType").value("existing"));

        verify(sessionManager).getCurrentSession();
        verify(decisionTreeService).extractConfigData(mockConfig);
    }

    @Test
    @DisplayName("Test Case 6: Validate Configuration")
    void testValidateCurrentConfig_Success() throws Exception {
        // Arrange
        Map<String, Object> validationResult = new HashMap<>();
        validationResult.put("valid", true);
        validationResult.put("errors", Arrays.asList());
        validationResult.put("warnings", Arrays.asList());
        
        Map<String, Object> summary = new HashMap<>();
        summary.put("strategy", "SQL");
        summary.put("readOnlyMode", true);
        summary.put("syncMode", true);
        summary.put("migrationType", "distcp");
        summary.put("connectionConfigured", true);
        validationResult.put("configurationSummary", summary);

        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.validateConfig(mockConfig)).thenReturn(validationResult);

        // Act & Assert
        mockMvc.perform(get("/api/v1/decision-tree/validate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").value(true))
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.warnings").isArray())
                .andExpect(jsonPath("$.configurationSummary.strategy").value("SQL"))
                .andExpect(jsonPath("$.configurationSummary.readOnlyMode").value(true));
    }

    @Test
    @DisplayName("Test Case 7: Reset Decision Tree")
    void testResetDecisionTree_Success() throws Exception {
        // Arrange
        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        doNothing().when(decisionTreeService).resetConfig(mockConfig);

        // Act & Assert
        mockMvc.perform(post("/api/v1/decision-tree/reset"))
                .andExpect(status().isOk());

        verify(sessionManager).getCurrentSession();
        verify(decisionTreeService).resetConfig(mockConfig);
    }

    @Test
    @DisplayName("Test Case 8: Invalid Strategy Error Handling")
    void testStartDecisionTree_InvalidStrategy_BadRequest() throws Exception {
        // Arrange
        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.initializeDecisionTree("INVALID", mockConfig))
                .thenThrow(new IllegalArgumentException("Invalid strategy: INVALID"));

        // Act & Assert
        mockMvc.perform(get("/api/v1/decision-tree/start/INVALID"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Test Case 9: Navigation with Invalid Input")
    void testNavigateDecisionTree_InvalidInput_BadRequest() throws Exception {
        // Arrange
        UserSelection invalidSelection = new UserSelection();
        invalidSelection.setStrategy("SQL");
        invalidSelection.setCurrentNodeId("invalidNode");
        invalidSelection.setSelectedOption("invalidOption");

        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        doThrow(new IllegalArgumentException("Invalid node: invalidNode"))
                .when(decisionTreeService).applyUserSelection(any(UserSelection.class), eq(mockConfig));

        // Act & Assert
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(invalidSelection)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Test Case 10: Service Exception Handling")
    void testGetCurrentConfig_ServiceException_BadRequest() throws Exception {
        // Arrange
        when(sessionManager.getCurrentSession()).thenReturn(mockSession);
        when(decisionTreeService.extractConfigData(mockConfig))
                .thenThrow(new RuntimeException("Service error"));

        // Act & Assert
        mockMvc.perform(get("/api/v1/decision-tree/current-config"))
                .andExpect(status().isBadRequest());
    }

    // Helper Methods

    private DecisionTreeNode createMockDecisionTreeNode() {
        DecisionTreeNode node = new DecisionTreeNode();
        node.setId("connectionSelection");
        node.setType(NodeType.CONNECTION_SELECT);
        node.setTitle("Connection Selection");
        node.setDescription("Select connection for LEFT environment");
        node.setRequired(true);
        
        DecisionOption option1 = new DecisionOption();
        option1.setValue("existing");
        option1.setLabel("Use existing LEFT connection");
        
        DecisionOption option2 = new DecisionOption();
        option2.setValue("new");
        option2.setLabel("Configure new LEFT connection");
        
        node.setOptions(Arrays.asList(option1, option2));
        return node;
    }

    private DecisionTreeNode createGeneralBehaviorNode() {
        DecisionTreeNode node = new DecisionTreeNode();
        node.setId("generalBehavior");
        node.setType(NodeType.MULTI_SELECT);
        node.setTitle("General Behavior Configuration");
        node.setDescription("Configure general migration behavior");
        
        DecisionOption option1 = new DecisionOption();
        option1.setValue("databaseOnly");
        option1.setLabel("Database Only");
        option1.setDescription("Migrate database metadata only");
        
        DecisionOption option2 = new DecisionOption();
        option2.setValue("readOnly");
        option2.setLabel("Read Only Mode");
        option2.setDescription("Run in read-only mode without making changes");
        
        DecisionOption option3 = new DecisionOption();
        option3.setValue("sync");
        option3.setLabel("Sync Mode");
        option3.setDescription("Enable synchronization mode");
        
        node.setOptions(Arrays.asList(option1, option2, option3));
        return node;
    }

    private DecisionTreeNode createMigrationTypesNode() {
        DecisionTreeNode node = new DecisionTreeNode();
        node.setId("migrationTypes");
        node.setType(NodeType.SINGLE_SELECT);
        node.setTitle("Migration Types");
        node.setDescription("Select migration approach");
        
        DecisionOption option1 = new DecisionOption();
        option1.setValue("distcp");
        option1.setLabel("DistCP Migration");
        option1.setDescription("Use Hadoop DistCP for data migration");
        
        DecisionOption option2 = new DecisionOption();
        option2.setValue("storage_migration");
        option2.setLabel("Storage Migration");
        option2.setDescription("Perform in-place storage migration");
        
        node.setOptions(Arrays.asList(option1, option2));
        return node;
    }
}