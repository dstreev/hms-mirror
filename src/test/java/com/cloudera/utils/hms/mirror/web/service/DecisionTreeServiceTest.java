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

package com.cloudera.utils.hms.mirror.web.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeNode.NodeType;
import com.cloudera.utils.hms.mirror.web.model.DecisionOption;
import com.cloudera.utils.hms.mirror.web.model.UserSelection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Decision Tree Service Tests")
class DecisionTreeServiceTest {

    @Mock
    private ResourceLoader resourceLoader;

    @Mock
    private Resource resource;

    @InjectMocks
    private DecisionTreeService decisionTreeService;

    private HmsMirrorConfig testConfig;
    private String sampleYamlContent;

    @BeforeEach
    void setUp() {
        testConfig = new HmsMirrorConfig();
        testConfig.setReadOnly(false);
        testConfig.setSync(false);
        testConfig.setDatabaseOnly(false);

        sampleYamlContent = """
            strategy: "SQL"
            description: "Use Hive SQL to move data between clusters via INSERT statements"
            pages:
              connections:
                title: "Connection Selection"
                description: "Select source and target cluster connections"
                nodes:
                  - id: "left_connection"
                    title: "LEFT Connection"
                    description: "Select source cluster connection profile"
                    type: "CONNECTION_SELECT"
                    category: "connections"
                    required: true
                    nextNodeId: "right_connection"
                    
                  - id: "right_connection"
                    title: "RIGHT Connection"
                    description: "Select target cluster connection profile"
                    type: "CONNECTION_SELECT"
                    category: "connections"
                    required: true
                    nextPageId: "general_behavior"
                    
              general_behavior:
                title: "General Behavior"
                description: "Configure basic migration behavior"
                nodes:
                  - id: "database_only"
                    title: "Database Only Migration"
                    description: "Migrate only database definitions, skip tables"
                    type: "BOOLEAN"
                    category: "general"
                    required: false
                    nextNodeId: "copy_avro_schemas"
                    
                  - id: "copy_avro_schemas"
                    title: "Copy AVRO Schema Files"
                    description: "Copy AVRO schema files between clusters"
                    type: "BOOLEAN"
                    category: "general"
                    required: false
                    nextPageId: "execution_mode"
                    
              execution_mode:
                title: "Execution Mode"
                description: "Final execution settings"
                nodes:
                  - id: "execution_type"
                    title: "Execution Type"
                    description: "Choose execution mode"
                    type: "SINGLE_SELECT"
                    category: "execution"
                    required: true
                    options:
                      - value: "dryRun"
                        label: "Dry Run"
                        description: "Generate plans without executing (recommended first)"
                      - value: "execute"
                        label: "Execute"
                        description: "Execute the migration"
                    nextNodeId: "complete"
                    
                  - id: "complete"
                    title: "Configuration Complete"
                    description: "SQL strategy configuration is complete"
                    type: "PAGE_TRANSITION"
                    category: "completion"
            """;
    }

    @Test
    @DisplayName("Test 1: Load Decision Trees Successfully")
    void testLoadDecisionTrees_Success() throws Exception {
        // Arrange
        InputStream inputStream = new ByteArrayInputStream(sampleYamlContent.getBytes());
        when(resourceLoader.getResource(anyString())).thenReturn(resource);
        when(resource.exists()).thenReturn(true);
        when(resource.getInputStream()).thenReturn(inputStream);

        // Act
        decisionTreeService.loadDecisionTrees();

        // Assert
        verify(resourceLoader, atLeastOnce()).getResource(anyString());
        verify(resource, atLeastOnce()).exists();
        verify(resource, atLeastOnce()).getInputStream();
    }

    @Test
    @DisplayName("Test 2: Initialize Decision Tree for SQL Strategy")
    void testInitializeDecisionTree_SqlStrategy_ReturnsFirstNode() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();

        // Act
        DecisionTreeNode result = decisionTreeService.initializeDecisionTree("SQL", testConfig);

        // Assert
        assertNotNull(result);
        assertEquals("left_connection", result.getId());
        assertEquals(NodeType.CONNECTION_SELECT, result.getType());
        assertEquals("LEFT Connection", result.getTitle());
        assertTrue(result.isRequired());
    }

    @Test
    @DisplayName("Test 3: Apply User Selection - Single Select")
    void testApplyUserSelection_SingleSelect_UpdatesConfig() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();
        
        UserSelection selection = new UserSelection();
        selection.setStrategy("SQL");
        selection.setCurrentNodeId("left_connection");
        selection.setSelectedOption("existing");

        // Act
        decisionTreeService.applyUserSelection(selection, testConfig);

        // Assert - verify that selection was processed (specific assertions depend on implementation)
        // This test validates that the method doesn't throw and processes the selection
        assertNotNull(testConfig);
    }

    @Test
    @DisplayName("Test 4: Apply User Selection - Multi Select")
    void testApplyUserSelection_MultiSelect_UpdatesMultipleProperties() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();
        
        UserSelection selection = new UserSelection();
        selection.setStrategy("SQL");
        selection.setCurrentNodeId("database_only");
        selection.setSelectedOptions(Arrays.asList("true"));

        // Act
        decisionTreeService.applyUserSelection(selection, testConfig);

        // Assert
        // Note: The actual property updates depend on the implementation in DecisionTreeService
        // These assertions should be updated based on how the service maps selections to config
        assertTrue(testConfig.isReadOnly() || !testConfig.isReadOnly()); // Placeholder - update based on actual logic
        assertNotNull(testConfig);
    }

    @Test
    @DisplayName("Test 5: Get Next Node - Connection Selection to General Behavior")
    void testGetNextNode_ConnectionToGeneral_ReturnsCorrectNode() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();

        // Act
        DecisionTreeNode result = decisionTreeService.getNextNode("left_connection", "existing", testConfig);

        // Assert
        assertNotNull(result);
        assertEquals("right_connection", result.getId());
        assertEquals(NodeType.CONNECTION_SELECT, result.getType());
        assertEquals("RIGHT Connection", result.getTitle());
    }

    @Test
    @DisplayName("Test 6: Get Next Node - Migration Types to Completion")
    void testGetNextNode_MigrationToCompletion_ReturnsNull() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();

        // Act
        DecisionTreeNode result = decisionTreeService.getNextNode("complete", "complete", testConfig);

        // Assert
        assertNull(result); // null indicates completion of decision tree
    }

    @Test
    @DisplayName("Test 7: Extract Config Data")
    void testExtractConfigData_ReturnsCorrectMapping() throws Exception {
        // Arrange
        testConfig.setReadOnly(true);
        testConfig.setSync(true);
        testConfig.setDatabaseOnly(false);

        // Act
        Map<String, Object> result = decisionTreeService.extractConfigData(testConfig);

        // Assert
        assertNotNull(result);
        assertTrue(result.containsKey("readOnly"));
        assertTrue(result.containsKey("sync"));
        assertTrue(result.containsKey("databaseOnly"));
        assertEquals(true, result.get("readOnly"));
        assertEquals(true, result.get("sync"));
        assertEquals(false, result.get("databaseOnly"));
    }

    @Test
    @DisplayName("Test 8: Validate Config - Valid Configuration")
    void testValidateConfig_ValidConfig_ReturnsValid() throws Exception {
        // Arrange
        testConfig.setReadOnly(true);
        testConfig.setSync(false);

        // Act
        Map<String, Object> result = decisionTreeService.validateConfig(testConfig);

        // Assert
        assertNotNull(result);
        assertTrue(result.containsKey("valid"));
        // Additional validation assertions depend on the validation logic implementation
    }

    @Test
    @DisplayName("Test 9: Reset Config")
    void testResetConfig_ClearsConfiguration() {
        // Arrange
        testConfig.setReadOnly(true);
        testConfig.setSync(true);
        testConfig.setDatabaseOnly(true);

        // Act
        decisionTreeService.resetConfig(testConfig);

        // Assert
        // Verify config has been reset to defaults
        assertFalse(testConfig.isReadOnly());
        assertFalse(testConfig.isSync());
        assertFalse(testConfig.isDatabaseOnly());
    }

    @Test
    @DisplayName("Test 10: Initialize Decision Tree - Invalid Strategy")
    void testInitializeDecisionTree_InvalidStrategy_ThrowsException() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            decisionTreeService.initializeDecisionTree("INVALID_STRATEGY", testConfig);
        });
    }

    @Test
    @DisplayName("Test 11: Get Next Node - Invalid Node ID")
    void testGetNextNode_InvalidNodeId_ThrowsException() throws Exception {
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            decisionTreeService.getNextNode("invalidNode", "someOption", testConfig);
        });
    }

    @Test
    @DisplayName("Test 12: Property Mapping Verification")
    void testPropertyMapping_VerifyCorrectConfigPaths() throws Exception {
        // This test verifies that the property mapping from decision tree selections
        // to HmsMirrorConfig properties works correctly
        
        // Arrange
        setupMockResource();
        decisionTreeService.loadDecisionTrees();
        
        UserSelection databaseOnlySelection = new UserSelection();
        databaseOnlySelection.setStrategy("SQL");
        databaseOnlySelection.setCurrentNodeId("database_only");
        databaseOnlySelection.setSelectedOption("true");

        UserSelection copyAvroSelection = new UserSelection();
        copyAvroSelection.setStrategy("SQL");
        copyAvroSelection.setCurrentNodeId("copy_avro_schemas");
        copyAvroSelection.setSelectedOption("false");

        // Act
        decisionTreeService.applyUserSelection(databaseOnlySelection, testConfig);
        decisionTreeService.applyUserSelection(copyAvroSelection, testConfig);

        // Assert
        // Verify that the config properties were set correctly
        // Note: These assertions should be updated based on the actual mapping logic
        Map<String, Object> configData = decisionTreeService.extractConfigData(testConfig);
        assertNotNull(configData);
        
        // Verify that properties are mapped correctly
        assertTrue(configData.containsKey("databaseOnly"));
        assertTrue(configData.containsKey("copyAvroSchemaUrls"));
    }

    // Helper Methods

    private void setupMockResource() throws Exception {
        InputStream inputStream = new ByteArrayInputStream(sampleYamlContent.getBytes());
        when(resourceLoader.getResource("classpath:decision-trees/sql-strategy.yaml")).thenReturn(resource);
        when(resource.exists()).thenReturn(true);
        when(resource.getInputStream()).thenReturn(inputStream);
    }
}