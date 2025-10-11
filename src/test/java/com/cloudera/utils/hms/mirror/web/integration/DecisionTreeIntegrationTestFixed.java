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

import com.cloudera.utils.hms.mirror.web.Mirror;
import com.cloudera.utils.hms.mirror.web.model.DecisionTreeResponse;
import com.cloudera.utils.hms.mirror.web.model.UserSelection;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.context.TestPropertySource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Arrays;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
    "hms-mirror.ui.version=react",
    "spring.main.allow-bean-definition-overriding=true"
})
public class DecisionTreeIntegrationTestFixed {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Integration Test: Comprehensive HmsMirrorConfig Building Through Decision Tree Navigation with Detailed Sub-Object Configuration")
    void testComprehensiveConfigBuildingThroughDecisionTreeWithDetailedSubObjects() throws Exception {
        // Step 1: Initialize SQL strategy decision tree
        MvcResult startResult = mockMvc.perform(get("/api/v1/decision-tree/start/SQL"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.strategy").value("SQL"))
                .andExpect(jsonPath("$.complete").value(false))
                .andExpect(jsonPath("$.sessionId").exists())
                .andReturn();

        DecisionTreeResponse startResponse = objectMapper.readValue(
            startResult.getResponse().getContentAsString(), 
            DecisionTreeResponse.class
        );
        
        String sessionId = startResponse.getSessionId();
        assertNotNull(sessionId);

        // Get initial configuration - should have default values
        MvcResult initialConfigResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> initialConfig = objectMapper.readValue(
            initialConfigResult.getResponse().getContentAsString(), 
            Map.class
        );

        // Verify initial default values
        assertNotNull(initialConfig);
        assertEquals(false, initialConfig.get("databaseOnly"));
        assertEquals(false, initialConfig.get("copyAvroSchemaUrls"));
        assertEquals(false, initialConfig.get("skipLinkCheck"));
        assertEquals(false, initialConfig.get("execute"));
        assertEquals(false, initialConfig.get("readOnly"));
        assertEquals(false, initialConfig.get("sync"));

        // Step 2: Navigate through decision tree making specific selections
        
        // Set databaseOnly = true (database_only node)
        UserSelection databaseOnlySelection = new UserSelection();
        databaseOnlySelection.setSessionId(sessionId);
        databaseOnlySelection.setStrategy("SQL");
        databaseOnlySelection.setCurrentNodeId("database_only");
        databaseOnlySelection.setSelectedOption("true");
        
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(databaseOnlySelection)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sessionId").value(sessionId));

        // Verify databaseOnly was set
        MvcResult afterDatabaseOnlyResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> afterDatabaseOnlyConfig = objectMapper.readValue(
            afterDatabaseOnlyResult.getResponse().getContentAsString(), 
            Map.class
        );
        
        assertEquals(true, afterDatabaseOnlyConfig.get("databaseOnly"));

        // Set copyAvroSchemaUrls = true (copy_avro_schemas node)
        UserSelection copyAvroSelection = new UserSelection();
        copyAvroSelection.setSessionId(sessionId);
        copyAvroSelection.setStrategy("SQL");
        copyAvroSelection.setCurrentNodeId("copy_avro_schemas");
        copyAvroSelection.setSelectedOption("true");
        
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(copyAvroSelection)))
                .andExpect(status().isOk());

        // Verify copyAvroSchemaUrls was set
        MvcResult afterCopyAvroResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> afterCopyAvroConfig = objectMapper.readValue(
            afterCopyAvroResult.getResponse().getContentAsString(), 
            Map.class
        );
        
        assertEquals(true, afterCopyAvroConfig.get("copyAvroSchemaUrls"));
        assertEquals(true, afterCopyAvroConfig.get("databaseOnly")); // Should still be true

        // Set skipLinkCheck = true (skip_link_check node)
        UserSelection skipLinkCheckSelection = new UserSelection();
        skipLinkCheckSelection.setSessionId(sessionId);
        skipLinkCheckSelection.setStrategy("SQL");
        skipLinkCheckSelection.setCurrentNodeId("skip_link_check");
        skipLinkCheckSelection.setSelectedOption("true");
        
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(skipLinkCheckSelection)))
                .andExpect(status().isOk());

        // Set readOnly = true (execution_options node)
        UserSelection readOnlySelection = new UserSelection();
        readOnlySelection.setSessionId(sessionId);
        readOnlySelection.setStrategy("SQL");
        readOnlySelection.setCurrentNodeId("execution_options");
        readOnlySelection.setSelectedOptions(Arrays.asList("readOnly"));
        
        mockMvc.perform(post("/api/v1/decision-tree/navigate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(readOnlySelection)))
                .andExpect(status().isOk());

        // Step 3: Get final configuration and validate all settings
        MvcResult finalConfigResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> finalConfig = objectMapper.readValue(
            finalConfigResult.getResponse().getContentAsString(), 
            Map.class
        );

        // Verify all our selections were applied correctly
        assertNotNull(finalConfig);
        assertEquals(true, finalConfig.get("databaseOnly"));
        assertEquals(true, finalConfig.get("copyAvroSchemaUrls"));
        assertEquals(true, finalConfig.get("skipLinkCheck"));
        assertEquals(true, finalConfig.get("readOnly"));
        
        // Properties we didn't set should remain false
        assertEquals(false, finalConfig.get("execute"));
        assertEquals(false, finalConfig.get("sync"));

        // Step 4: Validate configuration
        MvcResult validationResult = mockMvc.perform(get("/api/v1/decision-tree/validate")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> validation = objectMapper.readValue(
            validationResult.getResponse().getContentAsString(), 
            Map.class
        );

        assertNotNull(validation);
        assertTrue(validation.containsKey("isValid"));
        
        // Step 5: Test reset functionality preserves our changes until reset
        MvcResult beforeResetResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> beforeResetConfig = objectMapper.readValue(
            beforeResetResult.getResponse().getContentAsString(), 
            Map.class
        );
        
        // Should still have our changes
        assertEquals(true, beforeResetConfig.get("databaseOnly"));
        assertEquals(true, beforeResetConfig.get("copyAvroSchemaUrls"));
        assertEquals(true, beforeResetConfig.get("skipLinkCheck"));
        assertEquals(true, beforeResetConfig.get("readOnly"));

        // Reset and verify everything is back to defaults
        mockMvc.perform(post("/api/v1/decision-tree/reset")
                .param("sessionId", sessionId))
                .andExpect(status().isOk());

        MvcResult afterResetResult = mockMvc.perform(get("/api/v1/decision-tree/current-config")
                .param("sessionId", sessionId))
                .andExpect(status().isOk())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> afterResetConfig = objectMapper.readValue(
            afterResetResult.getResponse().getContentAsString(), 
            Map.class
        );

        // All should be back to false after reset
        assertEquals(false, afterResetConfig.get("databaseOnly"));
        assertEquals(false, afterResetConfig.get("copyAvroSchemaUrls"));
        assertEquals(false, afterResetConfig.get("skipLinkCheck"));
        assertEquals(false, afterResetConfig.get("execute"));
        assertEquals(false, afterResetConfig.get("readOnly"));
        assertEquals(false, afterResetConfig.get("sync"));
    }
}