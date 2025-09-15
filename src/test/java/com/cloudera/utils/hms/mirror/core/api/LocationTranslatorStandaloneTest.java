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

package com.cloudera.utils.hms.mirror.core.api;

import com.cloudera.utils.hms.mirror.core.impl.LocationTranslatorImpl;
import com.cloudera.utils.hms.mirror.core.model.GlobalLocationMapResult;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.cloudera.utils.hms.mirror.infrastructure.configuration.ConfigurationProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test demonstrating how HMS Mirror core business logic can be used 
 * independently of Spring framework and application context.
 * 
 * This shows the key benefit: pure business logic that is testable
 * and reusable across different architectural contexts.
 */
public class LocationTranslatorStandaloneTest {

    @Test
    @DisplayName("Core business logic works without Spring framework")
    public void testCoreBusinessLogicIndependence() {
        
        // Step 1: Create a mock configuration provider (no Spring needed)
        ConfigurationProvider mockConfigProvider = createMockConfigProvider();
        
        // Step 2: Create core business logic instance directly (no Spring needed)  
        LocationTranslator locationTranslator = new LocationTranslatorImpl(mockConfigProvider);
        
        // Step 3: Use the business logic
        GlobalLocationMapResult result = locationTranslator.processGlobalLocationMap(
            "hdfs://old-cluster/external/data", 
            true  // isExternalTable
        );
        
        // Step 4: Verify business logic worked correctly
        assertTrue(result.isMapped(), "Location should be mapped by GLM");
        assertEquals("hdfs://old-cluster/external/data", result.getOriginalDir());
        assertEquals("hdfs://new-cluster/external/data", result.getMappedDir());
        
        // This proves the business logic:
        // ✓ Works without Spring context
        // ✓ Can be easily unit tested
        // ✓ Has clean dependencies (only needs configuration)
        // ✓ Is reusable in any application architecture
    }
    
    @Test
    @DisplayName("Same business logic can be used in different contexts")
    public void testBusinessLogicReusability() {
        
        // Scenario 1: CLI Application Context
        ConfigurationProvider cliConfig = createCliConfigProvider();
        LocationTranslator cliTranslator = new LocationTranslatorImpl(cliConfig);
        
        GlobalLocationMapResult cliResult = cliTranslator.processGlobalLocationMap(
            "s3://old-bucket/data", true);
        
        // Scenario 2: Web Service Context
        ConfigurationProvider webConfig = createWebServiceConfigProvider(); 
        LocationTranslator webTranslator = new LocationTranslatorImpl(webConfig);
        
        GlobalLocationMapResult webResult = webTranslator.processGlobalLocationMap(
            "gs://old-project/data", true);
        
        // Same business logic, different configurations
        assertNotNull(cliResult, "CLI context should work");
        assertNotNull(webResult, "Web service context should work");
        
        // This demonstrates how the same core business logic can be:
        // ✓ Used in CLI tools
        // ✓ Used in web services  
        // ✓ Used in batch processing jobs
        // ✓ Used with different configuration sources
        // ✓ Tested independently of application framework
    }
    
    @Test
    @DisplayName("Business logic is easily testable with mocked dependencies")
    public void testBusinessLogicTestability() {
        
        // Arrange: Set up test scenario with mocks
        ConfigurationProvider testConfig = mock(ConfigurationProvider.class);
        HmsMirrorConfig mockConfig = createTestConfiguration();
        when(testConfig.getConfig()).thenReturn(mockConfig);
        
        LocationTranslator translator = new LocationTranslatorImpl(testConfig);
        
        // Act: Test specific business scenario
        GlobalLocationMapResult result = translator.processGlobalLocationMap(
            "hdfs://test-cluster/test-data", true);
        
        // Assert: Verify business logic behavior
        assertTrue(result.isMapped(), "Test GLM should map the location");
        
        // Verify interaction with dependencies
        verify(testConfig, atLeastOnce()).getConfig();
        
        // This shows how easy it is to:
        // ✓ Mock infrastructure dependencies
        // ✓ Test pure business logic in isolation
        // ✓ Create focused, fast unit tests
        // ✓ Verify specific business scenarios
    }
    
    // Helper methods to create different configuration providers
    // (In real applications, these would read from different sources)
    
    private ConfigurationProvider createMockConfigProvider() {
        ConfigurationProvider provider = mock(ConfigurationProvider.class);
        HmsMirrorConfig config = new HmsMirrorConfig();
        
        // Set up Global Location Map for testing
        Translator translator = new Translator();
        Map<String, Map<TableType, String>> glm = new LinkedHashMap<>();
        
        Map<TableType, String> mapping = new HashMap<>();
        mapping.put(TableType.EXTERNAL_TABLE, "hdfs://new-cluster/external/");
        glm.put("hdfs://old-cluster/external/", mapping);
        
        translator.setOrderedGlobalLocationMap(glm);
        config.setTranslator(translator);
        
        when(provider.getConfig()).thenReturn(config);
        return provider;
    }
    
    private ConfigurationProvider createCliConfigProvider() {
        return new ConfigurationProvider() {
            @Override
            public HmsMirrorConfig getConfig() {
                // In real CLI app, would parse command line arguments
                HmsMirrorConfig config = new HmsMirrorConfig();
                config.setTranslator(new Translator());
                config.getTranslator().setOrderedGlobalLocationMap(createS3GLM());
                return config;
            }
            
            @Override
            public void updateConfig(HmsMirrorConfig config) {}
            @Override
            public boolean validateConfig() { return true; }
            @Override
            public void reloadConfig() {}
        };
    }
    
    private ConfigurationProvider createWebServiceConfigProvider() {
        return new ConfigurationProvider() {
            @Override  
            public HmsMirrorConfig getConfig() {
                // In real web service, would get from request parameters or database
                HmsMirrorConfig config = new HmsMirrorConfig();
                config.setTranslator(new Translator());
                config.getTranslator().setOrderedGlobalLocationMap(createGCSGLM());
                return config;
            }
            
            @Override
            public void updateConfig(HmsMirrorConfig config) {}
            @Override
            public boolean validateConfig() { return true; }
            @Override
            public void reloadConfig() {}
        };
    }
    
    private HmsMirrorConfig createTestConfiguration() {
        HmsMirrorConfig config = new HmsMirrorConfig();
        Translator translator = new Translator();
        
        Map<String, Map<TableType, String>> glm = new LinkedHashMap<>();
        Map<TableType, String> mapping = new HashMap<>();
        mapping.put(TableType.EXTERNAL_TABLE, "hdfs://new-test-cluster/mapped-data");
        glm.put("hdfs://test-cluster/", mapping);
        
        translator.setOrderedGlobalLocationMap(glm);
        config.setTranslator(translator);
        return config;
    }
    
    private Map<String, Map<TableType, String>> createS3GLM() {
        Map<String, Map<TableType, String>> glm = new LinkedHashMap<>();
        Map<TableType, String> s3Mapping = new HashMap<>();
        s3Mapping.put(TableType.EXTERNAL_TABLE, "s3://new-bucket/");
        glm.put("s3://old-bucket/", s3Mapping);
        return glm;
    }
    
    private Map<String, Map<TableType, String>> createGCSGLM() {
        Map<String, Map<TableType, String>> glm = new LinkedHashMap<>();
        Map<TableType, String> gcsMapping = new HashMap<>();
        gcsMapping.put(TableType.EXTERNAL_TABLE, "gs://new-project/");
        glm.put("gs://old-project/", gcsMapping);
        return glm;
    }
}