/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import com.cloudera.utils.hms.mirror.utils.ConfigTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class HmsMirrorConfigDeepCloneTest {

    private HmsMirrorConfig config;

    @BeforeEach
    public void setUp() throws IOException {
        log.info("Setting up HmsMirrorConfigDeepCloneTest");
        config = ConfigTest.deserializeResource("/config/clone/full_test_01.yaml");
    }

    @Test
    public void testDeepCloneNoSharedReferences() {
        log.info("Testing deep clone for no shared references");
        
        // Setup original config with various data
        setupComplexConfig();
        
        // Clone the config
        HmsMirrorConfig clone = config.clone();
        
        // Verify basic cloning worked
        assertNotNull(clone);
        assertNotSame(config, clone);
        
        // Test Date objects are new instances
        testDateObjectsIndependence(config, clone);
        
        // Test complex objects are new instances
        testComplexObjectsIndependence(config, clone);
        
        // Test collections are new instances
        testCollectionsIndependence(config, clone);
        
        // Test clusters map is deeply cloned
        testClustersMapIndependence(config, clone);
        
        // Verify modifications to clone don't affect original
        testModificationIndependence(config, clone);
    }
    
    private void setupComplexConfig() {
        // Set up a complex configuration with nested objects and collections
        config.setInitDate(new Date());
        
        // Add databases
        if (config.getDatabases() == null) {
            config.setDatabases(new TreeSet<>());
        }
        config.getDatabases().add("test_db1");
        config.getDatabases().add("test_db2");
        
        // Setup acceptance
        if (config.getAcceptance() == null) {
            config.setAcceptance(new Acceptance());
        }
        config.getAcceptance().setSilentOverride(true);
        
        // Setup filter
        if (config.getFilter() == null) {
            config.setFilter(new Filter());
        }
        config.getFilter().setDbRegEx(".*_test");
        
        // Setup clusters with complex nested objects
        if (config.getClusters() == null) {
            config.setClusters(new HashMap<>());
        }
        
        Cluster leftCluster = new Cluster();
        leftCluster.setPlatformType(PlatformType.CDP7_1);
        leftCluster.setHcfsNamespace("hdfs://left-cluster");
        
        HiveServer2Config hs2Config = new HiveServer2Config();
        hs2Config.setUri("jdbc:hive2://left-cluster:10000");
        leftCluster.setHiveServer2(hs2Config);
        
        config.getClusters().put(Environment.LEFT, leftCluster);
        
        // Setup transfer ownership
        if (config.getOwnershipTransfer() == null) {
            config.setOwnershipTransfer(new TransferOwnership());
        }
        config.getOwnershipTransfer().setDatabase(true);
        config.getOwnershipTransfer().setTable(true);
        
        // Setup iceberg conversion with properties
        if (config.getIcebergConversion() == null) {
            config.setIcebergConversion(new IcebergConversion());
        }
        config.getIcebergConversion().setEnable(true);
        config.getIcebergConversion().getTableProperties().put("test.property", "test.value");
        
        // Setup legacy translations
        if (config.getLegacyTranslations() == null) {
            config.setLegacyTranslations(new com.cloudera.utils.hms.mirror.feature.LegacyTranslations());
        }
        config.getLegacyTranslations().getRowSerde().put("test.serde", "new.serde");
    }
    
    private void testDateObjectsIndependence(HmsMirrorConfig original, HmsMirrorConfig clone) {
        if (original.getInitDate() != null) {
            assertNotNull(clone.getInitDate());
            assertNotSame(original.getInitDate(), clone.getInitDate());
            assertEquals(original.getInitDate(), clone.getInitDate());
            
            // Modify clone date and verify original is unaffected
            Date originalDate = new Date(original.getInitDate().getTime());
            clone.getInitDate().setTime(clone.getInitDate().getTime() + 1000);
            assertEquals(originalDate, original.getInitDate());
        }
    }
    
    private void testComplexObjectsIndependence(HmsMirrorConfig original, HmsMirrorConfig clone) {
        // Test acceptance object
        if (original.getAcceptance() != null) {
            assertNotNull(clone.getAcceptance());
            assertNotSame(original.getAcceptance(), clone.getAcceptance());
            assertEquals(original.getAcceptance().isSilentOverride(), clone.getAcceptance().isSilentOverride());
        }
        
        // Test filter object
        if (original.getFilter() != null) {
            assertNotNull(clone.getFilter());
            assertNotSame(original.getFilter(), clone.getFilter());
        }
        
        // Test ownership transfer
        if (original.getOwnershipTransfer() != null) {
            assertNotNull(clone.getOwnershipTransfer());
            assertNotSame(original.getOwnershipTransfer(), clone.getOwnershipTransfer());
            assertEquals(original.getOwnershipTransfer().isDatabase(), clone.getOwnershipTransfer().isDatabase());
        }
        
        // Test iceberg conversion
        if (original.getIcebergConversion() != null) {
            assertNotNull(clone.getIcebergConversion());
            assertNotSame(original.getIcebergConversion(), clone.getIcebergConversion());
            
            // Test table properties map is cloned
            if (original.getIcebergConversion().getTableProperties() != null) {
                assertNotNull(clone.getIcebergConversion().getTableProperties());
                assertNotSame(original.getIcebergConversion().getTableProperties(), 
                             clone.getIcebergConversion().getTableProperties());
            }
        }
        
        // Test legacy translations
        if (original.getLegacyTranslations() != null) {
            assertNotNull(clone.getLegacyTranslations());
            assertNotSame(original.getLegacyTranslations(), clone.getLegacyTranslations());
            
            // Test row serde map is cloned
            assertNotSame(original.getLegacyTranslations().getRowSerde(), 
                         clone.getLegacyTranslations().getRowSerde());
        }
    }
    
    private void testCollectionsIndependence(HmsMirrorConfig original, HmsMirrorConfig clone) {
        // Test databases set
        if (original.getDatabases() != null) {
            assertNotNull(clone.getDatabases());
            assertNotSame(original.getDatabases(), clone.getDatabases());
            assertEquals(original.getDatabases().size(), clone.getDatabases().size());
            assertTrue(clone.getDatabases().containsAll(original.getDatabases()));
        }
    }
    
    private void testClustersMapIndependence(HmsMirrorConfig original, HmsMirrorConfig clone) {
        if (original.getClusters() != null) {
            assertNotNull(clone.getClusters());
            assertNotSame(original.getClusters(), clone.getClusters());
            
            // Test each cluster is deeply cloned
            for (Environment env : original.getClusters().keySet()) {
                Cluster originalCluster = original.getClusters().get(env);
                Cluster clonedCluster = clone.getClusters().get(env);
                
                if (originalCluster != null) {
                    assertNotNull(clonedCluster);
                    assertNotSame(originalCluster, clonedCluster);
                    
                    // Test HiveServer2Config is cloned
                    if (originalCluster.getHiveServer2() != null) {
                        assertNotNull(clonedCluster.getHiveServer2());
                        assertNotSame(originalCluster.getHiveServer2(), clonedCluster.getHiveServer2());
                    }
                }
            }
        }
    }
    
    private void testModificationIndependence(HmsMirrorConfig original, HmsMirrorConfig clone) {
        // Test modifying clone doesn't affect original
        
        // Modify databases collection
        if (clone.getDatabases() != null) {
            int originalSize = original.getDatabases().size();
            clone.getDatabases().add("new_clone_db");
            assertEquals(originalSize, original.getDatabases().size());
            assertFalse(original.getDatabases().contains("new_clone_db"));
        }
        
        // Modify iceberg conversion properties
        if (clone.getIcebergConversion() != null && clone.getIcebergConversion().getTableProperties() != null) {
            clone.getIcebergConversion().getTableProperties().put("new.property", "new.value");
            if (original.getIcebergConversion() != null && original.getIcebergConversion().getTableProperties() != null) {
                assertFalse(original.getIcebergConversion().getTableProperties().containsKey("new.property"));
            }
        }
        
        // Modify legacy translations
        if (clone.getLegacyTranslations() != null) {
            clone.getLegacyTranslations().getRowSerde().put("new.serde", "modified.serde");
            assertFalse(original.getLegacyTranslations().getRowSerde().containsKey("new.serde"));
        }
        
        // Modify cluster properties
        if (clone.getClusters() != null && !clone.getClusters().isEmpty()) {
            Cluster clonedCluster = clone.getClusters().values().iterator().next();
            if (clonedCluster.getHiveServer2() != null) {
                clonedCluster.getHiveServer2().setUri("jdbc:hive2://modified-cluster:10000");
                
                // Find corresponding original cluster and verify it's unchanged
                Cluster originalCluster = original.getClusters().values().iterator().next();
                if (originalCluster.getHiveServer2() != null) {
                    assertNotEquals("jdbc:hive2://modified-cluster:10000", originalCluster.getHiveServer2().getUri());
                }
            }
        }
    }
    
    @Test
    public void testCloneNullSafety() {
        log.info("Testing clone null safety");
        
        // Test with minimal config (many nulls)
        HmsMirrorConfig minimalConfig = new HmsMirrorConfig();
        HmsMirrorConfig clone = minimalConfig.clone();
        
        assertNotNull(clone);
        assertNotSame(minimalConfig, clone);
        
        // initDate is initialized by default, so it should not be null
        assertNotNull(clone.getInitDate());
        
        // Some fields may have default values based on annotations or initialization 
        // The important thing is that the clone is independent of the original
        // We'll test that the objects are different instances
        if (minimalConfig.getDatabases() != null) {
            assertNotSame(minimalConfig.getDatabases(), clone.getDatabases());
        }
        
        if (minimalConfig.getAcceptance() != null) {
            assertNotSame(minimalConfig.getAcceptance(), clone.getAcceptance());
        }
        
        if (minimalConfig.getFilter() != null) {
            assertNotSame(minimalConfig.getFilter(), clone.getFilter());
        }
        
        if (minimalConfig.getClusters() != null) {
            assertNotSame(minimalConfig.getClusters(), clone.getClusters());
        }
        
        if (minimalConfig.getOwnershipTransfer() != null) {
            assertNotSame(minimalConfig.getOwnershipTransfer(), clone.getOwnershipTransfer());
        }
        
        if (minimalConfig.getIcebergConversion() != null) {
            assertNotSame(minimalConfig.getIcebergConversion(), clone.getIcebergConversion());
        }
        
        if (minimalConfig.getLegacyTranslations() != null) {
            assertNotSame(minimalConfig.getLegacyTranslations(), clone.getLegacyTranslations());
        }
    }
}