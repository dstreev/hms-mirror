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

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ConversionRequestDeepCloneTest {

    private ConversionRequest conversionRequest;

    @BeforeEach
    public void setUp() {
        log.info("Setting up ConversionRequestDeepCloneTest");
        conversionRequest = new ConversionRequest();
    }

    @Test
    public void testDeepCloneNoSharedReferences() {
        log.info("Testing deep clone for no shared references");
        
        // Setup original with complex data
        setupComplexConversionRequest();
        
        // Clone the conversion request
        ConversionRequest clone = conversionRequest.clone();
        
        // Verify basic cloning worked
        assertNotNull(clone);
        assertNotSame(conversionRequest, clone);
        
        // Test databases map is deeply cloned
        testDatabasesMapIndependence(conversionRequest, clone);
        
        // Verify modifications to clone don't affect original
        testModificationIndependence(conversionRequest, clone);
    }
    
    private void setupComplexConversionRequest() {
        // Set up a complex configuration with nested collections
        if (conversionRequest.getDatabases() == null) {
            conversionRequest.setDatabases(new TreeMap<>());
        }
        
        // Add databases with tables
        List<String> db1Tables = new ArrayList<>(Arrays.asList("table1", "table2", "table3"));
        conversionRequest.getDatabases().put("database1", db1Tables);
        
        List<String> db2Tables = new ArrayList<>(Arrays.asList("users", "orders", "products"));
        conversionRequest.getDatabases().put("database2", db2Tables);
        
        List<String> db3Tables = new ArrayList<>(Arrays.asList("log_data"));
        conversionRequest.getDatabases().put("database3", db3Tables);
    }
    
    private void testDatabasesMapIndependence(ConversionRequest original, ConversionRequest clone) {
        if (original.getDatabases() != null) {
            assertNotNull(clone.getDatabases());
            assertNotSame(original.getDatabases(), clone.getDatabases());
            assertEquals(original.getDatabases().size(), clone.getDatabases().size());
            
            // Test that both maps contain the same keys
            assertTrue(clone.getDatabases().keySet().containsAll(original.getDatabases().keySet()));
            
            // Test that each list is deeply cloned
            for (String dbName : original.getDatabases().keySet()) {
                List<String> originalTables = original.getDatabases().get(dbName);
                List<String> clonedTables = clone.getDatabases().get(dbName);
                
                if (originalTables != null) {
                    assertNotNull(clonedTables);
                    assertNotSame(originalTables, clonedTables);
                    assertEquals(originalTables.size(), clonedTables.size());
                    assertTrue(clonedTables.containsAll(originalTables));
                }
            }
        }
    }
    
    private void testModificationIndependence(ConversionRequest original, ConversionRequest clone) {
        // Test modifying clone doesn't affect original
        
        // Modify databases collection - add new database
        if (clone.getDatabases() != null) {
            int originalSize = original.getDatabases().size();
            List<String> newTables = new ArrayList<>(Arrays.asList("new_table1", "new_table2"));
            clone.getDatabases().put("new_database", newTables);
            assertEquals(originalSize, original.getDatabases().size());
            assertFalse(original.getDatabases().containsKey("new_database"));
            assertTrue(clone.getDatabases().containsKey("new_database"));
        }
        
        // Modify table lists within existing database
        if (clone.getDatabases() != null && !clone.getDatabases().isEmpty()) {
            String firstDb = clone.getDatabases().keySet().iterator().next();
            List<String> cloneTables = clone.getDatabases().get(firstDb);
            List<String> originalTables = original.getDatabases().get(firstDb);
            
            if (cloneTables != null && originalTables != null) {
                int originalTableCount = originalTables.size();
                cloneTables.add("clone_only_table");
                
                // Verify original list is unchanged
                assertEquals(originalTableCount, originalTables.size());
                assertFalse(originalTables.contains("clone_only_table"));
                assertTrue(cloneTables.contains("clone_only_table"));
            }
        }
    }
    
    @Test
    public void testCloneNullSafety() {
        log.info("Testing clone null safety");
        
        // Test with minimal conversion request (many nulls)
        ConversionRequest minimalRequest = new ConversionRequest();
        ConversionRequest clone = minimalRequest.clone();
        
        assertNotNull(clone);
        assertNotSame(minimalRequest, clone);
        
        // The databases field should be initialized to new TreeMap()
        assertNotNull(clone.getDatabases());
        
        // Test that the objects are different instances
        if (minimalRequest.getDatabases() != null) {
            assertNotSame(minimalRequest.getDatabases(), clone.getDatabases());
        }
    }
    
    @Test
    public void testCloneWithEmptyDatabases() {
        log.info("Testing clone with empty databases");
        
        // Setup with empty databases map
        conversionRequest.setDatabases(new TreeMap<>());
        
        ConversionRequest clone = conversionRequest.clone();
        
        assertNotNull(clone);
        assertNotSame(conversionRequest, clone);
        assertNotNull(clone.getDatabases());
        assertNotSame(conversionRequest.getDatabases(), clone.getDatabases());
        assertTrue(clone.getDatabases().isEmpty());
    }
    
    @Test
    public void testCloneWithEmptyTableLists() {
        log.info("Testing clone with empty table lists");
        
        // Setup with empty table lists
        conversionRequest.setDatabases(new TreeMap<>());
        conversionRequest.getDatabases().put("empty_db", new ArrayList<>());
        
        ConversionRequest clone = conversionRequest.clone();
        
        assertNotNull(clone);
        assertNotSame(conversionRequest, clone);
        assertNotNull(clone.getDatabases());
        assertNotSame(conversionRequest.getDatabases(), clone.getDatabases());
        
        assertTrue(clone.getDatabases().containsKey("empty_db"));
        List<String> cloneTables = clone.getDatabases().get("empty_db");
        List<String> originalTables = conversionRequest.getDatabases().get("empty_db");
        
        assertNotNull(cloneTables);
        assertNotNull(originalTables);
        assertNotSame(originalTables, cloneTables);
        assertTrue(cloneTables.isEmpty());
        assertTrue(originalTables.isEmpty());
    }
    
    @Test
    public void testCloneDeepListModification() {
        log.info("Testing clone deep list modification");
        
        setupComplexConversionRequest();
        ConversionRequest clone = conversionRequest.clone();
        
        // Get the first database and its tables
        String firstDb = conversionRequest.getDatabases().keySet().iterator().next();
        List<String> originalTables = conversionRequest.getDatabases().get(firstDb);
        List<String> cloneTables = clone.getDatabases().get(firstDb);
        
        int originalSize = originalTables.size();
        int cloneSize = cloneTables.size();
        assertEquals(originalSize, cloneSize);
        
        // Modify clone table list
        cloneTables.add("new_clone_table");
        cloneTables.remove(0); // Remove first element
        
        // Verify original is unchanged
        assertEquals(originalSize, originalTables.size());
        assertEquals(originalSize + 1 - 1, cloneTables.size()); // Added one, removed one
        assertTrue(cloneTables.contains("new_clone_table"));
        assertFalse(originalTables.contains("new_clone_table"));
    }
    
    @Test
    public void testCloneStringContentEquality() {
        log.info("Testing clone string content equality");
        
        setupComplexConversionRequest();
        ConversionRequest clone = conversionRequest.clone();
        
        // Verify all string content is equal but not same references
        for (String dbName : conversionRequest.getDatabases().keySet()) {
            List<String> originalTables = conversionRequest.getDatabases().get(dbName);
            List<String> cloneTables = clone.getDatabases().get(dbName);
            
            assertEquals(originalTables.size(), cloneTables.size());
            
            for (int i = 0; i < originalTables.size(); i++) {
                // String content should be equal
                assertEquals(originalTables.get(i), cloneTables.get(i));
                // But for interned strings, they might be the same reference
                // This is expected behavior for String literals
            }
        }
    }
}