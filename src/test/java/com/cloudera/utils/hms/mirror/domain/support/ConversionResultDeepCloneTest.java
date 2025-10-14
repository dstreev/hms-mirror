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

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class ConversionResultDeepCloneTest {

    private ConversionResult conversionResult;

    @BeforeEach
    public void setUp() {
        log.info("Setting up ConversionResultDeepCloneTest");
        conversionResult = new ConversionResult();
    }

    @Test
    public void testDeepCloneNoSharedReferences() {
        log.info("Testing deep clone for no shared references");
        
        // Setup original with databases
        setupComplexConversionResult();
        
        // Clone the conversion result
        ConversionResult clone = conversionResult.clone();
        
        // Verify basic cloning worked
        assertNotNull(clone);
        assertNotSame(conversionResult, clone);
        
        // Test databases map is deeply cloned
        testDatabasesMapIndependence(conversionResult, clone);
        
        // Verify modifications to clone don't affect original
        testModificationIndependence(conversionResult, clone);
    }
    
    private void setupComplexConversionResult() {
        // Set up a complex configuration with nested objects and collections
        if (conversionResult.getDatabases() == null) {
            conversionResult.setDatabases(new TreeMap<>());
        }
        
        // Add some test databases
        DBMirror db1 = new DBMirror();
        db1.setName("test_db1");
        conversionResult.getDatabases().put("test_db1", db1);
        
        DBMirror db2 = new DBMirror();
        db2.setName("test_db2");
        conversionResult.getDatabases().put("test_db2", db2);
    }
    
    private void testDatabasesMapIndependence(ConversionResult original, ConversionResult clone) {
        if (original.getDatabases() != null) {
            assertNotNull(clone.getDatabases());
            assertNotSame(original.getDatabases(), clone.getDatabases());
            assertEquals(original.getDatabases().size(), clone.getDatabases().size());
            
            // Test that both maps contain the same keys
            assertTrue(clone.getDatabases().keySet().containsAll(original.getDatabases().keySet()));
            
            // Since DBMirror now has clone method, we expect different object references
            for (String key : original.getDatabases().keySet()) {
                DBMirror originalDB = original.getDatabases().get(key);
                DBMirror clonedDB = clone.getDatabases().get(key);
                
                if (originalDB != null) {
                    assertNotNull(clonedDB);
                    // DBMirror clone is now implemented, so objects should be different instances
                    assertNotSame(originalDB, clonedDB);
                    assertEquals(originalDB.getName(), clonedDB.getName());
                }
            }
        }
    }
    
    private void testModificationIndependence(ConversionResult original, ConversionResult clone) {
        // Test modifying clone doesn't affect original
        
        // Modify databases collection
        if (clone.getDatabases() != null) {
            int originalSize = original.getDatabases().size();
            DBMirror newDB = new DBMirror();
            newDB.setName("new_clone_db");
            clone.getDatabases().put("new_clone_db", newDB);
            assertEquals(originalSize, original.getDatabases().size());
            assertFalse(original.getDatabases().containsKey("new_clone_db"));
            assertTrue(clone.getDatabases().containsKey("new_clone_db"));
        }
    }
    
    @Test
    public void testCloneNullSafety() {
        log.info("Testing clone null safety");
        
        // Test with minimal conversion result (many nulls)
        ConversionResult minimalResult = new ConversionResult();
        ConversionResult clone = minimalResult.clone();
        
        assertNotNull(clone);
        assertNotSame(minimalResult, clone);
        
        // The databases field should be initialized to new TreeMap()
        assertNotNull(clone.getDatabases());
        
        // Test that the objects are different instances
        if (minimalResult.getDatabases() != null) {
            assertNotSame(minimalResult.getDatabases(), clone.getDatabases());
        }
    }
    
    @Test
    public void testCloneWithEmptyDatabases() {
        log.info("Testing clone with empty databases");
        
        // Setup with empty databases map
        conversionResult.setDatabases(new TreeMap<>());
        
        ConversionResult clone = conversionResult.clone();
        
        assertNotNull(clone);
        assertNotSame(conversionResult, clone);
        assertNotNull(clone.getDatabases());
        assertNotSame(conversionResult.getDatabases(), clone.getDatabases());
        assertTrue(clone.getDatabases().isEmpty());
    }
    
    @Test
    public void testCloneIndependentSizeChanges() {
        log.info("Testing clone independent size changes");
        
        setupComplexConversionResult();
        ConversionResult clone = conversionResult.clone();
        
        int originalSize = conversionResult.getDatabases().size();
        int cloneSize = clone.getDatabases().size();
        assertEquals(originalSize, cloneSize);
        
        // Add to clone
        DBMirror newDB = new DBMirror();
        newDB.setName("clone_only_db");
        clone.getDatabases().put("clone_only_db", newDB);
        
        // Verify sizes are different
        assertEquals(originalSize, conversionResult.getDatabases().size());
        assertEquals(originalSize + 1, clone.getDatabases().size());
        
        // Remove from original
        if (!conversionResult.getDatabases().isEmpty()) {
            String firstKey = conversionResult.getDatabases().keySet().iterator().next();
            conversionResult.getDatabases().remove(firstKey);
            
            // Verify clone still has all its items
            assertEquals(originalSize + 1, clone.getDatabases().size());
            assertTrue(clone.getDatabases().containsKey("clone_only_db"));
        }
    }
}