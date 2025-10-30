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

package com.cloudera.utils.hms.mirror.domain.testdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to verify successful deserialization of YAML files from test/resources/test_data
 * into DBMirrorTest objects.
 */
class DBMirrorTestDeserializationTest {

    private static ObjectMapper objectMapper;

    @BeforeAll
    static void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.findAndRegisterModules();
    }

    /**
     * Provides a stream of YAML test data files for parameterized testing.
     */
    static Stream<Path> yamlTestDataFiles() throws IOException {
        URL resource = DBMirrorTestDeserializationTest.class.getClassLoader().getResource("test_data");
        assertNotNull(resource, "test_data directory not found in test resources");

        Path testDataPath = Paths.get(resource.getPath());
        return Files.walk(testDataPath)
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".yaml") || path.toString().endsWith(".yml"));
    }

    /**
     * Parameterized test that attempts to deserialize each YAML file in test_data.
     * This test validates that the YAML structure can be successfully deserialized into DBMirrorTest objects.
     */
    @ParameterizedTest(name = "Deserialize {0}")
    @MethodSource("yamlTestDataFiles")
    void testDeserializeYamlFiles(Path yamlFile) throws IOException {
        // Act
        // The YAML files contain a wrapper object with "databases" key
        Map<String, Object> root = objectMapper.readValue(yamlFile.toFile(), Map.class);

        assertNotNull(root, "Root object should not be null for file: " + yamlFile.getFileName());
        assertTrue(root.containsKey("databases"), "YAML should contain 'databases' key for file: " + yamlFile.getFileName());

        Map<String, Map<String, Object>> databases = (Map<String, Map<String, Object>>) root.get("databases");
        assertNotNull(databases, "Databases map should not be null for file: " + yamlFile.getFileName());
        assertFalse(databases.isEmpty(), "Databases map should not be empty for file: " + yamlFile.getFileName());

        // Convert each database entry to DBMirrorTest
        for (Map.Entry<String, Map<String, Object>> entry : databases.entrySet()) {
            String dbName = entry.getKey();
            Map<String, Object> dbData = entry.getValue();

            // Convert the database data to DBMirrorTest
            DBMirrorTest dbMirrorTest = objectMapper.convertValue(dbData, DBMirrorTest.class);

            // Assert
            assertNotNull(dbMirrorTest, "DBMirrorTest should not be null for database: " + dbName);
            assertNotNull(dbMirrorTest.getName(), "Database name should not be null");
            assertEquals(dbName, dbMirrorTest.getName(), "Database name should match the key");

            // Validate basic structure
            // TODO: FIX
            /*
            assertNotNull(dbMirrorTest.getFilteredOut(), "FilteredOut map should not be null");
            assertNotNull(dbMirrorTest.getSql(), "SQL map should not be null");
            assertNotNull(dbMirrorTest.getIssues(), "Issues map should not be null");
            assertNotNull(dbMirrorTest.getProblemSQL(), "ProblemSQL map should not be null");
             */
        }
    }

    /**
     * Test deserialization of a specific known file to validate detailed structure.
     */
    @Test
    void testDeserializeSpecificFile() throws IOException {
        // Arrange
        URL resource = getClass().getClassLoader().getResource("test_data/assorted_tbls_01.yaml");
        assertNotNull(resource, "assorted_tbls_01.yaml not found in test resources");

        File yamlFile = new File(resource.getPath());

        // Act
        Map<String, Object> root = objectMapper.readValue(yamlFile, Map.class);
        Map<String, Map<String, Object>> databases = (Map<String, Map<String, Object>>) root.get("databases");

        assertNotNull(databases, "Databases should not be null");
        assertTrue(databases.containsKey("assorted_test_db"), "Should contain assorted_test_db");

        DBMirrorTest dbMirrorTest = objectMapper.convertValue(databases.get("assorted_test_db"), DBMirrorTest.class);

        // Assert
        assertNotNull(dbMirrorTest, "DBMirrorTest should not be null");
        assertEquals("assorted_test_db", dbMirrorTest.getName(), "Database name should be assorted_test_db");

        // Validate the internal structure
        assertNotNull(dbMirrorTest.getTableMirrors(), "TableMirrors should not be null");
        assertFalse(dbMirrorTest.getTableMirrors().isEmpty(), "TableMirrors should not be empty");

        // Validate that specific tables exist
        assertTrue(dbMirrorTest.getTableMirrors().containsKey("acid_01"),
                "Should contain acid_01 table");
    }

    /**
     * Test that DBMirrorTest can be cloned successfully after deserialization.
     * TODO: Do we need this?  I don't think so.
     */
    @Test
    void testCloneDeserializedObject() throws IOException {
        // Arrange
        URL resource = getClass().getClassLoader().getResource("test_data/assorted_tbls_01.yaml");
        assertNotNull(resource, "Test file not found");

        File yamlFile = new File(resource.getPath());
        Map<String, Object> root = objectMapper.readValue(yamlFile, Map.class);
        Map<String, Map<String, Object>> databases = (Map<String, Map<String, Object>>) root.get("databases");
        DBMirrorTest original = objectMapper.convertValue(databases.get("assorted_test_db"), DBMirrorTest.class);

        // Act
//        DBMirrorTest cloned = original.clone();

        // Assert
//        assertNotNull(cloned, "Cloned object should not be null");
//        assertEquals(original.getName(), cloned.getName(), "Names should match");
//        assertNotSame(original, cloned, "Should be different instances");

        // Verify deep copy - modifying clone doesn't affect original
        // TODO: Fix
        /*
        if (!original.getFilteredOut().isEmpty()) {
            String firstKey = original.getFilteredOut().keySet().iterator().next();
            cloned.getFilteredOut().remove(firstKey);
            assertTrue(original.getFilteredOut().containsKey(firstKey),
                    "Original should still contain the removed key");
        }
         */
    }

    /**
     * Test round-trip serialization: deserialize -> serialize -> deserialize.
     */
    @Test
    void testRoundTripSerialization() throws IOException {
        // Arrange
        URL resource = getClass().getClassLoader().getResource("test_data/assorted_tbls_01.yaml");
        assertNotNull(resource, "Test file not found");

        File yamlFile = new File(resource.getPath());
        Map<String, Object> root = objectMapper.readValue(yamlFile, Map.class);
        Map<String, Map<String, Object>> databases = (Map<String, Map<String, Object>>) root.get("databases");
        DBMirrorTest original = objectMapper.convertValue(databases.get("assorted_test_db"), DBMirrorTest.class);

        // Act - serialize to YAML string
        String serialized = objectMapper.writeValueAsString(original);

        // Deserialize back
        DBMirrorTest roundTripped = objectMapper.readValue(serialized, DBMirrorTest.class);

        // Assert
        assertNotNull(roundTripped, "Round-tripped object should not be null");
        assertEquals(original.getName(), roundTripped.getName(), "Names should match after round-trip");
        // TODO: Fix
//        assertEquals(original.getFilteredOut().size(), roundTripped.getFilteredOut().size(),
//                "FilteredOut size should match");
    }

    /**
     * Test that all YAML files can be successfully loaded without exceptions.
     */
    @Test
    void testAllFilesCanBeLoaded() throws IOException {
        // Arrange
        List<Path> yamlFiles = yamlTestDataFiles().collect(Collectors.toList());
        assertFalse(yamlFiles.isEmpty(), "Should have at least one YAML file");

        int successCount = 0;
        StringBuilder failures = new StringBuilder();

        // Act
        for (Path yamlFile : yamlFiles) {
            try {
                Map<String, Object> root = objectMapper.readValue(yamlFile.toFile(), Map.class);
                Map<String, Map<String, Object>> databases = (Map<String, Map<String, Object>>) root.get("databases");

                for (Map.Entry<String, Map<String, Object>> entry : databases.entrySet()) {
                    DBMirrorTest dbMirrorTest = objectMapper.convertValue(entry.getValue(), DBMirrorTest.class);
                    assertNotNull(dbMirrorTest, "DBMirrorTest should not be null for " + yamlFile.getFileName());
                }
                successCount++;
            } catch (Exception e) {
                failures.append("Failed to deserialize ")
                        .append(yamlFile.getFileName())
                        .append(": ")
                        .append(e.getMessage())
                        .append("\n");
            }
        }

        // Assert
        assertEquals(yamlFiles.size(), successCount,
                "All files should deserialize successfully. Failures:\n" + failures);
    }
}
