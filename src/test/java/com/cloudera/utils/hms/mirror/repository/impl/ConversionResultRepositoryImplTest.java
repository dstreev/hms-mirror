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

package com.cloudera.utils.hms.mirror.repository.impl;

import com.cloudera.utils.hms.mirror.domain.core.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
import com.cloudera.utils.hms.mirror.repository.rocksDbImpl.ConversionResultRepositoryImpl;
import com.cloudera.utils.hms.mirror.testutils.ConversionResultTestFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.exceptions.AssertionFailedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ConversionResultRepositoryImpl that creates test data programmatically
 * and validates save/retrieve functionality with RocksDB.
 */
class ConversionResultRepositoryImplTest {

    @TempDir
    Path tempDir;

    private RocksDB rocksDB;
    private ColumnFamilyHandle conversionResultColumnFamily;
    private ConversionResultRepository repository;
    private ConversionResult originalConversionResult;

    @BeforeEach
    void setUp() throws RocksDBException {
        // Initialize RocksDB
        RocksDB.loadLibrary();

        // Create column family descriptors
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
            new ColumnFamilyDescriptor("conversionResult".getBytes(), new ColumnFamilyOptions())
        );

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);

        rocksDB = RocksDB.open(options, tempDir.toString(), columnFamilyDescriptors, columnFamilyHandles);
        conversionResultColumnFamily = columnFamilyHandles.get(1); // sessions column family

        // Initialize repository
        ObjectMapper objectMapper = new ObjectMapper();
        repository = new ConversionResultRepositoryImpl(rocksDB, conversionResultColumnFamily, objectMapper);

        // Create test data programmatically
        loadTestData();
    }

    @AfterEach
    void tearDown() {
        if (rocksDB != null) {
            rocksDB.close();
        }
    }

    /**
     * Create ConversionResult test data programmatically.
     */
    private void loadTestData() {
        // Create a new ConversionResult with test data
        originalConversionResult = ConversionResultTestFactory.createSimpleSchemaOnlyConversion();
    }

    @Test
    void testSaveAndRetrieveConversionResult() throws RepositoryException {
        // Given
        String sessionId = "test-session-123";
        long createTimestamp = System.currentTimeMillis();

        // When - Save the conversion result
        repository.save(originalConversionResult);

        // Then - Verify it exists
        assertTrue(repository.existsById(originalConversionResult.getKey()));

        // When - Retrieve the conversion result
        ConversionResult retrievedResult = repository.findByKey(originalConversionResult.getKey()).orElseThrow(() ->
                new AssertionFailedException("Failed to retrieve from Repository")); //rebuildConversionResult(sessionId, createTimestamp);

        // Then - Verify it was retrieved successfully
        assertNotNull(retrievedResult);
        assertConversionResultsEqual(originalConversionResult, retrievedResult);
    }

    @Test
    void testDeleteConversionResult() throws RepositoryException {
        // Given
        String sessionId = "test-session-delete";
        long createTimestamp = System.currentTimeMillis();
        repository.save(originalConversionResult);

        // Verify it exists
        ConversionResult fetched = repository.findByKey(originalConversionResult.getKey()).orElseThrow(() ->
                new AssertionFailedException("Could load from repo."));

        // When - Delete the conversion result
        assertTrue(repository.deleteById(originalConversionResult.getKey()), "Failed to delete conversion result");

        // Then - Verify it no longer exists
        assertFalse(repository.existsById(originalConversionResult.getKey()), "Conversion result should not exist");

    }


    /*
    @Test
    void testDatabaseOperations() throws RocksDBException {
        // Given
        String sessionId = "test-session-db";
        long createTimestamp = System.currentTimeMillis();
        repository.saveConversionResult(sessionId, createTimestamp, originalConversionResult);

        // When - Get databases
        Set<String> databases = repository.getDatabases(sessionId, createTimestamp);

        // Then - Verify database exists
        assertNotNull(databases);
        assertTrue(databases.contains("ext_purge_odd_parts"));

        // When - Get tables for database
        Set<String> tables = repository.getTables(sessionId, createTimestamp, "ext_purge_odd_parts");

        // Then - Verify table exists
        assertNotNull(tables);
        assertTrue(tables.contains("web_sales"));
    }

    @Test
    void testEnvironmentOperations() throws RocksDBException {
        // Given
        String sessionId = "test-session-env";
        long createTimestamp = System.currentTimeMillis();
        String database = "ext_purge_odd_parts";
        String table = "web_sales";
        Environment environment = Environment.LEFT;

        repository.saveConversionResult(sessionId, createTimestamp, originalConversionResult);

        // When - Get environment table
        EnvironmentTable envTable = repository.getEnvironmentTable(sessionId, createTimestamp, database, table, environment);

        // Then - Verify environment table properties
        assertNotNull(envTable);
        assertTrue(envTable.isExists());
        assertNotNull(envTable.getDefinition());
        assertFalse(envTable.getDefinition().isEmpty());
        
        // Verify partitions
        assertNotNull(envTable.getPartitions());
        assertFalse(envTable.getPartitions().isEmpty());
        assertTrue(envTable.getPartitions().containsKey("ws_sold_date_sk=2451180"));
    }

    @Test
    void testSqlOperationsWithPairs() throws RocksDBException {
        // Given
        String sessionId = "test-session-sql";
        long createTimestamp = System.currentTimeMillis();
        String database = "test_db";
        String table = "test_table";
        Environment environment = Environment.RIGHT;

        List<Pair> testSqls = Arrays.asList(
            new Pair("Create table", "CREATE TABLE test (id INT)"),
            new Pair("Insert data", "INSERT INTO test VALUES (1)"),
            new Pair("Update data", "UPDATE test SET id = 2")
        );

        List<Pair> testCleanupSqls = Arrays.asList(
            new Pair("Drop table", "DROP TABLE test"),
            new Pair("Clean temp", "DROP TABLE temp_test")
        );

        // When - Save SQL operations
        repository.saveEnvironmentSqls(sessionId, createTimestamp, database, table, environment, testSqls);
        repository.saveEnvironmentCleanupSqls(sessionId, createTimestamp, database, table, environment, testCleanupSqls);

        // Then - Retrieve and verify SQL operations
        List<Pair> retrievedSqls = repository.getEnvironmentSqls(sessionId, createTimestamp, database, table, environment);
        List<Pair> retrievedCleanupSqls = repository.getEnvironmentCleanupSqls(sessionId, createTimestamp, database, table, environment);

        // Verify regular SQLs
        assertNotNull(retrievedSqls);
        assertEquals(testSqls.size(), retrievedSqls.size());
        for (int i = 0; i < testSqls.size(); i++) {
            assertEquals(testSqls.get(i).getDescription(), retrievedSqls.get(i).getDescription());
            assertEquals(testSqls.get(i).getAction(), retrievedSqls.get(i).getAction());
        }

        // Verify cleanup SQLs
        assertNotNull(retrievedCleanupSqls);
        assertEquals(testCleanupSqls.size(), retrievedCleanupSqls.size());
        for (int i = 0; i < testCleanupSqls.size(); i++) {
            assertEquals(testCleanupSqls.get(i).getDescription(), retrievedCleanupSqls.get(i).getDescription());
            assertEquals(testCleanupSqls.get(i).getAction(), retrievedCleanupSqls.get(i).getAction());
        }
    }

    @Test
    void testPartitionOperations() throws RocksDBException {
        // Given
        String sessionId = "test-session-partition";
        long createTimestamp = System.currentTimeMillis();
        String database = "test_db";
        String table = "test_table";
        Environment environment = Environment.LEFT;

        String partitionKey = "year=2023";
        String partitionLocation = "hdfs://cluster/data/year=2023";

        // When - Save partition
        repository.saveEnvironmentPartition(sessionId, createTimestamp, database, table, environment, partitionKey, partitionLocation);

        // Then - Retrieve and verify partition
        Object retrievedPartition = repository.getEnvironmentPartition(sessionId, createTimestamp, database, table, environment, partitionKey);
        assertNotNull(retrievedPartition);
        assertEquals(partitionLocation, retrievedPartition.toString());

        // When - Get all partitions
        Map<String, Object> allPartitions = repository.getEnvironmentPartitions(sessionId, createTimestamp, database, table, environment);

        // Then - Verify all partitions
        assertNotNull(allPartitions);
        assertTrue(allPartitions.containsKey(partitionKey));
        assertEquals(partitionLocation, allPartitions.get(partitionKey).toString());
    }

    @Test
    void testOrderingWithAssortedTestDatabase() throws RocksDBException, IOException {
        // Given - Load complex test data with multiple tables and SQL statements
        DBMirror assortedDbMirror;
        try (InputStream inputStream = getClass().getClassLoader()
                .getResourceAsStream("test_data/assorted_test_db_hms-mirror.yaml")) {
            assertNotNull(inputStream, "Test data file not found: test_data/assorted_test_db_hms-mirror.yaml");
            assortedDbMirror = yamlMapper.readValue(inputStream, DBMirror.class);
            assertNotNull(assortedDbMirror, "Failed to load DBMirror from YAML");
        }
        
        // Create a ConversionResult containing the database
        ConversionResult assortedTestConversionResult = new ConversionResult();
        assortedTestConversionResult.getDatabases().put("assorted_test_db", assortedDbMirror);

        String sessionId = "test-session-ordering";
        long createTimestamp = System.currentTimeMillis();

        // When - Save the complex conversion result
        repository.saveConversionResult(sessionId, createTimestamp, assortedTestConversionResult);

        // Then - Retrieve and verify SQL ordering preservation
        ConversionResult retrievedResult = repository.rebuildConversionResult(sessionId, createTimestamp);
        assertNotNull(retrievedResult);

        // Verify SQL statement ordering for each table in each environment
        if (assortedTestConversionResult.getDatabases() != null) {
            for (String dbName : assortedTestConversionResult.getDatabases().keySet()) {
                var originalDb = assortedTestConversionResult.getDatabases().get(dbName);
                var retrievedDb = retrievedResult.getDatabases().get(dbName);
                
                if (originalDb.getTableMirrors() != null) {
                    for (String tableName : originalDb.getTableMirrors().keySet()) {
                        var originalTable = originalDb.getTableMirrors().get(tableName);
                        var retrievedTable = retrievedDb.getTableMirrors().get(tableName);
                        
                        // Test SQL ordering for each environment
                        for (Environment env : Environment.values()) {
                            EnvironmentTable originalEnvTable = originalTable.getEnvironmentTable(env);
                            EnvironmentTable retrievedEnvTable = retrievedTable.getEnvironmentTable(env);
                            
                            if (originalEnvTable != null && retrievedEnvTable != null) {
                                // Verify SQL statement ordering
                                List<Pair> originalSqls = originalEnvTable.getSql();
                                List<Pair> retrievedSqls = retrievedEnvTable.getSql();
                                
                                if (originalSqls != null && !originalSqls.isEmpty()) {
                                    assertNotNull(retrievedSqls, 
                                        String.format("SQL list should not be null for %s.%s.%s", dbName, tableName, env));
                                    assertEquals(originalSqls.size(), retrievedSqls.size(),
                                        String.format("SQL count mismatch for %s.%s.%s", dbName, tableName, env));
                                    
                                    // Verify exact order preservation
                                    for (int i = 0; i < originalSqls.size(); i++) {
                                        assertEquals(originalSqls.get(i).getDescription(), retrievedSqls.get(i).getDescription(),
                                            String.format("SQL description order mismatch at index %d for %s.%s.%s", i, dbName, tableName, env));
                                        assertEquals(originalSqls.get(i).getAction(), retrievedSqls.get(i).getAction(),
                                            String.format("SQL action order mismatch at index %d for %s.%s.%s", i, dbName, tableName, env));
                                    }
                                }
                                
                                // Verify cleanup SQL ordering
                                List<Pair> originalCleanupSqls = originalEnvTable.getCleanUpSql();
                                List<Pair> retrievedCleanupSqls = retrievedEnvTable.getCleanUpSql();
                                
                                if (originalCleanupSqls != null && !originalCleanupSqls.isEmpty()) {
                                    assertNotNull(retrievedCleanupSqls,
                                        String.format("Cleanup SQL list should not be null for %s.%s.%s", dbName, tableName, env));
                                    assertEquals(originalCleanupSqls.size(), retrievedCleanupSqls.size(),
                                        String.format("Cleanup SQL count mismatch for %s.%s.%s", dbName, tableName, env));
                                    
                                    // Verify exact order preservation
                                    for (int i = 0; i < originalCleanupSqls.size(); i++) {
                                        assertEquals(originalCleanupSqls.get(i).getDescription(), retrievedCleanupSqls.get(i).getDescription(),
                                            String.format("Cleanup SQL description order mismatch at index %d for %s.%s.%s", i, dbName, tableName, env));
                                        assertEquals(originalCleanupSqls.get(i).getAction(), retrievedCleanupSqls.get(i).getAction(),
                                            String.format("Cleanup SQL action order mismatch at index %d for %s.%s.%s", i, dbName, tableName, env));
                                    }
                                }
                                
                                // Verify partition ordering
                                Map<String, String> originalPartitions = originalEnvTable.getPartitions();
                                Map<String, String> retrievedPartitions = retrievedEnvTable.getPartitions();
                                
                                if (originalPartitions != null && !originalPartitions.isEmpty()) {
                                    assertNotNull(retrievedPartitions,
                                        String.format("Partitions should not be null for %s.%s.%s", dbName, tableName, env));
                                    assertEquals(originalPartitions.size(), retrievedPartitions.size(),
                                        String.format("Partition count mismatch for %s.%s.%s", dbName, tableName, env));
                                    
                                    // Verify partition key ordering (LinkedHashMap should preserve insertion order)
                                    List<String> originalPartitionKeys = new ArrayList<>(originalPartitions.keySet());
                                    List<String> retrievedPartitionKeys = new ArrayList<>(retrievedPartitions.keySet());
                                    
                                    assertEquals(originalPartitionKeys, retrievedPartitionKeys,
                                        String.format("Partition key ordering mismatch for %s.%s.%s", dbName, tableName, env));
                                    
                                    // Verify partition values match
                                    for (String partitionKey : originalPartitionKeys) {
                                        assertEquals(originalPartitions.get(partitionKey), 
                                                   retrievedPartitions.get(partitionKey),
                                            String.format("Partition value mismatch for key %s in %s.%s.%s", partitionKey, dbName, tableName, env));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    */
    /**
     * Helper method to compare two ConversionResult objects for equality.
     * This performs a deep comparison of the key properties.
     */
    private void assertConversionResultsEqual(ConversionResult original, ConversionResult retrieved) {
        assertNotNull(retrieved);
        
        if (original.getDatabases() != null) {
            assertNotNull(retrieved.getDatabases());
            assertEquals(original.getDatabases().size(), retrieved.getDatabases().size());
            
            for (String dbName : original.getDatabases().keySet()) {
                assertTrue(retrieved.getDatabases().containsKey(dbName));
                
                var originalDb = original.getDatabases().get(dbName);
                var retrievedDb = retrieved.getDatabases().get(dbName);
                
                if (originalDb.getTableMirrors() != null) {
                    assertNotNull(retrievedDb.getTableMirrors());
                    assertEquals(originalDb.getTableMirrors().size(), retrievedDb.getTableMirrors().size());
                    
                    for (String tableName : originalDb.getTableMirrors().keySet()) {
                        assertTrue(retrievedDb.getTableMirrors().containsKey(tableName));
                        
                        var originalTable = originalDb.getTableMirrors().get(tableName);
                        var retrievedTable = retrievedDb.getTableMirrors().get(tableName);
                        
                        assertEquals(originalTable.getName(), retrievedTable.getName());
                        assertEquals(originalTable.isReMapped(), retrievedTable.isReMapped());
                        
                        // Compare environment tables
                        for (Environment env : Environment.values()) {
                            EnvironmentTable originalEnvTable = originalTable.getEnvironmentTable(env);
                            EnvironmentTable retrievedEnvTable = retrievedTable.getEnvironmentTable(env);
                            
                            if (originalEnvTable != null) {
                                assertNotNull(retrievedEnvTable);
                                assertEquals(originalEnvTable.isExists(), retrievedEnvTable.isExists());
                                
                                // Compare partitions if they exist
                                if (originalEnvTable.getPartitions() != null) {
                                    assertNotNull(retrievedEnvTable.getPartitions());
                                    assertEquals(originalEnvTable.getPartitions().size(), 
                                               retrievedEnvTable.getPartitions().size());
                                    
                                    for (String partitionKey : originalEnvTable.getPartitions().keySet()) {
                                        assertTrue(retrievedEnvTable.getPartitions().containsKey(partitionKey));
                                        assertEquals(originalEnvTable.getPartitions().get(partitionKey),
                                                   retrievedEnvTable.getPartitions().get(partitionKey));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}