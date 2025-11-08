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