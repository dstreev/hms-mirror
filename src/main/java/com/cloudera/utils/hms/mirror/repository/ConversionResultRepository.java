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

package com.cloudera.utils.hms.mirror.repository;

import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Repository interface for flattened ConversionResult storage using hierarchical keys.
 * 
 * Key Structure:
 * sessions/{sessionId}/{createTimestamp}/conversionResult/databases/{database}/tables/{table}/...
 * 
 * Provides fine-grained access to conversion result data with support for:
 * - Table-level operations (steps, strategy, phases, duration)
 * - Environment-specific data (LEFT, RIGHT, SHADOW)
 * - SQL storage with indexing (001-999, 0000-9999)
 * - Partition management
 * - Properties and statistics
 * - Issues and error tracking
 */
public interface ConversionResultRepository {

    // === Core Operations ===
    
    /**
     * Save complete conversion result in flattened format.
     */
    void saveConversionResult(String sessionId, long createTimestamp, ConversionResult conversionResult) throws RocksDBException;
    
    /**
     * Rebuild complete conversion result from flattened data.
     */
    ConversionResult rebuildConversionResult(String sessionId, long createTimestamp) throws RocksDBException;
    
    /**
     * Delete all conversion result data for a session.
     */
    void deleteConversionResult(String sessionId, long createTimestamp) throws RocksDBException;
    
    /**
     * Check if conversion result exists for session.
     */
    boolean conversionResultExists(String sessionId, long createTimestamp) throws RocksDBException;

    // === Database Operations ===
    
    /**
     * Get all databases in conversion result.
     */
    Set<String> getDatabases(String sessionId, long createTimestamp) throws RocksDBException;
    
    /**
     * Get all tables in a database.
     */
    Set<String> getTables(String sessionId, long createTimestamp, String database) throws RocksDBException;

    // === Table-Level Operations ===
    
    /**
     * Save table steps.
     */
    void saveTableSteps(String sessionId, long createTimestamp, String database, String table, List<String> steps) throws RocksDBException;
    
    /**
     * Get table steps.
     */
    List<String> getTableSteps(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;
    
    /**
     * Save table start time.
     */
    void saveTableStart(String sessionId, long createTimestamp, String database, String table, long startTime) throws RocksDBException;
    
    /**
     * Get table start time.
     */
    Long getTableStart(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;
    
    /**
     * Save table remapped flag.
     */
    void saveTableReMapped(String sessionId, long createTimestamp, String database, String table, boolean reMapped) throws RocksDBException;
    
    /**
     * Get table remapped flag.
     */
    Boolean getTableReMapped(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;
    
    /**
     * Save table strategy.
     */
    void saveTableStrategy(String sessionId, long createTimestamp, String database, String table, String strategy) throws RocksDBException;
    
    /**
     * Get table strategy.
     */
    String getTableStrategy(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;
    
    /**
     * Save table current phase.
     */
    void saveTableCurrentPhase(String sessionId, long createTimestamp, String database, String table, int currentPhase) throws RocksDBException;
    
    /**
     * Get table current phase.
     */
    Integer getTableCurrentPhase(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;
    
    /**
     * Save table total phase count.
     */
    void saveTableTotalPhaseCount(String sessionId, long createTimestamp, String database, String table, int totalPhaseCount) throws RocksDBException;
    
    /**
     * Get table total phase count.
     */
    Integer getTableTotalPhaseCount(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;
    
    /**
     * Save table stage duration.
     */
    void saveTableStageDuration(String sessionId, long createTimestamp, String database, String table, long stageDuration) throws RocksDBException;
    
    /**
     * Get table stage duration.
     */
    Long getTableStageDuration(String sessionId, long createTimestamp, String database, String table) throws RocksDBException;

    // === Environment Operations ===
    
    /**
     * Save environment table data.
     */
    void saveEnvironmentTable(String sessionId, long createTimestamp, String database, String table, Environment environment, EnvironmentTable environmentTable) throws RocksDBException;
    
    /**
     * Get environment table data.
     */
    EnvironmentTable getEnvironmentTable(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save environment sqls with indexing.
     */
    void saveEnvironmentSqls(String sessionId, long createTimestamp, String database, String table, Environment environment, List<Pair> sqls) throws RocksDBException;
    
    /**
     * Get environment sqls.
     */
    List<Pair> getEnvironmentSqls(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save environment cleanup sqls with indexing.
     */
    void saveEnvironmentCleanupSqls(String sessionId, long createTimestamp, String database, String table, Environment environment, List<Pair> cleanupSqls) throws RocksDBException;
    
    /**
     * Get environment cleanup sqls.
     */
    List<Pair> getEnvironmentCleanupSqls(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save environment exists flag.
     */
    void saveEnvironmentExists(String sessionId, long createTimestamp, String database, String table, Environment environment, boolean exists) throws RocksDBException;
    
    /**
     * Get environment exists flag.
     */
    Boolean getEnvironmentExists(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save environment create strategy.
     */
    void saveEnvironmentCreateStrategy(String sessionId, long createTimestamp, String database, String table, Environment environment, String createStrategy) throws RocksDBException;
    
    /**
     * Get environment create strategy.
     */
    String getEnvironmentCreateStrategy(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save environment definition.
     */
    void saveEnvironmentDefinition(String sessionId, long createTimestamp, String database, String table, Environment environment, String definition) throws RocksDBException;
    
    /**
     * Get environment definition.
     */
    String getEnvironmentDefinition(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save environment owner.
     */
    void saveEnvironmentOwner(String sessionId, long createTimestamp, String database, String table, Environment environment, String owner) throws RocksDBException;
    
    /**
     * Get environment owner.
     */
    String getEnvironmentOwner(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;

    // === Partition Operations ===
    
    /**
     * Save environment partition.
     */
    void saveEnvironmentPartition(String sessionId, long createTimestamp, String database, String table, Environment environment, String partition, Object partitionData) throws RocksDBException;
    
    /**
     * Get environment partition.
     */
    Object getEnvironmentPartition(String sessionId, long createTimestamp, String database, String table, Environment environment, String partition) throws RocksDBException;
    
    /**
     * Get all environment partitions.
     */
    Map<String, Object> getEnvironmentPartitions(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Delete environment partition.
     */
    void deleteEnvironmentPartition(String sessionId, long createTimestamp, String database, String table, Environment environment, String partition) throws RocksDBException;

    // === Properties Operations ===
    
    /**
     * Save environment add property.
     */
    void saveEnvironmentAddProperty(String sessionId, long createTimestamp, String database, String table, Environment environment, String key, String value) throws RocksDBException;
    
    /**
     * Get environment add property.
     */
    String getEnvironmentAddProperty(String sessionId, long createTimestamp, String database, String table, Environment environment, String key) throws RocksDBException;
    
    /**
     * Get all environment add properties.
     */
    Map<String, String> getEnvironmentAddProperties(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Delete environment add property.
     */
    void deleteEnvironmentAddProperty(String sessionId, long createTimestamp, String database, String table, Environment environment, String key) throws RocksDBException;

    // === Statistics Operations ===
    
    /**
     * Save environment statistics.
     */
    void saveEnvironmentStatistics(String sessionId, long createTimestamp, String database, String table, Environment environment, Map<String, Object> statistics) throws RocksDBException;
    
    /**
     * Get environment statistics.
     */
    Map<String, Object> getEnvironmentStatistics(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Save specific environment statistic.
     */
    void saveEnvironmentStatistic(String sessionId, long createTimestamp, String database, String table, Environment environment, String statKey, Object statValue) throws RocksDBException;
    
    /**
     * Get specific environment statistic.
     */
    Object getEnvironmentStatistic(String sessionId, long createTimestamp, String database, String table, Environment environment, String statKey) throws RocksDBException;

    // === Issues and Errors Operations ===
    
    /**
     * Save environment issues with indexing (0000-9999).
     */
    void saveEnvironmentIssues(String sessionId, long createTimestamp, String database, String table, Environment environment, List<String> issues) throws RocksDBException;
    
    /**
     * Get environment issues.
     */
    List<String> getEnvironmentIssues(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Add environment issue.
     */
    void addEnvironmentIssue(String sessionId, long createTimestamp, String database, String table, Environment environment, String issue) throws RocksDBException;
    
    /**
     * Save environment errors with indexing (0000-9999).
     */
    void saveEnvironmentErrors(String sessionId, long createTimestamp, String database, String table, Environment environment, List<String> errors) throws RocksDBException;
    
    /**
     * Get environment errors.
     */
    List<String> getEnvironmentErrors(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException;
    
    /**
     * Add environment error.
     */
    void addEnvironmentError(String sessionId, long createTimestamp, String database, String table, Environment environment, String error) throws RocksDBException;

    // === Utility Operations ===
    
    /**
     * Get all keys with given prefix.
     */
    Set<String> getKeysWithPrefix(String sessionId, long createTimestamp, String prefix) throws RocksDBException;
    
    /**
     * Get flattened data as key-value pairs.
     */
    Map<String, Object> getFlattenedData(String sessionId, long createTimestamp) throws RocksDBException;
    
    /**
     * Get specific flattened value by key.
     */
    Object getFlattenedValue(String sessionId, long createTimestamp, String key) throws RocksDBException;
    
    /**
     * Save specific flattened value by key.
     */
    void saveFlattenedValue(String sessionId, long createTimestamp, String key, Object value) throws RocksDBException;
    
    /**
     * Delete specific flattened value by key.
     */
    void deleteFlattenedValue(String sessionId, long createTimestamp, String key) throws RocksDBException;
}