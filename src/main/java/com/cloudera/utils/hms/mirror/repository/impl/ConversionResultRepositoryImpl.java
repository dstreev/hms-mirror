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

import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Implementation of ConversionResultRepository for flattened storage in RocksDB.
 * 
 * Uses hierarchical keys with the format:
 * sessions/{sessionId}/{createTimestamp}/conversionResult/...
 * 
 * Provides fine-grained access to conversion result data with efficient
 * storage and retrieval of large conversion results.
 */
@Repository
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true")
@Slf4j
public class ConversionResultRepositoryImpl implements ConversionResultRepository {

    private static final String SESSIONS_PREFIX = "sessions/";
    private static final String CONVERSION_RESULT_PREFIX = "/conversionResult";
    private static final String DATABASES_PREFIX = "/databases";
    private static final String TABLES_PREFIX = "/tables";
    private static final String SEPARATOR = "/";

    // Table-level keys
    private static final String STEPS_KEY = "/steps";
    private static final String START_KEY = "/start";
    private static final String REMAPPED_KEY = "/reMapped";
    private static final String STRATEGY_KEY = "/strategy";
    private static final String CURRENT_PHASE_KEY = "/currentPhase";
    private static final String TOTAL_PHASE_COUNT_KEY = "/totalPhaseCount";
    private static final String STAGE_DURATION_KEY = "/stageDuration";

    // Environment-level keys
    private static final String SQLS_KEY = "/sqls";
    private static final String CLEANUP_SQLS_KEY = "/cleanupSqls";
    private static final String EXISTS_KEY = "/exists";
    private static final String CREATE_STRATEGY_KEY = "/createStrategy";
    private static final String DEFINITION_KEY = "/definition";
    private static final String OWNER_KEY = "/owner";
    private static final String PARTITIONS_KEY = "/partitions";
    private static final String ADD_PROPERTIES_KEY = "/addProperties";
    private static final String STATISTICS_KEY = "/statistics";
    private static final String ISSUES_KEY = "/issues";
    private static final String ERRORS_KEY = "/errors";

    private final RocksDB rocksDB;
    private final ColumnFamilyHandle sessionsColumnFamily;
    private final ObjectMapper objectMapper;
    private final ObjectMapper yamlMapper;

    public ConversionResultRepositoryImpl(RocksDB rocksDB,
                                         @Qualifier("conversionResultColumnFamily") ColumnFamilyHandle sessionsColumnFamily,
                                         ObjectMapper objectMapper) {
        this.rocksDB = rocksDB;
        this.sessionsColumnFamily = sessionsColumnFamily;
        this.objectMapper = objectMapper;
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    // === Core Operations ===

    @Override
    public void saveConversionResult(String sessionId, long createTimestamp, ConversionResult conversionResult) throws RocksDBException {
        if (conversionResult == null) {
            return;
        }
        
        try {
            // Flatten the conversion result into individual key-value pairs
            Map<String, Object> flattenedData = flattenConversionResult(conversionResult);
            
            for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
                String fullKey = buildConversionResultPath(sessionId, createTimestamp, entry.getKey());
                byte[] keyBytes = fullKey.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = objectMapper.writeValueAsBytes(entry.getValue());
                rocksDB.put(sessionsColumnFamily, keyBytes, valueBytes);
            }
            
            log.debug("Saved flattened conversion result for session {}:{}", sessionId, createTimestamp);
        } catch (JsonProcessingException e) {
            throw new RocksDBException("Failed to serialize conversion result: " + e.getMessage());
        }
    }

    @Override
    public ConversionResult rebuildConversionResult(String sessionId, long createTimestamp) throws RocksDBException {
        try {
            Map<String, Object> flattenedData = getFlattenedData(sessionId, createTimestamp);
            if (flattenedData.isEmpty()) {
                return null;
            }
            
            return unflattenToConversionResult(flattenedData);
        } catch (Exception e) {
            throw new RocksDBException("Failed to rebuild conversion result: " + e.getMessage());
        }
    }

    @Override
    public void deleteConversionResult(String sessionId, long createTimestamp) throws RocksDBException {
        String prefix = buildConversionResultPath(sessionId, createTimestamp, "");
        deleteKeysWithPrefix(prefix);
        log.debug("Deleted conversion result for session {}:{}", sessionId, createTimestamp);
    }

    @Override
    public boolean conversionResultExists(String sessionId, long createTimestamp) throws RocksDBException {
        Set<String> keys = getKeysWithPrefix(sessionId, createTimestamp, "");
        return !keys.isEmpty();
    }

    // === Database Operations ===

    @Override
    public Set<String> getDatabases(String sessionId, long createTimestamp) throws RocksDBException {
        String prefix = buildConversionResultPath(sessionId, createTimestamp, DATABASES_PREFIX);
        Set<String> allKeys = getKeysWithPrefix(sessionId, createTimestamp, DATABASES_PREFIX);
        
        return allKeys.stream()
                .filter(key -> key.startsWith(DATABASES_PREFIX + SEPARATOR))
                .map(key -> key.substring((DATABASES_PREFIX + SEPARATOR).length()))
                .map(key -> key.split(SEPARATOR)[0])
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getTables(String sessionId, long createTimestamp, String database) throws RocksDBException {
        String prefix = DATABASES_PREFIX + SEPARATOR + database + TABLES_PREFIX;
        Set<String> allKeys = getKeysWithPrefix(sessionId, createTimestamp, prefix);
        
        return allKeys.stream()
                .filter(key -> key.startsWith(prefix + SEPARATOR))
                .map(key -> key.substring(prefix.length() + 1))
                .map(key -> key.split(SEPARATOR)[0])
                .collect(Collectors.toSet());
    }

    // === Table-Level Operations ===

    @Override
    public void saveTableSteps(String sessionId, long createTimestamp, String database, String table, List<String> steps) throws RocksDBException {
        String key = buildTablePath(database, table) + STEPS_KEY;
        saveValue(sessionId, createTimestamp, key, steps);
    }

    @Override
    public List<String> getTableSteps(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + STEPS_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<List<String>>() {});
    }

    @Override
    public void saveTableStart(String sessionId, long createTimestamp, String database, String table, long startTime) throws RocksDBException {
        String key = buildTablePath(database, table) + START_KEY;
        saveValue(sessionId, createTimestamp, key, startTime);
    }

    @Override
    public Long getTableStart(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + START_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Long>() {});
    }

    @Override
    public void saveTableReMapped(String sessionId, long createTimestamp, String database, String table, boolean reMapped) throws RocksDBException {
        String key = buildTablePath(database, table) + REMAPPED_KEY;
        saveValue(sessionId, createTimestamp, key, reMapped);
    }

    @Override
    public Boolean getTableReMapped(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + REMAPPED_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Boolean>() {});
    }

    @Override
    public void saveTableStrategy(String sessionId, long createTimestamp, String database, String table, String strategy) throws RocksDBException {
        String key = buildTablePath(database, table) + STRATEGY_KEY;
        saveValue(sessionId, createTimestamp, key, strategy);
    }

    @Override
    public String getTableStrategy(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + STRATEGY_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<String>() {});
    }

    @Override
    public void saveTableCurrentPhase(String sessionId, long createTimestamp, String database, String table, int currentPhase) throws RocksDBException {
        String key = buildTablePath(database, table) + CURRENT_PHASE_KEY;
        saveValue(sessionId, createTimestamp, key, currentPhase);
    }

    @Override
    public Integer getTableCurrentPhase(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + CURRENT_PHASE_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Integer>() {});
    }

    @Override
    public void saveTableTotalPhaseCount(String sessionId, long createTimestamp, String database, String table, int totalPhaseCount) throws RocksDBException {
        String key = buildTablePath(database, table) + TOTAL_PHASE_COUNT_KEY;
        saveValue(sessionId, createTimestamp, key, totalPhaseCount);
    }

    @Override
    public Integer getTableTotalPhaseCount(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + TOTAL_PHASE_COUNT_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Integer>() {});
    }

    @Override
    public void saveTableStageDuration(String sessionId, long createTimestamp, String database, String table, long stageDuration) throws RocksDBException {
        String key = buildTablePath(database, table) + STAGE_DURATION_KEY;
        saveValue(sessionId, createTimestamp, key, stageDuration);
    }

    @Override
    public Long getTableStageDuration(String sessionId, long createTimestamp, String database, String table) throws RocksDBException {
        String key = buildTablePath(database, table) + STAGE_DURATION_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Long>() {});
    }

    // === Environment Operations ===

    @Override
    public void saveEnvironmentTable(String sessionId, long createTimestamp, String database, String table, Environment environment, EnvironmentTable environmentTable) throws RocksDBException {
        String baseKey = buildEnvironmentPath(database, table, environment);
        try {
            // Flatten the environment table into individual components
            if (environmentTable.getSql() != null && !environmentTable.getSql().isEmpty()) {
                saveEnvironmentSqls(sessionId, createTimestamp, database, table, environment, environmentTable.getSql());
            }
            if (environmentTable.getCleanUpSql() != null && !environmentTable.getCleanUpSql().isEmpty()) {
                saveEnvironmentCleanupSqls(sessionId, createTimestamp, database, table, environment, environmentTable.getCleanUpSql());
            }
            saveEnvironmentExists(sessionId, createTimestamp, database, table, environment, environmentTable.isExists());
            if (environmentTable.getCreateStrategy() != null) {
                saveEnvironmentCreateStrategy(sessionId, createTimestamp, database, table, environment, environmentTable.getCreateStrategy().toString());
            }
            if (environmentTable.getDefinition() != null && !environmentTable.getDefinition().isEmpty()) {
                saveEnvironmentDefinition(sessionId, createTimestamp, database, table, environment, String.join("\n", environmentTable.getDefinition()));
            }
            if (environmentTable.getOwner() != null) {
                saveEnvironmentOwner(sessionId, createTimestamp, database, table, environment, environmentTable.getOwner());
            }
            if (environmentTable.getPartitions() != null && !environmentTable.getPartitions().isEmpty()) {
                for (Map.Entry<String, String> partition : environmentTable.getPartitions().entrySet()) {
                    saveEnvironmentPartition(sessionId, createTimestamp, database, table, environment, partition.getKey(), partition.getValue());
                }
            }
            if (environmentTable.getAddProperties() != null && !environmentTable.getAddProperties().isEmpty()) {
                for (Map.Entry<String, String> property : environmentTable.getAddProperties().entrySet()) {
                    saveEnvironmentAddProperty(sessionId, createTimestamp, database, table, environment, property.getKey(), property.getValue());
                }
            }
            if (environmentTable.getStatistics() != null && !environmentTable.getStatistics().isEmpty()) {
                saveEnvironmentStatistics(sessionId, createTimestamp, database, table, environment, environmentTable.getStatistics());
            }
            if (environmentTable.getIssues() != null && !environmentTable.getIssues().isEmpty()) {
                saveEnvironmentIssues(sessionId, createTimestamp, database, table, environment, environmentTable.getIssues());
            }
        } catch (Exception e) {
            throw new RocksDBException("Failed to save environment table: " + e.getMessage());
        }
    }

    @Override
    public EnvironmentTable getEnvironmentTable(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        try {
            EnvironmentTable environmentTable = new EnvironmentTable();
            
            // Reconstruct environment table from individual components
            environmentTable.setSql(getEnvironmentSqls(sessionId, createTimestamp, database, table, environment));
            environmentTable.setCleanUpSql(getEnvironmentCleanupSqls(sessionId, createTimestamp, database, table, environment));
            Boolean exists = getEnvironmentExists(sessionId, createTimestamp, database, table, environment);
            if (exists != null) {
                environmentTable.setExists(exists);
            }
            
            String createStrategy = getEnvironmentCreateStrategy(sessionId, createTimestamp, database, table, environment);
            if (createStrategy != null) {
                // Note: You may need to add a method to parse CreateStrategy from string
                // environmentTable.setCreateStrategy(CreateStrategy.valueOf(createStrategy));
            }
            
            String definition = getEnvironmentDefinition(sessionId, createTimestamp, database, table, environment);
            if (definition != null) {
                environmentTable.setDefinition(Arrays.asList(definition.split("\n")));
            }
            
            environmentTable.setOwner(getEnvironmentOwner(sessionId, createTimestamp, database, table, environment));
            environmentTable.setPartitions(getEnvironmentPartitions(sessionId, createTimestamp, database, table, environment).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
            environmentTable.setAddProperties(getEnvironmentAddProperties(sessionId, createTimestamp, database, table, environment));
            environmentTable.setStatistics(getEnvironmentStatistics(sessionId, createTimestamp, database, table, environment));
            environmentTable.setIssues(getEnvironmentIssues(sessionId, createTimestamp, database, table, environment));
            
            return environmentTable;
        } catch (Exception e) {
            throw new RocksDBException("Failed to get environment table: " + e.getMessage());
        }
    }

    @Override
    public void saveEnvironmentSqls(String sessionId, long createTimestamp, String database, String table, Environment environment, List<Pair> sqls) throws RocksDBException {
        saveIndexedPairList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + SQLS_KEY, sqls, "001", 999);
    }

    @Override
    public List<Pair> getEnvironmentSqls(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        return getIndexedPairList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + SQLS_KEY, "001", 999);
    }

    @Override
    public void saveEnvironmentCleanupSqls(String sessionId, long createTimestamp, String database, String table, Environment environment, List<Pair> cleanupSqls) throws RocksDBException {
        saveIndexedPairList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + CLEANUP_SQLS_KEY, cleanupSqls, "001", 999);
    }

    @Override
    public List<Pair> getEnvironmentCleanupSqls(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        return getIndexedPairList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + CLEANUP_SQLS_KEY, "001", 999);
    }

    @Override
    public void saveEnvironmentExists(String sessionId, long createTimestamp, String database, String table, Environment environment, boolean exists) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + EXISTS_KEY;
        saveValue(sessionId, createTimestamp, key, exists);
    }

    @Override
    public Boolean getEnvironmentExists(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + EXISTS_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Boolean>() {});
    }

    @Override
    public void saveEnvironmentCreateStrategy(String sessionId, long createTimestamp, String database, String table, Environment environment, String createStrategy) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + CREATE_STRATEGY_KEY;
        saveValue(sessionId, createTimestamp, key, createStrategy);
    }

    @Override
    public String getEnvironmentCreateStrategy(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + CREATE_STRATEGY_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<String>() {});
    }

    @Override
    public void saveEnvironmentDefinition(String sessionId, long createTimestamp, String database, String table, Environment environment, String definition) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + DEFINITION_KEY;
        saveValue(sessionId, createTimestamp, key, definition);
    }

    @Override
    public String getEnvironmentDefinition(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + DEFINITION_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<String>() {});
    }

    @Override
    public void saveEnvironmentOwner(String sessionId, long createTimestamp, String database, String table, Environment environment, String owner) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + OWNER_KEY;
        saveValue(sessionId, createTimestamp, key, owner);
    }

    @Override
    public String getEnvironmentOwner(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + OWNER_KEY;
        return getValue(sessionId, createTimestamp, key, new TypeReference<String>() {});
    }

    // === Partition Operations ===

    @Override
    public void saveEnvironmentPartition(String sessionId, long createTimestamp, String database, String table, Environment environment, String partition, Object partitionData) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + PARTITIONS_KEY + SEPARATOR + partition;
        saveValue(sessionId, createTimestamp, key, partitionData);
    }

    @Override
    public Object getEnvironmentPartition(String sessionId, long createTimestamp, String database, String table, Environment environment, String partition) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + PARTITIONS_KEY + SEPARATOR + partition;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Object>() {});
    }

    @Override
    public Map<String, Object> getEnvironmentPartitions(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String prefix = buildEnvironmentPath(database, table, environment) + PARTITIONS_KEY + SEPARATOR;
        return getValuesWithPrefix(sessionId, createTimestamp, prefix);
    }

    @Override
    public void deleteEnvironmentPartition(String sessionId, long createTimestamp, String database, String table, Environment environment, String partition) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + PARTITIONS_KEY + SEPARATOR + partition;
        deleteValue(sessionId, createTimestamp, key);
    }

    // === Properties Operations ===

    @Override
    public void saveEnvironmentAddProperty(String sessionId, long createTimestamp, String database, String table, Environment environment, String key, String value) throws RocksDBException {
        String fullKey = buildEnvironmentPath(database, table, environment) + ADD_PROPERTIES_KEY + SEPARATOR + key;
        saveValue(sessionId, createTimestamp, fullKey, value);
    }

    @Override
    public String getEnvironmentAddProperty(String sessionId, long createTimestamp, String database, String table, Environment environment, String key) throws RocksDBException {
        String fullKey = buildEnvironmentPath(database, table, environment) + ADD_PROPERTIES_KEY + SEPARATOR + key;
        return getValue(sessionId, createTimestamp, fullKey, new TypeReference<String>() {});
    }

    @Override
    public Map<String, String> getEnvironmentAddProperties(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String prefix = buildEnvironmentPath(database, table, environment) + ADD_PROPERTIES_KEY + SEPARATOR;
        Map<String, Object> values = getValuesWithPrefix(sessionId, createTimestamp, prefix);
        return values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }

    @Override
    public void deleteEnvironmentAddProperty(String sessionId, long createTimestamp, String database, String table, Environment environment, String key) throws RocksDBException {
        String fullKey = buildEnvironmentPath(database, table, environment) + ADD_PROPERTIES_KEY + SEPARATOR + key;
        deleteValue(sessionId, createTimestamp, fullKey);
    }

    // === Statistics Operations ===

    @Override
    public void saveEnvironmentStatistics(String sessionId, long createTimestamp, String database, String table, Environment environment, Map<String, Object> statistics) throws RocksDBException {
        String baseKey = buildEnvironmentPath(database, table, environment) + STATISTICS_KEY + SEPARATOR;
        for (Map.Entry<String, Object> entry : statistics.entrySet()) {
            saveValue(sessionId, createTimestamp, baseKey + entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Map<String, Object> getEnvironmentStatistics(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        String prefix = buildEnvironmentPath(database, table, environment) + STATISTICS_KEY + SEPARATOR;
        return getValuesWithPrefix(sessionId, createTimestamp, prefix);
    }

    @Override
    public void saveEnvironmentStatistic(String sessionId, long createTimestamp, String database, String table, Environment environment, String statKey, Object statValue) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + STATISTICS_KEY + SEPARATOR + statKey;
        saveValue(sessionId, createTimestamp, key, statValue);
    }

    @Override
    public Object getEnvironmentStatistic(String sessionId, long createTimestamp, String database, String table, Environment environment, String statKey) throws RocksDBException {
        String key = buildEnvironmentPath(database, table, environment) + STATISTICS_KEY + SEPARATOR + statKey;
        return getValue(sessionId, createTimestamp, key, new TypeReference<Object>() {});
    }

    // === Issues and Errors Operations ===

    @Override
    public void saveEnvironmentIssues(String sessionId, long createTimestamp, String database, String table, Environment environment, List<String> issues) throws RocksDBException {
        saveIndexedList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + ISSUES_KEY, issues, "0000", 9999);
    }

    @Override
    public List<String> getEnvironmentIssues(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        return getIndexedList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + ISSUES_KEY, "0000", 9999);
    }

    @Override
    public void addEnvironmentIssue(String sessionId, long createTimestamp, String database, String table, Environment environment, String issue) throws RocksDBException {
        List<String> issues = getEnvironmentIssues(sessionId, createTimestamp, database, table, environment);
        if (issues == null) {
            issues = new ArrayList<>();
        }
        issues.add(issue);
        saveEnvironmentIssues(sessionId, createTimestamp, database, table, environment, issues);
    }

    @Override
    public void saveEnvironmentErrors(String sessionId, long createTimestamp, String database, String table, Environment environment, List<String> errors) throws RocksDBException {
        saveIndexedList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + ERRORS_KEY, errors, "0000", 9999);
    }

    @Override
    public List<String> getEnvironmentErrors(String sessionId, long createTimestamp, String database, String table, Environment environment) throws RocksDBException {
        return getIndexedList(sessionId, createTimestamp, buildEnvironmentPath(database, table, environment) + ERRORS_KEY, "0000", 9999);
    }

    @Override
    public void addEnvironmentError(String sessionId, long createTimestamp, String database, String table, Environment environment, String error) throws RocksDBException {
        List<String> errors = getEnvironmentErrors(sessionId, createTimestamp, database, table, environment);
        if (errors == null) {
            errors = new ArrayList<>();
        }
        errors.add(error);
        saveEnvironmentErrors(sessionId, createTimestamp, database, table, environment, errors);
    }

    // === Utility Operations ===

    @Override
    public Set<String> getKeysWithPrefix(String sessionId, long createTimestamp, String prefix) throws RocksDBException {
        String fullPrefix = buildConversionResultPath(sessionId, createTimestamp, prefix);
        Set<String> keys = new HashSet<>();
        
        try (RocksIterator iterator = rocksDB.newIterator(sessionsColumnFamily)) {
            byte[] prefixBytes = fullPrefix.getBytes(StandardCharsets.UTF_8);
            iterator.seek(prefixBytes);
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(fullPrefix)) {
                    break;
                }
                // Return the relative key (remove the session path prefix)
                String conversionResultPrefix = buildConversionResultPath(sessionId, createTimestamp, "");
                String relativeKey = key.substring(conversionResultPrefix.length());
                keys.add(relativeKey);
                iterator.next();
            }
        }
        
        return keys;
    }

    @Override
    public Map<String, Object> getFlattenedData(String sessionId, long createTimestamp) throws RocksDBException {
        return getValuesWithPrefix(sessionId, createTimestamp, "");
    }

    @Override
    public Object getFlattenedValue(String sessionId, long createTimestamp, String key) throws RocksDBException {
        return getValue(sessionId, createTimestamp, key, new TypeReference<Object>() {});
    }

    @Override
    public void saveFlattenedValue(String sessionId, long createTimestamp, String key, Object value) throws RocksDBException {
        saveValue(sessionId, createTimestamp, key, value);
    }

    @Override
    public void deleteFlattenedValue(String sessionId, long createTimestamp, String key) throws RocksDBException {
        deleteValue(sessionId, createTimestamp, key);
    }

    // === Private Helper Methods ===

    private String buildConversionResultPath(String sessionId, long createTimestamp, String suffix) {
        StringBuilder sb = new StringBuilder();
        sb.append(SESSIONS_PREFIX).append(sessionId).append(SEPARATOR).append(createTimestamp);
        sb.append(CONVERSION_RESULT_PREFIX);
        if (suffix != null && !suffix.isEmpty()) {
            if (!suffix.startsWith(SEPARATOR)) {
                sb.append(SEPARATOR);
            }
            sb.append(suffix);
        }
        return sb.toString();
    }

    private String buildTablePath(String database, String table) {
        return DATABASES_PREFIX + SEPARATOR + database + TABLES_PREFIX + SEPARATOR + table;
    }

    private String buildEnvironmentPath(String database, String table, Environment environment) {
        return buildTablePath(database, table) + SEPARATOR + environment.name();
    }

    private <T> void saveValue(String sessionId, long createTimestamp, String key, T value) throws RocksDBException {
        try {
            String fullKey = buildConversionResultPath(sessionId, createTimestamp, key);
            byte[] keyBytes = fullKey.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = objectMapper.writeValueAsBytes(value);
            rocksDB.put(sessionsColumnFamily, keyBytes, valueBytes);
        } catch (JsonProcessingException e) {
            throw new RocksDBException("Failed to serialize value: " + e.getMessage());
        }
    }

    private <T> T getValue(String sessionId, long createTimestamp, String key, TypeReference<T> typeRef) throws RocksDBException {
        try {
            String fullKey = buildConversionResultPath(sessionId, createTimestamp, key);
            byte[] keyBytes = fullKey.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = rocksDB.get(sessionsColumnFamily, keyBytes);
            
            if (valueBytes == null) {
                return null;
            }
            
            return objectMapper.readValue(valueBytes, typeRef);
        } catch (Exception e) {
            throw new RocksDBException("Failed to deserialize value: " + e.getMessage());
        }
    }

    private void deleteValue(String sessionId, long createTimestamp, String key) throws RocksDBException {
        String fullKey = buildConversionResultPath(sessionId, createTimestamp, key);
        byte[] keyBytes = fullKey.getBytes(StandardCharsets.UTF_8);
        rocksDB.delete(sessionsColumnFamily, keyBytes);
    }

    private Map<String, Object> getValuesWithPrefix(String sessionId, long createTimestamp, String prefix) throws RocksDBException {
        String fullPrefix = buildConversionResultPath(sessionId, createTimestamp, prefix);
        Map<String, Object> values = new HashMap<>();
        
        try (RocksIterator iterator = rocksDB.newIterator(sessionsColumnFamily)) {
            byte[] prefixBytes = fullPrefix.getBytes(StandardCharsets.UTF_8);
            iterator.seek(prefixBytes);
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(fullPrefix)) {
                    break;
                }
                
                // Extract the relative key name
                String relativeKey = key.substring(fullPrefix.length());
                if (!relativeKey.isEmpty() && relativeKey.startsWith(SEPARATOR)) {
                    relativeKey = relativeKey.substring(1);
                }
                
                Object value = objectMapper.readValue(iterator.value(), Object.class);
                values.put(relativeKey, value);
                iterator.next();
            }
        } catch (Exception e) {
            throw new RocksDBException("Failed to get values with prefix: " + e.getMessage());
        }
        
        return values;
    }

    private void deleteKeysWithPrefix(String prefix) throws RocksDBException {
        try (RocksIterator iterator = rocksDB.newIterator(sessionsColumnFamily)) {
            byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
            iterator.seek(prefixBytes);
            
            List<byte[]> keysToDelete = new ArrayList<>();
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) {
                    break;
                }
                keysToDelete.add(iterator.key());
                iterator.next();
            }
            
            for (byte[] key : keysToDelete) {
                rocksDB.delete(sessionsColumnFamily, key);
            }
        }
    }


    private void saveIndexedList(String sessionId, long createTimestamp, String baseKey, List<String> items, String indexPrefix, int maxIndex) throws RocksDBException {
        if (items == null) {
            return;
        }
        
        // First, clear any existing indexed items
        String searchPrefix = buildConversionResultPath(sessionId, createTimestamp, baseKey + SEPARATOR);
        deleteKeysWithPrefix(searchPrefix);
        
        // Save new items with proper indexing
        for (int i = 0; i < items.size() && i <= maxIndex; i++) {
            String index = String.format("%0" + indexPrefix.length() + "d", i + 1);
            String key = baseKey + SEPARATOR + index;
            saveValue(sessionId, createTimestamp, key, items.get(i));
        }
    }
    
    private void saveIndexedPairList(String sessionId, long createTimestamp, String baseKey, List<Pair> pairs, String indexPrefix, int maxIndex) throws RocksDBException {
        if (pairs == null) {
            return;
        }
        
        // First, clear any existing indexed items
        String searchPrefix = buildConversionResultPath(sessionId, createTimestamp, baseKey + SEPARATOR);
        deleteKeysWithPrefix(searchPrefix);
        
        // Save new items with proper indexing using YAML serialization
        for (int i = 0; i < pairs.size() && i <= maxIndex; i++) {
            try {
                String index = String.format("%0" + indexPrefix.length() + "d", i + 1);
                String key = baseKey + SEPARATOR + index;
                String fullKey = buildConversionResultPath(sessionId, createTimestamp, key);
                
                // Serialize Pair as YAML
                String yamlDocument = yamlMapper.writeValueAsString(pairs.get(i));
                byte[] keyBytes = fullKey.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = yamlDocument.getBytes(StandardCharsets.UTF_8);
                
                rocksDB.put(sessionsColumnFamily, keyBytes, valueBytes);
            } catch (JsonProcessingException e) {
                throw new RocksDBException("Failed to serialize Pair to YAML: " + e.getMessage());
            }
        }
    }

    private List<String> getIndexedList(String sessionId, long createTimestamp, String baseKey, String indexPrefix, int maxIndex) throws RocksDBException {
        Map<String, Object> values = getValuesWithPrefix(sessionId, createTimestamp, baseKey + SEPARATOR);
        
        List<String> result = new ArrayList<>();
        for (int i = 1; i <= maxIndex; i++) {
            String index = String.format("%0" + indexPrefix.length() + "d", i);
            Object value = values.get(index);
            if (value != null) {
                result.add(value.toString());
            } else {
                break; // Stop when we find a gap in the sequence
            }
        }
        
        return result;
    }
    
    private List<Pair> getIndexedPairList(String sessionId, long createTimestamp, String baseKey, String indexPrefix, int maxIndex) throws RocksDBException {
        String fullPrefix = buildConversionResultPath(sessionId, createTimestamp, baseKey + SEPARATOR);
        Map<Integer, Pair> indexedPairs = new TreeMap<>();
        
        try (RocksIterator iterator = rocksDB.newIterator(sessionsColumnFamily)) {
            byte[] prefixBytes = fullPrefix.getBytes(StandardCharsets.UTF_8);
            iterator.seek(prefixBytes);
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(fullPrefix)) {
                    break;
                }
                
                // Extract index from key
                String indexPart = key.substring(fullPrefix.length());
                if (indexPart.matches("\\d{" + indexPrefix.length() + "}")) {
                    try {
                        int index = Integer.parseInt(indexPart);
                        if (index <= maxIndex) {
                            String yamlDocument = new String(iterator.value(), StandardCharsets.UTF_8);
                            Pair pair = yamlMapper.readValue(yamlDocument, Pair.class);
                            indexedPairs.put(index, pair);
                        }
                    } catch (JsonProcessingException e) {
                        throw new RocksDBException("Failed to deserialize YAML to Pair: " + e.getMessage());
                    }
                }
                
                iterator.next();
            }
        }
        
        return new ArrayList<>(indexedPairs.values());
    }

    private Map<String, Object> flattenConversionResult(ConversionResult conversionResult) {
        Map<String, Object> flattened = new HashMap<>();
        
        if (conversionResult.getDatabases() != null) {
            for (Map.Entry<String, DBMirror> dbEntry : conversionResult.getDatabases().entrySet()) {
                String database = dbEntry.getKey();
                DBMirror dbMirror = dbEntry.getValue();
                
                if (dbMirror.getTableMirrors() != null) {
                    for (Map.Entry<String, TableMirror> tableEntry : dbMirror.getTableMirrors().entrySet()) {
                        String table = tableEntry.getKey();
                        TableMirror tableMirror = tableEntry.getValue();
                        String tablePrefix = buildTablePath(database, table);
                        
                        // Table-level properties
                        if (tableMirror.getSteps() != null) {
                            flattened.put(tablePrefix + STEPS_KEY, tableMirror.getSteps());
                        }
                        if (tableMirror.getStart() != null) {
                            flattened.put(tablePrefix + START_KEY, tableMirror.getStart());
                        }
                        flattened.put(tablePrefix + REMAPPED_KEY, tableMirror.isReMapped());
                        if (tableMirror.getStrategy() != null) {
                            flattened.put(tablePrefix + STRATEGY_KEY, tableMirror.getStrategy().toString());
                        }
                        flattened.put(tablePrefix + CURRENT_PHASE_KEY, tableMirror.getCurrentPhase());
                        flattened.put(tablePrefix + TOTAL_PHASE_COUNT_KEY, tableMirror.getTotalPhaseCount());
                        if (tableMirror.getStageDuration() != null) {
                            flattened.put(tablePrefix + STAGE_DURATION_KEY, tableMirror.getStageDuration());
                        }
                        
                        // Environment tables
                        for (Environment env : Environment.values()) {
                            EnvironmentTable envTable = tableMirror.getEnvironmentTable(env);
                            if (envTable != null) {
                                String envPrefix = buildEnvironmentPath(database, table, env);
                                flattenEnvironmentTable(flattened, envPrefix, envTable);
                            }
                        }
                    }
                }
            }
        }
        
        return flattened;
    }

    private void flattenEnvironmentTable(Map<String, Object> flattened, String envPrefix, EnvironmentTable envTable) {
        if (envTable.getSql() != null && !envTable.getSql().isEmpty()) {
            for (int i = 0; i < envTable.getSql().size(); i++) {
                String index = String.format("%03d", i + 1);
                flattened.put(envPrefix + SQLS_KEY + SEPARATOR + index, envTable.getSql().get(i));
            }
        }
        
        if (envTable.getCleanUpSql() != null && !envTable.getCleanUpSql().isEmpty()) {
            for (int i = 0; i < envTable.getCleanUpSql().size(); i++) {
                String index = String.format("%03d", i + 1);
                flattened.put(envPrefix + CLEANUP_SQLS_KEY + SEPARATOR + index, envTable.getCleanUpSql().get(i));
            }
        }
        
        flattened.put(envPrefix + EXISTS_KEY, envTable.isExists());
        
        if (envTable.getCreateStrategy() != null) {
            flattened.put(envPrefix + CREATE_STRATEGY_KEY, envTable.getCreateStrategy().toString());
        }
        
        if (envTable.getDefinition() != null && !envTable.getDefinition().isEmpty()) {
            flattened.put(envPrefix + DEFINITION_KEY, String.join("\n", envTable.getDefinition()));
        }
        
        if (envTable.getOwner() != null) {
            flattened.put(envPrefix + OWNER_KEY, envTable.getOwner());
        }
        
        if (envTable.getPartitions() != null) {
            for (Map.Entry<String, String> partition : envTable.getPartitions().entrySet()) {
                flattened.put(envPrefix + PARTITIONS_KEY + SEPARATOR + partition.getKey(), partition.getValue());
            }
        }
        
        if (envTable.getAddProperties() != null) {
            for (Map.Entry<String, String> property : envTable.getAddProperties().entrySet()) {
                flattened.put(envPrefix + ADD_PROPERTIES_KEY + SEPARATOR + property.getKey(), property.getValue());
            }
        }
        
        if (envTable.getStatistics() != null) {
            for (Map.Entry<String, Object> stat : envTable.getStatistics().entrySet()) {
                flattened.put(envPrefix + STATISTICS_KEY + SEPARATOR + stat.getKey(), stat.getValue());
            }
        }
        
        if (envTable.getIssues() != null && !envTable.getIssues().isEmpty()) {
            for (int i = 0; i < envTable.getIssues().size(); i++) {
                String index = String.format("%04d", i + 1);
                flattened.put(envPrefix + ISSUES_KEY + SEPARATOR + index, envTable.getIssues().get(i));
            }
        }
    }

    private ConversionResult unflattenToConversionResult(Map<String, Object> flattenedData) {
        ConversionResult result = new ConversionResult();
        Map<String, DBMirror> databases = new HashMap<>();
        
        
        // Group data by database and table
        Map<String, Map<String, Map<String, Object>>> groupedData = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : flattenedData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            // Keys from getValuesWithPrefix have leading separator removed, so we expect:
            // databases/ext_purge_odd_parts/tables/web_sales/LEFT/exists
            if (key.startsWith(DATABASES_PREFIX.substring(1))) { // Remove leading "/" from check
                String remainder = key.substring(DATABASES_PREFIX.length() - 1); // Adjust offset for missing "/"
                String[] parts = remainder.split(SEPARATOR);
                
                // After splitting "/ext_purge_odd_parts/tables/web_sales/LEFT/exists", we get:
                // ["", "ext_purge_odd_parts", "tables", "web_sales", "LEFT", "exists"]
                // So: parts[1] = database, parts[2] = "tables", parts[3] = table
                if (parts.length >= 4 && parts[2].equals("tables")) { 
                    String database = parts[1];
                    String table = parts[3];
                    System.out.println("Found database: " + database + ", table: " + table);
                    
                    groupedData.computeIfAbsent(database, k -> new HashMap<>())
                            .computeIfAbsent(table, k -> new HashMap<>())
                            .put("/" + key, value); // Restore leading separator for reconstruction methods
                }
            }
        }
        
        System.out.println("GroupedData contains " + groupedData.size() + " databases");
        
        // Reconstruct the ConversionResult
        for (Map.Entry<String, Map<String, Map<String, Object>>> dbEntry : groupedData.entrySet()) {
            String database = dbEntry.getKey();
            DBMirror dbMirror = new DBMirror();
            dbMirror.setName(database);
            Map<String, TableMirror> tableMirrors = new HashMap<>();
            
            for (Map.Entry<String, Map<String, Object>> tableEntry : dbEntry.getValue().entrySet()) {
                String table = tableEntry.getKey();
                TableMirror tableMirror = new TableMirror();
                tableMirror.setName(table);

                // Reconstruct table mirror from flattened data
                reconstructTableMirror(tableMirror, tableEntry.getValue(), database, table);
                tableMirrors.put(table, tableMirror);
            }
            
            dbMirror.setTableMirrors(tableMirrors);
            databases.put(database, dbMirror);
        }
        
        result.setDatabases(databases);
        return result;
    }

    private void reconstructTableMirror(TableMirror tableMirror, Map<String, Object> tableData, String database, String table) {
        String tablePrefix = buildTablePath(database, table);
        
        // Set table-level properties
        for (Map.Entry<String, Object> entry : tableData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if (key.equals(tablePrefix + REMAPPED_KEY)) {
                tableMirror.setReMapped((Boolean) value);
            } else if (key.equals(tablePrefix + CURRENT_PHASE_KEY)) {
                tableMirror.setCurrentPhase(new AtomicInteger((Integer) value));
            } else if (key.equals(tablePrefix + TOTAL_PHASE_COUNT_KEY)) {
                tableMirror.setTotalPhaseCount(new AtomicInteger((Integer) value));
            }
        }
        
        // Reconstruct environment tables
        for (Environment env : Environment.values()) {
            String envPrefix = buildEnvironmentPath(database, table, env);
            Map<String, Object> envData = new HashMap<>();
            
            // Collect all data for this environment
            for (Map.Entry<String, Object> entry : tableData.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(envPrefix)) {
                    envData.put(key, entry.getValue());
                }
            }
            
            if (!envData.isEmpty()) {
                reconstructEnvironmentTable(tableMirror, env, envData, database, table);
            }
        }
    }

    private void reconstructEnvironmentTable(TableMirror tableMirror, Environment env, Map<String, Object> envData, String database, String table) {
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(env);
        if (envTable == null) {
            envTable = new EnvironmentTable(tableMirror);
            tableMirror.getEnvironments().put(env, envTable);
        }
        
        String envPrefix = buildEnvironmentPath(database, table, env);
        
        // Reconstruct environment table from flattened data
        for (Map.Entry<String, Object> entry : envData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if (key.startsWith(envPrefix)) {
                String subKey = key.substring(envPrefix.length());
                
                if (subKey.equals(EXISTS_KEY)) {
                    envTable.setExists((Boolean) value);
                } else if (subKey.equals(OWNER_KEY)) {
                    envTable.setOwner((String) value);
                } else if (subKey.equals(DEFINITION_KEY)) {
                    // Parse the definition back to List<String>
                    try {
                        String yamlDefinition = (String) value;
                        List<String> definition = yamlMapper.readValue(yamlDefinition, new TypeReference<List<String>>() {});
                        envTable.setDefinition(definition);
                    } catch (Exception e) {
                        log.warn("Failed to parse definition for {}/{}/{}: {}", database, table, env, e.getMessage());
                    }
                } else if (subKey.startsWith(PARTITIONS_KEY + SEPARATOR)) {
                    // Handle partitions
                    String partitionKey = subKey.substring((PARTITIONS_KEY + SEPARATOR).length());
                    Map<String, String> partitions = envTable.getPartitions();
                    if (partitions == null) {
                        partitions = new HashMap<>();
                        envTable.setPartitions(partitions);
                    }
                    partitions.put(partitionKey, value.toString());
                }
            }
        }
        
        // Handle indexed SQL lists separately to avoid duplicate processing
        List<Pair> sqls = reconstructIndexedPairList(envData, envPrefix + SQLS_KEY);
        if (!sqls.isEmpty()) {
            envTable.setSql(sqls);
        }
        
        List<Pair> cleanupSqls = reconstructIndexedPairList(envData, envPrefix + CLEANUP_SQLS_KEY);
        if (!cleanupSqls.isEmpty()) {
            envTable.setCleanUpSql(cleanupSqls);
        }
    }

    private List<Pair> reconstructIndexedPairList(Map<String, Object> tableData, String prefix) {
        List<Pair> result = new ArrayList<>();
        Map<Integer, String> indexedValues = new HashMap<>();
        
        // Collect all indexed values for this prefix
        for (Map.Entry<String, Object> entry : tableData.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix + SEPARATOR)) {
                String indexStr = key.substring((prefix + SEPARATOR).length());
                try {
                    int index = Integer.parseInt(indexStr);
                    String yamlContent = (String) entry.getValue();
                    indexedValues.put(index, yamlContent);
                } catch (NumberFormatException e) {
                    // Skip non-numeric indexes
                }
            }
        }
        
        // Reconstruct the list in order
        for (int i = 1; i <= indexedValues.size(); i++) {
            String yamlContent = indexedValues.get(i);
            if (yamlContent != null) {
                try {
                    Pair pair = yamlMapper.readValue(yamlContent, Pair.class);
                    result.add(pair);
                } catch (Exception e) {
                    log.warn("Failed to deserialize Pair from YAML: " + yamlContent, e);
                }
            }
        }
        
        return result;
    }
}