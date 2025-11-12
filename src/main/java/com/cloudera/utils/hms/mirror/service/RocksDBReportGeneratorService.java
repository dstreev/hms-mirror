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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.domain.testdata.LegacyDBMirror;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
import com.cloudera.utils.hms.mirror.repository.RunStatusRepository;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Service for generating reports from RocksDB data.
 * This service reads ConversionResult, RunStatus, DBMirror, and TableMirror objects
 * from RocksDB and generates the same YAML reports that the CLI produces.
 */
@Service
@Slf4j
@Getter
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
public class RocksDBReportGeneratorService {

    @NonNull
    private final ConversionResultRepository conversionResultRepository;
    @NonNull
    private final RunStatusRepository runStatusRepository;
    @NonNull
    private final DBMirrorRepository dbMirrorRepository;
    @NonNull
    private final TableMirrorRepository tableMirrorRepository;
    @NonNull
    private final ObjectMapper yamlMapper;
    @NonNull
    private final DatabaseService databaseService;
    @NonNull
    private final ConversionResultService conversionResultService;

    public RocksDBReportGeneratorService(
            @NonNull ConversionResultRepository conversionResultRepository,
            @NonNull RunStatusRepository runStatusRepository,
            @NonNull DBMirrorRepository dbMirrorRepository,
            @NonNull TableMirrorRepository tableMirrorRepository,
            @NonNull @Qualifier("yamlMapper") ObjectMapper yamlMapper,
            @NonNull DatabaseService databaseService,
            @NonNull ConversionResultService conversionResultService) {
        this.conversionResultRepository = conversionResultRepository;
        this.runStatusRepository = runStatusRepository;
        this.dbMirrorRepository = dbMirrorRepository;
        this.tableMirrorRepository = tableMirrorRepository;
        this.yamlMapper = yamlMapper;
        this.databaseService = databaseService;
        this.conversionResultService = conversionResultService;
    }

    /**
     * Container for all generated report files
     */
    @Getter
    public static class GeneratedReports {
        private final Map<String, String> files = new HashMap<>();
        private String conversionKey;

        public void addFile(String filename, String content) {
            files.put(filename, content);
        }

        public void setConversionKey(String key) {
            this.conversionKey = key;
        }
    }

    /**
     * Generate all reports for a given conversion result key.
     *
     * @param conversionKey The ConversionResult key
     * @return GeneratedReports containing all report files
     * @throws RepositoryException if data cannot be retrieved from RocksDB
     */
    public GeneratedReports generateReports(String conversionKey) throws RepositoryException {
        log.info("Generating reports for conversion key: {}", conversionKey);

        GeneratedReports reports = new GeneratedReports();
        reports.setConversionKey(conversionKey);

        // Load ConversionResult
        Optional<ConversionResult> conversionResultOpt = conversionResultRepository.findByKey(conversionKey);
        if (conversionResultOpt.isEmpty()) {
            throw new RepositoryException("ConversionResult not found for key: " + conversionKey);
        }
        ConversionResult conversionResult = conversionResultOpt.get();

        // Generate session-config.yaml
        String sessionConfig = generateSessionConfig(conversionResult);
        reports.addFile("session-config.yaml", sessionConfig);

        // Generate session-translator.yaml if translator exists
        if (conversionResult.getTranslator() != null) {
            String translator = generateTranslator(conversionResult);
            reports.addFile("session-translator.yaml", translator);
        }

        // Load and generate run-status.yaml
        Optional<RunStatus> runStatusOpt = runStatusRepository.findByKey(conversionKey);
        if (runStatusOpt.isPresent()) {
            String runStatus = generateRunStatus(runStatusOpt.get());
            reports.addFile("run-status.yaml", runStatus);
        } else {
            log.warn("RunStatus not found for key: {}", conversionKey);
        }

        // Load databases and generate database reports
        Map<String, DBMirror> databases = dbMirrorRepository.findByConversionKey(conversionKey);
        log.info("Found {} databases for conversion key: {}", databases.size(), conversionKey);

        databases.forEach((databaseName, dbMirror) -> {
            try {
                generateDatabaseReports(reports, conversionKey, databaseName, dbMirror, conversionResult);
            } catch (Exception e) {
                log.error("Error generating reports for database: {}", databaseName, e);
            }
        });

        log.info("Generated {} report files for conversion key: {}", reports.getFiles().size(), conversionKey);
        return reports;
    }

    /**
     * Generate session-config.yaml content
     */
    private String generateSessionConfig(ConversionResult conversionResult) {
        try {
            String yamlStr = yamlMapper.writeValueAsString(conversionResult);
            // Mask User/Passwords in Control File
            yamlStr = yamlStr.replaceAll("user:\\s\".*\"", "user: \"*****\"");
            yamlStr = yamlStr.replaceAll("password:\\s\".*\"", "password: \"*****\"");
            return yamlStr;
        } catch (Exception e) {
            log.error("Error generating session config YAML", e);
            return "# Error generating session config: " + e.getMessage();
        }
    }

    /**
     * Generate session-translator.yaml content
     */
    private String generateTranslator(ConversionResult conversionResult) {
        try {
            String yamlStr = yamlMapper.writeValueAsString(conversionResult.getTranslator());
            // Mask User/Passwords in Control File
            yamlStr = yamlStr.replaceAll("user:\\s\".*\"", "user: \"*****\"");
            yamlStr = yamlStr.replaceAll("password:\\s\".*\"", "password: \"*****\"");
            return yamlStr;
        } catch (Exception e) {
            log.error("Error generating translator YAML", e);
            return "# Error generating translator: " + e.getMessage();
        }
    }

    /**
     * Generate run-status.yaml content
     */
    private String generateRunStatus(RunStatus runStatus) {
        try {
            return yamlMapper.writeValueAsString(runStatus);
        } catch (Exception e) {
            log.error("Error generating run status YAML", e);
            return "# Error generating run status: " + e.getMessage();
        }
    }

    /**
     * Generate database-specific reports
     */
    private void generateDatabaseReports(GeneratedReports reports, String conversionKey,
                                          String databaseName, DBMirror dbMirror,
                                          ConversionResult conversionResult) throws RepositoryException {
        log.debug("Generating reports for database: {}", databaseName);

        // Create LegacyDBMirror from DBMirror
        LegacyDBMirror legacyDBMirror = LegacyDBMirror.fromDBMirror(dbMirror);

        // Load TableMirror objects for this database
        Map<String, TableMirror> tableMirrors = tableMirrorRepository.findByDatabase(conversionKey, databaseName);
        legacyDBMirror.setTableMirrors(tableMirrors);

        log.debug("Loaded {} tables for database: {}", tableMirrors.size(), databaseName);

        // Calculate statistics
        Map<String, Number> leftSummaryStats = databaseService.getEnvironmentSummaryStatistics(dbMirror, Environment.LEFT);
        dbMirror.getEnvironmentStatistics().put(Environment.LEFT, leftSummaryStats);

        // Generate database YAML file
        String dbYamlFilename = databaseName + "_hms-mirror.yaml";
        String dbYaml = generateDatabaseYaml(legacyDBMirror);
        reports.addFile(dbYamlFilename, dbYaml);

        // Generate SQL execution files
        generateSQLFiles(reports, databaseName, conversionKey, conversionResult);
    }

    /**
     * Generate database YAML content
     */
    private String generateDatabaseYaml(LegacyDBMirror legacyDBMirror) {
        try {
            return yamlMapper.writeValueAsString(legacyDBMirror);
        } catch (Exception e) {
            log.error("Error generating database YAML for: {}", legacyDBMirror.getName(), e);
            return "# Error generating database YAML: " + e.getMessage();
        }
    }

    /**
     * Generate SQL execution files
     */
    private void generateSQLFiles(GeneratedReports reports, String databaseName, String conversionKey, ConversionResult conversionResult) {
        // LEFT execute SQL
        String leftExecute = conversionResultService.executeSql(conversionResult, Environment.LEFT, databaseName);
        if (leftExecute != null && !leftExecute.trim().isEmpty()) {
            reports.addFile(databaseName + "_LEFT_execute.sql", leftExecute);
        }

        // LEFT cleanup SQL
        String leftCleanup = conversionResultService.executeCleanUpSql(conversionResult, Environment.LEFT, databaseName);
        if (leftCleanup != null && !leftCleanup.trim().isEmpty()) {
            reports.addFile(databaseName + "_LEFT_CleanUp_execute.sql", leftCleanup);
        }

        // RIGHT execute SQL
        String rightExecute = conversionResultService.executeSql(conversionResult, Environment.RIGHT, databaseName);
        if (rightExecute != null && !rightExecute.trim().isEmpty()) {
            reports.addFile(databaseName + "_RIGHT_execute.sql", rightExecute);
        }

        // RIGHT cleanup SQL
        String rightCleanup = conversionResultService.executeCleanUpSql(conversionResult, Environment.RIGHT, databaseName);
        if (rightCleanup != null && !rightCleanup.trim().isEmpty()) {
            reports.addFile(databaseName + "_RIGHT_CleanUp_execute.sql", rightCleanup);
        }
    }

    /**
     * Get a specific report file
     */
    public Optional<String> getReportFile(String conversionKey, String filename) {
        try {
            GeneratedReports reports = generateReports(conversionKey);
            return Optional.ofNullable(reports.getFiles().get(filename));
        } catch (RepositoryException e) {
            log.error("Error generating report file: {} for key: {}", filename, conversionKey, e);
            return Optional.empty();
        }
    }

    /**
     * List all available report files for a conversion key
     */
    public Map<String, String> listReportFiles(String conversionKey) {
        try {
            GeneratedReports reports = generateReports(conversionKey);
            return reports.getFiles();
        } catch (RepositoryException e) {
            log.error("Error listing report files for key: {}", conversionKey, e);
            return new HashMap<>();
        }
    }
}
