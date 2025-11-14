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
import com.cloudera.utils.hms.mirror.domain.core.Translator;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Service for importing filesystem-based reports into RocksDB.
 * This service scans the $HOME/.hms-mirror/reports directory for report directories
 * containing session-config.yaml and run-status.yaml files and imports them into RocksDB.
 */
@Service
@Slf4j
@Getter
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
public class ReportImportService {

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
    private final ExecuteSessionService executeSessionService;

    public ReportImportService(
            @NonNull ConversionResultRepository conversionResultRepository,
            @NonNull RunStatusRepository runStatusRepository,
            @NonNull DBMirrorRepository dbMirrorRepository,
            @NonNull TableMirrorRepository tableMirrorRepository,
            @NonNull @Qualifier("yamlMapper") ObjectMapper yamlMapper,
            @NonNull ExecuteSessionService executeSessionService) {
        this.conversionResultRepository = conversionResultRepository;
        this.runStatusRepository = runStatusRepository;
        this.dbMirrorRepository = dbMirrorRepository;
        this.tableMirrorRepository = tableMirrorRepository;
        this.yamlMapper = yamlMapper;
        this.executeSessionService = executeSessionService;
    }

    /**
     * Result of an import operation
     */
    @Getter
    public static class ImportResult {
        private int scanned = 0;
        private int skipped = 0;
        private int imported = 0;
        private int failed = 0;
        private final List<String> errors = new ArrayList<>();
        private final List<String> importedPaths = new ArrayList<>();
        private final List<String> skippedPaths = new ArrayList<>();

        public void incrementScanned() {
            scanned++;
        }

        public void incrementSkipped(String path) {
            skipped++;
            skippedPaths.add(path);
        }

        public void incrementImported(String path) {
            imported++;
            importedPaths.add(path);
        }

        public void incrementFailed(String path, String error) {
            failed++;
            errors.add(path + ": " + error);
        }
    }

    /**
     * Get the reports directory path
     */
    private String getReportsDirectory() {
        // Use the same directory as report writer
        return executeSessionService.getReportOutputDirectory();
    }

    /**
     * Import all reports from the filesystem into RocksDB.
     * Scans recursively for directories containing session-config.yaml and run-status.yaml files.
     *
     * @return ImportResult containing statistics about the import operation
     */
    public ImportResult importAllReports() {
        ImportResult result = new ImportResult();
        String reportsDir = getReportsDirectory();

        log.info("Starting report import from directory: {}", reportsDir);

        Path reportsPath = Paths.get(reportsDir);
        if (!Files.exists(reportsPath)) {
            log.warn("Reports directory does not exist: {}", reportsDir);
            return result;
        }

        try {
            scanAndImport(reportsPath, "", result);
        } catch (Exception e) {
            log.error("Error during report import", e);
            result.incrementFailed(reportsDir, e.getMessage());
        }

        log.info("Report import complete. Scanned: {}, Imported: {}, Skipped: {}, Failed: {}",
                result.getScanned(), result.getImported(), result.getSkipped(), result.getFailed());

        return result;
    }

    /**
     * Recursively scan directories for reports to import
     */
    private void scanAndImport(Path basePath, String relativePath, ImportResult result) throws IOException {
        try (Stream<Path> paths = Files.list(basePath)) {
            paths.filter(Files::isDirectory).forEach(dir -> {
                try {
                    String dirName = dir.getFileName().toString();
                    String newRelativePath = relativePath.isEmpty() ? dirName : relativePath + "/" + dirName;

                    // Check if this directory contains report files
                    Path sessionConfigPath = dir.resolve("session-config.yaml");
                    Path runStatusPath = dir.resolve("run-status.yaml");

                    if (Files.exists(sessionConfigPath) && Files.exists(runStatusPath)) {
                        // This is a report directory
                        result.incrementScanned();
                        importReport(newRelativePath, sessionConfigPath, runStatusPath, result);
                    } else {
                        // Continue scanning subdirectories
                        scanAndImport(dir, newRelativePath, result);
                    }
                } catch (Exception e) {
                    log.error("Error scanning directory: {}", dir, e);
                    result.incrementFailed(dir.toString(), e.getMessage());
                }
            });
        }
    }

    /**
     * Import a single report from the filesystem
     *
     * @param key               The key to use (relative path from reports directory)
     * @param sessionConfigPath Path to session-config.yaml
     * @param runStatusPath     Path to run-status.yaml
     * @param result            Result object to update
     */
    private void importReport(String key, Path sessionConfigPath, Path runStatusPath, ImportResult result) {
        try {
            // Check if already imported
            if (conversionResultRepository.findByKey(key).isPresent()) {
                log.debug("Report already imported, skipping: {}", key);
                result.incrementSkipped(key);
                return;
            }

            log.info("Importing report: {}", key);

            // Read and deserialize session-config.yaml
            ConversionResult conversionResult = yamlMapper.readValue(
                    sessionConfigPath.toFile(),
                    ConversionResult.class
            );

            // Set the key from the relative path
            conversionResult.setKey(key);
            conversionResult.getJob().setKey(key);
            conversionResult.getJob().setName(key);

            // Import session-translator.yaml if it exists
            Path translatorPath = sessionConfigPath.getParent().resolve("session-translator.yaml");
            if (Files.exists(translatorPath)) {
                log.debug("Found session-translator.yaml, importing...");
                try {
                    Translator translator = yamlMapper.readValue(
                            translatorPath.toFile(),
                            Translator.class
                    );
                    conversionResult.setTranslator(translator);
                    log.debug("Successfully imported translator");
                } catch (Exception e) {
                    log.warn("Error reading session-translator.yaml: {}", e.getMessage());
                }
            }

            // Read and deserialize run-status.yaml
            RunStatus runStatus = yamlMapper.readValue(
                    runStatusPath.toFile(),
                    RunStatus.class
            );

            // Save ConversionResult and RunStatus to RocksDB
            // Note: RunStatusRepository.save(key, runStatus) automatically handles the key mapping
            conversionResultRepository.save(conversionResult);
            runStatusRepository.save(key, runStatus);

            // Import database report files (*_hms-mirror.yaml)
            importDatabaseReports(key, sessionConfigPath.getParent());

            log.info("Successfully imported report: {}", key);
            result.incrementImported(key);

        } catch (RepositoryException e) {
            log.error("Repository error importing report: {}", key, e);
            result.incrementFailed(key, "Repository error: " + e.getMessage());
        } catch (IOException e) {
            log.error("IO error reading report files: {}", key, e);
            result.incrementFailed(key, "IO error: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error importing report: {}", key, e);
            result.incrementFailed(key, "Unexpected error: " + e.getMessage());
        }
    }

    /**
     * Import database report files (*_hms-mirror.yaml)
     *
     * @param conversionKey The conversion result key
     * @param reportDir     The report directory containing database YAML files
     */
    private void importDatabaseReports(String conversionKey, Path reportDir) throws IOException, RepositoryException {
        // Find all *_hms-mirror.yaml files
        List<Path> databaseReportFiles;
        try (Stream<Path> paths = Files.list(reportDir)) {
            databaseReportFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith("_hms-mirror.yaml"))
                    .collect(Collectors.toList());
        }

        if (databaseReportFiles.isEmpty()) {
            log.debug("No database report files found in: {}", reportDir);
            return;
        }

        log.info("Found {} database report files to import", databaseReportFiles.size());

        for (Path dbReportFile : databaseReportFiles) {
            try {
                importDatabaseReport(conversionKey, dbReportFile);
            } catch (Exception e) {
                log.error("Error importing database report: {}", dbReportFile, e);
                // Continue with next file
            }
        }
    }

    /**
     * Import a single database report file
     *
     * @param conversionKey  The conversion result key
     * @param dbReportFile   Path to the database YAML file
     */
    private void importDatabaseReport(String conversionKey, Path dbReportFile) throws IOException, RepositoryException {
        String filename = dbReportFile.getFileName().toString();
        log.debug("Importing database report: {}", filename);

        // Read the legacy DBMirror format
        LegacyDBMirror legacyDBMirror = yamlMapper.readValue(
                dbReportFile.toFile(),
                LegacyDBMirror.class
        );

        // Create a new DBMirror from the legacy data
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName(legacyDBMirror.getName());

        // Copy properties from legacy format
        if (legacyDBMirror.getProperties() != null) {
            dbMirror.setProperties(legacyDBMirror.getProperties());
        }

        // Save DBMirror (repository will build the composite key using conversionKey + database name)
        dbMirrorRepository.save(conversionKey, dbMirror);
        log.debug("Saved DBMirror: {}", dbMirror.getName());

        // Extract and save TableMirror objects
        Map<String, TableMirror> tableMirrors = legacyDBMirror.getTableMirrors();
        if (tableMirrors != null && !tableMirrors.isEmpty()) {
            for (Map.Entry<String, TableMirror> entry : tableMirrors.entrySet()) {
                TableMirror tableMirror = entry.getValue();
                // Save the table (repository will build the composite key using conversionKey + database name + table name)
                tableMirrorRepository.save(conversionKey, dbMirror.getName(), tableMirror);
            }
            log.debug("Saved {} tables for database: {}", tableMirrors.size(), dbMirror.getName());
        }
    }

    /**
     * Import a specific report by its relative path
     *
     * @param relativePath The relative path from the reports directory
     * @return true if successfully imported, false otherwise
     */
    public boolean importReport(String relativePath) {
        ImportResult result = new ImportResult();
        String reportsDir = getReportsDirectory();
        Path reportDir = Paths.get(reportsDir, relativePath);

        if (!Files.exists(reportDir)) {
            log.warn("Report directory does not exist: {}", reportDir);
            return false;
        }

        Path sessionConfigPath = reportDir.resolve("session-config.yaml");
        Path runStatusPath = reportDir.resolve("run-status.yaml");

        if (!Files.exists(sessionConfigPath) || !Files.exists(runStatusPath)) {
            log.warn("Required files not found in directory: {}", reportDir);
            return false;
        }

        result.incrementScanned();
        importReport(relativePath, sessionConfigPath, runStatusPath, result);

        return result.getImported() > 0;
    }
}
