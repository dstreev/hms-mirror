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

package com.cloudera.utils.hms.mirror.web.controller.api.v1;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import com.cloudera.utils.hms.mirror.repository.RunStatusRepository;
import com.cloudera.utils.hms.mirror.service.RocksDBReportGeneratorService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * REST Controller for ConversionResult (Runtime Reports) management.
 * Provides endpoints for listing, viewing, and managing conversion results.
 */
@CrossOrigin
@RestController
@RequestMapping("/api/v1/runtime/reports")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Runtime Reports", description = "Conversion result tracking and management")
@RequiredArgsConstructor
@Slf4j
public class ConversionResultController {

    private final ConversionResultRepository conversionResultRepository;
    private final DBMirrorRepository dbMirrorRepository;
    private final TableMirrorRepository tableMirrorRepository;
    private final RunStatusRepository runStatusRepository;
    private final RocksDBReportGeneratorService reportGeneratorService;

    @GetMapping(produces = "application/json")
    @Operation(summary = "List conversion results",
               description = "Returns paginated list of conversion results, most recent first")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Conversion results retrieved successfully"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve conversion results")
    })
    public ResponseEntity<Map<String, Object>> listConversionResults(
            @Parameter(description = "Page number (0-based)", required = false)
            @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size", required = false)
            @RequestParam(defaultValue = "10") int pageSize) {

        log.info("ConversionResultController.listConversionResults() called - page: {}, pageSize: {}", page, pageSize);

        try {
            // Validate pagination parameters
            if (page < 0) {
                return ResponseEntity.badRequest().body(Map.of(
                    "status", "error",
                    "message", "Page number must be non-negative"
                ));
            }
            if (pageSize < 1 || pageSize > 100) {
                return ResponseEntity.badRequest().body(Map.of(
                    "status", "error",
                    "message", "Page size must be between 1 and 100"
                ));
            }

            List<ConversionResult> allResults = conversionResultRepository.findAllAsList();

            // Sort by created date (most recent first)
            List<ConversionResult> sortedResults = allResults.stream()
                    .sorted((a, b) -> {
                        if (a.getCreated() == null && b.getCreated() == null) return 0;
                        if (a.getCreated() == null) return 1;
                        if (b.getCreated() == null) return -1;
                        return b.getCreated().compareTo(a.getCreated());
                    })
                    .collect(Collectors.toList());

            // Calculate pagination
            int totalCount = sortedResults.size();
            int startIndex = page * pageSize;
            int endIndex = Math.min(startIndex + pageSize, totalCount);

            // Get the page of results
            List<ConversionResult> pageResults = (startIndex < totalCount)
                    ? sortedResults.subList(startIndex, endIndex)
                    : Collections.emptyList();

            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("data", pageResults);
            response.put("page", page);
            response.put("pageSize", pageSize);
            response.put("totalCount", totalCount);
            response.put("totalPages", (int) Math.ceil((double) totalCount / pageSize));
            response.put("hasMore", endIndex < totalCount);

            return ResponseEntity.ok(response);

        } catch (RepositoryException e) {
            log.error("Error listing conversion results", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to retrieve conversion results: " + e.getMessage()
            ));
        }
    }

    @GetMapping(produces = "application/json", params = "key")
    @Operation(summary = "Get conversion result by key",
               description = "Retrieves details of a specific conversion result")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Conversion result retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Conversion result not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve conversion result")
    })
    public ResponseEntity<Map<String, Object>> getConversionResult(
            @Parameter(description = "Conversion result key", required = true)
            @RequestParam String key) {

        log.info("ConversionResultController.getConversionResult() called - key: {}", key);

        try {
            Optional<ConversionResult> resultOpt = conversionResultRepository.findByKey(key);

            if (resultOpt.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "SUCCESS");
                response.put("conversionResult", resultOpt.get());
                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "NOT_FOUND");
                response.put("message", "Conversion result with key '" + key + "' not found");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (RepositoryException e) {
            log.error("Error retrieving conversion result {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to retrieve conversion result: " + e.getMessage()
            ));
        }
    }

    @DeleteMapping(produces = "application/json", params = "key")
    @Operation(summary = "Delete conversion result",
               description = "Deletes a specific conversion result")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Conversion result deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Conversion result not found"),
        @ApiResponse(responseCode = "500", description = "Failed to delete conversion result")
    })
    public ResponseEntity<Map<String, Object>> deleteConversionResult(
            @Parameter(description = "Conversion result key", required = true)
            @RequestParam String key) {

        log.info("ConversionResultController.deleteConversionResult() called - key: {}", key);

        try {
            boolean existed = conversionResultRepository.deleteById(key);

            Map<String, Object> response = new HashMap<>();
            if (existed) {
                response.put("status", "SUCCESS");
                response.put("message", "Conversion result deleted successfully");
                return ResponseEntity.ok(response);
            } else {
                response.put("status", "NOT_FOUND");
                response.put("message", "Conversion result with key '" + key + "' not found");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (RepositoryException e) {
            log.error("Error deleting conversion result {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to delete conversion result: " + e.getMessage()
            ));
        }
    }

    @GetMapping(value = "/files", produces = "application/json")
    @Operation(summary = "List available report files",
               description = "Lists all generated report files for a conversion result")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Report files listed successfully"),
        @ApiResponse(responseCode = "404", description = "Conversion result not found"),
        @ApiResponse(responseCode = "500", description = "Failed to generate reports")
    })
    public ResponseEntity<Map<String, Object>> listReportFiles(
            @Parameter(description = "Conversion result key", required = true)
            @RequestParam String key) {

        log.info("ConversionResultController.listReportFiles() called - key: {}", key);

        try {
            Map<String, String> files = reportGeneratorService.listReportFiles(key);

            if (files.isEmpty()) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "NOT_FOUND");
                response.put("message", "No reports found for key: " + key);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("key", key);
            response.put("fileCount", files.size());
            response.put("files", files.keySet()); // Return list of filenames
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error listing report files for {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to list report files: " + e.getMessage()
            ));
        }
    }

    @GetMapping(value = "/file", produces = {"text/plain", "application/x-yaml"})
    @Operation(summary = "Download a specific report file",
               description = "Downloads a specific generated report file")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Report file retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Report file not found"),
        @ApiResponse(responseCode = "500", description = "Failed to generate report")
    })
    public ResponseEntity<String> downloadReportFile(
            @Parameter(description = "Conversion result key", required = true)
            @RequestParam String key,
            @Parameter(description = "Report filename", required = true)
            @RequestParam String filename) {

        log.info("ConversionResultController.downloadReportFile() called - key: {}, filename: {}", key, filename);

        try {
            java.util.Optional<String> fileContent = reportGeneratorService.getReportFile(key, filename);

            if (fileContent.isEmpty()) {
                return ResponseEntity.notFound().build();
            }

            // Set content type based on file extension
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            if (filename.endsWith(".sql")) {
                headers.setContentType(org.springframework.http.MediaType.TEXT_PLAIN);
            } else if (filename.endsWith(".yaml") || filename.endsWith(".yml")) {
                headers.setContentType(org.springframework.http.MediaType.valueOf("application/x-yaml"));
            } else if (filename.endsWith(".md")) {
                headers.setContentType(org.springframework.http.MediaType.TEXT_MARKDOWN);
            } else {
                headers.setContentType(org.springframework.http.MediaType.TEXT_PLAIN);
            }
            headers.setContentDispositionFormData("attachment", filename);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(fileContent.get());

        } catch (Exception e) {
            log.error("Error downloading report file {} for {}", filename, key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping(value = "/details", produces = "application/json", params = "key")
    @Operation(summary = "Get detailed report information",
               description = "Returns comprehensive report details including databases, tables, and statistics")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Report details retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Report not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve report details")
    })
    public ResponseEntity<Map<String, Object>> getReportDetails(
            @Parameter(description = "Conversion result key", required = true)
            @RequestParam String key) {

        log.info("ConversionResultController.getReportDetails() called - key: {}", key);

        try {
            // Get the conversion result
            Optional<ConversionResult> conversionOpt = conversionResultRepository.findByKey(key);
            if (conversionOpt.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "NOT_FOUND",
                    "message", "Report not found: " + key
                ));
            }

            ConversionResult conversion = conversionOpt.get();

            // Get all databases for this conversion
            Map<String, DBMirror> databases = dbMirrorRepository.findByConversionKey(key);

            // Get run status
            Optional<RunStatus> runStatusOpt = runStatusRepository.findByKey(key);

            // Build table list with aggregated data
            List<Map<String, Object>> tables = new ArrayList<>();
            int totalTables = 0;
            int successfulTables = 0;
            int failedTables = 0;

            for (Map.Entry<String, DBMirror> dbEntry : databases.entrySet()) {
                String dbName = dbEntry.getValue().getName();

                // Get all tables for this database
                Map<String, TableMirror> dbTables = tableMirrorRepository.findByDatabase(key, dbName);

                for (Map.Entry<String, TableMirror> tableEntry : dbTables.entrySet()) {
                    TableMirror table = tableEntry.getValue();
                    totalTables++;

                    // Determine status based on phase state
                    String status;
                    if (table.getPhaseState() == PhaseState.SUCCESS) {
                        status = "completed";
                        successfulTables++;
                    } else if (table.getPhaseState() == PhaseState.ERROR) {
                        status = "failed";
                        failedTables++;
                    } else {
                        status = "partial";
                    }

                    // Build table info
                    Map<String, Object> tableInfo = new HashMap<>();
                    tableInfo.put("name", table.getName());
                    tableInfo.put("database", dbName);
                    tableInfo.put("status", status);
                    tableInfo.put("strategy", table.getStrategy() != null ? table.getStrategy().toString() : "N/A");
                    tableInfo.put("phaseState", table.getPhaseState() != null ? table.getPhaseState().toString() : "UNKNOWN");

                    // Get issues and errors from LEFT environment
                    List<String> issues = table.getIssues(Environment.LEFT);
                    List<String> errors = table.getErrors(Environment.LEFT);
                    tableInfo.put("issues", issues != null ? issues : new ArrayList<String>());
                    tableInfo.put("errors", errors != null ? errors : new ArrayList<String>());

                    // Check if table has LEFT and RIGHT environments
                    tableInfo.put("hasLeft", table.getEnvironments().containsKey(Environment.LEFT));
                    tableInfo.put("hasRight", table.getEnvironments().containsKey(Environment.RIGHT));

                    tables.add(tableInfo);
                }
            }

            // Build summary
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalTables", totalTables);
            summary.put("successfulTables", successfulTables);
            summary.put("failedTables", failedTables);
            summary.put("databaseCount", databases.size());

            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("key", key);
            response.put("name", conversion.getConfig() != null ? conversion.getConfig().getName() : "Migration Report");
            response.put("timestamp", conversion.getCreated() != null ? conversion.getCreated().toString() : null);
            response.put("config", conversion.getConfig());
            response.put("dataset", conversion.getDataset());
            response.put("summary", summary);
            response.put("tables", tables);
            response.put("databases", databases.values().stream().map(db -> {
                Map<String, Object> dbInfo = new HashMap<>();
                dbInfo.put("name", db.getName());
                return dbInfo;
            }).collect(Collectors.toList()));

            // Add strategy from job if available
            if (conversion.getJob() != null && conversion.getJob().getStrategy() != null) {
                response.put("strategy", conversion.getJob().getStrategy().toString());
            }

            // Add run status if available
            if (runStatusOpt.isPresent()) {
                RunStatus runStatus = runStatusOpt.get();
                Map<String, Object> runStatusMap = new HashMap<>();
                runStatusMap.put("start", runStatus.getStart());
                runStatusMap.put("end", runStatus.getEnd());
                runStatusMap.put("comment", runStatus.getComment());
                runStatusMap.put("progress", runStatus.getProgress());
                runStatusMap.put("errorMessages", runStatus.getErrorMessages());
                runStatusMap.put("warningMessages", runStatus.getWarningMessages());
                runStatusMap.put("configMessages", runStatus.getConfigMessages());
                runStatusMap.put("operationStatistics", runStatus.getOperationStatistics());
                runStatusMap.put("stages", runStatus.getStages());

                // Calculate duration
                if (runStatus.getStart() != null && runStatus.getEnd() != null) {
                    runStatusMap.put("duration", runStatus.getDuration());
                }

                response.put("runStatus", runStatusMap);
            }

            return ResponseEntity.ok(response);

        } catch (RepositoryException e) {
            log.error("Error retrieving report details for {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "ERROR",
                "message", "Failed to retrieve report details: " + e.getMessage()
            ));
        }
    }

    @GetMapping(value = "/table-details", produces = "application/json")
    @Operation(summary = "Get detailed table information",
               description = "Returns comprehensive details for a specific table")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Table details retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Table not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve table details")
    })
    public ResponseEntity<Map<String, Object>> getTableDetails(
            @Parameter(description = "Conversion result key", required = true)
            @RequestParam String key,
            @Parameter(description = "Database name", required = true)
            @RequestParam String database,
            @Parameter(description = "Table name", required = true)
            @RequestParam String table,
            @Parameter(description = "Environment (LEFT or RIGHT)", required = true)
            @RequestParam String environment) {

        log.info("ConversionResultController.getTableDetails() called - key: {}, database: {}, table: {}, environment: {}",
                key, database, table, environment);

        try {
            // Get the table
            Optional<TableMirror> tableOpt = tableMirrorRepository.findByName(key, database, table);

            if (tableOpt.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "NOT_FOUND",
                    "message", "Table not found: " + database + "." + table
                ));
            }

            TableMirror tableMirror = tableOpt.get();
            Environment env = Environment.valueOf(environment);

            // Build table details response
            Map<String, Object> details = new HashMap<>();
            details.put("tableName", tableMirror.getName());
            details.put("environment", environment);
            details.put("name", tableMirror.getName(env));
            details.put("exists", tableMirror.getEnvironmentTable(env) != null);
            details.put("owner", "N/A"); // TODO: Add owner field if available
            details.put("strategy", tableMirror.getStrategy() != null ? tableMirror.getStrategy().toString() : "N/A");
            details.put("phaseState", tableMirror.getPhaseState() != null ? tableMirror.getPhaseState().toString() : "UNKNOWN");

            // Get issues, errors, SQL, and definition from the environment
            details.put("issues", tableMirror.getIssues(env));
            details.put("errors", tableMirror.getErrors(env));
            details.put("definition", tableMirror.getTableDefinition(env));
            details.put("sql", tableMirror.getSql(env));
            details.put("addProperties", tableMirror.getPropAdd(env));

            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("data", details);

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                "status", "ERROR",
                "message", "Invalid environment: " + environment
            ));
        } catch (RepositoryException e) {
            log.error("Error retrieving table details", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "ERROR",
                "message", "Failed to retrieve table details: " + e.getMessage()
            ));
        }
    }
}
