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

package com.cloudera.utils.hms.mirror.web.controller;

import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/reports")
@Slf4j
public class ReportsController {

    @Value("${hms.mirror.reports.dir:${user.home}/.hms-mirror/reports}")
    private String reportsBaseDir;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping(value = "/browse")
    public ResponseEntity<String> browseReports(@RequestParam(value = "path", required = false) String path) {
        try {
            String currentPath = path != null ? path : "";
            Path reportsPath = Paths.get(reportsBaseDir, currentPath).normalize();
            
            log.info("Browsing reports directory: {}", reportsPath);
            
            if (!Files.exists(reportsPath)) {
                Map<String, Object> response = Map.of(
                    "currentPath", currentPath,
                    "directories", Collections.emptyList(),
                    "reports", Collections.emptyList(),
                    "message", "Reports directory does not exist: " + reportsPath.toString()
                );
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                return ResponseEntity.ok()
                        .headers(headers)
                        .body(objectMapper.writeValueAsString(response));
            }
            
            List<Map<String, Object>> directories = new ArrayList<>();
            List<Map<String, Object>> reports = new ArrayList<>();
            
            try (var stream = Files.list(reportsPath)) {
                stream.forEach(entry -> {
                    if (Files.isDirectory(entry)) {
                        String dirName = entry.getFileName().toString();
                        if (!dirName.startsWith(".")) { // Skip hidden directories
                            // Check if this directory contains execution reports
                            if (isExecutionDirectory(entry)) {
                                // This is an execution directory - create a report card
                                try {
                                    Map<String, Object> report = createReportCard(entry, currentPath);
                                    if (report != null) {
                                        reports.add(report);
                                    }
                                } catch (Exception e) {
                                    log.warn("Failed to create report card for: {}", entry, e);
                                }
                            } else {
                                // This is a navigation directory
                                directories.add(Map.of(
                                    "name", dirName,
                                    "path", currentPath.isEmpty() ? dirName : currentPath + "/" + dirName
                                ));
                            }
                        }
                    }
                });
            }
            
            // Sort directories and reports
            directories.sort(Comparator.comparing(d -> (String) d.get("name")));
            reports.sort(Comparator.comparing(r -> (String) r.get("timestamp"), Comparator.reverseOrder()));
            
            Map<String, Object> response = Map.of(
                "currentPath", currentPath,
                "directories", directories,
                "reports", reports
            );
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(objectMapper.writeValueAsString(response));
            
        } catch (Exception e) {
            log.error("Failed to browse reports directory", e);
            try {
                return ResponseEntity.internalServerError().body(objectMapper.writeValueAsString(Map.of("error", "Failed to browse reports")));
            } catch (JsonProcessingException ex) {
                return ResponseEntity.internalServerError().body("{\"error\":\"Failed to browse reports\"}");
            }
        }
    }
    
    @GetMapping(value = "/details")
    public ResponseEntity<String> getReportDetails(@RequestParam(value = "path") String path) {
        try {
            Path reportPath = Paths.get(reportsBaseDir, path).normalize();
            
            log.info("Getting report details for: {}", reportPath);
            
            if (!Files.exists(reportPath) || !Files.isDirectory(reportPath)) {
                return ResponseEntity.notFound().build();
            }
            
            Map<String, Object> details = createDetailedReport(reportPath, path);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(objectMapper.writeValueAsString(details));
            
        } catch (Exception e) {
            log.error("Failed to get report details for path: {}", path, e);
            try {
                return ResponseEntity.internalServerError().body(objectMapper.writeValueAsString(Map.of("error", "Failed to get report details")));
            } catch (JsonProcessingException ex) {
                return ResponseEntity.internalServerError().body("{\"error\":\"Failed to get report details\"}");
            }
        }
    }
    
    @GetMapping(value = "/artifacts")
    public ResponseEntity<String> getArtifacts(@RequestParam(value = "path") String path) {
        try {
            Path reportPath = Paths.get(reportsBaseDir, path).normalize();
            
            if (!Files.exists(reportPath) || !Files.isDirectory(reportPath)) {
                return ResponseEntity.notFound().build();
            }
            
            List<Map<String, Object>> artifacts = new ArrayList<>();
            
            try (var stream = Files.list(reportPath)) {
                stream.filter(Files::isRegularFile)
                      .forEach(file -> {
                          String fileName = file.getFileName().toString();
                          if (!fileName.startsWith(".")) {
                              try {
                                  artifacts.add(Map.of(
                                      "name", fileName,
                                      "size", formatFileSize(Files.size(file)),
                                      "path", path + "/" + fileName,
                                      "type", getFileType(fileName),
                                      "description", getFileDescription(fileName)
                                  ));
                              } catch (IOException e) {
                                  log.warn("Failed to get file info for: {}", file, e);
                              }
                          }
                      });
            }
            
            // Sort artifacts by type and name
            artifacts.sort((a, b) -> {
                String typeA = (String) a.get("type");
                String typeB = (String) b.get("type");
                int typeCompare = typeA.compareTo(typeB);
                if (typeCompare != 0) return typeCompare;
                return ((String) a.get("name")).compareTo((String) b.get("name"));
            });
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(objectMapper.writeValueAsString(artifacts));
            
        } catch (Exception e) {
            log.error("Failed to get artifacts for path: {}", path, e);
            try {
                return ResponseEntity.internalServerError().body(objectMapper.writeValueAsString(Map.of("error", "Failed to get artifacts")));
            } catch (JsonProcessingException ex) {
                return ResponseEntity.internalServerError().body("{\"error\":\"Failed to get artifacts\"}");
            }
        }
    }
    
    @GetMapping(value = "/table-details")
    public ResponseEntity<String> getTableDetails(@RequestParam(value = "path") String path, 
                                                   @RequestParam(value = "table") String tableName,
                                                   @RequestParam(value = "environment") String environment) {
        try {
            Path reportPath = Paths.get(reportsBaseDir, path).normalize();
            
            if (!Files.exists(reportPath) || !Files.isDirectory(reportPath)) {
                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    return ResponseEntity.status(404)
                            .headers(headers)
                            .body(objectMapper.writeValueAsString(Map.of("error", "Report path not found")));
                } catch (JsonProcessingException ex) {
                    return ResponseEntity.status(404).body("{\"error\":\"Report path not found\"}");
                }
            }
            
            Map<String, Object> tableDetails = getTableEnvironmentDetails(reportPath, tableName, environment);
            if (tableDetails.isEmpty()) {
                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    return ResponseEntity.status(404)
                            .headers(headers)
                            .body(objectMapper.writeValueAsString(Map.of("error", "Table details not found")));
                } catch (JsonProcessingException ex) {
                    return ResponseEntity.status(404).body("{\"error\":\"Table details not found\"}");
                }
            }
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(objectMapper.writeValueAsString(tableDetails));
            
        } catch (Exception e) {
            log.error("Failed to get table details for path: {}, table: {}, environment: {}", path, tableName, environment, e);
            try {
                return ResponseEntity.internalServerError().body(objectMapper.writeValueAsString(Map.of("error", "Failed to get table details")));
            } catch (JsonProcessingException ex) {
                return ResponseEntity.internalServerError().body("{\"error\":\"Failed to get table details\"}");
            }
        }
    }
    
    @GetMapping("/artifacts/download")
    public ResponseEntity<org.springframework.core.io.Resource> downloadArtifact(@RequestParam(value = "path") String path) {
        try {
            Path filePath = Paths.get(reportsBaseDir, path).normalize();
            
            // Security check - ensure the file is within the reports directory
            if (!filePath.startsWith(Paths.get(reportsBaseDir).normalize())) {
                log.warn("Attempted to access file outside reports directory: {}", filePath);
                return ResponseEntity.badRequest().build();
            }
            
            if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
                return ResponseEntity.notFound().build();
            }
            
            org.springframework.core.io.Resource resource = new org.springframework.core.io.FileSystemResource(filePath.toFile());
            
            String contentType = Files.probeContentType(filePath);
            if (contentType == null) {
                contentType = "application/octet-stream";
            }
            
            return ResponseEntity.ok()
                    .contentType(org.springframework.http.MediaType.parseMediaType(contentType))
                    .header(org.springframework.http.HttpHeaders.CONTENT_DISPOSITION, 
                           "attachment; filename=\"" + filePath.getFileName().toString() + "\"")
                    .body(resource);
            
        } catch (Exception e) {
            log.error("Failed to download artifact for path: {}", path, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    private boolean isExecutionDirectory(Path directory) {
        // Check if directory contains execution output files
        try (var stream = Files.list(directory)) {
            return stream.anyMatch(file -> {
                String fileName = file.getFileName().toString();
                return fileName.equals("run-status.yaml") || 
                       fileName.equals("session-config.yaml") ||
                       fileName.endsWith("hms-mirror.yaml") ||
                       fileName.endsWith("hms-mirror.html");
            });
        } catch (IOException e) {
            return false;
        }
    }
    
    private Map<String, Object> createReportCard(Path executionDir, String basePath) {
        try {
            String dirName = executionDir.getFileName().toString();
            String fullPath = basePath.isEmpty() ? dirName : basePath + "/" + dirName;
            
            // Try to parse execution timestamp from directory name
            String timestamp = parseTimestamp(dirName);
            
            // Read run status if available
            Map<String, Object> runStatus = readYamlFile(executionDir.resolve("run-status.yaml"));
            Map<String, Object> sessionConfig = readYamlFile(executionDir.resolve("session-config.yaml"));
            
            // Build report card
            Map<String, Object> report = new HashMap<>();
            report.put("id", fullPath.replace("/", "_"));
            report.put("name", generateReportName(dirName, sessionConfig));
            report.put("path", fullPath);
            report.put("timestamp", timestamp);
            report.put("status", determineStatus(runStatus));
            
            // Extract summary information (single database focus)
            Map<String, Object> summary = extractSummary(runStatus, sessionConfig, executionDir);
            report.put("summary", summary);
            
            // Get database name from the main report file
            String databaseName = extractDatabaseName(executionDir);
            report.put("database", databaseName);
            
            // Check available artifacts
            Map<String, Boolean> artifacts = checkArtifacts(executionDir);
            report.put("artifacts", artifacts);
            
            return report;
            
        } catch (Exception e) {
            log.warn("Failed to create report card for: {}", executionDir, e);
            return null;
        }
    }
    
    private Map<String, Object> createDetailedReport(Path executionDir, String path) {
        Map<String, Object> details = new HashMap<>();
        
        try {
            String dirName = executionDir.getFileName().toString();
            
            // Read all relevant files
            Map<String, Object> runStatus = readYamlFile(executionDir.resolve("run-status.yaml"));
            Map<String, Object> sessionConfig = readYamlFile(executionDir.resolve("session-config.yaml"));
            
            details.put("id", path.replace("/", "_"));
            details.put("name", generateReportName(dirName, sessionConfig));
            details.put("path", path);
            details.put("timestamp", parseTimestamp(dirName));
            details.put("status", determineStatus(runStatus));
            details.put("summary", extractSummary(runStatus, sessionConfig, executionDir));
            details.put("database", extractDatabaseName(executionDir));
            details.put("config", sessionConfig);
            details.put("runStatus", runStatus);
            
            // Extract table-level details from the main database report file
            details.put("tables", extractTableDetails(executionDir));
            
            return details;
            
        } catch (Exception e) {
            log.warn("Failed to create detailed report for: {}", executionDir, e);
            return details;
        }
    }
    
    private Map<String, Object> readYamlFile(Path yamlFile) {
        try {
            if (!Files.exists(yamlFile)) {
                return null;
            }
            
            Yaml yaml = new Yaml();
            try (FileInputStream fis = new FileInputStream(yamlFile.toFile())) {
                return yaml.load(fis);
            }
        } catch (Exception e) {
            log.warn("Failed to read YAML file: {}", yamlFile, e);
            return null;
        }
    }
    
    private String parseTimestamp(String dirName) {
        // Try to extract timestamp from directory name (format: YYYY-MM-DD_HH-MM-SS)
        if (dirName.matches(".*\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}.*")) {
            try {
                String timestampPart = dirName.replaceAll(".*?(\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}).*", "$1");
                LocalDateTime dateTime = LocalDateTime.parse(timestampPart, DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
                return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            } catch (Exception e) {
                log.debug("Failed to parse timestamp from directory name: {}", dirName);
            }
        }
        return dirName; // Fallback to directory name
    }
    
    private String generateReportName(String dirName, Map<String, Object> sessionConfig) {
        if (sessionConfig != null && sessionConfig.containsKey("name")) {
            return (String) sessionConfig.get("name");
        }
        // Generate a friendly name from directory structure
        return dirName.replace("_", " ").replace("-", " ");
    }
    
    private String determineStatus(Map<String, Object> runStatus) {
        if (runStatus == null) {
            return "unknown";
        }
        
        // Check for status indicators in the YAML
        if (runStatus.containsKey("success")) {
            Boolean success = (Boolean) runStatus.get("success");
            if (success != null) {
                return success ? "completed" : "failed";
            }
        }
        
        if (runStatus.containsKey("status")) {
            String status = (String) runStatus.get("status");
            if (status != null) {
                return status.toLowerCase();
            }
        }
        
        // Try to infer from other indicators
        if (runStatus.containsKey("errors") || runStatus.containsKey("failures")) {
            return "failed";
        }
        
        return "completed"; // Default assumption
    }
    
    private Map<String, Object> extractSummary(Map<String, Object> runStatus, Map<String, Object> sessionConfig, Path executionDir) {
        Map<String, Object> summary = new HashMap<>();
        
        // Default values - remove database count as we focus on single database
        summary.put("totalTables", 0);
        summary.put("successfulTables", 0);
        summary.put("failedTables", 0);
        
        if (runStatus != null && runStatus.containsKey("operationStatistics")) {
            // Extract summary data from operation statistics
            Map<String, Object> opStats = (Map<String, Object>) runStatus.get("operationStatistics");
            if (opStats.containsKey("counts")) {
                Map<String, Object> counts = (Map<String, Object>) opStats.get("counts");
                summary.put("totalTables", counts.getOrDefault("tables", 0));
            }
            if (opStats.containsKey("successes")) {
                Map<String, Object> successes = (Map<String, Object>) opStats.get("successes");
                summary.put("successfulTables", successes.getOrDefault("tables", 0));
            }
            if (opStats.containsKey("failures")) {
                Map<String, Object> failures = (Map<String, Object>) opStats.get("failures");
                summary.put("failedTables", failures.getOrDefault("tables", 0));
            }
        }
        
        return summary;
    }
    
    private String extractDatabaseName(Path executionDir) {
        // Look for the main database report file (ends with _hms-mirror.yaml)
        try (var stream = Files.list(executionDir)) {
            Optional<Path> dbReportFile = stream
                .filter(Files::isRegularFile)
                .filter(file -> file.getFileName().toString().endsWith("_hms-mirror.yaml"))
                .findFirst();
                
            if (dbReportFile.isPresent()) {
                Map<String, Object> dbReport = readYamlFile(dbReportFile.get());
                if (dbReport != null && dbReport.containsKey("name")) {
                    return (String) dbReport.get("name");
                }
            }
        } catch (IOException e) {
            log.warn("Failed to extract database name from: {}", executionDir, e);
        }
        return "Unknown Database";
    }
    
    private List<Map<String, Object>> extractTableDetails(Path executionDir) {
        List<Map<String, Object>> tables = new ArrayList<>();
        
        // Look for the main database report file (ends with _hms-mirror.yaml)
        try (var stream = Files.list(executionDir)) {
            Optional<Path> dbReportFile = stream
                .filter(Files::isRegularFile)
                .filter(file -> file.getFileName().toString().endsWith("_hms-mirror.yaml"))
                .findFirst();
                
            if (dbReportFile.isPresent()) {
                Map<String, Object> dbReport = readYamlFile(dbReportFile.get());
                if (dbReport != null && dbReport.containsKey("tableMirrors")) {
                    Map<String, Object> tableMirrors = (Map<String, Object>) dbReport.get("tableMirrors");
                    
                    for (Map.Entry<String, Object> entry : tableMirrors.entrySet()) {
                        String tableName = entry.getKey();
                        Map<String, Object> tableData = (Map<String, Object>) entry.getValue();
                        
                        Map<String, Object> tableInfo = new HashMap<>();
                        tableInfo.put("name", tableName);
                        tableInfo.put("status", determineTableStatus(tableData));
                        tableInfo.put("strategy", tableData.getOrDefault("strategy", "Unknown"));
                        tableInfo.put("phaseState", tableData.getOrDefault("phaseState", "Unknown"));
                        
                        // Extract issues and errors
                        List<String> issues = extractTableIssues(tableData);
                        List<String> errors = extractTableErrors(tableData);
                        tableInfo.put("issues", issues);
                        tableInfo.put("errors", errors);
                        
                        // Check for LEFT and RIGHT availability
                        Map<String, Object> environments = (Map<String, Object>) tableData.get("environments");
                        if (environments != null) {
                            tableInfo.put("hasLeft", environments.containsKey("LEFT") && 
                                hasTableDefinition(environments, "LEFT"));
                            tableInfo.put("hasRight", environments.containsKey("RIGHT") && 
                                hasTableDefinition(environments, "RIGHT"));
                        } else {
                            tableInfo.put("hasLeft", false);
                            tableInfo.put("hasRight", false);
                        }
                        
                        tables.add(tableInfo);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to extract table details from: {}", executionDir, e);
        }
        
        return tables;
    }
    
    private String determineTableStatus(Map<String, Object> tableData) {
        String phaseState = (String) tableData.get("phaseState");
        if ("ERROR".equals(phaseState)) {
            return "failed";
        } else if ("SUCCESS".equals(phaseState)) {
            return "completed";
        } else {
            return "partial";
        }
    }
    
    private List<String> extractTableIssues(Map<String, Object> tableData) {
        List<String> allIssues = new ArrayList<>();
        
        // Extract issues from environments
        Map<String, Object> environments = (Map<String, Object>) tableData.get("environments");
        if (environments != null) {
            for (Object envObj : environments.values()) {
                Map<String, Object> env = (Map<String, Object>) envObj;
                List<String> issues = (List<String>) env.get("issues");
                if (issues != null) {
                    allIssues.addAll(issues);
                }
            }
        }
        
        return allIssues;
    }
    
    private List<String> extractTableErrors(Map<String, Object> tableData) {
        List<String> allErrors = new ArrayList<>();
        
        // Extract errors from environments
        Map<String, Object> environments = (Map<String, Object>) tableData.get("environments");
        if (environments != null) {
            for (Object envObj : environments.values()) {
                Map<String, Object> env = (Map<String, Object>) envObj;
                List<String> errors = (List<String>) env.get("errors");
                if (errors != null) {
                    allErrors.addAll(errors);
                }
            }
        }
        
        return allErrors;
    }
    
    private boolean hasTableDefinition(Map<String, Object> environments, String envName) {
        Map<String, Object> env = (Map<String, Object>) environments.get(envName);
        if (env == null) return false;
        
        List<String> definition = (List<String>) env.get("definition");
        return definition != null && !definition.isEmpty();
    }
    
    private Map<String, Object> getTableEnvironmentDetails(Path executionDir, String tableName, String environment) {
        Map<String, Object> details = new HashMap<>();
        
        try (var stream = Files.list(executionDir)) {
            Optional<Path> dbReportFile = stream
                .filter(Files::isRegularFile)
                .filter(file -> file.getFileName().toString().endsWith("_hms-mirror.yaml"))
                .findFirst();
                
            if (dbReportFile.isPresent()) {
                Map<String, Object> dbReport = readYamlFile(dbReportFile.get());
                if (dbReport != null && dbReport.containsKey("tableMirrors")) {
                    Map<String, Object> tableMirrors = (Map<String, Object>) dbReport.get("tableMirrors");
                    
                    if (tableMirrors.containsKey(tableName)) {
                        Map<String, Object> tableData = (Map<String, Object>) tableMirrors.get(tableName);
                        Map<String, Object> environments = (Map<String, Object>) tableData.get("environments");
                        
                        if (environments != null && environments.containsKey(environment)) {
                            Map<String, Object> envData = (Map<String, Object>) environments.get(environment);
                            
                            details.put("tableName", tableName);
                            details.put("environment", environment);
                            details.put("name", envData.get("name"));
                            details.put("exists", envData.get("exists"));
                            details.put("createStrategy", envData.get("createStrategy"));
                            details.put("definition", envData.get("definition"));
                            details.put("owner", envData.get("owner"));
                            details.put("partitions", envData.get("partitions"));
                            details.put("addProperties", envData.get("addProperties"));
                            details.put("statistics", envData.get("statistics"));
                            details.put("issues", envData.get("issues"));
                            details.put("errors", envData.get("errors"));
                            details.put("sql", envData.get("sql"));
                            details.put("cleanUpSql", envData.get("cleanUpSql"));
                            
                            // Add table-level information
                            details.put("strategy", tableData.get("strategy"));
                            details.put("phaseState", tableData.get("phaseState"));
                            details.put("start", tableData.get("start"));
                            details.put("stageDuration", tableData.get("stageDuration"));
                            details.put("steps", tableData.get("steps"));
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to extract table environment details from: {}", executionDir, e);
        }
        
        return details;
    }
    
    private Map<String, Boolean> checkArtifacts(Path executionDir) {
        Map<String, Boolean> artifacts = new HashMap<>();
        
        artifacts.put("migrationReport", 
            Files.exists(executionDir.resolve("hms-mirror.html")) ||
            findFileWithPattern(executionDir, ".*hms-mirror\\.html"));
        
        artifacts.put("errorLog", 
            Files.exists(executionDir.resolve("run-status.yaml")) ||
            findFileWithPattern(executionDir, ".*\\.log"));
        
        artifacts.put("sqlScripts", 
            findFileWithPattern(executionDir, ".*\\.sql"));
        
        artifacts.put("executionPlan", 
            Files.exists(executionDir.resolve("session-config.yaml")) ||
            findFileWithPattern(executionDir, ".*hms-mirror\\.yaml"));
        
        return artifacts;
    }
    
    private boolean findFileWithPattern(Path directory, String pattern) {
        try (var stream = Files.list(directory)) {
            return stream.anyMatch(file -> file.getFileName().toString().matches(pattern));
        } catch (IOException e) {
            return false;
        }
    }
    
    private String getFileType(String fileName) {
        String lowerName = fileName.toLowerCase();
        if (lowerName.endsWith(".html")) return "report";
        if (lowerName.endsWith(".yaml") || lowerName.endsWith(".yml")) return "config";
        if (lowerName.endsWith(".sql")) return "sql";
        if (lowerName.endsWith(".log")) return "log";
        if (lowerName.endsWith(".md")) return "documentation";
        if (lowerName.endsWith(".sh")) return "script";
        return "file";
    }
    
    private String getFileDescription(String fileName) {
        String lowerName = fileName.toLowerCase();
        if (lowerName.contains("hms-mirror.html")) return "HMS Mirror Migration Report";
        if (lowerName.contains("run-status")) return "Execution Status and Results";
        if (lowerName.contains("session-config")) return "Session Configuration";
        if (lowerName.contains("execute.sql")) return "SQL Execution Scripts";
        if (lowerName.contains("distcp")) return "DistCP Transfer Scripts";
        if (lowerName.contains("runbook")) return "Migration Runbook";
        if (lowerName.endsWith(".log")) return "Execution Log File";
        return "Migration Artifact";
    }
    
    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}