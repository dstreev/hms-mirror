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

import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
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
}
