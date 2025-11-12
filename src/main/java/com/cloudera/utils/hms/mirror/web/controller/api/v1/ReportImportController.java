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

import com.cloudera.utils.hms.mirror.service.ReportImportService;
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

import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for importing filesystem reports into RocksDB.
 */
@CrossOrigin
@RestController
@RequestMapping("/api/v1/reports/import")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Report Import", description = "Import filesystem reports into RocksDB")
@RequiredArgsConstructor
@Slf4j
public class ReportImportController {

    private final ReportImportService reportImportService;

    @PostMapping(value = "/all", produces = "application/json")
    @Operation(summary = "Import all filesystem reports",
               description = "Scans the reports directory and imports all reports that haven't been imported yet")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Import completed successfully"),
        @ApiResponse(responseCode = "500", description = "Import failed")
    })
    public ResponseEntity<Map<String, Object>> importAll() {
        log.info("ReportImportController.importAll() called");

        try {
            ReportImportService.ImportResult result = reportImportService.importAllReports();

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("scanned", result.getScanned());
            response.put("imported", result.getImported());
            response.put("skipped", result.getSkipped());
            response.put("failed", result.getFailed());
            response.put("importedPaths", result.getImportedPaths());
            response.put("skippedPaths", result.getSkippedPaths());
            response.put("errors", result.getErrors());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error importing reports", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to import reports: " + e.getMessage()
            ));
        }
    }

    @PostMapping(value = "/{path:.*}", produces = "application/json")
    @Operation(summary = "Import a specific report",
               description = "Imports a specific report by its relative path from the reports directory")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Report imported successfully"),
        @ApiResponse(responseCode = "404", description = "Report not found"),
        @ApiResponse(responseCode = "500", description = "Import failed")
    })
    public ResponseEntity<Map<String, Object>> importReport(
            @Parameter(description = "Relative path from reports directory", required = true)
            @PathVariable String path) {

        log.info("ReportImportController.importReport() called - path: {}", path);

        try {
            boolean success = reportImportService.importReport(path);

            if (success) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "success");
                response.put("message", "Report imported successfully");
                response.put("path", path);
                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "error");
                response.put("message", "Report not found or already imported");
                response.put("path", path);
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
            }

        } catch (Exception e) {
            log.error("Error importing report: {}", path, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to import report: " + e.getMessage(),
                "path", path
            ));
        }
    }
}
