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

import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.RunStatusService;
import com.cloudera.utils.hms.mirror.web.service.RuntimeService;
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
 * REST Controller for RunStatus (Runtime Jobs) management.
 * Provides endpoints for listing, viewing, and managing job execution status.
 */
@CrossOrigin
@RestController
@RequestMapping("/api/v1/runtime/jobs")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Runtime Jobs", description = "Runtime job status tracking and management")
@RequiredArgsConstructor
@Slf4j
public class RunStatusController {

    private final RunStatusService runStatusService;
    private final RuntimeService runtimeService;

    @GetMapping(produces = "application/json")
    @Operation(summary = "List runtime job statuses",
               description = "Returns paginated list of job statuses, with running jobs first followed by completed jobs (most recent first)")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job statuses retrieved successfully"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve job statuses")
    })
    public ResponseEntity<Map<String, Object>> listRunStatuses(
            @Parameter(description = "Page number (0-based)", required = false)
            @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size", required = false)
            @RequestParam(defaultValue = "10") int pageSize) {

        log.info("RunStatusController.listRunStatuses() called - page: {}, pageSize: {}", page, pageSize);

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

            Map<String, Object> result = runStatusService.list(page, pageSize);

            if ("success".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else {
                log.error("Failed to list run statuses: {}", result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error listing run statuses", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to retrieve job statuses: " + e.getMessage()
            ));
        }
    }

    @GetMapping(value = "/{key}", produces = "application/json")
    @Operation(summary = "Get job status by key",
               description = "Retrieves details of a specific job execution")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job status retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Job status not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve job status")
    })
    public ResponseEntity<Map<String, Object>> getRunStatus(
            @Parameter(description = "Job status key", required = true)
            @PathVariable String key) {

        log.info("RunStatusController.getRunStatus() called - key: {}", key);

        try {
            Map<String, Object> result = runStatusService.get(key);

            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to get run status {}: {}", key, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error retrieving run status {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to retrieve job status: " + e.getMessage()
            ));
        }
    }

    @DeleteMapping(value = "/{key}", produces = "application/json")
    @Operation(summary = "Delete job status",
               description = "Deletes a specific job execution status")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job status deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Job status not found"),
        @ApiResponse(responseCode = "500", description = "Failed to delete job status")
    })
    public ResponseEntity<Map<String, Object>> deleteRunStatus(
            @Parameter(description = "Job status key", required = true)
            @PathVariable String key) {

        log.info("RunStatusController.deleteRunStatus() called - key: {}", key);

        try {
            Map<String, Object> result = runStatusService.delete(key);

            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to delete run status {}: {}", key, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error deleting run status {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "status", "error",
                "message", "Failed to delete job status: " + e.getMessage()
            ));
        }
    }

    @PostMapping(value = "/start", consumes = "application/json", produces = "application/json")
    @Operation(summary = "Start a job execution",
               description = "Starts a job execution with the specified job key and dry-run option")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job started successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "500", description = "Failed to start job")
    })
    public ResponseEntity<Map<String, Object>> startJob(
            @Parameter(description = "Start job request containing jobKey and dryRun flag", required = true)
            @RequestBody Map<String, Object> request) {

        log.info("RunStatusController.startJob() called - request: {}", request);

        try {
            // Extract parameters from request body
            String jobKey = (String) request.get("jobKey");
            Boolean dryRun = request.get("dryRun") != null ? (Boolean) request.get("dryRun") : true;

            if (jobKey == null || jobKey.trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "jobKey is required");
                return ResponseEntity.badRequest().body(errorResponse);
            }

            log.info("Starting job: {} with dryRun: {}", jobKey, dryRun);

            // Call RuntimeService to start the job
            try {
                RunStatus runStatus = runtimeService.start(jobKey, dryRun);

                Map<String, Object> response = new HashMap<>();
                response.put("status", "SUCCESS");
                response.put("message", "Job started successfully");
                response.put("jobKey", jobKey);
                response.put("dryRun", dryRun);
                response.put("runStatus", runStatus);

                return ResponseEntity.ok(response);
            } catch (RequiredConfigurationException | MismatchException | SessionException | EncryptionException e) {
                log.error("Error starting job {}: {}", jobKey, e.getMessage(), e);
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Failed to start job: " + e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
            }

        } catch (Exception e) {
            log.error("Error processing job start request", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to process request: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
