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
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecutionContextService;
import com.cloudera.utils.hms.mirror.service.JobManagementService;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for HMS Mirror Job Management.
 * Provides job creation and management operations.
 */
@CrossOrigin
@RestController
@RequestMapping("/api/v1/jobs")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Job Management", description = "HMS Mirror job operations")
@Slf4j
public class JobsController {

    private final JobManagementService jobManagementService;
    private final ExecutionContextService executionContextService;
    private final ConfigService configService;

    @Autowired
    public JobsController(JobManagementService jobManagementService,
                          ExecutionContextService executionContextService,
                          ConfigService configService) {
        this.jobManagementService = jobManagementService;
        this.executionContextService = executionContextService;
        this.configService = configService;
    }

    @PostMapping(value = "/{jobKey}", consumes = "application/json", produces = "application/json")
    @Operation(summary = "Create or update job", 
               description = "Creates a new HMS Mirror job or updates an existing one")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job saved successfully"),
        @ApiResponse(responseCode = "201", description = "Job created successfully"),
        @ApiResponse(responseCode = "400", description = "Invalid job data"),
        @ApiResponse(responseCode = "500", description = "Failed to save job")
    })
    public ResponseEntity<Map<String, Object>> saveJob(
            @Parameter(description = "Job key/identifier", required = true)
            @PathVariable String jobKey,
            @Parameter(description = "Job data", required = true)
            @RequestBody JobDto jobDto) {
        
        log.info("JobsController.saveJob() called - key: {}, name: {}", 
                jobKey, jobDto.getName());
        
        try {
            // Validate job data first
            Map<String, Object> validationResult = jobManagementService.validate(jobDto);
            if (!"success".equals(validationResult.get("status"))) {
                return ResponseEntity.badRequest().body(validationResult);
            }
            
            // Check if job already exists
            boolean isUpdate = jobManagementService.exists(jobKey);
            
            // Save the job
            Map<String, Object> result = jobManagementService.save(jobDto);
            
            if ("SUCCESS".equals(result.get("status"))) {
                HttpStatus status = isUpdate ? HttpStatus.OK : HttpStatus.CREATED;
                result.put("operation", isUpdate ? "updated" : "created");
                return ResponseEntity.status(status).body(result);
            } else {
                log.error("Failed to save job {}: {}", jobDto.getName(), result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error saving job {}", jobDto.getName(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to save job: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    @GetMapping(value = "/{jobKey}", produces = "application/json")
    @Operation(summary = "Get job by key", 
               description = "Retrieves a specific HMS Mirror job by key")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job retrieved successfully"),
        @ApiResponse(responseCode = "404", description = "Job not found"),
        @ApiResponse(responseCode = "500", description = "Failed to retrieve job")
    })
    public ResponseEntity<Map<String, Object>> getJob(
            @Parameter(description = "Job key", required = true)
            @PathVariable String jobKey) {
        
        log.info("JobsController.getJob() called - key: {}", jobKey);
        
        try {
            Map<String, Object> result = jobManagementService.load(jobKey);
            
            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to load job {}: {}", jobKey, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error retrieving job {}", jobKey, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve job: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    
    @GetMapping(produces = "application/json")
    @Operation(summary = "List all jobs",
               description = "Returns all HMS Mirror jobs")
    @ApiResponse(responseCode = "200", description = "Jobs retrieved successfully",
                 content = @Content(schema = @Schema(implementation = Map.class)))
    public ResponseEntity<Map<String, Object>> getJobs() {
        log.info("JobsController.getJobs() called");

        try {
            Map<String, Object> result = jobManagementService.list();

            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else {
                log.error("Failed to list jobs: {}", result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error listing jobs", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve jobs: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping(value = "/{jobKey}", produces = "application/json")
    @Operation(summary = "Delete job",
               description = "Deletes an HMS Mirror job by key")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job deleted successfully"),
        @ApiResponse(responseCode = "404", description = "Job not found"),
        @ApiResponse(responseCode = "500", description = "Failed to delete job")
    })
    public ResponseEntity<Map<String, Object>> deleteJob(
            @Parameter(description = "Job key", required = true)
            @PathVariable String jobKey) {

        log.info("JobsController.deleteJob() called - key: {}", jobKey);

        try {
            Map<String, Object> result = jobManagementService.delete(jobKey);

            if ("SUCCESS".equals(result.get("status"))) {
                return ResponseEntity.ok(result);
            } else if ("NOT_FOUND".equals(result.get("status"))) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            } else {
                log.error("Failed to delete job {}: {}", jobKey, result.get("message"));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
            }

        } catch (Exception e) {
            log.error("Error deleting job {}", jobKey, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to delete job: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping(value = "/{jobKey}/validate", produces = "application/json")
    @Operation(summary = "Validate job configuration",
               description = "Validates an HMS Mirror job's configuration and dependencies")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Job validation completed"),
        @ApiResponse(responseCode = "404", description = "Job not found"),
        @ApiResponse(responseCode = "500", description = "Failed to validate job")
    })
    public ResponseEntity<Map<String, Object>> validateJob(
            @Parameter(description = "Job key", required = true)
            @PathVariable String jobKey) {

        log.info("JobsController.validateJob() called - key: {}", jobKey);

        try {
            // Build ConversionResult from job
            log.info("About to call jobManagementService.buildConversionResultFromJobId with key: {}", jobKey);
            ConversionResult conversionResult = jobManagementService.buildConversionResultFromJobId(jobKey);
            log.info("buildConversionResultFromJobId returned: {}", conversionResult != null ? "non-null" : "null");

            if (conversionResult == null) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("status", "NOT_FOUND");
                errorResponse.put("message", "Job not found or failed to build conversion result");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
            }

            // Get or initialize RunStatus
            RunStatus runStatus = conversionResult.getRunStatus();

            // Set the ConversionResult and RunStatus in the execution context
            executionContextService.setConversionResult(conversionResult);
            executionContextService.setRunStatus(runStatus);

            try {
                // Validate the configuration
                Boolean isValid = configService.validate();

                // Prepare the response
                Map<String, Object> response = new HashMap<>();
                response.put("status", "SUCCESS");
                response.put("valid", isValid);
                response.put("jobKey", jobKey);

                // Collect errors and warnings from RunStatus
                List<String> errors = new ArrayList<>();
                List<String> warnings = new ArrayList<>();

                if (runStatus.getErrors() != null) {
                    errors.addAll(runStatus.getErrorMessages());
                }
                if (runStatus.getWarnings() != null) {
                    warnings.addAll(runStatus.getWarningMessages());
                }

                response.put("errors", errors);
                response.put("warnings", warnings);

                if (isValid) {
                    response.put("message", "Job configuration is valid");
                } else {
                    response.put("message", "Job configuration has validation errors");
                }

                return ResponseEntity.ok(response);

            } finally {
                // Clean up the execution context
                executionContextService.reset();
            }

        } catch (Exception e) {
            log.error("Error validating job {}", jobKey, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "ERROR");
            errorResponse.put("message", "Failed to validate job: " + e.getMessage());
            errorResponse.put("valid", false);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

}