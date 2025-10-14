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

import com.cloudera.utils.hms.mirror.service.JobManagementService;
import com.cloudera.utils.hms.mirror.web.model.JobDto;
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

import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for HMS Mirror Job Management.
 * Provides job creation and management operations.
 */
@RestController
@RequestMapping("/api/v1/jobs")
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Tag(name = "Job Management", description = "HMS Mirror job operations")
@Slf4j
public class JobsController {

    private final JobManagementService jobManagementService;

    @Autowired
    public JobsController(JobManagementService jobManagementService) {
        this.jobManagementService = jobManagementService;
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
            Map<String, Object> validationResult = jobManagementService.validateJob(jobDto);
            if (!"success".equals(validationResult.get("status"))) {
                return ResponseEntity.badRequest().body(validationResult);
            }
            
            // Check if job already exists
            boolean isUpdate = jobManagementService.jobExists(jobKey);
            
            // Save the job
            Map<String, Object> result = jobManagementService.saveJob(jobKey, jobDto);
            
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
            Map<String, Object> result = jobManagementService.loadJob(jobKey);
            
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
            Map<String, Object> result = jobManagementService.listJobs();
            
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
}