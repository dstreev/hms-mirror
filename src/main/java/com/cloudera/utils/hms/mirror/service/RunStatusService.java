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

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.RunStatusRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for managing RunStatus persistence and retrieval.
 * Provides CRUD operations and specialized queries for runtime job tracking.
 *
 * On startup, this service automatically cleans up any orphaned RunStatus objects
 * (those that started but never finished due to application restart).
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class RunStatusService {

    private final RunStatusRepository runStatusRepository;

    public RunStatusService(RunStatusRepository runStatusRepository) {
        this.runStatusRepository = runStatusRepository;
    }

    /**
     * Initialize the service and clean up terminated jobs on startup.
     * This method runs automatically after the bean is constructed.
     */
    @PostConstruct
    public void init() {
        log.info("Initializing RunStatusService and cleaning up terminated jobs");
        try {
            int terminatedCount = cleanupTerminatedJobs();
            if (terminatedCount > 0) {
                log.warn("Marked {} orphaned job(s) as terminated due to application restart", terminatedCount);
            } else {
                log.info("No orphaned jobs found during startup");
            }
        } catch (Exception e) {
            log.error("Error during startup cleanup of terminated jobs", e);
        }
    }

    /**
     * Clean up jobs that were running when the application stopped.
     * These are identified by having a start time but no end time.
     *
     * @return Number of jobs that were terminated
     */
    public int cleanupTerminatedJobs() {
        try {
            List<RunStatus> allRunStatuses = runStatusRepository.findAllAsList();
            int terminatedCount = 0;
            LocalDateTime terminationTime = LocalDateTime.now();

            for (RunStatus runStatus : allRunStatuses) {
                // Find jobs that started but never finished
                if (runStatus.getStart() != null && runStatus.getEnd() == null) {
                    log.info("Terminating orphaned job with key: {}", runStatus.getKey());

                    // Set end time to now
                    runStatus.setEnd(terminationTime);

                    // Add terminated job error message
                    runStatus.addError(MessageCode.TERMINATED_JOB);

                    // Save the updated status
                    runStatusRepository.save(runStatus);
                    terminatedCount++;
                }
            }

            return terminatedCount;
        } catch (RepositoryException e) {
            log.error("Error cleaning up terminated jobs", e);
            return 0;
        }
    }

    /**
     * Get a paginated list of RunStatus objects, ordered by:
     * 1. Currently running jobs (end == null) - most recent first
     * 2. Completed jobs - most recent first
     *
     * @param page Page number (0-based)
     * @param pageSize Number of items per page
     * @return Map containing status, data, and pagination info
     */
    public Map<String, Object> list(int page, int pageSize) {
        log.debug("Listing RunStatus objects - page: {}, pageSize: {}", page, pageSize);

        try {
            List<RunStatus> allRunStatuses = runStatusRepository.findAllAsList();

            // Separate running and completed jobs
            List<RunStatus> runningJobs = allRunStatuses.stream()
                    .filter(rs -> rs.getStart() != null && rs.getEnd() == null)
                    .sorted((a, b) -> b.getStart().compareTo(a.getStart())) // Most recent first
                    .collect(Collectors.toList());

            List<RunStatus> completedJobs = allRunStatuses.stream()
                    .filter(rs -> rs.getEnd() != null)
                    .sorted((a, b) -> b.getEnd().compareTo(a.getEnd())) // Most recent first
                    .collect(Collectors.toList());

            // Combine lists: running jobs first, then completed
            List<RunStatus> orderedList = new ArrayList<>();
            orderedList.addAll(runningJobs);
            orderedList.addAll(completedJobs);

            // Calculate pagination
            int totalCount = orderedList.size();
            int startIndex = page * pageSize;
            int endIndex = Math.min(startIndex + pageSize, totalCount);

            // Get the page of results
            List<RunStatus> pageResults = (startIndex < totalCount)
                    ? orderedList.subList(startIndex, endIndex)
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

            return response;

        } catch (RepositoryException e) {
            log.error("Error listing RunStatus objects", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve run statuses: " + e.getMessage());
            return errorResponse;
        }
    }

    /**
     * Get a specific RunStatus by its key.
     *
     * @param key The RunStatus key
     * @return Map containing status and RunStatus data
     */
    public Map<String, Object> get(String key) {
        log.debug("Getting RunStatus by key: {}", key);

        try {
            // The key in RunStatus includes the "/runStatus" suffix, so we need to look it up directly
            Optional<RunStatus> runStatusOpt = Optional.empty();
            // Try to find by the full key first
            List<RunStatus> allStatuses = runStatusRepository.findAllAsList();
            runStatusOpt = allStatuses.stream()
                    .filter(rs -> key.equals(rs.getKey()))
                    .findFirst();

            if (runStatusOpt.isPresent()) {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "SUCCESS");
                response.put("runStatus", runStatusOpt.get());
                return response;
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("status", "NOT_FOUND");
                response.put("message", "RunStatus with key '" + key + "' not found");
                return response;
            }

        } catch (RepositoryException e) {
            log.error("Error getting RunStatus by key: {}", key, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve run status: " + e.getMessage());
            return errorResponse;
        }
    }

    /**
     * Delete a RunStatus by its key.
     *
     * @param key The RunStatus key
     * @return Map containing status and message
     */
    public Map<String, Object> delete(String key) {
        log.debug("Deleting RunStatus by key: {}", key);

        try {
            // Find the RunStatus first to check if it exists
            List<RunStatus> allStatuses = runStatusRepository.findAllAsList();
            Optional<RunStatus> runStatusOpt = allStatuses.stream()
                    .filter(rs -> key.equals(rs.getKey()))
                    .findFirst();

            boolean existed = false;
            if (runStatusOpt.isPresent()) {
                existed = runStatusRepository.delete(runStatusOpt.get());
            }

            Map<String, Object> response = new HashMap<>();
            if (existed) {
                response.put("status", "SUCCESS");
                response.put("message", "RunStatus deleted successfully");
            } else {
                response.put("status", "NOT_FOUND");
                response.put("message", "RunStatus with key '" + key + "' not found");
            }
            return response;

        } catch (RepositoryException e) {
            log.error("Error deleting RunStatus by key: {}", key, e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to delete run status: " + e.getMessage());
            return errorResponse;
        }
    }

    /**
     * Check if a RunStatus exists by its key.
     *
     * @param key The RunStatus key
     * @return true if exists, false otherwise
     */
    public boolean exists(String key) {
        try {
            List<RunStatus> allStatuses = runStatusRepository.findAllAsList();
            return allStatuses.stream()
                    .anyMatch(rs -> key.equals(rs.getKey()));
        } catch (RepositoryException e) {
            log.error("Error checking RunStatus existence for key: {}", key, e);
            return false;
        }
    }
}
