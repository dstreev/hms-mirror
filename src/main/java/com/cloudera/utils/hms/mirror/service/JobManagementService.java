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

import com.cloudera.utils.hms.mirror.web.model.JobDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Service for managing HMS Mirror Job persistence and retrieval.
 * This service acts as a dedicated layer for job CRUD operations,
 * abstracting the underlying storage mechanism (RocksDB) from the web controllers.
 * 
 * Jobs define migration tasks with their dataset, configuration, and connection references.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@Slf4j
public class JobManagementService {

    private final RocksDB rocksDB;
    private final ColumnFamilyHandle jobsColumnFamily;
    private final ObjectMapper yamlMapper;
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    @Autowired
    public JobManagementService(RocksDB rocksDB,
                               @Qualifier("jobsColumnFamily") ColumnFamilyHandle jobsColumnFamily) {
        this.rocksDB = rocksDB;
        this.jobsColumnFamily = jobsColumnFamily;
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    /**
     * Lists all jobs with their metadata.
     *
     * @return Map containing job listing results
     */
    public Map<String, Object> listJobs() {
        log.debug("Listing all jobs");
        try {
            Map<String, Object> result = new HashMap<>();
            List<Map<String, Object>> jobsList = new ArrayList<>();
            
            // Iterate through all keys in the jobs column family
            try (RocksIterator iterator = rocksDB.newIterator(jobsColumnFamily)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    String jobKey = new String(iterator.key());
                    byte[] jobValue = iterator.value();
                    
                    try {
                        // Parse the YAML to extract job information
                        JobDto jobDto = yamlMapper.readValue(jobValue, JobDto.class);
                        
                        // Create job metadata object
                        Map<String, Object> jobInfo = new HashMap<>();
                        jobInfo.put("jobKey", jobKey);
                        jobInfo.put("name", jobDto.getName());
                        jobInfo.put("description", jobDto.getDescription());
                        jobInfo.put("createdDate", jobDto.getCreatedDate());
                        jobInfo.put("modifiedDate", jobDto.getModifiedDate());
                        jobInfo.put("datasetReference", jobDto.getDatasetReference());
                        jobInfo.put("configReference", jobDto.getConfigReference());
                        jobInfo.put("leftConnectionReference", jobDto.getLeftConnectionReference());
                        jobInfo.put("rightConnectionReference", jobDto.getRightConnectionReference());
                        jobInfo.put("strategy", jobDto.getStrategy());
                        jobInfo.put("disasterRecovery", jobDto.getDisasterRecovery());
                        jobInfo.put("sync", jobDto.getSync());
                        
                        jobsList.add(jobInfo);
                    } catch (Exception e) {
                        log.warn("Failed to parse job {}, skipping", jobKey, e);
                    }
                    iterator.next();
                }
            }
            
            // Sort jobs by name
            jobsList.sort(Comparator.comparing(job -> (String) job.get("name")));
            
            result.put("status", "SUCCESS");
            result.put("jobs", jobsList);
            result.put("count", jobsList.size());
            return result;
            
        } catch (Exception e) {
            log.error("Error listing jobs", e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "error");
            errorResult.put("message", "Failed to list jobs: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Loads a specific job by key.
     *
     * @param jobKey The job key
     * @return Map containing the job load results
     */
    public Map<String, Object> loadJob(String jobKey) {
        log.debug("Loading job: {}", jobKey);
        try {
            byte[] value = rocksDB.get(jobsColumnFamily, jobKey.getBytes());
            
            Map<String, Object> result = new HashMap<>();
            if (value != null) {
                try {
                    JobDto jobDto = yamlMapper.readValue(value, JobDto.class);
                    result.put("status", "SUCCESS");
                    result.put("job", jobDto);
                    result.put("jobKey", jobKey);
                } catch (Exception parseException) {
                    log.error("Error parsing job {}", jobKey, parseException);
                    result.put("status", "ERROR");
                    result.put("message", "Failed to parse job data: " + parseException.getMessage());
                }
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Job not found: " + jobKey);
            }
            
            return result;
            
        } catch (RocksDBException e) {
            log.error("Error loading job {}", jobKey, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to load job: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Saves a job using the JobDto format.
     *
     * @param jobKey The job key
     * @param jobDto The job DTO to save
     * @return Map containing the save operation results
     */
    public Map<String, Object> saveJob(String jobKey, JobDto jobDto) {
        log.debug("Saving job: {}", jobKey);
        try {
            // Set timestamps
            String currentTime = LocalDateTime.now().format(dateFormatter);
            if (jobDto.getCreatedDate() == null) {
                jobDto.setCreatedDate(currentTime);
            }
            jobDto.setModifiedDate(currentTime);
            
            // Convert JobDto to YAML for storage
            String yamlJob = yamlMapper.writeValueAsString(jobDto);
            
            rocksDB.put(jobsColumnFamily, jobKey.getBytes(), yamlJob.getBytes());
            
            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Job saved successfully");
            result.put("jobKey", jobKey);
            result.put("job", jobDto);
            return result;
            
        } catch (Exception e) {
            log.error("Error saving job {}", jobKey, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to save job: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Updates an existing job.
     *
     * @param jobKey The job key
     * @param jobDto The updated job DTO
     * @return Map containing the update operation results
     */
    public Map<String, Object> updateJob(String jobKey, JobDto jobDto) {
        log.debug("Updating job: {}", jobKey);
        try {
            // Check if job exists first
            byte[] existingValue = rocksDB.get(jobsColumnFamily, jobKey.getBytes());
            Map<String, Object> result = new HashMap<>();
            
            if (existingValue != null) {
                // Preserve original creation date if it exists
                try {
                    JobDto existingJob = yamlMapper.readValue(existingValue, JobDto.class);
                    if (existingJob.getCreatedDate() != null) {
                        jobDto.setCreatedDate(existingJob.getCreatedDate());
                    }
                } catch (Exception e) {
                    log.warn("Could not parse existing job for timestamp preservation: {}", e.getMessage());
                }
                
                // Save the updated job
                return saveJob(jobKey, jobDto);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Job not found: " + jobKey);
                return result;
            }
            
        } catch (RocksDBException e) {
            log.error("Error updating job {}", jobKey, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to update job: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Deletes a job by key.
     *
     * @param jobKey The job key
     * @return Map containing the delete operation results
     */
    public Map<String, Object> deleteJob(String jobKey) {
        log.debug("Deleting job: {}", jobKey);
        try {
            // Check if the job exists first
            byte[] existingValue = rocksDB.get(jobsColumnFamily, jobKey.getBytes());
            Map<String, Object> result = new HashMap<>();
            
            if (existingValue != null) {
                // Delete the job
                rocksDB.delete(jobsColumnFamily, jobKey.getBytes());
                result.put("status", "SUCCESS");
                result.put("message", "Job deleted successfully");
                result.put("jobKey", jobKey);
            } else {
                result.put("status", "NOT_FOUND");
                result.put("message", "Job not found: " + jobKey);
            }
            
            return result;
            
        } catch (RocksDBException e) {
            log.error("Error deleting job {}", jobKey, e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("status", "ERROR");
            errorResult.put("message", "Failed to delete job: " + e.getMessage());
            return errorResult;
        }
    }

    /**
     * Checks if a job exists.
     *
     * @param jobKey The job key
     * @return true if the job exists, false otherwise
     */
    public boolean jobExists(String jobKey) {
        log.debug("Checking if job exists: {}", jobKey);
        try {
            Map<String, Object> result = loadJob(jobKey);
            return "SUCCESS".equals(result.get("status"));
        } catch (Exception e) {
            log.warn("Error checking job existence {}", jobKey, e);
            return false;
        }
    }

    /**
     * Validates job data before saving.
     *
     * @param jobDto The job DTO to validate
     * @return Map containing validation results
     */
    public Map<String, Object> validateJob(JobDto jobDto) {
        log.debug("Validating job: {}", jobDto.getName());
        Map<String, Object> result = new HashMap<>();
        List<String> errors = new ArrayList<>();
        
        try {
            // Basic validation
            if (jobDto.getName() == null || jobDto.getName().trim().isEmpty()) {
                errors.add("Job name is required");
            }
            
            if (jobDto.getDatasetReference() == null || jobDto.getDatasetReference().trim().isEmpty()) {
                errors.add("Dataset reference is required");
            }
            
            if (jobDto.getConfigReference() == null || jobDto.getConfigReference().trim().isEmpty()) {
                errors.add("Configuration reference is required");
            }
            
            if (jobDto.getLeftConnectionReference() == null || jobDto.getLeftConnectionReference().trim().isEmpty()) {
                errors.add("Left connection reference is required");
            }
            
            if (jobDto.getRightConnectionReference() == null || jobDto.getRightConnectionReference().trim().isEmpty()) {
                errors.add("Right connection reference is required");
            }
            
            if (jobDto.getStrategy() == null) {
                errors.add("Data strategy is required");
            }
            
            if (errors.isEmpty()) {
                result.put("status", "success");
                result.put("message", "Job is valid");
            } else {
                result.put("status", "error");
                result.put("message", "Job validation failed");
                result.put("errors", errors);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Error validating job", e);
            result.put("status", "error");
            result.put("message", "Validation failed: " + e.getMessage());
            return result;
        }
    }
}