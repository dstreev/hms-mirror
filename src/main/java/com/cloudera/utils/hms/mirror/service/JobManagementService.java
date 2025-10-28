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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.core.Cluster;
import com.cloudera.utils.hms.mirror.domain.core.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.core.PartitionDiscovery;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConnectionPoolType;
import com.cloudera.utils.hms.mirror.domain.support.ConversionRequest;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import com.cloudera.utils.hms.mirror.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service for managing HMS Mirror Job persistence and retrieval.
 * This service acts as a dedicated layer for job CRUD operations,
 * delegating persistence to the JobRepository.
 *
 * Jobs define migration tasks with their dataset, configuration, and connection references.
 */
@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class JobManagementService {

    private final JobRepository jobRepository;
    private final ConnectionService connectionService;
    private final DatasetManagementService datasetManagementService;
    private final ConfigurationManagementService configurationManagementService;

    /**
     * Lists all jobs with their metadata.
     *
     * @return Map containing job listing results
     */
    public Map<String, Object> listJobs() {
        log.debug("Listing all jobs");
        try {
            List<JobDto> jobs = jobRepository.findAllSortedByName();
            List<Map<String, Object>> jobsList = new ArrayList<>();

            for (JobDto jobDto : jobs) {
                Map<String, Object> jobInfo = new HashMap<>();
                jobInfo.put("jobKey", jobDto.getName());
                jobInfo.put("name", jobDto.getName());
                jobInfo.put("description", jobDto.getDescription());
                jobInfo.put("createdDate", jobDto.getCreatedDate());
                jobInfo.put("modifiedDate", jobDto.getModifiedDate());
                jobInfo.put("datasetReference", jobDto.getDatasetReference());
                jobInfo.put("configReference", jobDto.getConfigReference());
                jobInfo.put("leftConnectionReference", jobDto.getLeftConnectionReference());
                jobInfo.put("rightConnectionReference", jobDto.getRightConnectionReference());
                jobInfo.put("strategy", jobDto.getStrategy());
                jobInfo.put("disasterRecovery", jobDto.isDisasterRecovery());
                jobInfo.put("sync", jobDto.isSync());

                jobsList.add(jobInfo);
            }

            Map<String, Object> result = new HashMap<>();
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
            Optional<JobDto> jobOpt = jobRepository.findById(jobKey);

            Map<String, Object> result = new HashMap<>();
            if (jobOpt.isPresent()) {
                result.put("status", "SUCCESS");
                result.put("job", jobOpt.get());
                result.put("jobKey", jobKey);
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
            JobDto savedJob = jobRepository.save(jobKey, jobDto);

            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", "Job saved successfully");
            result.put("jobKey", jobKey);
            result.put("job", savedJob);
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
            Optional<JobDto> existingJobOpt = jobRepository.findById(jobKey);
            Map<String, Object> result = new HashMap<>();

            if (existingJobOpt.isPresent()) {
                // Preserve original creation date
                JobDto existingJob = existingJobOpt.get();
                if (existingJob.getCreatedDate() != null) {
                    jobDto.setCreatedDate(existingJob.getCreatedDate());
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
            boolean existed = jobRepository.deleteById(jobKey);
            Map<String, Object> result = new HashMap<>();

            if (existed) {
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
            return jobRepository.existsById(jobKey);
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

    /**
     * Creates a ConversionRequest from a DatasetDto by mapping databases to their tables.
     *
     * @param datasetDto The dataset DTO containing database and table specifications
     * @return ConversionRequest with database-to-tables mapping
     */
    public ConversionRequest createConversionRequestFromDataset(DatasetDto datasetDto) {
        log.debug("Creating ConversionRequest from dataset: {}", datasetDto.getName());
        ConversionRequest conversionRequest = new ConversionRequest();

        // Iterate through each database specification in the dataset
        for (DatasetDto.DatabaseSpec dbSpec : datasetDto.getDatabases()) {
            String databaseName = dbSpec.getDatabaseName();
            List<String> tables = new ArrayList<>();

            // If tables are explicitly specified, use them
            if (dbSpec.getTables() != null && !dbSpec.getTables().isEmpty()) {
                tables.addAll(dbSpec.getTables());
            }
            // Note: If using filters (includePattern, excludePattern), those would be
            // applied during actual table discovery, not during ConversionRequest creation

            conversionRequest.getDatabases().put(databaseName, tables);
        }

        return conversionRequest;
    }

    /**
     * Converts a ConnectionDto to a Cluster object for use in HmsMirrorConfig.
     *
     * @param connectionDto The connection DTO to convert
     * @param environment The environment (LEFT or RIGHT) for this cluster
     * @return Cluster object configured from the connection DTO
     */
    private Cluster convertConnectionDtoToCluster(ConnectionDto connectionDto, Environment environment) {
        log.debug("Converting ConnectionDto {} to Cluster for environment {}",
                connectionDto.getName(), environment);

        Cluster cluster = new Cluster();
        cluster.setEnvironment(environment);

        // Set platform type
        if (connectionDto.getPlatformType() != null) {
            cluster.setPlatformType(connectionDto.getPlatformType());
        }

        // Set HCFS namespace
        cluster.setHcfsNamespace(connectionDto.getHcfsNamespace());

        // Configure HiveServer2
        HiveServer2Config hs2Config = new HiveServer2Config();
        hs2Config.setUri(connectionDto.getHs2Uri());

        // Set connection properties
        if (connectionDto.getHs2ConnectionProperties() != null) {
            Properties props = new Properties();
            props.putAll(connectionDto.getHs2ConnectionProperties());
            // Add username and password if provided
            if (connectionDto.getHs2Username() != null) {
                props.setProperty("user", connectionDto.getHs2Username());
            }
            if (connectionDto.getHs2Password() != null) {
                props.setProperty("password", connectionDto.getHs2Password());
            }
            hs2Config.setConnectionProperties(props);
        }

        cluster.setHiveServer2(hs2Config);

        // Configure Metastore Direct if enabled
        if (connectionDto.isMetastoreDirectEnabled() &&
            connectionDto.getMetastoreDirectUri() != null) {
            DBStore metastoreDirect = new DBStore();
            metastoreDirect.setUri(connectionDto.getMetastoreDirectUri());

            // Convert string type to DBStore.DB_TYPE enum
            if (connectionDto.getMetastoreDirectType() != null) {
                try {
                    metastoreDirect.setType(DBStore.DB_TYPE.valueOf(connectionDto.getMetastoreDirectType()));
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid metastore direct type: {}. Using default.", connectionDto.getMetastoreDirectType());
                }
            }

            // Set connection properties for metastore
            Properties metastoreProps = new Properties();
            if (connectionDto.getMetastoreDirectUsername() != null) {
                metastoreProps.setProperty("user", connectionDto.getMetastoreDirectUsername());
            }
            if (connectionDto.getMetastoreDirectPassword() != null) {
                metastoreProps.setProperty("password", connectionDto.getMetastoreDirectPassword());
            }
            metastoreDirect.setConnectionProperties(metastoreProps);

            // Note: Connection pool min/max settings are not directly set on DBStore
            // They are managed by the connection pooling infrastructure

            cluster.setMetastoreDirect(metastoreDirect);
        }

        // Configure partition discovery
        PartitionDiscovery partitionDiscovery = new PartitionDiscovery();
        partitionDiscovery.setAuto(connectionDto.isPartitionDiscoveryAuto());
        partitionDiscovery.setInitMSCK(connectionDto.isPartitionDiscoveryInitMSCK());
        // Note: partitionBucketLimit is not part of PartitionDiscovery - it's stored in ConnectionDto only
        cluster.setPartitionDiscovery(partitionDiscovery);

        // Set additional cluster settings
        cluster.setCreateIfNotExists(connectionDto.isCreateIfNotExists());
        cluster.setEnableAutoTableStats(connectionDto.isEnableAutoTableStats());
        cluster.setEnableAutoColumnStats(connectionDto.isEnableAutoColumnStats());

        return cluster;
    }

    /**
     * Builds a ConversionResult from a JobDto by looking up the job by ID
     * and assembling all referenced DTOs (connections, dataset, configuration).
     *
     * @param jobId The job ID to look up
     * @return ConversionResult instance constructed from the JobDto's references, or null if not found
     */
    public ConversionResult buildConversionResultFromJobId(String jobId) {
        log.debug("Building ConversionResult from jobId: {}", jobId);

        try {
            // First, look up the JobDto by ID
            Optional<JobDto> jobOpt = jobRepository.findById(jobId);
            if (!jobOpt.isPresent()) {
                log.error("Job not found: {}", jobId);
                return null;
            }

            JobDto jobDto = jobOpt.get();
            log.debug("Found job: {}", jobDto.getName());

            // Fetch referenced DTOs
            ConnectionDto leftConnection = connectionService.getConnectionById(jobDto.getLeftConnectionReference()).orElse(null);
            if (leftConnection == null) {
                log.error("Left connection not found: {}", jobDto.getLeftConnectionReference());
                return null;
            }

            ConnectionDto rightConnection = connectionService.getConnectionById(jobDto.getRightConnectionReference()).orElse(null);
            if (rightConnection == null) {
                log.error("Right connection not found: {}", jobDto.getRightConnectionReference());
                return null;
            }

            Map<String, Object> datasetResult = datasetManagementService.loadDataset(jobDto.getDatasetReference());
            if (!"SUCCESS".equals(datasetResult.get("status"))) {
                log.error("Dataset not found: {}", jobDto.getDatasetReference());
                return null;
            }
            DatasetDto datasetDto = (DatasetDto) datasetResult.get("dataset");

            Map<String, Object> configResult = configurationManagementService.loadConfiguration(jobDto.getConfigReference());
            if (!"SUCCESS".equals(configResult.get("status"))) {
                log.error("Configuration not found: {}", jobDto.getConfigReference());
                return null;
            }
            ConfigLiteDto configLiteDto = (ConfigLiteDto) configResult.get("configuration");

            // Construct ConversionResult
            ConversionResult conversionResult = new ConversionResult();

            // Set the ConfigLiteDto
            conversionResult.setConfig(configLiteDto);

            // Set the DatasetDto
            conversionResult.setDataset(datasetDto);

            // Set the connections
            Map<Environment, ConnectionDto> connections = new HashMap<>();
            connections.put(Environment.LEFT, leftConnection);
            connections.put(Environment.RIGHT, rightConnection);
            conversionResult.setConnections(connections);

            // Set the JobDto
            conversionResult.setJob(jobDto);

            // Initialize RunStatus
//            RunStatus runStatus = new RunStatus();
//            conversionResult.setRunStatus(runStatus);

            log.debug("ConversionResult built successfully from job: {}", jobDto.getName());
            return conversionResult;

        } catch (RocksDBException e) {
            log.error("RocksDB error building ConversionResult from jobId {}", jobId, e);
            return null;
        } catch (Exception e) {
            log.error("Error building ConversionResult from jobId {}", jobId, e);
            return null;
        }
    }

    /**
     * Determines the appropriate connection pool type based on platform type.
     *
     * @param platformType The platform type
     * @return The recommended connection pool type
     */
    private ConnectionPoolType determineConnectionPoolType(PlatformType platformType) {
        switch (platformType) {
            case CDP7_0:
            case CDP7_1:
            case CDP7_1_9_SP1:
            case CDP7_2:
            case CDP7_3:
                return ConnectionPoolType.HIKARICP;
//            case EMR4:
            case HDP2:
            case HDP3:
            case CDH5:
            case CDH6:
                return ConnectionPoolType.DBCP2;
//            case APACHE_HIVE1:
            case APACHE_HIVE2:
            case APACHE_HIVE3:
            case APACHE_HIVE4:
//            case EMR5:
            case EMR6:
            case EMR7:
//            case MAPR:
            default:
                return ConnectionPoolType.HYBRID;
        }
    }
}