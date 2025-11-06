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

import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.domain.testdata.LegacyConversionWrapper;
import com.cloudera.utils.hms.mirror.domain.testdata.LegacyDBMirror;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.reporting.ReportingConf;
import com.cloudera.utils.hms.mirror.repository.*;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Conversion;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service for handling ConversionResult business operations.
 * This service contains the business logic that was previously embedded in the ConversionResult domain object.
 */
@Service
@Getter
//@RequiredArgsConstructor
@Slf4j
public class ConversionResultService {

    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final com.cloudera.utils.hms.mirror.repository.ConfigurationRepository configurationRepository;
    @NonNull
    private final com.cloudera.utils.hms.mirror.repository.DatasetRepository datasetRepository;
    @NonNull
    private final com.cloudera.utils.hms.mirror.repository.ConnectionRepository connectionRepository;
    @NonNull
    private final com.cloudera.utils.hms.mirror.repository.JobRepository jobRepository;
    @NonNull
    private final com.cloudera.utils.hms.mirror.repository.TableMirrorRepository tableMirrorRepository;
    @NonNull
    private final com.cloudera.utils.hms.mirror.repository.DBMirrorRepository dbMirrorRepository;
    @NonNull
    private final ObjectMapper yamlMapper;

    public ConversionResultService(@NonNull ExecutionContextService executionContextService,
                                   @NonNull ConfigurationRepository configurationRepository,
                                   @NonNull DatasetRepository datasetRepository,
                                   @NonNull ConnectionRepository connectionRepository,
                                   @NonNull JobRepository jobRepository,
                                   @NonNull TableMirrorRepository tableMirrorRepository,
                                   @NonNull DBMirrorRepository dbMirrorRepository,
                                   @NonNull @Qualifier("yamlMapper") ObjectMapper yamlMapper) {
        this.executionContextService = executionContextService;
        this.configurationRepository = configurationRepository;
        this.datasetRepository = datasetRepository;
        this.connectionRepository = connectionRepository;
        this.jobRepository = jobRepository;
        this.tableMirrorRepository = tableMirrorRepository;
        this.dbMirrorRepository = dbMirrorRepository;
        this.yamlMapper = yamlMapper;
    }

    /**
     * This method uses a job ID to look up the JobDto from the repository and build out a fully resolved
     * ConversionResult object. It delegates to the {@link #fromJob(JobDto)} method.
     *
     * @param jobKey The ID of the job to look up
     * @return A fully populated ConversionResult with deep clones of all referenced objects
     * @throws RuntimeException if job is not found or repository access fails
     */
    public ConversionResult fromJob(String jobKey) {
        if (isBlank(jobKey)) {
            throw new IllegalArgumentException("Job ID cannot be null or blank");
        }

        log.debug("Looking up JobDto by ID: {}", jobKey);

        try {
            Optional<JobDto> jobOpt = jobRepository.findByKey(jobKey);
            if (jobOpt.isPresent()) {
                JobDto job = jobOpt.get();
                log.debug("Found JobDto with ID: {}", jobKey);
                return fromJob(job);
            } else {
                log.error("Job with ID '{}' not found in repository", jobKey);
                throw new IllegalStateException("Job with ID '" + jobKey + "' not found");
            }
        } catch (com.cloudera.utils.hms.mirror.exceptions.RepositoryException e) {
            log.error("Repository error while looking up JobDto with ID: {}", jobKey, e);
            throw new RuntimeException("Failed to load job from repository", e);
        }
    }

    /**
     * This method uses a JobDto object to build out a fully resolved ConversionResult object that has the clones of the
     * references kept in the JobDto to Connections, Dataset, Config, etc.  It also makes a clone of the JobDto and sets
     * the job field in the conversionResult.
     *
     * @param job The JobDto containing references to configuration, dataset, and connections
     * @return A fully populated ConversionResult with deep clones of all referenced objects
     * @throws RuntimeException if required references are missing or repository access fails
     */
    public ConversionResult fromJob(JobDto job) {
        if (isNull(job)) {
            throw new IllegalArgumentException("JobDto cannot be null");
        }

        log.debug("Building ConversionResult from JobDto: {}", job.getKey());

        ConversionResult conversionResult = new ConversionResult();
        RunStatus runStatus = new RunStatus();
        conversionResult.setRunStatus(runStatus);

        try {
            // Load and deep clone the configuration
            if (!isBlank(job.getConfigReference())) {
                log.debug("Loading config reference: {}", job.getConfigReference());
                ConfigLiteDto config = configurationRepository.findByKey(job.getConfigReference())
                        .orElseThrow(() -> {
                            log.warn("Config reference '{}' not found in repository", job.getConfigReference());
                            return new IllegalStateException("Config reference '" + job.getConfigReference() + "' not found");
                        })
                        .clone();
                conversionResult.setConfig(config);
                log.debug("Config loaded and cloned successfully");
            } else {
                log.warn("No config reference provided in JobDto");
            }

            // Load and deep clone the dataset
            if (!isBlank(job.getDatasetReference())) {
                log.debug("Loading dataset reference: {}", job.getDatasetReference());
                DatasetDto dataset = datasetRepository.findByKey(job.getDatasetReference())
                        .orElseThrow(() -> {
                            log.warn("Dataset reference '{}' not found in repository", job.getDatasetReference());
                            return new IllegalStateException("Dataset reference '" + job.getDatasetReference() + "' not found");
                        })
                        .clone();
                conversionResult.setDataset(dataset);
                log.debug("Dataset loaded and cloned successfully");
            } else {
                log.warn("No dataset reference provided in JobDto");
            }

            // Load and clone the LEFT connection
            if (!isBlank(job.getLeftConnectionReference())) {
                log.debug("Loading left connection reference: {}", job.getLeftConnectionReference());
                com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto leftConn =
                        connectionRepository.findByKey(job.getLeftConnectionReference())
                                .orElseThrow(() -> {
                                    log.warn("Left connection reference '{}' not found in repository", job.getLeftConnectionReference());
                                    return new IllegalStateException("Left connection reference '" + job.getLeftConnectionReference() + "' not found");
                                })
                                .clone();
                conversionResult.setConnection(Environment.LEFT, leftConn);
                log.debug("Left connection loaded and cloned successfully");
            } else {
                log.warn("No left connection reference provided in JobDto");
            }

            // Load and clone the RIGHT connection
            if (!isBlank(job.getRightConnectionReference())) {
                log.debug("Loading right connection reference: {}", job.getRightConnectionReference());
                com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto rightConn =
                        connectionRepository.findByKey(job.getRightConnectionReference())
                                .orElseThrow(() -> {
                                    log.warn("Right connection reference '{}' not found in repository", job.getRightConnectionReference());
                                    return new IllegalStateException("Right connection reference '" + job.getRightConnectionReference() + "' not found");
                                })
                                .clone();
                conversionResult.setConnection(Environment.RIGHT, rightConn);
                log.debug("Right connection loaded and cloned successfully");
            } else {
                log.warn("No right connection reference provided in JobDto");
            }

            // Deep clone the JobDto itself and set it in the conversionResult
            JobDto clonedJob = job.clone();
            conversionResult.setJob(clonedJob);
            log.debug("JobDto cloned and set in ConversionResult");

            log.info("ConversionResult successfully built from JobDto: {}", job.getKey());
            return conversionResult;

        } catch (com.cloudera.utils.hms.mirror.exceptions.RepositoryException e) {
            log.error("Repository error while loading references for JobDto: {}", job.getKey(), e);
            throw new RuntimeException("Failed to load references from repository", e);
        } catch (CloneNotSupportedException e) {
            log.error("Failed to clone objects for JobDto: {}", job.getKey(), e);
            throw new RuntimeException("Failed to clone objects", e);
        }
    }

//    private final ExecuteSessionService executeSessionService;

//    /**
//     * Generates action SQL script for a specific environment and database.
//     * NOTE: Currently commented out because it depends on TableMirror.getTableActions() and
//     * EnvironmentTable.actions field which are also commented out.
//     *
//     * @param conversionResult The conversion result containing database mirrors
//     * @param env             The target environment
//     * @param database        The database name
//     * @return SQL script as a string
//     */
//    public String actionsSql(ConversionResult conversionResult, Environment env, String database) {
//        StringBuilder sb = new StringBuilder();
//        sb.append("-- ACTION script for ").append(env).append(" cluster\n\n");
//        sb.append("-- HELPER Script to assist with MANUAL updates.\n");
//        sb.append("-- RUN AT OWN RISK  !!!\n");
//        sb.append("-- REVIEW and UNDERSTAND the adjustments below before running.\n\n");
//        DBMirror dbMirror = conversionResult.getDatabases().get(database);
//        sb.append("-- DATABASE: ").append(database).append("\n");
//        Set<String> tables = dbMirror.getTableMirrors().keySet();
//        for (String table : tables) {
//            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
//            sb.append("--    Table: ").append(table).append("\n");
//            // LEFT Table Actions
//            for (String item : tblMirror.getTableActions(env)) {
//                sb.append(item).append(";\n");
//            }
//            sb.append("\n");
//        }
//        return sb.toString();
//    }

//    public DBMirror getDatabase(String database) {
//        ConversionResult conversionResult = executionContextService.getConversionResult().orElseThrow(() ->
//                new IllegalStateException("ConversionResult not set."));
//        // TODO: Go to the ConversionResult Repo and get the DBMirror from persistence.
//
//        return null;
//    }

    public boolean convertManaged() {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive() &&
                !conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public String getResolvedDB(String database) {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        String rtn = null;
        // Set Local Value for adjustments
        String lclDb = database;
        // When dbp, set new value
        DatasetDto.DatabaseSpec dbSpec = conversionResult.getDataset().getDatabase(database);
        if (nonNull(dbSpec) && !isBlank(dbSpec.getDbPrefix())) {
            lclDb = dbSpec.getDbPrefix() + lclDb;
        }
        // Rename overrides prefix, otherwise use lclDb as its been set.
        rtn = (!isBlank(dbSpec.getDbRename()) ? dbSpec.getDbRename() : lclDb);
        return rtn;
    }

    public boolean possibleConversions() {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        boolean conversions = Boolean.FALSE;
        if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive() &&
                (nonNull(conversionResult.getConnection(Environment.RIGHT)) &&
                        !conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive())) {
            conversions = Boolean.TRUE;
        }

        return conversions;
    }

    @JsonIgnore
    /*
    Target Namespace is a hierarchy check of the 'common' storage location, the RIGHT cluster's namespace.
     */
    public String getTargetNamespace() throws RequiredConfigurationException {
        String rtn = null;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        rtn = conversionResult.getTargetNamespace();

        if (isBlank(rtn)) {
            throw new RequiredConfigurationException("Target Namespace is required.  Please set 'targetNamespace'.");
        }
        return rtn;
    }

    /**
     * Determines if the current configuration represents a legacy migration.
     * A legacy migration is when one cluster is using legacy Hive and the other isn't.
     *
     * @param config The HMS Mirror configuration
     * @return true if this is a legacy migration, false otherwise
     */
    public Boolean legacyMigration() {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));

        if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive() !=
                conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
            if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment, TableMirror tableMirror) {
        EnvironmentTable et = tableMirror.getEnvironments().get(environment);
        if (isNull(et)) {
            et = new EnvironmentTable(tableMirror);
            tableMirror.getEnvironments().put(environment, et);
        }
        return et;
    }

    /*

     */
    public void loadLegacyConversionWrapperForTestData(LegacyConversionWrapper legacyConversionWrapper) {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        // Add the DatabaseSpec
        DatasetDto dataset = null;
        if (isNull(conversionResult.getDataset())) {
            dataset = new DatasetDto();
            conversionResult.setDataset(dataset);
        } else {
            dataset = conversionResult.getDataset();
        }

        DatasetDto finalDataset = dataset;
        legacyConversionWrapper.getDatabases().forEach((dbName, dbMirror) -> {
            // Break apart the tables.
            Map<String, TableMirror> tableMirrorHolder = new HashMap<>();//
            // Make new map
            tableMirrorHolder.putAll(dbMirror.getTableMirrors());
            // Remove tables from DBMirror
            dbMirror.getTableMirrors().clear();
            // Persist the DBMirror
            try {
                dbMirror = getDbMirrorRepository().save(conversionResult.getKey(), dbMirror);
            } catch (RepositoryException e) {
                throw new RuntimeException("Failed to load references from repository", e);
            }

            DatasetDto.DatabaseSpec dbSpec = finalDataset.getDatabase(dbName);
            if (isNull(dbSpec)) {
                dbSpec = new DatasetDto.DatabaseSpec();
                finalDataset.getDatabases().add(dbSpec);
                dbSpec.setDatabaseName(dbName);
            }
            DBMirror finalDbMirror = dbMirror;
            DatasetDto.DatabaseSpec finalDbSpec = dbSpec;
            tableMirrorHolder.forEach((tableName, tableMirror) -> {
                // Persist each table.
                tableMirror.getEnvironments().forEach((env, et) -> {
                    // Cleanup any previous run values.
                    et.getSql().clear();
                    et.getCleanUpSql().clear();
                    et.getIssues().clear();
                    et.getErrors().clear();
                });
                try {
                    getTableMirrorRepository().save(conversionResult.getKey(), dbName, tableMirror);
                } catch (RepositoryException e) {
                    throw new RuntimeException("Failed to load references from repository", e);
                }
                finalDbSpec.getTables().add(tableName);
            });
        });
    }

    public Boolean AVROCheck(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        // Check for AVRO
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        if (TableUtils.isAVROSchemaBased(let)) {
            log.info("{}: is an AVRO table.", let.getName());
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            log.debug("{}: Original AVRO Schema path: {}", let.getName(), leftPath);
                /* Checks:
                - Is Path prefixed with a protocol?
                    - (Y) Does it match the LEFT's hcfsNamespace.
                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
                                 can't guarantee that we can retrieve the file.
                    - (N) Leave it and copy the file to the same relative path on the RIGHT
                 */
            String leftNamespace = NamespaceUtils.getNamespace(leftPath);
            if (nonNull(leftNamespace)) {
                log.info("{}: Namespace found: {}", let.getName(), leftNamespace);
                rightPath = NamespaceUtils.replaceNamespace(leftPath, conversionResult.getTargetNamespace());
            } else {
                // No Protocol defined.  So we're assuming that its a relative path to the
                // defaultFS
                String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                log.info("{}: {}", let.getName(), rpath);
                ret.addIssue(rpath);
                rightPath = leftPath;
                relative = Boolean.TRUE;
            }

            // TODO: Fix
                /*
                if (nonNull(leftPath) && nonNull(rightPath) && config.isCopyAvroSchemaUrls() && job.isExecute()) {
                    // Copy over.
                    log.info("{}: Attempting to copy AVRO schema file to target cluster.", let.getName());
                    try {
                        CommandReturn cr = null;
                        if (relative) {
                            // checked..
                            rightPath = conversionResult.getTargetNamespace() + rightPath;
                        }
                        log.info("AVRO Schema COPY from: {} to {}", leftPath, rightPath);
                        // Ensure the path for the right exists.
                        String parentDirectory = NamespaceUtils.getParentDirectory(rightPath);
                        if (nonNull(parentDirectory)) {
                            cr = cliEnvironment.processInput("mkdir -p " + parentDirectory);
                            if (cr.isError()) {
                                ret.addError("Problem creating directory " + parentDirectory + ". " + cr.getError());
                                rtn = Boolean.FALSE;
                            } else {
                                cr = cliEnvironment.processInput("cp -f " + leftPath + " " + rightPath);
                                if (cr.isError()) {
                                    ret.addError("Problem copying AVRO schema file from " + leftPath + " to " + parentDirectory + ".\n```" + cr.getError() + "```");
                                    rtn = Boolean.FALSE;
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.error("{}: AVRO file copy issue", ret.getName(), t);
                        ret.addError(t.getMessage());
                        rtn = Boolean.FALSE;
                    }
                } else {
                    log.info("{}: did NOT attempt to copy AVRO schema file to target cluster.", let.getName());
                }
                tableMirror.addStep("AVRO", "Checked");

                 */
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }
        return rtn;
    }


    /**
     * Generates cleanup SQL script for a specific environment and database.
     *
     * @param conversionResult The conversion result containing database mirrors
     * @param environment      The target environment
     * @param database         The database name
     * @return SQL cleanup script as a string, or null if no cleanup is needed
     */
    public String executeCleanUpSql(ConversionResult conversionResult, Environment environment, String database) {
        StringBuilder sb = new StringBuilder();
        AtomicBoolean found = new AtomicBoolean(Boolean.FALSE);
        sb.append("-- EXECUTION CLEANUP script for ").append(database).append(" on ").append(environment).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date())).append("\n\n");

        try {
            DBMirror dbMirror = getDbMirrorRepository().findByName(conversionResult.getKey(), database).orElseThrow(() ->
                    new IllegalStateException("Couldn't locate database: " + database + " for conversion " + conversionResult.getKey()));
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        Map<String, TableMirror> tableMirrors = null;
        try {
            tableMirrors = getTableMirrorRepository().findByDatabase(conversionResult.getKey(), database);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        tableMirrors.forEach((tableName, tableMirror) -> {
            if (tableMirror.isThereCleanupSql(environment)) {
                sb.append("\n--    Cleanup script: ").append(tableName).append("\n");
                for (Pair pair : tableMirror.getCleanUpSql(environment)) {
                    sb.append(pair.getAction());
                    // Skip ';' when it's a comment
                    // https://github.com/cloudera-labs/hms-mirror/issues/33
                    if (!pair.getAction().trim().startsWith("--")) {
                        sb.append(";\n");
                        found.set(Boolean.TRUE);
                    } else {
                        sb.append("\n");
                    }
                }
            } else {
                sb.append("\n");
            }
        });

        if (found.get())
            return sb.toString();
        else
            return null;
    }

    /**
     * Generates execution SQL script for a specific environment and database.
     *
     * @param conversionResult The conversion result containing database mirrors
     * @param environment      The target environment
     * @param database         The database name
     * @return SQL execution script as a string, or null if no SQL is needed
     */
    public String executeSql(ConversionResult conversionResult, Environment environment, String database) {
        StringBuilder sb = new StringBuilder();
        AtomicBoolean found = new AtomicBoolean(false);
        sb.append("-- EXECUTION script for ").append(database).append(" on ").append(environment).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
        sb.append("-- These are the command run on the ").append(environment).append(" cluster when `-e` is used.\n");

        DBMirror dbMirror = null;
        try {
            dbMirror = getDbMirrorRepository().findByName(conversionResult.getKey(), database).orElseThrow(() ->
                    new IllegalStateException("Couldn't locate DBMirror for " + database + " in " + conversionResult.getKey() + " conversion result."));
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        List<Pair> dbSql = dbMirror.getSql(environment);
        if (dbSql != null && !dbSql.isEmpty()) {
            for (Pair sqlPair : dbSql) {
                sb.append("-- ").append(sqlPair.getDescription()).append("\n");
                sb.append(sqlPair.getAction()).append(";\n");
                found.set(Boolean.TRUE);
            }
        }

        Map<String, TableMirror> tableMirrors = null;
        try {
            tableMirrors = getTableMirrorRepository().findByDatabase(conversionResult.getKey(), dbMirror.getName());
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        tableMirrors.forEach((tableName, tableMirror) -> {
            sb.append("\n--    Table: ").append(tableName).append("\n");
            if (tableMirror.isThereSql(environment)) {
                for (Pair pair : tableMirror.getSql(environment)) {
                    sb.append(pair.getAction()).append(";\n");
                    found.set(Boolean.TRUE);
                }
            } else {
                sb.append("\n");
            }

        });
        if (found.get())
            return sb.toString();
        else
            return null;
    }

    /**
     * Generates a detailed report for a specific database.
     *
     * @return Markdown report as a string
     * @throws JsonProcessingException if there's an error processing JSON/YAML
     * @LegacyDBMirror
     */
    public String toReport(LegacyDBMirror dbMirror) throws JsonProcessingException {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
//        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        // TODO: Should we build this from ConversionResult?
        ConversionResult config = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
//        HmsMirrorConfig config = getExecutionContextService().getHmsMirrorConfig().orElseThrow(() ->
//                new IllegalStateException("HmsMirrorConfig not set."));
        RunStatus runStatus = getExecutionContextService().getRunStatus().orElseThrow(() ->
                new IllegalStateException("RunStatus not set."));

        StringBuilder sb = new StringBuilder();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sb.append("# HMS-Mirror for: ").append(dbMirror.getName()).append("\n\n");
        sb.append(ReportingConf.substituteVariablesFromManifest("v.${HMS-Mirror-Version}")).append("\n");
        sb.append("---\n").append("## Run Log\n\n");
        sb.append("| Date | Elapsed Time | Status\n");
        sb.append("|:---|:---|:---|\n");
        Date current = new Date();
        BigDecimal elsecs = new BigDecimal(runStatus.getDuration())
                .divide(new BigDecimal(1000), 2, RoundingMode.HALF_UP);
        DecimalFormat eldecf = new DecimalFormat("#,###.00");
        DecimalFormat lngdecf = new DecimalFormat("#,###");
        String elsecStr = eldecf.format(elsecs);

        sb.append("| ").append(df.format(new Date()))
                .append(" | ").append(elsecStr).append(" secs | ")
                .append(runStatus.getProgress()).append("|\n\n");

        sb.append("**Comment**\n");
        sb.append("> ").append(runStatus.getComment()).append("\n\n");

        sb.append("## Config:\n");

//        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String yamlStr = yamlMapper.writeValueAsString(config);
        // Mask User/Passwords in Control File
        yamlStr = yamlStr.replaceAll("user:\\s\".*\"", "user: \"*****\"");
        yamlStr = yamlStr.replaceAll("password:\\s\".*\"", "password: \"*****\"");
        sb.append("```\n");
        sb.append(yamlStr).append("\n");
        sb.append("```\n\n");

        if (runStatus.hasConfigMessages()) {
            sb.append("### Config Adjustment Messages:\n");
            for (String message : runStatus.getConfigMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }
        if (runStatus.hasErrors()) {
            sb.append("### Config Errors:\n");
            for (String message : runStatus.getErrorMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }
        if (runStatus.hasWarnings()) {
            sb.append("### Config Warnings:\n");
            for (String message : runStatus.getWarningMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }

//        DBMirror dbMirror = conversionResult.getDatabases().get(database);

        sb.append("## Database SQL Statement(s)").append("\n\n");

        for (Environment environment : Environment.values()) {
            if (dbMirror.getSql(environment) != null && !dbMirror.getSql(environment).isEmpty()) {
                sb.append("### ").append(environment.toString()).append("\n\n");
                sb.append("```\n");
                for (Pair sqlPair : dbMirror.getSql(environment)) {
                    sb.append("-- ").append(sqlPair.getDescription()).append("\n");
                    sb.append(sqlPair.getAction()).append("\n");
                }
                sb.append("```\n");
            }
        }

        sb.append("\n");

        sb.append("## DB Issues").append("\n\n");
        // Issues
        if (dbMirror.isThereAnIssue()) {
            Set<Environment> environments = new HashSet<>();
            environments.add(Environment.LEFT);
            environments.add(Environment.RIGHT);

            for (Environment env : environments) {
                if (dbMirror.getProblemSQL().get(env) != null) {
                    sb.append("### Problem SQL Statements for ").append(env.toString()).append("\n\n");
                    sb.append("```\n");
                    // For each entry in the dbMirror.getProblemSQL().get(environment) print the SQL
                    for (Map.Entry<String, String> sqlItem : dbMirror.getProblemSQL().get(env).entrySet()) {
                        sb.append("-- ").append(sqlItem.getValue()).append("\n");
                        sb.append(sqlItem.getKey()).append("\n");
                    }
                    sb.append("```\n");
                }

                List<String> issues = dbMirror.getIssuesList(env);
                if (issues != null) {
                    sb.append("### Advisories for ").append(env).append("\n\n");
                    for (String issue : issues) {
                        sb.append("* ").append(issue).append("\n");
                    }
                }
            }

        } else {
            sb.append("none\n");
        }

        sb.append("\n## Table Status (").append(dbMirror.getTableMirrors().size()).append(")  ");
        sb.append(dbMirror.getPhaseSummaryString()).append("\n\n");

        sb.append("*NOTE* SQL in this report may be altered by the renderer.  Do NOT COPY/PASTE from this report.  Use the LEFT|RIGHT_execution.sql files for accurate scripts\n\n");

        sb.append("<table>").append("\n");
        sb.append("<tr>").append("\n");
        sb.append("<th style=\"test-align:left\">Table</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Strategy</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Source<br/>Managed</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Source<br/>ACID</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Phase<br/>State</th>").append("\n");
        sb.append("<th style=\"test-align:right\">Duration</th>").append("\n");
//        sb.append("<th style=\"test-align:right\">Partition<br/>Count</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Steps</th>").append("\n");
//        if (dbMirror.hasActions()) {
//            sb.append("<th style=\"test-align:left\">Actions</th>").append("\n");
//        }
        if (dbMirror.hasAddedProperties()) {
            sb.append("<th style=\"test-align:left\">Added<br/>Properties</th>").append("\n");
        }
        if (dbMirror.hasStatistics()) {
            sb.append("<th style=\"test-align:left\">Stats</th>").append("\n");
        }
        if (dbMirror.hasIssues() || dbMirror.hasErrors()) {
            sb.append("<th style=\"test-align:left\">Issues</th>").append("\n");
        }
        sb.append("<th style=\"test-align:left\">SQL</th>").append("\n");
        sb.append("</tr>").append("\n");

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            sb.append("<tr>").append("\n");
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
            // table
            sb.append("<td>").append(table).append("</td>").append("\n");
            // Strategy
            sb.append("<td>").append(tblMirror.getStrategy()).append("</td>").append("\n");
            // Source Managed
            sb.append("<td>");
            if (TableUtils.isManaged(let)) {
                sb.append("X");
            }
            sb.append("</td>").append("\n");
            // Source ACID
            sb.append("<td>").append("\n");
            if (TableUtils.isACID(let)) {
                sb.append("X");
            }
            sb.append("</td>").append("\n");
            // phase state
            sb.append("<td>").append(tblMirror.getPhaseState().toString()).append("</td>").append("\n");

            // Stage Duration
            BigDecimal secs = new BigDecimal(tblMirror.getStageDuration()).divide(new BigDecimal(1000));///1000
            DecimalFormat decf = new DecimalFormat("#,###.00");
            String secStr = decf.format(secs);
            sb.append("<td>").append(secStr).append("</td>").append("\n");

            // Partition Count
//            sb.append("<td>").append(let.getPartitioned() ?
//                    let.getPartitions().size() : " ").append("</td>").append("\n");

            // Steps
            sb.append("<td>\n");
            sb.append("<table>\n");
            for (com.cloudera.utils.hms.mirror.Marker entry : tblMirror.getSteps()) {
                sb.append("<tr>\n");
                sb.append("<td>");
                sb.append(entry.getMark());
                sb.append("</td>");
                sb.append("<td>");
                sb.append(entry.getDescription());
                sb.append("</td>");
                sb.append("<td>");
                if (entry.getAction() != null)
                    sb.append(entry.getAction());
                sb.append("</td>");
                sb.append("</tr>\n");
            }
            sb.append("</table>\n");
            sb.append("</td>\n");

            // Actions
//            if (dbMirror.hasActions()) {
//                // LEFT Table Actions
//                Iterator<String> a1Iter = tblMirror.getTableActions(Environment.LEFT).iterator();
//                sb.append("<td>").append("\n");
//                sb.append("<table>");
//                while (a1Iter.hasNext()) {
//                    sb.append("<tr>");
//                    String item = a1Iter.next();
//                    sb.append("<td style=\"text-align:left\">").append(item).append(";</td>");
//                    sb.append("</tr>");
//                }
//                sb.append("</table>");
//                sb.append("</td>").append("\n");
//
//                // RIGHT Table Actions
//                Iterator<String> a2Iter = tblMirror.getTableActions(Environment.RIGHT).iterator();
//                sb.append("<td>").append("\n");
//                sb.append("<table>");
//                while (a2Iter.hasNext()) {
//                    sb.append("<tr>");
//                    String item = a2Iter.next();
//                    sb.append("<td style=\"text-align:left\">").append(item).append(";</td>");
//                    sb.append("</tr>");
//                }
//                sb.append("</table>");
//                sb.append("</td>").append("\n");
//            }

            // Properties
            if (dbMirror.hasAddedProperties()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (!entry.getValue().getAddProperties().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (Map.Entry<String, String> prop : entry.getValue().getAddProperties().entrySet()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(prop.getKey());
                            sb.append("</td>\n");
                            sb.append("<td>");
                            sb.append(prop.getValue());
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // Statistics
            if (dbMirror.hasStatistics()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (!entry.getValue().getStatistics().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (Map.Entry<String, Object> prop : entry.getValue().getStatistics().entrySet()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(prop.getKey());
                            sb.append("</td>\n");
                            sb.append("<td>");
                            if (prop.getValue() instanceof Double || prop.getValue() instanceof Long) {
                                sb.append(lngdecf.format(prop.getValue()));
                            } else {
                                sb.append(prop.getValue().toString());
                            }
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                        if (entry.getValue().getPartitioned()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(MirrorConf.PARTITION_COUNT);
                            sb.append("</td>\n");
                            sb.append("<td>");
                            sb.append(entry.getValue().getPartitions().size());
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // Issues Reporting
            if (dbMirror.hasIssues() || dbMirror.hasErrors()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (!entry.getValue().getIssues().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th>");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (String issue : entry.getValue().getIssues()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(issue);
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                    if (!entry.getValue().getErrors().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th>");
                        sb.append(entry.getKey()).append(" - Error(s)");
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append("<ul>");
                        for (String error : entry.getValue().getErrors()) {
                            sb.append("<li>");
                            sb.append(error);
                            sb.append("</li>");
                        }
                        sb.append("</ul>");
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // SQL Output
            sb.append("<td>\n");
            sb.append("<table>");
            for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                if (!entry.getValue().getSql().isEmpty()) {
                    sb.append("<tr>\n");
                    sb.append("<th colspan=\"2\">");
                    sb.append(entry.getKey());
                    sb.append("</th>\n");
                    sb.append("</tr>").append("\n");

                    for (Pair pair : entry.getValue().getSql()) {
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append(pair.getDescription());
                        sb.append("</td>\n");
                        sb.append("<td>");
                        sb.append(pair.getAction());
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                    if (!entry.getValue().getCleanUpSql().isEmpty()) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append("=== SQL CleanUp ===");
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");
                    }
                    for (Pair pair : entry.getValue().getCleanUpSql()) {
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append(pair.getDescription());
                        sb.append("</td>\n");
                        sb.append("<td>");
                        sb.append(pair.getAction());
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                }
            }
            sb.append("</table>");
            sb.append("</td>").append("\n");
            sb.append("</tr>").append("\n");
        }
        sb.append("</table>").append("\n");

        if (!dbMirror.getFilteredOut().isEmpty()) {
            sb.append("\n## Skipped Tables/Views\n\n");

            sb.append("| Table / View | Reason |\n");
            sb.append("|:---|:---|\n");
            for (Map.Entry<String, String> entry : dbMirror.getFilteredOut().entrySet()) {
                sb.append("| ").append(entry.getKey()).append(" | ").append(entry.getValue()).append(" |\n");
            }
        }
        return sb.toString();
    }

    /**
     * Counts the number of tables that have not been successfully processed.
     * Tables are considered unsuccessful if their phase state is not CALCULATED_SQL or PROCESSED.
     *
     * @param conversionResult The conversion result containing database mirrors
     * @return The count of unsuccessful tables
     */
    public int getUnsuccessfullTableCount(ConversionResult conversionResult) {
        int count = 0;
        for (DBMirror dbMirror : conversionResult.getDatabases().values()) {
            for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                switch (tableMirror.getPhaseState()) {
                    // Don't count successful conversions.
                    case CALCULATED_SQL:
                    case PROCESSED:
                        break;
                    default:
                        count++;
                }
            }
        }
        return count;
    }

    /**
     * Adds a database to the conversion result or returns existing one.
     * If the database already exists in the databases map, it returns the existing DBMirror.
     * Otherwise, creates a new DBMirror, adds it to the map, and returns it.
     *
     * @param conversionResult The conversion result to add the database to
     * @param database         The database name
     * @return The DBMirror for the database
    public DBMirror addDatabase(ConversionResult conversionResult, String database) {
    if (conversionResult.getDatabases().containsKey(database)) {
    return conversionResult.getDatabases().get(database);
    } else {
    DBMirror dbs = new DBMirror();
    dbs.setName(database);
    conversionResult.getDatabases().put(database, dbs);
    return dbs;
    }
    }
     */

    /**
     * Retrieves a database from the conversion result.
     *
     * @param conversionResult The conversion result containing databases
     * @param database         The database name
     * @return The DBMirror for the database, or null if not found
    public DBMirror getDatabase(ConversionResult conversionResult, String database) {
    return conversionResult.getDatabases().get(database);
    }
     */

}
