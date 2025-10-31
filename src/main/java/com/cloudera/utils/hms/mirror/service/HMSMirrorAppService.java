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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.*;
import com.cloudera.utils.hms.mirror.repository.ConversionResultRepository;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
import com.cloudera.utils.hms.mirror.repository.RunStatusRepository;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import com.cloudera.utils.hms.stage.ReturnStatus;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static java.lang.Thread.sleep;
import static java.util.Objects.nonNull;

/**
 * The HMSMirrorAppService class provides the main application service logic for handling
 * tasks such as database operations, configuration management, reporting, and data transfer.
 * It integrates various specialized services to perform its operations efficiently.
 * Fields:
 * - configService: Manages application configuration parameters.
 * - connectionPoolService: Handles the connection pooling for database connections.
 * - databaseService: Manages operations related to database interaction.
 * - environmentService: Provides services related to the application environment.
 * - executeSessionService: Controls execution sessions for the application.
 * - reportWriterService: Handles generating and writing reports.
 * - tableService: Manages interactions related to database tables.
 * - translatorService: Executes data translation or mapping logic.
 * - transferService: Responsible for managing data transfer operations.
 * - log: Used for logging information or events related to the application.
 */
@Service
@Getter
@Slf4j
@RequiredArgsConstructor
public class HMSMirrorAppService {

    @NonNull
    private final ConfigService configService;
    @NonNull
    private final ConnectionPoolService connectionPoolService;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final DatabaseService databaseService;
    @NonNull
    private final EnvironmentService environmentService;
    @NonNull
    private final ReportWriterService reportWriterService;
    @NonNull
    private final SessionManager sessionManager;
    @NonNull
    private final TableService tableService;
    @NonNull
    private final TranslatorService translatorService;
    @NonNull
    private final TransferService transferService;
    @NonNull
    private final CliEnvironment cliEnvironment;
    @NonNull
    private final JobManagementService jobManagementService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final ConversionResultRepository conversionResultRepository;
    @NonNull
    private final RunStatusRepository runStatusRepository;
    @NonNull
    private final ReportService reportService;
    @NonNull
    private final DBMirrorRepository dbMirrorRepository;
    @NonNull
    private final TableMirrorRepository tableMirrorRepository;

    public long getReturnCode() {
        long rtn = 0L;
        // TODO: Fix
        /*
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        ConversionResult conversionResult = executeSessionService.getSession().getConversionResult();
        rtn = runStatus.getReturnCode();
        // If app ran, then check for unsuccessful table conversions.
        if (rtn == 0) {
            rtn = conversionResultService.getUnsuccessfullTableCount(conversionResult);
        }
        */
        return rtn;
    }

    public long getWarningCode() {
        long rtn = 0L;
        // TODO: Fix;
        /*
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        ConversionResult conversionResult = executeSessionService.getSession().getConversionResult();
        rtn = runStatus.getWarningCode();
         */
        return rtn;
    }

    /*
    In this case, we want the session to remain the same as the calling thread.
    The session for this thread is the same via the "ThreadPoolConfigurator" and the Decorator process that inherits
    the session from the calling thread.
     */
    @Async("cliExecutionThreadPool")
    public CompletableFuture<Boolean> cliRun(ConversionResult conversionResult) {
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

        initializeExecutionContext();

        // Save the ConversionResult object.
        try {
            getConversionResultRepository().save(conversionResult);
        } catch (RepositoryException e) {
            log.error("Issue saving ConversionResult", e);
        }
        // Save the RunStatus object
        try {
            getRunStatusRepository().saveByKey(conversionResult.getKey(), conversionResult.getRunStatus());
        } catch (RepositoryException e) {
            log.error("Issue saving RunStatus", e);
        }

        CompletableFuture<Boolean> theWork;

        theWork = CompletableFuture.supplyAsync(() -> theWork(conversionResult));

        return theWork;
    }

    /*
    This method is where all the work starts.  The configuration and asks are configured before this point.  The passed in
    objects are 'deep' copies of the working objects in the ExecuteSession and will be used to drive forward.
     */
    @Async("executionThreadPool")
    public CompletableFuture<Boolean> run(ConversionResult conversionResult) {
        // Add the conversionResult to the current context.
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

        initializeExecutionContext();

        // Save the ConversionResult object.
        try {
            getConversionResultRepository().save(conversionResult);
        } catch (RepositoryException e) {
            log.error("Issue saving ConversionResult", e);
        }
        // Save the RunStatus object
        try {
            getRunStatusRepository().saveByKey(conversionResult.getKey(), conversionResult.getRunStatus());
        } catch (RepositoryException e) {
            log.error("Issue saving RunStatus", e);
        }
        CompletableFuture<Boolean> theWork;

        theWork = CompletableFuture.supplyAsync(() -> theWork(conversionResult));

        return theWork;
    }

    /**
     * Initializes the execution context and prepares the ConversionResult for processing.
     * Sets up comment, start time, and initial progress status.
     *
     * @return ConversionResult from the execution context
     * @throws IllegalStateException if ConversionResult is not set in thread context
     */
    private ConversionResult initializeExecutionContext() {
        // Validate that we are in the same thread and that the conversionResult is set.
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult is not set in the current thread context."));

        RunStatus runStatus = getExecutionContextService().getRunStatus().orElseThrow(() ->
                new IllegalStateException("RunStatus is not set in the current thread context"));

        // Transfer the Comment.
        if (runStatus.getComment() != null) {
            // Comment already set in RunStatus
        } else {
            runStatus.setComment("No comments provided for this run.  Consider adding one for easier tracking.");
        }

        // Reset Start time to the actual 'execution' start time.
        runStatus.setStart(new Date());
        runStatus.setProgress(ProgressEnum.STARTED);
        log.info("Starting HMSMirrorAppService.run()");

        return conversionResult;
    }

    /**
     * Validates the application configuration.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @return true if configuration is valid, false otherwise
     */
    private Boolean validateConfiguration(ConversionResult conversionResult, RunStatus runStatus) {
        runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.IN_PROGRESS);
        if (configService.validate()) {
            runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.COMPLETED);
            return Boolean.TRUE;
        } else {
            runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.ERRORED);
            reportWriterService.wrapup();
            runStatus.setProgress(ProgressEnum.FAILED);
            try {
                getConversionResultRepository().save(conversionResult);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            try {
                getRunStatusRepository().saveByKey(conversionResult.getKey(), runStatus);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            return Boolean.FALSE;
        }
    }

    /**
     * Sets up database connections and initializes the Kerberos environment.
     * Skipped for mock test datasets.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @return true if connections are successfully established, false otherwise
     */
    private Boolean setupConnections(ConversionResult conversionResult, RunStatus runStatus) {
        if (!conversionResult.isMockTestDataset()) {
            try {
                runStatus.setStage(StageEnum.VALIDATE_CONNECTION_CONFIG, CollectionEnum.IN_PROGRESS);
                configService.validateForConnections();
                runStatus.setStage(StageEnum.VALIDATE_CONNECTION_CONFIG, CollectionEnum.COMPLETED);

                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.IN_PROGRESS);

                // Make sure that the Kerberos Environment is initialized.
                environmentService.setupGSS();

                // The 'reset' should close and init all connections.
                boolean lclConnCheck = connectionPoolService.init();
                if (lclConnCheck) {
                    runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                    runStatus.setProgress(ProgressEnum.FAILED);
                    connectionPoolService.close();
                    return Boolean.FALSE;
                }
//            } catch (SQLException sqle) {
//                log.error("Issue refreshing connections pool", sqle);
//                runStatus.addError(CONNECTION_ISSUE, sqle.getMessage());
//                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
//                connectionPoolService.close();
//                runStatus.setProgress(ProgressEnum.FAILED);
//                return Boolean.FALSE;
//            } catch (URISyntaxException e) {
//                log.error("URI issue with connections pool", e);
//                runStatus.addError(CONNECTION_ISSUE, e.getMessage());
//                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
//                connectionPoolService.close();
//                runStatus.setProgress(ProgressEnum.FAILED);
//                return Boolean.FALSE;
//            } catch (SessionException se) {
//                log.error("Issue with Session", se);
//                runStatus.addError(SESSION_ISSUE, se.getMessage());
//                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
//                connectionPoolService.close();
//                runStatus.setProgress(ProgressEnum.FAILED);
//                return Boolean.FALSE;
//            } catch (EncryptionException ee) {
//                log.error("Issue with Decryption", ee);
//                runStatus.addError(ENCRYPTION_ISSUE, ee.getMessage());
//                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
//                connectionPoolService.close();
//                runStatus.setProgress(ProgressEnum.FAILED);
//                return Boolean.FALSE;
            } catch (RuntimeException rte) {
                log.error("Runtime Issue", rte);
                runStatus.addError(SESSION_ISSUE, rte.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            }
        } else {
            runStatus.setStage(StageEnum.VALIDATE_CONNECTION_CONFIG, CollectionEnum.SKIPPED);
            runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.SKIPPED);
        }

        try {
            getRunStatusRepository().saveByKey(conversionResult.getKey(), runStatus);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        return Boolean.TRUE;
    }

    /**
     * Loads environment variables from the database clusters.
     * Skipped for mock test datasets.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @return true if environment variables loaded successfully, false otherwise
     */
    private Boolean loadEnvironmentVariables(ConversionResult conversionResult, RunStatus runStatus) {
        if (!conversionResult.isMockTestDataset()) {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.IN_PROGRESS);
            Boolean rtn = databaseService.loadEnvironmentVars();
            if (rtn) {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
                return Boolean.TRUE;
            } else {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.ERRORED);
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            }
        } else {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.SKIPPED);
            return Boolean.TRUE;
        }
    }

    /**
     * Discovers databases from the dataset and creates DBMirror instances.
     * Retrieves database definitions from LEFT and RIGHT clusters.
     * Skipped for mock test datasets.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @return list of futures for table collection operations
     */
    private List<CompletableFuture<ReturnStatus>> discoverDatabases(ConversionResult conversionResult, RunStatus runStatus) {
        List<CompletableFuture<ReturnStatus>> gtf = new ArrayList<>();
        Boolean rtn = Boolean.TRUE;

        log.info("RunStatus Stage: {} is {}", StageEnum.DATABASES, runStatus.getStage(StageEnum.DATABASES));
        if (!conversionResult.isMockTestDataset()) {
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.IN_PROGRESS);

            for (DatasetDto.DatabaseSpec database : conversionResult.getDataset().getDatabases()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
                DBMirror dbMirror = new DBMirror();
//                dbMirror.setKey(conversionResult.getKey());
                dbMirror.setName(database.getDatabaseName());

                try {
                    // Get the Database definitions for the LEFT and RIGHT clusters.
                    if (getDatabaseService().getDatabase(dbMirror, Environment.LEFT)) {
                        getDatabaseService().getDatabase(dbMirror, Environment.RIGHT);
                    } else {
                        // LEFT DB doesn't exist.
                        dbMirror.addIssue(Environment.LEFT, "DB doesn't exist. Check permissions for user running process");
                        runStatus.getOperationStatistics().getFailures().incrementDatabases();
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    log.error("Issue getting databases", se);
                    runStatus.addError(MISC_ERROR, "Issue getting databases");
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    reportWriterService.wrapup();
                    connectionPoolService.close();
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return null;
                } catch (RuntimeException rte) {
                    log.error("Runtime Issue", rte);
                    runStatus.addError(MISC_ERROR, rte.getMessage());
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    reportWriterService.wrapup();
                    connectionPoolService.close();
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return null;
                }

                try {
                    dbMirror = getDbMirrorRepository().save(conversionResult.getKey(), dbMirror);
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }

                // Build out the table list in a database (launches async table collection)
                if (!conversionResult.getJob().isDatabaseOnly()) {
                    runStatus.setStage(StageEnum.TABLES, CollectionEnum.IN_PROGRESS);
                    CompletableFuture<ReturnStatus> gt = getTableService().getTables(conversionResult, dbMirror);
                    gtf.add(gt);
                }
            }

            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.COMPLETED);

            try {
                getRunStatusRepository().saveByKey(conversionResult.getKey(), runStatus);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }

        } else {
            log.warn("DAVID DEBUG: Skipping database and table stages for test data");
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.SKIPPED);
            runStatus.setStage(StageEnum.TABLES, CollectionEnum.SKIPPED);
        }

        return gtf;
    }

    /**
     * Collects tables by waiting for table collection futures to complete.
     * Checks success/failure status of table collection operations.
     * Skipped for database-only migrations or mock test datasets.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @param gtf list of table collection futures
     * @return true if table collection succeeded, false otherwise
     */
    private Boolean collectTables(ConversionResult conversionResult, RunStatus runStatus,
                                   List<CompletableFuture<ReturnStatus>> gtf) {
        Boolean rtn = Boolean.TRUE;

        if (!conversionResult.isMockTestDataset() && !conversionResult.getJob().isDatabaseOnly()) {
            // Wait for all the CompletableFutures to finish. These were collecting the Databases and Table list.
            CompletableFuture.allOf(gtf.toArray(new CompletableFuture[0])).join();

            // Check that all the CompletableFutures in 'gtf' passed with ReturnStatus.Status.SUCCESS.
            for (CompletableFuture<ReturnStatus> sf : gtf) {
                try {
                    ReturnStatus rs = sf.get();
                    if (nonNull(rs)) {
                        if (rs.getStatus() == ReturnStatus.Status.SUCCESS) {
                            runStatus.getOperationStatistics().getSuccesses().incrementDatabases();
                        } else {
                            rtn = Boolean.FALSE;
                            runStatus.getOperationStatistics().getFailures().incrementDatabases();
                        }
                    } else {
                        log.error("ReturnStatus is null in gathering table.");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Interrupted Table collection", e);
                    rtn = Boolean.FALSE;
                }
            }

            runStatus.setStage(StageEnum.TABLES, CollectionEnum.COMPLETED);
            gtf.clear(); // reset

            // Failure, report and exit with FALSE
            if (!rtn) {
                runStatus.setStage(StageEnum.TABLES, CollectionEnum.ERRORED);
                runStatus.addError(MessageCode.COLLECTING_TABLES);
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            }
        }

        return rtn;
    }

    /**
     * Builds the Global Location Map from warehouse plans if configured.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @return true if GLM build succeeded, false otherwise
     */
    private Boolean buildGlobalLocationMap(ConversionResult conversionResult, RunStatus runStatus) {
        if (conversionResult.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
            runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.SKIPPED);
            return Boolean.TRUE;
        } else {
            try {
                int defaultConsolidationLevel = 1;
                runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.IN_PROGRESS);

                if (conversionResult.getConfig().loadMetadataDetails()) {
                    databaseService.buildDatabaseSources(defaultConsolidationLevel, false);
                    translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(false, defaultConsolidationLevel);
                    runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.SKIPPED);
                }

                return Boolean.TRUE;
            } catch (EncryptionException | SessionException | RequiredConfigurationException | MismatchException e) {
                log.error("Issue validating configuration", e);
                runStatus.addError(RUNTIME_EXCEPTION, e.getMessage());
                log.error("Configuration is not valid.  Exiting.");

                runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.ERRORED);
                runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.ERRORED);

                reportWriterService.wrapup();
                connectionPoolService.close();

                runStatus.setProgress(ProgressEnum.FAILED);

                return Boolean.FALSE;
            }
        }
    }

    /**
     * Builds database DDL for migration.
     * Skipped for VIEW migrations.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @return true if database building succeeded, false otherwise
     */
    private Boolean buildDatabases(ConversionResult conversionResult, RunStatus runStatus) {
        runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.IN_PROGRESS);
        Boolean rtn = Boolean.TRUE;

        if (!conversionResult.getConfig().getMigrateVIEW().isOn()) {
            if (getDatabaseService().build()) {
                runStatus.getOperationStatistics().getSuccesses().incrementDatabases();
            } else {
                runStatus.addError(MessageCode.DATABASE_CREATION);
                runStatus.getOperationStatistics().getFailures().incrementDatabases();
                rtn = Boolean.FALSE;
            }
            if (rtn) {
                runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.ERRORED);
                runStatus.addError(MessageCode.BUILDING_DATABASES_ISSUE);
            }
        } else {
            runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.SKIPPED);
            conversionResult.getDatabases().values().forEach(db -> {
                        db.addIssue(Environment.LEFT, "No Database DDL when migrating 'views'." +
                                "All database constructs expected to be in-place already.");
                        db.addIssue(Environment.RIGHT, "No Database DDL when migrating 'views'." +
                                "All database constructs expected to be in-place already.");
                    }
            );
        }

        return rtn;
    }

    /**
     * Processes (executes) database DDL SQL statements.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @param rtn current success status
     * @return true if database processing succeeded, false otherwise
     */
    private Boolean processDatabases(ConversionResult conversionResult, RunStatus runStatus, Boolean rtn) {
        runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.IN_PROGRESS);

        if (rtn) {
            if (conversionResult.getJobExecution().isExecute()) {
                if (getDatabaseService().execute()) {
                    runStatus.getOperationStatistics().getSuccesses().incrementDatabases();
                    runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.COMPLETED);
                } else {
                    runStatus.addError(MessageCode.DATABASE_CREATION);
                    runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.ERRORED);
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    rtn = Boolean.FALSE;
                }
            } else {
                runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.SKIPPED);
            }

            // Set error if issue during processing.
            if (!rtn)
                runStatus.addError(MessageCode.PROCESSING_DATABASES_ISSUE);
        } else {
            runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.SKIPPED);
        }

        return rtn;
    }

    /**
     * Loads table metadata and builds migration plans.
     * This is the most complex stage that handles table metadata loading, migration building,
     * and iterative completion checking.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @param gtf list of futures from table collection
     * @param rtn current success status
     * @return Set of migration executions ready for processing, or null if failed
     */
    private Set<ReturnStatus> loadTableMetadataAndBuild(ConversionResult conversionResult, RunStatus runStatus,
                                                         List<CompletableFuture<ReturnStatus>> gtf, Boolean rtn) {
        if (conversionResult.getJob().isDatabaseOnly()) {
            return new HashSet<>();
        }

        log.warn("DEBUG: Processing tables: isDatabaseOnly={}, isLoadingTestData={}, collectedDbs={}",
                conversionResult.getJob().isDatabaseOnly(), conversionResult.isMockTestDataset(),
                conversionResult.getDatabases().keySet());
        log.warn("DEBUG: Entering table processing section");

        Set<String> collectedDbs = conversionResult.getDatabases().keySet();
        List<CompletableFuture<ReturnStatus>> migrationFuture = new ArrayList<>();

        runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.IN_PROGRESS);
        if (rtn) {
            for (String database : collectedDbs) {
                DBMirror dbMirror;
                try {
                    // Load from DBMirror Repository.
                    dbMirror = dbMirrorRepository.findByName(conversionResult.getKey(), database).orElseThrow(() ->
                            new IllegalStateException("DBMirror Not Found"));
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
                Set<String> tables = dbMirror.getTableMirrors().keySet();
                for (String table : tables) {
                    TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                    gtf.add(tableService.getTableMetadata(conversionResult, dbMirror, tableMirror));
                }
            }

            runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.IN_PROGRESS);

            // Wait for all CompletableFutures to complete
            while (!gtf.isEmpty()) {
                Iterator<CompletableFuture<ReturnStatus>> iterator = gtf.iterator();
                while (iterator.hasNext()) {
                    CompletableFuture<ReturnStatus> future = iterator.next();
                    if (future.isDone()) {
                        try {
                            ReturnStatus returnStatus = future.get();
                            TableMirror lclTableMirror = returnStatus.getTableMirror();
                            DBMirror lclDBMirror = returnStatus.getDbMirror();

                            if (lclTableMirror.isRemove()) {
                                // Add to list of tables to be removed.
                                lclDBMirror.getFilteredOut().put(lclTableMirror.getName(), lclTableMirror.getRemoveReason());
                                // Remove the table from persistence.
                                try {
                                    getTableMirrorRepository().deleteByName(conversionResult.getKey(), lclDBMirror.getName(), lclTableMirror.getName());
                                } catch (RepositoryException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                // Save the TableMirrors.
                                try {
                                    getTableMirrorRepository().save(conversionResult.getKey(), lclDBMirror.getName(), lclTableMirror);
                                } catch (RepositoryException re) {
                                    throw new RuntimeException(re);
                                }

                                if (nonNull(returnStatus)) {
                                    switch (returnStatus.getStatus()) {
                                        case SUCCESS:
                                            runStatus.getOperationStatistics().getCounts().incrementTables();
                                            future.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                            runStatus.getOperationStatistics().getSuccesses().incrementTables();
                                            migrationFuture.add(getTransferService().build(conversionResult, lclDBMirror, lclTableMirror));
                                            break;
                                        case ERROR:
                                            runStatus.getOperationStatistics().getCounts().incrementTables();
                                            future.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                            break;
                                        case FATAL:
                                            runStatus.getOperationStatistics().getCounts().incrementTables();
                                            runStatus.getOperationStatistics().getFailures().incrementTables();
                                            rtn = Boolean.FALSE;
                                            future.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                            log.error("FATAL: ", future.get().getException());
                                        case NEXTSTEP:
                                            break;
                                        case SKIP:
                                            runStatus.getOperationStatistics().getCounts().incrementTables();
                                            runStatus.getOperationStatistics().getSkipped().incrementTables();
                                            future.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                            break;
                                    }
                                }
                            }
                        } catch (InterruptedException | ExecutionException | RuntimeException e) {
                            log.error("Interrupted Table collection", e);
                            rtn = Boolean.FALSE;
                        }
                        iterator.remove();
                    }
                }

                // If there are still futures pending, sleep briefly to avoid busy waiting
                if (!gtf.isEmpty()) {
                    try {
                        Thread.sleep(100); // Sleep for 100ms between checks
                    } catch (InterruptedException e) {
                        log.warn("Thread interrupted while waiting for futures to complete", e);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if (rtn) {
                runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.ERRORED);
                runStatus.addError(MessageCode.COLLECTING_TABLE_DEFINITIONS);
            }

            gtf.clear(); // reset
        } else {
            runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.SKIPPED);
            runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.SKIPPED);
        }

        Set<ReturnStatus> migrationExecutions = new HashSet<>();

        // Check the Migration Futures are done.
        while (!migrationFuture.isEmpty()) {
            Iterator<CompletableFuture<ReturnStatus>> iterator = migrationFuture.iterator();
            while (iterator.hasNext()) {
                CompletableFuture<ReturnStatus> future = iterator.next();
                if (future.isDone()) {
                    try {
                        ReturnStatus rs = future.get();
                        if (nonNull(rs)) {
                            // Only push SUCCESSFUL tables to the migrationExecutions list.
                            if (rs.getStatus() == ReturnStatus.Status.SUCCESS) {
                                migrationExecutions.add(rs);
                            }
                        } else {
                            log.error("ReturnStatus is NULL in migration build");
                        }
                    } catch (InterruptedException | ExecutionException | RuntimeException e) {
                        log.error("Interrupted Building Migrations", e);
                        rtn = Boolean.FALSE;
                    }
                    iterator.remove();
                }
            }

            // If there are still futures pending, sleep briefly to avoid busy waiting
            if (!migrationFuture.isEmpty()) {
                try {
                    Thread.sleep(100); // Sleep for 100ms between checks
                } catch (InterruptedException e) {
                    log.warn("Thread interrupted while waiting for futures to complete", e);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (rtn) {
            runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.COMPLETED);
        } else {
            runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.ERRORED);
        }

        migrationFuture.clear(); // reset

        // Save RunStatus
        try {
            getRunStatusRepository().saveByKey(conversionResult.getKey(), runStatus);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        return migrationExecutions;
    }

    /**
     * Validates environment SET SQL statements for each database.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @param rtn current success status
     * @return true if validation succeeded, false otherwise
     */
    private Boolean validateEnvironmentSets(ConversionResult conversionResult, RunStatus runStatus, Boolean rtn) {
        Set<String> collectedDbs = conversionResult.getDatabases().keySet();
        runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.IN_PROGRESS);

        if (rtn) {
            // Check the Unique SET statements.
            for (String database : collectedDbs) {
                DBMirror dbMirror = null;//conversionResultService.getDatabase(database);
                try {
                    dbMirror = getDbMirrorRepository().findByName(conversionResult.getKey(), database).orElseThrow(() ->
                            new IllegalStateException("DBMirror Not Found for  " + database));
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
                if (!databaseService.checkSqlStatements(dbMirror)) {
                    rtn = Boolean.FALSE;
                }
            }
            if (rtn) {
                runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.ERRORED);
                runStatus.addError(MessageCode.VALIDATE_SQL_STATEMENT_ISSUE);
            }
        } else {
            runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.SKIPPED);
        }

        return rtn;
    }

    /**
     * Executes table migration SQL statements.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @param rtn current success status
     * @param migrationExecutions set of migrations ready for execution
     * @return true if execution succeeded, false otherwise
     */
    private Boolean executeTables(ConversionResult conversionResult, RunStatus runStatus, Boolean rtn,
                                  Set<ReturnStatus> migrationExecutions) {
        runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.IN_PROGRESS);

        if (rtn) {
            if (conversionResult.getJobExecution().isExecute()) {
                List<CompletableFuture<ReturnStatus>> migrationFuture = new ArrayList<>();
                // Using the migrationExecute List, create futures for the table executions.
                for (ReturnStatus returnStatus : migrationExecutions) {
                    migrationFuture.add(getTransferService().execute(conversionResult, returnStatus.getDbMirror(), returnStatus.getTableMirror()));
                }

                // Wait for all the CompletableFutures to finish.
                CompletableFuture.allOf(migrationFuture.toArray(new CompletableFuture[0])).join();
                // Check that all the CompletableFutures in 'migrationFuture' passed with ReturnStatus.Status.SUCCESS.
                for (CompletableFuture<ReturnStatus> sf : migrationFuture) {
                    try {
                        ReturnStatus rs = sf.get();
                        if (nonNull(rs)) {
                            TableMirror tableMirror = rs.getTableMirror();
                            if (rs.getStatus() == ReturnStatus.Status.ERROR) {
                                // Check if the table was removed, so that's not a processing error.
                                if (tableMirror != null) {
                                    if (!tableMirror.isRemove()) {
                                        rtn = Boolean.FALSE;
                                    }
                                }
                            }
                        } else {
                            log.error("ReturnStatus is NULL in migrationFuture");
                        }
                    } catch (InterruptedException | ExecutionException | RuntimeException e) {
                        log.error("Interrupted Migration Executions", e);
                        rtn = Boolean.FALSE;
                    }
                }

                // If still TRUE, then we're good.
                if (rtn) {
                    runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.ERRORED);
                    runStatus.addError(MessageCode.PROCESSING_TABLES_ISSUE);
                }
            } else {
                runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.SKIPPED);
            }
        } else {
            runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.SKIPPED);
        }

        return rtn;
    }

    /**
     * Cleans up environment connections and saves reports.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     * @param rtn current success status
     * @return final success status
     */
    private Boolean cleanupAndSaveReports(ConversionResult conversionResult, RunStatus runStatus, Boolean rtn) {
        // Clean up Environment for Reports.
        switch (conversionResult.getJob().getStrategy()) {
            case STORAGE_MIGRATION:
            case DUMP:
                // Clean up RIGHT connection definition.
                if (nonNull(conversionResult.getConnection(Environment.RIGHT))) {
                    conversionResult.getConnections().remove(Environment.RIGHT);
                }
            default:
                if (nonNull(conversionResult.getConnection(Environment.SHADOW))) {
                    conversionResult.getConnections().remove(Environment.SHADOW);
                }
                if (nonNull(conversionResult.getConnection(Environment.TRANSFER))) {
                    conversionResult.getConnections().remove(Environment.TRANSFER);
                }
                break;
        }

        runStatus.setStage(StageEnum.SAVING_REPORTS, CollectionEnum.IN_PROGRESS);
        // Set RunStatus End Date.
        runStatus.setEnd(new Date());

        // Set Run Status Progress.
        if (rtn) {
            runStatus.setProgress(ProgressEnum.COMPLETED);
        } else {
            runStatus.setProgress(ProgressEnum.FAILED);
        }

        try {
            runStatus.setStage(StageEnum.SAVING_REPORTS, CollectionEnum.COMPLETED);
            reportWriterService.wrapup();
        } catch (RuntimeException rte) {
            log.error("Issue saving reports", rte);
            runStatus.addError(MISC_ERROR, rte.getMessage());
            runStatus.setStage(StageEnum.SAVING_REPORTS, CollectionEnum.ERRORED);
            rtn = Boolean.FALSE;
        } finally {
            // Close down the connections to free up resources.
            connectionPoolService.close();
        }

        return rtn;
    }

    /**
     * Prepares test dataset by clearing partition data for STORAGE_MIGRATION strategy.
     * Only executed for mock test datasets.
     *
     * @param conversionResult the conversion result context
     * @param runStatus the run status tracker
     */
    private void prepareTestDataset(ConversionResult conversionResult, RunStatus runStatus) {
        if (conversionResult.isMockTestDataset() &&
                (!conversionResult.getConfig().loadMetadataDetails() &&
                conversionResult.getJob().getStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            // Remove Partition Data to ensure we don't use it. Sets up a clean run like we're starting from scratch.
            log.info("Resetting Partition Data for Test Data Load");
            Map<String, DBMirror> dbMirrors;
            try {
                dbMirrors = getDbMirrorRepository().findByKey(conversionResult.getKey());

                dbMirrors.forEach((dbName, dbMirror) -> {
                    runStatus.getOperationStatistics().getCounts().incrementDatabases();
                    Map<String, TableMirror> tableMirrors;
                    try {
                        tableMirrors = getTableMirrorRepository().
                                findByDatabase(conversionResult.getKey(), dbName);
                    } catch (RepositoryException e) {
                        throw new RuntimeException(e);
                    }
                    tableMirrors.forEach((tablName, tableMirror) -> {
                        runStatus.getOperationStatistics().getCounts().incrementTables();
                        // Since we are asking NOT to loadMetadataDetails, we need to remove the partition data.
                        for (EnvironmentTable et : tableMirror.getEnvironments().values()) {
                            et.getPartitions().clear();
                            try {
                                getTableMirrorRepository().save(conversionResult.getKey(), dbName, tableMirror);
                            } catch (RepositoryException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                });
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            getRunStatusRepository().saveByKey(conversionResult.getKey(), runStatus);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Main workflow orchestration method that coordinates all stages of the HMS migration process.
     * This method has been refactored to delegate to specific stage methods for better maintainability.
     *
     * @return true if the workflow completed successfully, false otherwise
     */
    private Boolean theWork(ConversionResult conversionResult) {
        Boolean rtn = Boolean.TRUE;
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

        // Stage 1: Initialize execution context
//        ConversionResult conversionResult =
        RunStatus runStatus = conversionResult.getRunStatus();

        // Stage 2: Validate configuration
        if (!validateConfiguration(conversionResult, runStatus)) {
            return Boolean.FALSE;
        }

        // Stage 3: Setup database connections
        if (!setupConnections(conversionResult, runStatus)) {
            return Boolean.FALSE;
        }

        // Stage 4: Prepare test datasets if needed
        prepareTestDataset(conversionResult, runStatus);

        // Stage 5: Initialize workflow
        log.info("Starting Application Workflow");
        log.info("Debug: isMockTestDataset={}, isDatabaseOnly={}", conversionResult.isMockTestDataset(), conversionResult.getJob().isDatabaseOnly());
        runStatus.setProgress(ProgressEnum.IN_PROGRESS);

        // Stage 6: Load environment variables
        if (!loadEnvironmentVariables(conversionResult, runStatus)) {
            return Boolean.FALSE;
        }

        // Stage 7: Discover databases and collect tables
        List<CompletableFuture<ReturnStatus>> gtf = discoverDatabases(conversionResult, runStatus);
        if (gtf == null) {
            return Boolean.FALSE;
        }

        // Stage 8: Collect tables
        if (!collectTables(conversionResult, runStatus, gtf)) {
            return Boolean.FALSE;
        }

        // Stage 9: Build global location map
        if (!buildGlobalLocationMap(conversionResult, runStatus)) {
            return Boolean.FALSE;
        }

        // Stage 10: Build databases
        rtn = buildDatabases(conversionResult, runStatus);

        // Stage 11: Process databases
        rtn = processDatabases(conversionResult, runStatus, rtn);

        // Stage 12: Load table metadata and build migrations
        Set<ReturnStatus> migrationExecutions = loadTableMetadataAndBuild(conversionResult, runStatus, gtf, rtn);
        if (migrationExecutions == null) {
            return Boolean.FALSE;
        }

        // Stage 13: Validate environment sets
        rtn = validateEnvironmentSets(conversionResult, runStatus, rtn);

        // Stage 14: Execute tables
        rtn = executeTables(conversionResult, runStatus, rtn, migrationExecutions);

        // Stage 15: Cleanup and save reports
        return cleanupAndSaveReports(conversionResult, runStatus, rtn);
    }
}
