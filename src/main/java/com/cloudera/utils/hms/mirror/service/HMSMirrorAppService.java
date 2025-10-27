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
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.stage.ReturnStatus;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
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
    public CompletableFuture<Boolean> cliRun() {
        Boolean rtn = Boolean.TRUE;
        ExecuteSession session = sessionManager.getCurrentSession();
        rtn = theWork(session.getConversionResult());
        return CompletableFuture.completedFuture(rtn);

    }

    /*
    This method is where all the work starts.  The configuration and asks are configured before this point.  The passed in
    objects are 'deep' copies of the working objects in the ExecuteSession and will be used to drive forward.
     */
    @Async("executionThreadPool")
    public CompletableFuture<Boolean> run(RunStatus runStatus, String jobId) {
        Boolean rtn = Boolean.TRUE;
        CompletableFuture<Boolean> theWork;
        try {
            ConversionResult conversionResult = jobManagementService.buildConversionResultFromJobId(jobId);
            conversionResult.setRunStatus(runStatus);
            theWork = CompletableFuture.supplyAsync(() -> theWork(conversionResult));
        } catch (Exception e) {
            log.error("Issue building ConversionResult from JobId", e);
            runStatus.addError(MISC_ERROR, "Issue building ConversionResult from JobId");
            theWork = CompletableFuture.completedFuture(Boolean.FALSE);
        }
        // Now that we're in a new Thread, create a New Session for this thread and set the id to the masterSession id + subId.
        // TODO: Fix.
//        ExecuteSession session = sessionManager.createSession(masterSession.getNextSubSessionId(), config);
//        config.reset();
//        session.setRunStatus(runStatus);

        // Set cloned ConversionRequest and ConversionResult if not null.
//        if (conversionRequest != null) {
//            session.setConversionRequest(conversionRequest.clone());
//        }
//        if (conversionResult != null) {
//            session.setConversionResult(conversionResult.clone());
//        }
//        rtn = theWork(session);

        return theWork;

    }

    private Boolean theWork(ConversionResult conversionResult) {
        Boolean rtn = Boolean.TRUE;

        // Set the conversionResult for this thread's context.
        // Note: Other threaded calls will take the conversionResult from this thread (manually) and
        //       use that to assign to their ThreadLocal, since that context will be different.
        executionContextService.setConversionResult(conversionResult);

        RunStatus runStatus = conversionResult.getRunStatus();

        // Transfer the Comment.
        if (runStatus.getComment() != null) {
            // Comment already set in RunStatus
        } else {
            runStatus.setComment("No comments provided for this run.  Consider adding one for easier tracking.");
        }

        // Reset Start time to the actual 'execution' start time.
        runStatus.setStart(new Date());
        runStatus.setProgress(ProgressEnum.STARTED);
        OperationStatistics stats = runStatus.getOperationStatistics();
        log.info("Starting HMSMirrorAppService.run()");
        runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.IN_PROGRESS);
        if (configService.validate()) {
            runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.COMPLETED);
        } else {
            runStatus.setStage(StageEnum.VALIDATING_CONFIG, CollectionEnum.ERRORED);
            reportWriterService.wrapup();
            runStatus.setProgress(ProgressEnum.FAILED);
            return Boolean.FALSE;
        }

        if (!conversionResult.getConfigLite().isLoadingTestData()) {
            try{// Refresh the connections pool.
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
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return Boolean.FALSE;
                }
            } catch (SQLException sqle) {
                log.error("Issue refreshing connections pool", sqle);
                runStatus.addError(CONNECTION_ISSUE, sqle.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            } catch (URISyntaxException e) {
                log.error("URI issue with connections pool", e);
                runStatus.addError(CONNECTION_ISSUE, e.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            } catch (SessionException se) {
                log.error("Issue with Session", se);
                runStatus.addError(SESSION_ISSUE, se.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            } catch (EncryptionException ee) {
                log.error("Issue with Decryption", ee);
                runStatus.addError(ENCRYPTION_ISSUE, ee.getMessage());
                runStatus.setStage(StageEnum.CONNECTION, CollectionEnum.ERRORED);
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
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


        // Correct the load data issue ordering.
        if (conversionResult.getConfigLite().isLoadingTestData() &&
                (!conversionResult.getConfigLite().loadMetadataDetails() && conversionResult.getJob().getStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            // Remove Partition Data to ensure we don't use it.  Sets up a clean run like we're starting from scratch.
            log.info("Resetting Partition Data for Test Data Load");
            for (DBMirror dbMirror : conversionResult.getDatabases().values()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
                for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                    runStatus.getOperationStatistics().getCounts().incrementTables();
                    for (EnvironmentTable et : tableMirror.getEnvironments().values()) {
                        et.getPartitions().clear();
                    }
                }
            }
        }

        log.info("Starting Application Workflow");
        log.info("Debug: isLoadingTestData={}, isDatabaseOnly={}", conversionResult.getConfigLite().isLoadingTestData(), conversionResult.getJob().isDatabaseOnly());
        runStatus.setProgress(ProgressEnum.IN_PROGRESS);

        Date startTime = new Date();
        runStatus.setStage(StageEnum.GATHERING_DATABASES, CollectionEnum.IN_PROGRESS);

        if (conversionResult.getConfigLite().isLoadingTestData()) {
            // TODO: Need to rework whole test data loading.
            log.info("Loading Test Data.  Skipping Database Collection.");
            Set<String> databases = new TreeSet<>();
            for (DBMirror dbMirror : conversionResult.getDatabases().values()) {
                stats.getCounts().incrementDatabases();
                databases.add(dbMirror.getName());
            }
//            String[] dbs = databases.toArray(new String[0]);
            conversionResult.getDataset().setDatabases(databases);
        }
        /*
        TODO: I don't think this is relevant anymore because of the Datasets concept.

        else if (!isBlank(conversionResult.getDataset().getFilter().getDbRegEx())) {
            // Look for the dbRegEx.
            log.info("Using dbRegEx: {}", conversionResult.getDataset().getFilter().getDbRegEx());
            Connection conn = null;
            Statement stmt = null;
            Set<String> databases = new TreeSet<>();
            try {
                log.info("Getting LEFT Cluster Connection");
                conn = connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT);
                //getConfig().getCluster(Environment.LEFT).getConnection();
                if (nonNull(conn)) {
                    log.info("Retrieved LEFT Cluster Connection");
                    stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(MirrorConf.SHOW_DATABASES);
                    while (rs.next()) {
                        String db = rs.getString(1);
                        Matcher matcher = conversionResult.getDataset().getFilter().getDbFilterPattern().matcher(db);
                        if (matcher.find()) {
                            stats.getCounts().incrementDatabases();
                            databases.add(db);
                        }
                    }
                    conversionResult.getDataset().setDatabases(databases);
                }
            } catch (SQLException se) {
                // Issue
                log.error("Issue getting databases for dbRegEx", se);
                stats.getFailures().incrementDatabases();
                runStatus.setStage(StageEnum.GATHERING_DATABASES, CollectionEnum.ERRORED);
                runStatus.addError(MISC_ERROR, "LEFT:Issue getting databases for dbRegEx");
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            } finally {
                if (nonNull(conn)) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        log.error("Issue closing connections for LEFT", e);
                        runStatus.addError(MISC_ERROR, "LEFT:Issue closing connections.");
                    }
                }
            }
        }
         */
        runStatus.setStage(StageEnum.GATHERING_DATABASES, CollectionEnum.COMPLETED);
        log.info("Start Processing for databases: {}", String.join(",", conversionResult.getDataset().getDatabases()));

        if (!conversionResult.getConfigLite().isLoadingTestData()) {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.IN_PROGRESS);
            rtn = databaseService.loadEnvironmentVars();
            if (rtn) {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.ERRORED);
                reportWriterService.wrapup();
                connectionPoolService.close();
                runStatus.setProgress(ProgressEnum.FAILED);
                return Boolean.FALSE;
            }
        } else {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.SKIPPED);
        }

        /*
        TODO: I don't think this is relevant anymore because of the Datasets concept.
        if (isNull(conversionResult.getDataset().getDatabases()) || conversionResult.getDataset().getDatabases().isEmpty()) {
            log.error("No databases specified OR found if you used dbRegEx");
            runStatus.addError(MISC_ERROR, "No databases specified OR found if you used dbRegEx");
            connectionPoolService.close();
            runStatus.setProgress(ProgressEnum.FAILED);
            return Boolean.FALSE;
        }
        */

        List<CompletableFuture<ReturnStatus>> gtf = new ArrayList<>();
        // ========================================
        // Get the Database definitions for the LEFT and RIGHT clusters.
        // ========================================
        log.info("RunStatus Stage: {} is {}", StageEnum.DATABASES, runStatus.getStage(StageEnum.DATABASES));
        if (!conversionResult.getConfigLite().isLoadingTestData()) {
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.IN_PROGRESS);
            // From the DatasetDto, iterate through the DatabaseSpecs and build out the DBMirror instances and table list.

            // If there is a table list in the DatabaseSpec, go ahead and create the DBMirror instance and the TableMirror
            //    instances based on that list.

            // If there is a table filter, we need to get a connection to the cluster and get the list of tables that
            //    meet the filter criteria.

            for (DatasetDto.DatabaseSpec database : conversionResult.getDataset().getDatabaseSpecs()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
                DBMirror dbMirror = conversionResultService.addDatabase(conversionResult, database);
                try {
                    // Get the Database definitions for the LEFT and RIGHT clusters.
                    if (getDatabaseService().getDatabase(dbMirror, Environment.LEFT)) { //getConfig().getCluster(Environment.LEFT).getDatabase(config, dbMirror)) {
                        getDatabaseService().getDatabase(dbMirror, Environment.RIGHT);
                    } else {
                        // LEFT DB doesn't exists.
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
                    return Boolean.FALSE;
                } catch (RuntimeException rte) {
                    log.error("Runtime Issue", rte);
                    runStatus.addError(MISC_ERROR, rte.getMessage());
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    reportWriterService.wrapup();
                    connectionPoolService.close();
                    runStatus.setProgress(ProgressEnum.FAILED);
                    return Boolean.FALSE;
                }

                // Build out the table in a database.
                if (!conversionResult.getJob().isDatabaseOnly()) {
                    runStatus.setStage(StageEnum.TABLES, CollectionEnum.IN_PROGRESS);
                    CompletableFuture<ReturnStatus> gt = getTableService().getTables(dbMirror);
                    gtf.add(gt);
                }
            }

            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.COMPLETED);

            // Wait for all the CompletableFutures to finish.
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
        } else {
            log.warn("DAVID DEBUG: Skipping database and table stages for test data");
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.SKIPPED);
            runStatus.setStage(StageEnum.TABLES, CollectionEnum.SKIPPED);
        }

        // TODO: Translator not yet migrated to ConversionResult - accessing through session for now
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        if (config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
            runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.SKIPPED);
        } else {
            try {
                int defaultConsolidationLevel = 1;
                runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.IN_PROGRESS);

                if (conversionResult.getConfigLite().loadMetadataDetails()) {
                    databaseService.buildDatabaseSources(defaultConsolidationLevel, false);
                    translatorService.buildGlobalLocationMapFromWarehousePlansAndSources(false, defaultConsolidationLevel);
                    runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.GLM_BUILD, CollectionEnum.SKIPPED);
                }

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

        runStatus.setStage(StageEnum.BUILDING_DATABASES, CollectionEnum.IN_PROGRESS);
        // Don't build DB with VIEW Migrations.
        if (!conversionResult.getConfigLite().getMigrateVIEW().isOn()) {
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

        // Shortcut.  Only DB's.
        log.warn("DAVID DEBUG: Processing tables: isDatabaseOnly={}, isLoadingTestData={}, collectedDbs={}",
                conversionResult.getJob().isDatabaseOnly(), conversionResult.getConfigLite().isLoadingTestData(), conversionResult.getDatabases().keySet());
        if (!conversionResult.getJob().isDatabaseOnly()) {
            log.warn("DAVID DEBUG: Entering table processing section");
            Set<String> collectedDbs = conversionResult.getDatabases().keySet();
            // ========================================
            // Get the table METADATA for the tables collected in the databases.
            // ========================================
            List<CompletableFuture<ReturnStatus>> migrationFuture = new ArrayList<>();

            runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversionResultService.getDatabase(conversionResult, database);
                    Set<String> tables = dbMirror.getTableMirrors().keySet();
                    for (String table : tables) {
                        TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                        gtf.add(tableService.getTableMetadata(dbMirror, tableMirror));
                    }
                }

                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.IN_PROGRESS);

                // Go through the Futures and check status.
                // When SUCCESSFUL, move on to the next step.
                // ========================================
                // Check that a tables metadata has been retrieved.  When it has (ReturnStatus.Status.CALCULATED_SQL),
                // move on to the NEXTSTEP and actual do the transfer.
                // ========================================
                CompletableFuture.allOf(gtf.toArray(new CompletableFuture[0])).join();
                // Check that all the CompletableFutures in 'gtf' passed with ReturnStatus.Status.SUCCESS.
                for (CompletableFuture<ReturnStatus> sf : gtf) {
                    try {
                        ReturnStatus returnStatus = sf.get();
                        if (nonNull(returnStatus)) {
                            switch (returnStatus.getStatus()) {
                                case SUCCESS:
                                    runStatus.getOperationStatistics().getCounts().incrementTables();
                                    // Trigger next step and set status.
                                    // TODO: Next Step
                                    sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                    // Launch the next step, which is the transfer.
                                    runStatus.getOperationStatistics().getSuccesses().incrementTables();

                                    migrationFuture.add(getTransferService().build(conversionResult, sf.get().getDbMirror(), sf.get().getTableMirror()));
                                    break;
                                case ERROR:
                                    runStatus.getOperationStatistics().getCounts().incrementTables();
                                    sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                    break;
                                case FATAL:
                                    runStatus.getOperationStatistics().getCounts().incrementTables();
                                    runStatus.getOperationStatistics().getFailures().incrementTables();
                                    rtn = Boolean.FALSE;
                                    sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                    log.error("FATAL: ", sf.get().getException());
                                case NEXTSTEP:
                                    break;
                                case SKIP:
                                    runStatus.getOperationStatistics().getCounts().incrementTables();
                                    // Set for tables that are being removed.
                                    runStatus.getOperationStatistics().getSkipped().incrementTables();
                                    sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                    break;
                            }
                        }
                    } catch (InterruptedException | ExecutionException | RuntimeException e) {
                        log.error("Interrupted Table collection", e);
                        rtn = Boolean.FALSE;
                    }
                }

                if (rtn) {
                    runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.COMPLETED);
                } else {
                    runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.ERRORED);
                    runStatus.addError(MessageCode.COLLECTING_TABLE_DEFINITIONS);
                }

                gtf.clear(); // reset

                // Remove the tables that are marked for removal.
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversionResultService.getDatabase(conversionResult, database);
                    Set<String> tables = dbMirror.getTableMirrors().keySet();
                    for (String table : tables) {
                        TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                        if (tableMirror.isRemove()) {
                            // Setup the filtered out tables so they can be reported w/ reason.
//                        stats.getSkipped().incrementTables();
                            log.info("Table: {}.{} is being removed from further processing. Reason: {}", dbMirror.getName(), table, tableMirror.getRemoveReason());
                            dbMirror.getFilteredOut().put(table, tableMirror.getRemoveReason());
                        }
                    }
                    log.info("Removing tables marked for removal from further processing.");
                    dbMirror.getTableMirrors().values().removeIf(TableMirror::isRemove);
                    log.info("Tables marked for removal have been removed from further processing.");
                }

            } else {
                runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.SKIPPED);
                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.SKIPPED);
            }

            Set<ReturnStatus> migrationExecutions = new HashSet<>();

            // Check the Migration Futures are done.
            CompletableFuture.allOf(migrationFuture.toArray(new CompletableFuture[0])).join();

            // Check that all the CompletableFutures in 'migrationFuture' passed with ReturnStatus.Status.SUCCESS.
            for (CompletableFuture<ReturnStatus> sf : migrationFuture) {
                try {
                    ReturnStatus rs = sf.get();
                    if (nonNull(rs)) {
                        TableMirror tableMirror = rs.getTableMirror();
                        // Only push SUCCESSFUL tables to the migrationExecutions list.
                        if (rs.getStatus() == ReturnStatus.Status.SUCCESS) {
                            // Success means add table the execution list.
                            migrationExecutions.add(rs);
                        }
                    } else {
                        log.error("ReturnStatus is NULL in migration build");
                    }
                } catch (InterruptedException | ExecutionException | RuntimeException e) {
                    log.error("Interrupted Building Migrations", e);
                    rtn = Boolean.FALSE;
                }
            }

            if (rtn) {
                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.BUILDING_TABLES, CollectionEnum.ERRORED);
            }

            migrationFuture.clear(); // reset

            // Validate the SET statements.
            runStatus.setStage(StageEnum.VALIDATING_ENVIRONMENT_SETS, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                // Check the Unique SET statements.
                for (String database : collectedDbs) {
                    DBMirror dbMirror = conversionResultService.getDatabase(conversionResult, database);
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

            // Process the SQL for the Databases;
            runStatus.setStage(StageEnum.PROCESSING_DATABASES, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                if (conversionResult.getConfigLite().isExecute()) {
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

            // Process the SQL for the Tables;

            runStatus.setStage(StageEnum.PROCESSING_TABLES, CollectionEnum.IN_PROGRESS);
            if (rtn) {
                if (conversionResult.getConfigLite().isExecute()) {
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

        }

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
}
