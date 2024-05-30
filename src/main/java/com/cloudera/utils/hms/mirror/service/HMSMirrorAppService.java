/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.CollectionEnum;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.domain.support.StageEnum;
import com.cloudera.utils.hms.stage.ReturnStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.CONNECTION_ISSUE;
import static com.cloudera.utils.hms.mirror.MessageCode.MISC_ERROR;

@Service
@Getter
@Slf4j
public class HMSMirrorAppService {

    private final ConfigService configService;
    private final ConnectionPoolService connectionPoolService;
    private final DatabaseService databaseService;
    private final ExecuteSessionService executeSessionService;
    private final ReportWriterService reportWriterService;
    private final TableService tableService;
    private final TransferService transferService;

    public HMSMirrorAppService(ExecuteSessionService executeSessionService,
                               ConnectionPoolService connectionPoolService,
                               DatabaseService databaseService,
                               ReportWriterService reportWriterService,
                               TableService tableService,
                               TransferService transferService,
                               ConfigService configService) {
        this.executeSessionService = executeSessionService;
        this.connectionPoolService = connectionPoolService;
        this.databaseService = databaseService;
        this.reportWriterService = reportWriterService;
        this.tableService = tableService;
        this.transferService = transferService;
        this.configService = configService;
    }

    public long getReturnCode() {
        long rtn = 0L;
        RunStatus runStatus = executeSessionService.getActiveSession().getRunStatus();
        Conversion conversion = executeSessionService.getActiveSession().getConversion();
        rtn = runStatus.getErrors().getReturnCode();
        // If app ran, then check for unsuccessful table conversions.
        if (rtn == 0) {
            rtn = conversion.getUnsuccessfullTableCount();
        }
        return rtn;
    }

    @Async("executionThreadPool")
    public Future<Boolean> run() {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        RunStatus runStatus = executeSessionService.getActiveSession().getRunStatus();
        Conversion conversion = executeSessionService.getActiveSession().getConversion();

        connectionPoolService.close();

        try {// Refresh the connection pool.
            connectionPoolService.init();
        } catch (SQLException e) {
            log.error("Issue refreshing connection pool", e);
            runStatus.addError(CONNECTION_ISSUE, "Issue refreshing connection pool");
            return new AsyncResult<>(Boolean.FALSE);
        }

        if (!configService.validate()) {
            log.error("Configuration is not valid.  Exiting.");
            hmsMirrorConfig.setValidated(Boolean.FALSE);
            return new AsyncResult<>(Boolean.FALSE);
        } else {
            log.info("Configuration is valid.");
            hmsMirrorConfig.setValidated(Boolean.TRUE);
        }

        // Correct the load data issue ordering.
        if (hmsMirrorConfig.isLoadingTestData() &&
                (!hmsMirrorConfig.isEvaluatePartitionLocation() && hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            // Remove Partition Data to ensure we don't use it.  Sets up a clean run like we're starting from scratch.
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
                for (TableMirror tableMirror : dbMirror.getTableMirrors().values()) {
                    runStatus.getOperationStatistics().getCounts().incrementTables();
                    for (EnvironmentTable et : tableMirror.getEnvironments().values()) {
                        et.getPartitions().clear();
                    }
                }
            }
        }

        // Clear tables from test dataset if the database only flag is set.
//        if (hmsMirrorConfig.isLoadingTestData() && hmsMirrorConfig.isDatabaseOnly()) {
//            // Remove the tables from the database dataset.
//            for (DBMirror dbMirror : conversion.getDatabases().values()) {
//                dbMirror.getTableMirrors().clear();
//                log.info("Database Only processing, removing table from test dataset");
//            }
//        }

        log.info("Starting Application Workflow");

//        if (!hmsMirrorConfig.isValidated()) {
//            log.error("Configuration is not valid.  Exiting.");
//            return new AsyncResult<>(Boolean.FALSE);
////            getCliReporter().getMessages();
////            return;
//        }

        log.info("Setting 'running' to TRUE");
        getExecuteSessionService().getActiveSession().getRunning().set(Boolean.TRUE);

        Date startTime = new Date();
        log.info("GATHERING METADATA: Start Processing for databases: {}", String.join(",",hmsMirrorConfig.getDatabases()));

        if (hmsMirrorConfig.isLoadingTestData()) {
            List<String> databases = new ArrayList<>();
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                databases.add(dbMirror.getName());
            }
//            String[] dbs = databases.toArray(new String[0]);
            hmsMirrorConfig.setDatabases(databases);
        } else if (hmsMirrorConfig.getFilter().getDbRegEx() != null) {
            // Look for the dbRegEx.
            Connection conn = null;
            Statement stmt = null;
            List<String> databases = new ArrayList<>();
            try {
                conn = connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT);
                //getConfig().getCluster(Environment.LEFT).getConnection();
                if (conn != null) {
                    log.info("Retrieved LEFT Cluster Connection");
                    stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(MirrorConf.SHOW_DATABASES);
                    while (rs.next()) {
                        String db = rs.getString(1);
                        Matcher matcher = hmsMirrorConfig.getFilter().getDbFilterPattern().matcher(db);
                        if (matcher.find()) {
                            runStatus.getOperationStatistics().getCounts().incrementDatabases();
                            databases.add(db);
                        }
                    }
//                    String[] dbs = databases.toArray(new String[0]);
                    hmsMirrorConfig.setDatabases(databases);
                }
            } catch (SQLException se) {
                // Issue
                log.error("Issue getting databases for dbRegEx", se);
                executeSessionService.getActiveSession().addError(MISC_ERROR, "LEFT:Issue getting databases for dbRegEx");
                return new AsyncResult<>(Boolean.FALSE);
                //                wrapup();
                //                return;
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        log.error("Issue closing connection for LEFT", e);
                        executeSessionService.getActiveSession().addError(MISC_ERROR, "LEFT:Issue closing connection.");
                    }
                }
            }
        }

        if (!hmsMirrorConfig.isLoadingTestData()) {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.IN_PROGRESS);
            rtn = databaseService.loadEnvironmentVars();
            if (rtn) {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.ERRORED);
//                runStatus.addError(MessageCode.ENVIRONMENT_VARS);
                return new AsyncResult<>(Boolean.FALSE);
            }
        } else {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
        }

        if (hmsMirrorConfig.getDatabases() == null || hmsMirrorConfig.getDatabases().isEmpty()) {
            log.error("No databases specified OR found if you used dbRegEx");
            runStatus.addError(MISC_ERROR, "No databases specified OR found if you used dbRegEx");
            return new AsyncResult<>(Boolean.FALSE);
        }

        List<Future<ReturnStatus>> gtf = new ArrayList<>();
        // ========================================
        // Get the Database definitions for the LEFT and RIGHT clusters.
        // ========================================
        if (!hmsMirrorConfig.isLoadingTestData()) {
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.IN_PROGRESS);
            for (String database : hmsMirrorConfig.getDatabases()) {
                DBMirror dbMirror = conversion.addDatabase(database);
                try {
                    // Get the Database definitions for the LEFT and RIGHT clusters.

                    if (getDatabaseService().getDatabase(dbMirror, Environment.LEFT)) { //getConfig().getCluster(Environment.LEFT).getDatabase(config, dbMirror)) {
                        getDatabaseService().getDatabase(dbMirror, Environment.RIGHT);
                        runStatus.getOperationStatistics().getSuccesses().incrementDatabases();
                        //getConfig().getCluster(Environment.RIGHT).getDatabase(config, dbMirror);
                    } else {
                        // LEFT DB doesn't exists.
                        dbMirror.addIssue(Environment.LEFT, "DB doesn't exist. Check permissions for user running process");
                        runStatus.getOperationStatistics().getFailures().incrementDatabases();
                        rtn = Boolean.FALSE;
                    }
                } catch (SQLException se) {
                    log.error("Issue getting databases", se);
                    executeSessionService.getActiveSession().addError(MISC_ERROR, "Issue getting databases");
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    return new AsyncResult<>(Boolean.FALSE);
//                    wrapup();
//                    return;
                }

                // Build out the table in a database.
                if (!hmsMirrorConfig.isLoadingTestData() && !hmsMirrorConfig.isDatabaseOnly()) {
                    runStatus.setStage(StageEnum.TABLES, CollectionEnum.IN_PROGRESS);
                    Future<ReturnStatus> gt = getTableService().getTables(dbMirror);
                    gtf.add(gt);
                }
            }
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.COMPLETED);

            // Collect Table Information and ensure process is complete before moving on.
            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : gtf) {
                    if (!sf.isDone()) {
                        check = false;
                        break;
                    }
                    try {
                        if (sf.isDone() && sf.get() != null) {
                            if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                rtn = Boolean.FALSE;
//                            throw new RuntimeException(sf.get().getException());
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }
            runStatus.setStage(StageEnum.TABLES, CollectionEnum.COMPLETED);
            gtf.clear(); // reset

            // Failure, report and exit with FALSE
            if (!rtn) {
                runStatus.setStage(StageEnum.TABLES, CollectionEnum.ERRORED);
                runStatus.getErrors().set(MessageCode.COLLECTING_TABLES);
                return new AsyncResult<>(Boolean.FALSE);
//                wrapup();
//                return; //rtn = Boolean.FALSE;
            }
        }

        runStatus.setStage(StageEnum.CREATE_DATABASES, CollectionEnum.IN_PROGRESS);
        if (!getDatabaseService().createDatabases()) {
            runStatus.getErrors().set(MessageCode.DATABASE_CREATION);
            runStatus.setStage(StageEnum.CREATE_DATABASES, CollectionEnum.ERRORED);
            return new AsyncResult<>(Boolean.FALSE);
//            wrapup();
//            return; //rtn = Boolean.FALSE;
        }
        runStatus.setStage(StageEnum.CREATE_DATABASES, CollectionEnum.COMPLETED);

        // Create the databases we'll need on the LEFT and RIGHT
//        Callable<ReturnStatus> createDatabases = new CreateDatabases(conversion);
//        gtf.add(getConfig().getTransferThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

        // Check and Build DB's First.
//        while (true) {
//            boolean check = true;
//            for (Future<ReturnStatus> sf : gtf) {
//                if (!sf.isDone()) {
//                    check = false;
//                    break;
//                }
//                try {
//                    if (sf.isDone() && sf.get() != null) {
//                        ReturnStatus returnStatus = sf.get();
//                        if (returnStatus != null && returnStatus.getStatus() == ReturnStatus.Status.ERROR) {
////                            throw new RuntimeException(sf.get().getException());
//                            rtn = Boolean.FALSE;
//                        }
//                    }
//                } catch (InterruptedException | ExecutionException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//            if (check)
//                break;
//        }
//        gtf.clear(); // reset

        // Failure, report and exit with FALSE
//        if (!rtn) {
//            getProgression().getErrors().set(DATABASE_CREATION.getCode());
//            return Boolean.FALSE;
//        }

        // Shortcut.  Only DB's.
        if (!hmsMirrorConfig.isDatabaseOnly()) {
            // ========================================
            // Get the table METADATA for the tables collected in the databases.
            // ========================================
            runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.IN_PROGRESS);
            log.info(">>>>>>>>>>> Getting Table Metadata");
            Set<String> collectedDbs = conversion.getDatabases().keySet();
            for (String database : collectedDbs) {
                DBMirror dbMirror = conversion.getDatabase(database);
                Set<String> tables = dbMirror.getTableMirrors().keySet();
                for (String table : tables) {
                    TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                    gtf.add(tableService.getTableMetadata(tableMirror));
                }
            }

            List<Future<ReturnStatus>> migrationFuture = new ArrayList<>();
            runStatus.setStage(StageEnum.MIGRATE_TABLES, CollectionEnum.IN_PROGRESS);

            // Go through the Futures and check status.
            // When SUCCESSFUL, move on to the next step.
            // ========================================
            // Check that a tables metadata has been retrieved.  When it has (ReturnStatus.Status.SUCCESS),
            // move on to the NEXTSTEP and actual do the transfer.
            // ========================================
            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : gtf) {
                    if (!sf.isDone()) {
                        check = false;
                        break;
                    }
                    try {
                        if (sf.isDone() && sf.get() != null) {
//                                ReturnStatus sfStatus = sf.get(100, java.util.concurrent.TimeUnit.MILLISECONDS);
//                                if (sfStatus != null) {
                            switch (sf.get().getStatus()) {
                                case SUCCESS:
                                    // Trigger next step and set status.
                                    // TODO: Next Step
                                    sf.get().setStatus(ReturnStatus.Status.NEXTSTEP);
                                    // Launch the next step, which is the transfer.
                                    migrationFuture.add(getTransferService().transfer(sf.get().getTableMirror()));
                                    break;
                                case ERROR:
                                case FATAL:
                                    rtn = Boolean.FALSE;
                                    throw new RuntimeException(sf.get().getException());
                                case NEXTSTEP:
                                    break;
                            }
//                                } else {
//                                    check = false;
//                                }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }

            if (rtn) {
                runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.LOAD_TABLE_METADATA, CollectionEnum.ERRORED);
//                runStatus.addError(MessageCode.COLLECTING_TABLE_METADATA);
//                return new AsyncResult<>(Boolean.FALSE);
            }

            gtf.clear(); // reset

            // Remove the tables that are marked for removal.
            for (String database : collectedDbs) {
                DBMirror dbMirror = conversion.getDatabase(database);
                Set<String> tables = dbMirror.getTableMirrors().keySet();
                for (String table : tables) {
                    TableMirror tableMirror = dbMirror.getTableMirrors().get(table);
                    if (tableMirror.isRemove()) {
                        // Setup the filtered out tables so they can be reported w/ reason.
                        log.info("Table: {}.{} is being removed from further processing. Reason: {}", dbMirror.getName(), table, tableMirror.getRemoveReason());
                        dbMirror.getFilteredOut().put(table, tableMirror.getRemoveReason());
                    }
                }
                dbMirror.getTableMirrors().values().removeIf(TableMirror::isRemove);
            }

            if (!rtn) {
                runStatus.addError(MessageCode.COLLECTING_TABLE_DEFINITIONS);
            }

            // Check the Migration Futures are done.
            while (true) {
                boolean check = true;
                for (Future<ReturnStatus> sf : migrationFuture) {
                    if (!sf.isDone()) {
                        check = false;
                        break;
                    }
                    try {
                        if (sf.isDone() && sf.get() != null) {
                            if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                                // Check if the table was removed, so that's not a processing error.
                                TableMirror tableMirror = sf.get().getTableMirror();
                                if (tableMirror != null) {
                                    if (!tableMirror.isRemove()) {
                                        rtn = Boolean.FALSE;
                                    }
                                }
                            }
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (check)
                    break;
            }
            if (rtn) {
                runStatus.setStage(StageEnum.MIGRATE_TABLES, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.MIGRATE_TABLES, CollectionEnum.ERRORED);
            }
        }

        reportWriterService.wrapup();

        return new AsyncResult<>(rtn);
    }
}
