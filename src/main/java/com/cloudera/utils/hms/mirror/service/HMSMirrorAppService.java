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

import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

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
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        Conversion conversion = executeSessionService.getSession().getConversion();
        rtn = runStatus.getErrors().getReturnCode();
        // If app ran, then check for unsuccessful table conversions.
        if (rtn == 0) {
            rtn = conversion.getUnsuccessfullTableCount();
        }
        return rtn;
    }

    public long getWarningCode() {
        long rtn = 0L;
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        Conversion conversion = executeSessionService.getSession().getConversion();
        rtn = runStatus.getWarnings().getReturnCode();
        return rtn;
    }

    @Async("executionThreadPool")
    public Future<Boolean> run() {
        Boolean rtn = Boolean.TRUE;
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        Conversion conversion = session.getConversion();
        OperationStatistics stats = runStatus.getOperationStatistics();

        try {// Refresh the connections pool.
            connectionPoolService.init();
        } catch (SQLException e) {
            log.error("Issue refreshing connections pool", e);
            runStatus.addError(CONNECTION_ISSUE, "Issue refreshing connections pool");
            return new AsyncResult<>(Boolean.FALSE);
        } catch (SessionException se) {
            log.error("Issue with Session", se);
            runStatus.addError(CONNECTION_ISSUE, "Issue refreshing connections pool");
            return new AsyncResult<>(Boolean.FALSE);
        } catch (EncryptionException e) {
            log.error("Issue with Decryption", e);
            runStatus.addError(DECRYPTING_PASSWORD_ISSUE);
            return new AsyncResult<>(Boolean.FALSE);
        }

        // Correct the load data issue ordering.
        if (config.isLoadingTestData() &&
                (!config.isEvaluatePartitionLocation() && config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
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

        log.info("Starting Application Workflow");

        // Don't continue if config hasn't been validated.
        if (!config.isValidated()) {
            log.error("Configuration is not valid.  Exiting.");
            reportWriterService.wrapup();
            return new AsyncResult<>(Boolean.FALSE);
        }

        log.info("Setting 'running' to TRUE");
        session.getRunning().set(Boolean.TRUE);

        Date startTime = new Date();
        log.info("GATHERING METADATA: Start Processing for databases: {}", String.join(",",config.getDatabases()));

        if (config.isLoadingTestData()) {
            List<String> databases = new ArrayList<>();
            for (DBMirror dbMirror : conversion.getDatabases().values()) {
                stats.getCounts().incrementDatabases();
                databases.add(dbMirror.getName());
            }
//            String[] dbs = databases.toArray(new String[0]);
            config.setDatabases(databases);
        } else if (!isBlank(config.getFilter().getDbRegEx())) {
            // Look for the dbRegEx.
            Connection conn = null;
            Statement stmt = null;
            List<String> databases = new ArrayList<>();
            try {
                conn = connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT);
                //getConfig().getCluster(Environment.LEFT).getConnection();
                if (nonNull(conn)) {
                    log.info("Retrieved LEFT Cluster Connection");
                    stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(MirrorConf.SHOW_DATABASES);
                    while (rs.next()) {
                        String db = rs.getString(1);
                        Matcher matcher = config.getFilter().getDbFilterPattern().matcher(db);
                        if (matcher.find()) {
                            stats.getCounts().incrementDatabases();
                            databases.add(db);
                        }
                    }
                    config.setDatabases(databases);
                }
            } catch (SQLException se) {
                // Issue
                log.error("Issue getting databases for dbRegEx", se);
                stats.getFailures().incrementDatabases();
                executeSessionService.getSession().addError(MISC_ERROR, "LEFT:Issue getting databases for dbRegEx");
                reportWriterService.wrapup();
                return new AsyncResult<>(Boolean.FALSE);
            } finally {
                if (nonNull(conn)) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        log.error("Issue closing connections for LEFT", e);
                        executeSessionService.getSession().addError(MISC_ERROR, "LEFT:Issue closing connections.");
                    }
                }
            }
        }

        if (!config.isLoadingTestData()) {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.IN_PROGRESS);
            rtn = databaseService.loadEnvironmentVars();
            if (rtn) {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
            } else {
                runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.ERRORED);
                reportWriterService.wrapup();
                return new AsyncResult<>(Boolean.FALSE);
            }
        } else {
            runStatus.setStage(StageEnum.ENVIRONMENT_VARS, CollectionEnum.COMPLETED);
        }

        if (isNull(config.getDatabases()) || config.getDatabases().isEmpty()) {
            log.error("No databases specified OR found if you used dbRegEx");
            runStatus.addError(MISC_ERROR, "No databases specified OR found if you used dbRegEx");
            return new AsyncResult<>(Boolean.FALSE);
        }

        List<Future<ReturnStatus>> gtf = new ArrayList<>();
        // ========================================
        // Get the Database definitions for the LEFT and RIGHT clusters.
        // ========================================
        if (!config.isLoadingTestData()) {
            runStatus.setStage(StageEnum.DATABASES, CollectionEnum.IN_PROGRESS);
            for (String database : config.getDatabases()) {
                runStatus.getOperationStatistics().getCounts().incrementDatabases();
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
                    executeSessionService.getSession().addError(MISC_ERROR, "Issue getting databases");
                    runStatus.getOperationStatistics().getFailures().incrementDatabases();
                    runStatus.setStage(StageEnum.DATABASES, CollectionEnum.ERRORED);
                    reportWriterService.wrapup();
                    return new AsyncResult<>(Boolean.FALSE);
                }

                // Build out the table in a database.
                if (!config.isLoadingTestData() && !config.isDatabaseOnly()) {
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
                reportWriterService.wrapup();
                return new AsyncResult<>(Boolean.FALSE);
            }
        }

        runStatus.setStage(StageEnum.CREATE_DATABASES, CollectionEnum.IN_PROGRESS);
        if (!getDatabaseService().createDatabases()) {
            runStatus.addError(MessageCode.DATABASE_CREATION);
            runStatus.setStage(StageEnum.CREATE_DATABASES, CollectionEnum.ERRORED);
            reportWriterService.wrapup();
            return new AsyncResult<>(Boolean.FALSE);
        }
        runStatus.setStage(StageEnum.CREATE_DATABASES, CollectionEnum.COMPLETED);

        // Shortcut.  Only DB's.
        if (!config.isDatabaseOnly()) {
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
                        stats.getSkipped().incrementTables();
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

        log.info("Setting 'running' to FALSE");
        session.getRunning().set(Boolean.FALSE);


        reportWriterService.wrapup();

        return new AsyncResult<>(rtn);
    }
}
