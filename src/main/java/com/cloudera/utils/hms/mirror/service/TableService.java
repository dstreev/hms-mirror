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

//import com.cloudera.utils.hadoop.HadoopSession;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.core.model.ValidationResult;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.JobExecution;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import com.cloudera.utils.hms.stage.ReturnStatus;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;

import static com.cloudera.utils.hms.mirror.MessageCode.METASTORE_PARTITION_LOCATIONS_NOT_FETCHED;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.DUMP;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Service
@Getter
@Setter
@Slf4j
@RequiredArgsConstructor
public class TableService {
    private final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");

    @NonNull
    private final ConfigService configService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final ConnectionPoolService connectionPoolService;
    @NonNull
    private final QueryDefinitionsService queryDefinitionsService;
    @NonNull
    private final TranslatorService translatorService;
    @NonNull
    private final StatsCalculatorService statsCalculatorService;
    @NonNull
    private final CliEnvironment cliEnvironment;
    @NonNull
    private final TableMirrorRepository tableMirrorRepository;

    /**
     * Checks the table filter using the new core business logic.
     * This method now delegates to the pure business logic layer.
     */
    public void checkTableFilter(String dbMirror, TableMirror tableMirror, Environment environment) {
        log.debug("Checking table filter for table: {} in environment: {}", tableMirror, environment);
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        // NEW: Delegate to core business logic instead of doing it inline
        ValidationResult result = validateTableFilterImpl(conversionResult, dbMirror, tableMirror, environment);

        // Handle the result in Spring-specific way (side effects, metrics, etc.)
        if (!result.isValid()) {
            log.warn("Table filter validation failed: {}", result.getMessage());
            tableMirror.setRemove(Boolean.TRUE);
            tableMirror.setRemoveReason(result.getMessage());

            // Log errors for debugging
            result.getErrors().forEach(error -> log.error("Filter error: {}", error));
        } else {
            // For ACID tables that pass validation, add the TRANSACTIONAL step
            EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
            if (et != null && TableUtils.isManaged(et) && TableUtils.isACID(et)) {
                if (config.getMigrateACID().isOn()) {
                    tableMirror.addStep("TRANSACTIONAL", Boolean.TRUE);
                }
            }
        }

        log.trace("Table filter checked for table: {} - Valid: {}", tableMirror, result.isValid());
    }

    private ValidationResult validateTableFilterImpl(ConversionResult conversionResult, String databaseName, TableMirror tableMirror, Environment environment) {
        try {
            // Get configuration through the infrastructure abstraction
            ConfigLiteDto config = conversionResult.getConfig();
            JobDto job = conversionResult.getJob();

            EnvironmentTable et = tableMirror.getEnvironmentTable(environment);

            if (et == null || et.getDefinition().isEmpty()) {
                return ValidationResult.success(); // Nothing to validate
            }

            // Apply the same business logic that was in TableService.checkTableFilter()

            // Check VIEW processing rules
            if (config.getMigrateVIEW().isOn() && job.getStrategy() != com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.DUMP) {
                if (!com.cloudera.utils.hms.util.TableUtils.isView(et)) {
                    return ValidationResult.failure("VIEW's only processing selected, but table is not a view");
                }
            } else {
                // Check ACID table rules
                if (com.cloudera.utils.hms.util.TableUtils.isManaged(et)) {
                    if (com.cloudera.utils.hms.util.TableUtils.isACID(et)) {
                        if (!config.getMigrateACID().isOn()) {
                            return ValidationResult.failure("ACID table and ACID processing not selected (-ma|-mao)");
                        }
                    } else if (config.getMigrateACID().isOnly()) {
                        return ValidationResult.failure("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (com.cloudera.utils.hms.util.TableUtils.isHiveNative(et)) {
                    if (config.getMigrateACID().isOnly()) {
                        return ValidationResult.failure("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (com.cloudera.utils.hms.util.TableUtils.isView(et)) {
                    if (job.getStrategy() != com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.DUMP) {
                        return ValidationResult.failure("This is a VIEW and VIEW processing wasn't selected");
                    }
                } else {
                    // Non-Native Tables
                    if (!config.isMigrateNonNative()) {
                        return ValidationResult.failure("This is a Non-Native hive table and non-native process wasn't selected");
                    }
                }
            }

            // Check for storage migration flag
            if (job.getStrategy() == com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.STORAGE_MIGRATION) {
                String smFlag = com.cloudera.utils.hms.util.TableUtils.getTblProperty(
                        com.cloudera.utils.hms.mirror.TablePropertyVars.HMS_STORAGE_MIGRATION_FLAG, et);
                if (smFlag != null) {
                    return ValidationResult.failure("The table has already gone through the STORAGE_MIGRATION process on " + smFlag);
                }
            }

            // Check table size limit
            DatasetDto.DatabaseSpec dbSpec = conversionResult.getDataset().getDatabase(databaseName);
            if (dbSpec.getFilter() != null && dbSpec.getFilter().getMaxSizeMb() > 0) {
                Long dataSize = (Long) et.getStatistics().get(com.cloudera.utils.hms.mirror.MirrorConf.DATA_SIZE);
                if (dataSize != null) {
                    if (dbSpec.getFilter().getMaxSizeMb() * (1024 * 1024) < dataSize) {
                        return ValidationResult.failure("The table dataset size exceeds the specified table filter size limit: " +
                                dbSpec.getFilter().getMaxSizeMb() + "Mb < " + dataSize);
                    }
                }
            }

            return ValidationResult.success();

        } catch (Exception e) {
            return ValidationResult.failure("Error validating table filter: " + e.getMessage());
        }
    }

    public String getCreateStatement(TableMirror tableMirror, Environment environment) {
        log.info("Getting CREATE statement for table: {} in environment: {}", tableMirror.getName(), environment);
        String createStatement = null;
        try {
            // ...existing logic to get the statement...
            StringBuilder createStatementBldr = new StringBuilder();
            ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                    new IllegalStateException("No ConversionResult found in the execution context."));
            ConfigLiteDto config = conversionResult.getConfig();
            JobDto job = conversionResult.getJob();
            RunStatus runStatus = conversionResult.getRunStatus();

            Boolean cine = config.isCreateIfNotExists();

            List<String> tblDef = tableMirror.getTableDefinition(environment);
            if (tblDef != null) {
                Iterator<String> iter = tblDef.iterator();
                while (iter.hasNext()) {
                    String line = iter.next();
                    if (cine && line.startsWith("CREATE TABLE")) {
                        line = line.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                    } else if (cine && line.startsWith("CREATE EXTERNAL TABLE")) {
                        line = line.replace("CREATE EXTERNAL TABLE", "CREATE EXTERNAL TABLE IF NOT EXISTS");
                    }
                    createStatementBldr.append(line);
                    if (iter.hasNext()) {
                        createStatementBldr.append("\n");
                    }
                }
            } else {
                log.error("Couldn't location definition for table: {} in environment: {}", tableMirror.getName(), environment.toString());
            }
            createStatement = createStatementBldr.toString();
            log.debug("Fetched CREATE statement for table {}: {}", tableMirror.getName(), createStatement);
            return createStatement;
        } catch (Exception e) {
            log.error("Failed to get CREATE statement for table: {}, environment: {}", tableMirror, environment, e);
            throw e;
        }
    }

    /**
     * Get the table definition for the given table mirror and environment.
     *
     * @param tableMirror The table mirror object.
     * @param environment The environment (LEFT or RIGHT).
     */
    private void getTableDefinition(String databaseName, TableMirror tableMirror, EnvironmentTable environmentTable,
                                    Environment environment) throws SQLException {
        final String tableId = String.format("%s:%s.%s",
                environment, databaseName, tableMirror.getName());
        log.info("Fetching table definition for table: {} in environment: {}", tableMirror.getName(), environment);
        log.info("Starting to get table definition for {}", tableId);

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();
        DatasetDto.DatabaseSpec databaseSpec = conversionResult.getDataset().getDatabase(databaseName);

        // Fetch Table Definition
        // TODO: Fix for Test Data
        if (conversionResult.isMockTestDataset()) {
            log.debug("Loading test data is enabled. Skipping schema load for {}", tableId);
        } else {
            log.debug("Loading schema from catalog for {}", tableId);
            loadSchemaFromCatalog(databaseName, tableMirror, environment);
        }
        log.debug("Checking table filter for {}", tableId);
        checkTableFilter(databaseName, tableMirror, environment);

        if (!tableMirror.isRemove()
                && !conversionResult.isMockTestDataset()
        ) {
            log.debug("Table is not marked for removal. Proceeding with data strategy checks for {}", tableId);
            handleDataStrategy(tableMirror, environment, environmentTable, tableId);
            Boolean partitioned = TableUtils.isPartitioned(environmentTable);
            if (environment == Environment.LEFT && partitioned) {
                log.debug("Table is partitioned. Checking metadata details for {}", tableId);
                if (config.loadMetadataDetails()) {
                    log.debug("Loading partition metadata directly for {}", tableId);
                    loadTablePartitionMetadataDirect(databaseName, tableMirror, environment);
                }
            }

            Integer partLimit = databaseSpec.getFilter().getMaxPartitions();//.getFilter().getTblPartitionLimit();
            if (partLimit != null && partLimit > 0) {
                log.debug("Checking partition count filter for {}", tableId);
                if (environmentTable.getPartitions().size() > partLimit) {
                    log.info("Table partition count exceeds limit for {}. Limit: {}, Actual: {}",
                            tableId, partLimit, environmentTable.getPartitions().size());
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                            partLimit + " < " + environmentTable.getPartitions().size());
                }
            }
        }
        log.info("Completed table definition for {}", tableId);
        log.debug("Successfully fetched table definition for table: {}", tableMirror);
    }

    private void handleDataStrategy(TableMirror tableMirror, Environment environment,
                                    EnvironmentTable environmentTable, String tableId) {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        switch (job.getStrategy()) {
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
            case DUMP:
            case LINKED:
                log.debug("Data strategy {} does not require stats collection for {}", job.getStrategy(), tableId);
                break;
            case SQL:
            case HYBRID:
            case EXPORT_IMPORT:
            case STORAGE_MIGRATION:
            case COMMON:
            case ACID:
                if (!TableUtils.isView(environmentTable) && TableUtils.isHiveNative(environmentTable)) {
                    log.debug("Collecting table stats for {}", tableId);
                    try {
                        loadTableStats(tableMirror, environment);
                    } catch (DisabledException e) {
                        log.warn("Stats collection is disabled. Skipping stats collection for {}", tableId);
                    } catch (RuntimeException rte) {
                        log.error("Error loading table stats for {}", tableId, rte);
                        tableMirror.addIssue(environment, rte.getMessage());
                    }
                }
                break;
        }
    }

    @Async("metadataThreadPool")
    public CompletableFuture<ReturnStatus> getTableMetadata(ConversionResult conversionResult, String databaseName,
                                                            String tableName) {
        log.info("Fetching table metadata asynchronously for table: {}.{}", databaseName, tableName);
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

        RunStatus runStatus = conversionResult.getRunStatus();

        ReturnStatus rtn = new ReturnStatus();
        // Preset and overwrite the status when an issue or anomoly occurs.
        rtn.setStatus(ReturnStatus.Status.SUCCESS);

        JobDto job = conversionResult.getJob();

        return CompletableFuture.supplyAsync(() -> {
            // ...logic...
            getExecutionContextService().setConversionResult(conversionResult);
            getExecutionContextService().setRunStatus(runStatus);

            TableMirror tableMirror = null;
            try {
                tableMirror = getTableMirrorRepository().findByName(conversionResult.getKey(), databaseName, tableName)
                                .orElseThrow(() -> new IllegalStateException("Couldn't locate Table '" + databaseName + "." +
                                        tableName + " for Conversion Key: " + conversionResult.getKey()));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            rtn.setDatabaseName(databaseName);
            rtn.setTableName(tableName);
            EnvironmentTable leftEnvTable = tableMirror.getEnvironmentTable(Environment.LEFT);
            try {
                getTableDefinition(databaseName, tableMirror, leftEnvTable, Environment.LEFT);
                if (tableMirror.isRemove()) {
                    rtn.setStatus(ReturnStatus.Status.SKIP);
                    return rtn;
                } else {
                    switch (job.getStrategy()) {
                        case DUMP:
                        case STORAGE_MIGRATION:
                            // Make a clone of the left as a working copy.
                            try {
                                tableMirror.getEnvironments().put(Environment.RIGHT,
                                        tableMirror.getEnvironmentTable(Environment.LEFT).clone());
                            } catch (CloneNotSupportedException e) {
                                log.error("Clone not supported for table: {}.{}", databaseName, tableMirror.getName());
                            }
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                            break;
                        default:
                            EnvironmentTable rightEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
                            try {
                                getTableDefinition(databaseName, tableMirror, rightEnvTable, Environment.RIGHT);
                                rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                            } catch (SQLException se) {
                                // Can't find the table on the RIGHT.  This is OK if the table doesn't exist.
                                log.debug("No table definition for {}:{}", databaseName, tableMirror.getName(), se);
                            }
                    }
                }
            } catch (SQLException throwables) {
                // Check to see if the RIGHT exists.  This is for `--sync` mode.
                // If it doesn't exist, then this is OK.
                if (job.isSync()) {
                    EnvironmentTable rightEnvTable = tableMirror.getEnvironmentTable(Environment.RIGHT);
                    try {
                        getTableDefinition(databaseName, tableMirror, rightEnvTable, Environment.RIGHT);
                        rtn.setStatus(ReturnStatus.Status.SUCCESS);//successful = Boolean.TRUE;
                    } catch (SQLException se) {
                        // OK, if the db doesn't exist yet.
                        handleSqlException(throwables, tableMirror, rightEnvTable, Environment.RIGHT);
                        rtn.setStatus(ReturnStatus.Status.ERROR);
                        rtn.setException(throwables);
                    }
                } else {
                    handleSqlException(throwables, tableMirror, leftEnvTable, Environment.LEFT);
                    rtn.setStatus(ReturnStatus.Status.ERROR);
                }
            } finally {
                // Save the latest copy of TableMirror
                try {
                    getTableMirrorRepository().save(conversionResult.getKey(), databaseName, tableMirror);
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }
            log.debug("Metadata fetch completed for table: {}", tableMirror);
            return rtn;
        });
    }

    @Async("metadataThreadPool")
    public CompletableFuture<ReturnStatus> getTableListForProcessing(ConversionResult conversionResult, DBMirror dbMirror) {
        log.info("Fetching tables asynchronously for DBMirror: {}", dbMirror.getName());
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

        return CompletableFuture.supplyAsync(() -> {
            ReturnStatus rtn = new ReturnStatus();
            try {
                getExecutionContextService().setConversionResult(conversionResult);
                getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

                ConfigLiteDto config = conversionResult.getConfig();
                JobDto job = conversionResult.getJob();
                RunStatus runStatus = conversionResult.getRunStatus();
                log.debug("Getting tables for Database {}", dbMirror.getName());
                try {
                    getTablesFromMetastore(dbMirror, Environment.LEFT);
                    if (job.isSync()) {
                        // Get the tables on the RIGHT side.  Used to determine if a table has been dropped on the LEFT
                        // and later needs to be removed on the RIGHT.
                        try {
                            getTablesFromMetastore(dbMirror, Environment.RIGHT);
                        } catch (SQLException se) {
                            // OK, if the db doesn't exist yet.
                        }
                    }
                    rtn.setStatus(ReturnStatus.Status.SUCCESS);
                } catch (SQLException throwables) {
                    rtn.setStatus(ReturnStatus.Status.ERROR);
                    rtn.setException(throwables);
                } catch (RuntimeException rte) {
                    log.error("Runtime Issue getting tables for Database: {}", dbMirror.getName(), rte);
                    rtn.setStatus(ReturnStatus.Status.ERROR);
                    rtn.setException(rte);
                }
                log.debug("Tables fetch completed for DBMirror: {}", dbMirror);
                return rtn;
            } catch (Exception e) {
                log.error("Error occurred while fetching tables for DBMirror: {}", dbMirror.getName(), e);
                rtn.setStatus(ReturnStatus.Status.ERROR);
                rtn.setException(e);
                return rtn;
            } finally {
                getExecutionContextService().reset();
            }
        });
    }

    private void getTablesFromMetastore(DBMirror dbMirror, Environment environment) throws SQLException {
        log.info("Fetching tables for DBMirror: {} in environment: {}", dbMirror.getName(), environment);
        Connection conn = null;
        String database = null;
        try {
            ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                    new IllegalStateException("ConversionResult is null.  Unable to continue."));
            ConfigLiteDto config = conversionResult.getConfig();
            JobDto job = conversionResult.getJob();
            RunStatus runStatus = conversionResult.getRunStatus();

            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn == null) {
                log.error("Unable to obtain a connection for environment: {}", environment);
                dbMirror.addIssue(environment, "No connection available for environment.");
                return;
            }

            database = (environment == Environment.LEFT)
                    ? dbMirror.getName()
                    : getConversionResultService().getResolvedDB(dbMirror.getName());

            log.info("Loading tables for {}:{}", environment, database);

            List<String> showStatements = buildShowStatements(environment);

            try (Statement stmt = conn.createStatement()) { // try-with-resources for Statement
                setDatabaseContext(stmt, database);
                for (String show : showStatements) {
                    log.debug("Executing show statement: {}", show);
                    try (ResultSet rs = stmt.executeQuery(show)) { // try-with-resources for ResultSet
                        while (rs.next()) {
                            String tableName = rs.getString(1);
                            addTableForProcessing(dbMirror, tableName);
                        }
                    }
                }
            }
            log.debug("Fetched tables for DBMirror: {}, environment: {}", dbMirror, environment);
        } catch (SQLException e) {
            log.error("SQLException while fetching tables for DBMirror: {}, environment: {}", dbMirror.getName(), environment, e);
            dbMirror.addIssue(environment, (database != null ? database : "unknown") + " " + e.getMessage());
            throw e;
        } finally {
            if (conn != null) try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private List<String> buildShowStatements(Environment environment) {

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        List<String> shows = new ArrayList<>();
        if (!conversionResult.getConnection(environment).getPlatformType().isLegacyHive()) {
            if (config.getMigrateVIEW().isOn()) {
                shows.add(MirrorConf.SHOW_VIEWS);
                if (job.getStrategy() == DUMP) {
                    shows.add(MirrorConf.SHOW_TABLES);
                }
            } else {
                shows.add(MirrorConf.SHOW_TABLES);
            }
        } else {
            shows.add(MirrorConf.SHOW_TABLES);
        }
        return shows;
    }

    private void setDatabaseContext(Statement stmt, String database) throws SQLException {
        stmt.execute(MessageFormat.format(MirrorConf.USE, database));
        log.debug("Set Hive DB Session Context to {}", database);
    }

    private void addTableForProcessing(DBMirror dbMirror, String tableName) {
        if (tableName == null) return;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();
        DatasetDto.DatabaseSpec databaseSpec = conversionResult.getDataset().getDatabase(dbMirror.getName());

        if (startsWithAny(tableName, config.getTransfer().getTransferPrefix(), config.getTransfer().getShadowPrefix())
                || endsWith(tableName, config.getTransfer().getStorageMigrationPostfix())) {
            // TODO: Evaluate Removal process.  At this point, we're building the list from the list
            //       of tables in the metastore.  We haven't created them yet, unless this is TestData.
            //       so do we need this? Just don't add them.
//            markTableForRemoval(dbMirror.getName(), tableMirror, getRemovalReason(tableMirror.getName()));
            return;
        }

        DatasetDto.TableFilter filter = databaseSpec.getFilter();
//        Filter filter = config.getFilter();

        // Now check if table should be filtered out.
        boolean addTable = false;
        if (filter == null || (isBlank(filter.getIncludeRegEx()) && isBlank(filter.getExcludeRegEx()))) {
            addTable = true;
        } else {
            if (!isBlank(filter.getIncludeRegEx())) {
                Matcher matcher = filter.getIncludeRegExPattern().matcher(tableName);
                addTable = matcher.matches();
            } else if (!isBlank(filter.getExcludeRegEx())) {
                Matcher matcher = filter.getExcludeRegExPattern().matcher(tableName);
                addTable = !matcher.matches();
            }
        }

        if (addTable) {
            // Add to DBMirror for processing.
            TableMirror tableMirror = new TableMirror();
            tableMirror.setName(tableName);
            try {
                getTableMirrorRepository().save(conversionResult.getKey(), dbMirror.getName(), tableMirror);
                log.info("{}.{} added to processing list.", dbMirror.getName(), tableName);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        } else {
            log.info("{}.{} did not match filter and will NOT be added.", dbMirror.getName(), tableName);
        }
    }

    private static boolean startsWithAny(String value, String... prefixes) {
        for (String prefix : prefixes) {
            if (!isBlank(prefix) && value.startsWith(prefix)) return true;
        }
        return false;
    }

    private static boolean endsWith(String value, String postfix) {
        return !isBlank(postfix) && value.endsWith(postfix);
    }

    @Deprecated
    /*
    TODO: Not sure this is needed.  Maybe just for mock data of other filters like partition or table
            type filtering. TBD
     */
    private void markTableForRemoval(String databaseName, TableMirror tableMirror, String reason) {
        // TODO: Fix
//        TableMirror tableMirror = dbMirror.addTable(tableName);
        tableMirror.setRemove(true);
        tableMirror.setRemoveReason(reason);
        log.info("{}.{} was NOT added to list. Reason: {}", databaseName, tableMirror.getName(), reason);
    }

    private String getRemovalReason(String tableName) {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
            return "Table name matches the transfer prefix; likely remnant of a previous event.";
        } else if (tableName.startsWith(config.getTransfer().getShadowPrefix())) {
            return "Table name matches the shadow prefix; likely remnant of a previous event.";
        } else if (tableName.endsWith(config.getTransfer().getStorageMigrationPostfix())) {
            return "Table name matches the storage migration suffix; likely remnant of a previous event.";
        }
        return "Removed for unspecified reason";
    }

    private static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

// Refactored: Extracted helper methods, renamed vars, improved resource handling, modularized, reduced nesting

    private static final String OWNER_PREFIX = "owner";

    public void
    loadSchemaFromCatalog(String databaseName, TableMirror tableMirror, Environment environment) throws SQLException {
        log.info("Loading schema from catalog for table: {} in environment: {}", tableMirror.getName(), environment);
        // ...logic...
//        String database = resolveDatabaseName(dbMirror, tableMirror, environment);
        String database = getConversionResultService().getResolvedDB(databaseName);
        EnvironmentTable environmentTable = tableMirror.getEnvironmentTable(environment);

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        try (Connection connection = getConnectionPoolService().getHS2EnvironmentConnection(environment)) {
            if (connection == null) return;

            try (Statement statement = connection.createStatement()) {
                useDatabase(statement, database);
                List<String> tableDefinition = fetchTableDefinition(statement, tableMirror.getName(), database, environment);
                environmentTable.setDefinition(tableDefinition);
                environmentTable.setName(tableMirror.getName());
                environmentTable.setExists(Boolean.TRUE);
                tableMirror.addStep(environment.toString(), "Fetched Schema");

                if (config.getOwnershipTransfer().isTable()) {
                    String owner = fetchTableOwner(statement, tableMirror, database, environment);
                    if (owner != null) {
                        environmentTable.setOwner(owner);
                    }
                }
            }
        }
        log.debug("Loaded schema from catalog for table: {}", tableMirror);
    }

    private String resolveDatabaseName(DBMirror dbMirror, TableMirror tableMirror, Environment environment) {
        log.trace("Resolving database name for table: {} in environment: {}", tableMirror, environment);
        // ...logic...
        if (environment == Environment.LEFT) {
            return dbMirror.getName();
        } else {
            return getConversionResultService().getResolvedDB(dbMirror.getName());
        }
    }

    private void useDatabase(Statement statement, String database) throws SQLException {
        log.trace("Executing USE database statement: {}", database);
        // ...logic...
        String useStatement = MessageFormat.format(MirrorConf.USE, database);
        statement.execute(useStatement);
    }

    private List<String> fetchTableDefinition(Statement statement, String tableName, String database, Environment environment) throws SQLException {
        log.debug("Fetching table definition for table: {} from database: {} in environment: {}", tableName, database, environment);
        // ...logic...
        String showStatement = MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, tableName);
        List<String> tableDefinition = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(showStatement)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            if (metaData.getColumnCount() >= 1) {
                while (resultSet.next()) {
                    try {
                        tableDefinition.add(resultSet.getString(1).trim());
                    } catch (NullPointerException npe) {
                        log.error("Loading Table Definition. Issue with SHOW CREATE TABLE resultset. " +
                                        "ResultSet record(line) is null. Skipping. {}:{}.{}",
                                environment, database, tableName);
                    }
                }
            } else {
                log.error("Loading Table Definition. Issue with SHOW CREATE TABLE resultset. No Metadata. {}:{}.{}",
                        environment, database, tableName);
            }
        }
        return tableDefinition;
    }

    private String fetchTableOwner(Statement statement, TableMirror tableMirror, String database, Environment environment) {
        log.debug("Fetching owner for table: {} in database: {}", tableMirror, database);
        // ...logic...
        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
        try (ResultSet resultSet = statement.executeQuery(ownerStatement)) {
            while (resultSet.next()) {
                String value = resultSet.getString(1);
                if (value != null && value.startsWith(OWNER_PREFIX)) {
                    String[] ownerLine = value.split(":");
                    try {
                        return ownerLine[1];
                    } catch (Throwable t) {
                        log.error("Couldn't parse 'owner' value from: {} for table {}:{}.{}",
                                value, environment, database, tableMirror.getName());
                    }
                    break;
                }
            }
        } catch (SQLException ignored) {
            // Failed to gather owner details.
        }
        return null;
    }

    private void handleSqlException(SQLException exception, TableMirror tableMirror, EnvironmentTable environmentTable, Environment environment) {
        log.error("SQL Exception for table: {} in environmentTable: {}, environment: {}", tableMirror, environmentTable, environment, exception);
        // ...logic...
        String message = exception.getMessage();
        if (message.contains("Table not found") || message.contains("Database does not exist")) {
            tableMirror.addStep(environment.toString(), "No Schema");
        } else {
            log.error(message, exception);
            environmentTable.addError(message);
        }
    }

    protected void loadTableOwnership(DBMirror dbMirror, TableMirror tableMirror, Environment environment) {
        log.info("Loading ownership information for table: {} in environment: {}", tableMirror, environment);
        // ...logic...
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        if (config.getOwnershipTransfer().isTable()) {
            try {
                conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
                if (conn != null) {
                    stmt = conn.createStatement();

                    try {
                        String ownerStatement = MessageFormat.format(MirrorConf.SHOW_TABLE_EXTENDED, tableMirror.getName());
                        resultSet = stmt.executeQuery(ownerStatement);
                        String owner = null;
                        while (resultSet.next()) {

                            if (resultSet.getString(1).startsWith("owner")) {
                                String[] ownerLine = resultSet.getString(1).split(":");
                                try {
                                    owner = ownerLine[1];
                                } catch (Throwable t) {
                                    // Parsing issue.
                                    log.error("Couldn't parse 'owner' value from: {} for table: {}.{}", resultSet.getString(1), dbMirror.getName(), tableMirror.getName());
                                }
                                break;
                            }
                        }
                        if (owner != null) {
                            et.setOwner(owner);
                        }
                    } catch (SQLException sed) {
                        // Failed to gather owner details.
                    }

                }
            } catch (SQLException throwables) {
                if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                    // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                    tableMirror.addStep(environment.toString(), "No Schema");
                } else {
                    log.error(throwables.getMessage(), throwables);
                    et.addError(throwables.getMessage());
                }
            } finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException sqlException) {
                        // ignore
                    }
                }
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlException) {
                        // ignore
                    }
                }
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        }
    }

    protected void loadTablePartitionMetadata(DBMirror dbMirror, TableMirror tableMirror, Environment environment) throws SQLException {
        log.info("Loading partition metadata for table: {}, environment: {}", tableMirror, environment);
        // ...logic...
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String database = dbMirror.getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {

                stmt = conn.createStatement();
                log.debug("{}:{}.{}: Loading Partitions", environment, database, et.getName());

                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, et.getName()));
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), NOT_SET);
                }
                et.setPartitions(partDef);

            }
        } catch (SQLException throwables) {
            et.addError(throwables.getMessage());
            log.error("{}:{}.{}: Issue loading Partitions.", environment, database, et.getName(), throwables);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlException) {
                    // ignore
                }
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTablePartitionMetadataDirect(String databaseName, TableMirror tableMirror, Environment environment) {
        /*
        1. Get Metastore Direct Connection
        2. Get Query Definitions
        3. Get Query for 'part_locations'
        4. Execute Query
        5. Load Partition Data
         */
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();
        ConnectionDto connection = conversionResult.getConnection(environment);

        // TODO: Handle RIGHT Environment. At this point, we're only handling LEFT.
        if (!configService.isMetastoreDirectConfigured(connection)) {
            log.info("Metastore Direct Connection is not configured for {}.  Skipping.", environment);
            runStatus.addWarning(METASTORE_PARTITION_LOCATIONS_NOT_FETCHED);
            return;
        }

//        String database = dbMirror.getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getMetastoreDirectEnvironmentConnection(environment);
            log.info("Loading Partitions from Metastore Direct Connection {}:{}.{}", environment, databaseName, et.getName());
            QueryDefinitions queryDefinitions = getQueryDefinitionsService().getQueryDefinitions(environment);
            if (queryDefinitions != null) {
                String partLocationQuery = queryDefinitions.getQueryDefinition("part_locations").getStatement();
                pstmt = conn.prepareStatement(partLocationQuery);
                pstmt.setString(1, databaseName);
                pstmt.setString(2, et.getName());
                resultSet = pstmt.executeQuery();
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), resultSet.getString(2));
                }
                et.setPartitions(partDef);
            }
            log.info("Loaded Partitions from Metastore Direct Connection {}:{}.{}", environment, databaseName, et.getName());
        } catch (SQLException throwables) {
            et.addError(throwables.getMessage());
            log.error("Issue loading Partitions from Metastore Direct Connection. {}:{}.{}", environment, databaseName, et.getName());
            log.error(throwables.getMessage(), throwables);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTableStats(TableMirror tableMirror, Environment environment) throws DisabledException {
        // Considered only gathering stats for partitioned tables, but decided to gather for all tables to support
        //  smallfiles across the board.
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        if (config.getOptimization().isSkipStatsCollection()) {
            log.debug("{}:{}: Skipping Stats Collection.", environment, et.getName());
            return;
        }
        switch (job.getStrategy()) {
            case DUMP:
            case SCHEMA_ONLY:
                // We don't need stats for these.
                return;
            default:
                break;
        }

        // Determine File sizes in table or partitions.
        /*
        - Get Base location for table
        - Get HadoopSession
        - Do a 'count' of the location.
         */
        String location = TableUtils.getLocation(et.getName(), et.getDefinition());
        // Only run checks against hdfs and ozone namespaces.
        String[] locationParts = location.split(":");
        String protocol = locationParts[0];
        // Determine Table File Format
        TableUtils.getSerdeType(et);

        if (conversionResult.getSupportedFileSystems().contains(protocol)) {
            CliEnvironment cli = getCliEnvironment();

            String countCmd = "count " + location;
            CommandReturn cr = cli.processInput(countCmd);
            if (!cr.isError() && cr.getRecords().size() == 1) {
                // We should only get back one record.
                List<Object> countRecord = cr.getRecords().get(0);
                // 0 = Folder Count
                // 1 = File Count
                // 2 = Size Summary
                try {
                    Double avgFileSize = (double) (Long.parseLong(countRecord.get(2).toString()) /
                            Integer.parseInt(countRecord.get(1).toString()));
                    et.getStatistics().put(DIR_COUNT, Integer.valueOf(countRecord.get(0).toString()));
                    et.getStatistics().put(FILE_COUNT, Integer.valueOf(countRecord.get(1).toString()));
                    et.getStatistics().put(DATA_SIZE, Long.valueOf(countRecord.get(2).toString()));
                    et.getStatistics().put(AVG_FILE_SIZE, avgFileSize);
                    et.getStatistics().put(TABLE_EMPTY, Boolean.FALSE);
                } catch (ArithmeticException ae) {
                    // Directory is probably empty.
                    et.getStatistics().put(TABLE_EMPTY, Boolean.TRUE);
                }
            } else {
                // Issue getting count.

            }
        }
    }

    /**
     * From this cluster, run the SQL built up in the tblMirror(environment)
     *
     * @param tblMirror
     * @param environment Allows to override cluster environment
     * @return
     */
    public Boolean runTableSql(TableMirror tblMirror, Environment environment) {
        Connection conn = null;
        Boolean rtn = Boolean.FALSE;

        EnvironmentTable et = tblMirror.getEnvironmentTable(environment);

        rtn = runTableSql(et.getSql(), tblMirror, environment);

        return rtn;
    }

    public Boolean runTableSql(List<Pair> sqlList, TableMirror tblMirror, Environment environment) {
        Boolean rtn = Boolean.TRUE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        JobExecution jobExecution = conversionResult.getJobExecution();
        RunStatus runStatus = conversionResult.getRunStatus();

        // Check if there is anything to run.
        if (nonNull(sqlList) && !sqlList.isEmpty()) {
            // Check if the cluster is connected. This could happen if the cluster is a virtual cluster created as a place holder for processing.
            if (nonNull(conversionResult.getConnection(environment)) && !isBlank(conversionResult.getConnection(environment).getHs2Uri()) &&
                    conversionResult.getConnection(environment).isHs2Connected()) {
                // Skip this if using test data.
                if (!job.isLoadingTestData()) {

                    try (Connection conn = getConnectionPoolService().getHS2EnvironmentConnection(environment)) {
                        if (isNull(conn) && jobExecution.isExecute() && conversionResult.getConnection(environment).isHs2Connected()) {
                            // this is a problem.
                            rtn = Boolean.FALSE;
                            tblMirror.addIssue(environment, "Connection missing. This is a bug.");
                        }

                        if (isNull(conn) && !conversionResult.getConnection(environment).isHs2Connected()) {
                            tblMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                                    "The scripts will need to be run 'manually'.");
                        }

                        if (rtn && nonNull(conn)) {
                            try (Statement stmt = conn.createStatement()) {
                                for (Pair pair : sqlList) {
                                    String action = pair.getAction();
                                    if (action.trim().isEmpty() || action.trim().startsWith("--")) {
                                        continue;
                                    } else {
                                        log.debug("{}:SQL:{}:{}", environment, pair.getDescription(), pair.getAction());
                                        tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                                        if (jobExecution.isExecute()) {
                                            // Log the Return of 'set' commands.
                                            if (pair.getAction().trim().toLowerCase().startsWith("set")) {
                                                stmt.execute(pair.getAction());
                                                try {
                                                    // Check for a result set and print result if present.
                                                    ResultSet resultSet = stmt.getResultSet();
                                                    if (!isNull(resultSet)) {
                                                        while (resultSet.next()) {
                                                            tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription() + " : " + resultSet.getString(1));
                                                            log.info("{}:{}", pair.getAction(), resultSet.getString(1));
                                                        }
                                                    } else {
                                                        tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                                                    }
                                                } catch (SQLException se) {
                                                    // Otherwise, just log command.
                                                    tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                                                }
                                            } else {
                                                stmt.execute(pair.getAction());
                                                tblMirror.addStep(environment.toString(), "Sql Run Complete for: " + pair.getDescription());
                                            }
                                        } else {
                                            tblMirror.addStep(environment.toString(), "Sql Run SKIPPED (DRY-RUN) for: " + pair.getDescription());
                                        }
                                    }
                                }
                            } catch (SQLException throwables) {
                                log.error("{}:{}", environment.toString(), throwables.getMessage(), throwables);
                                String message = throwables.getMessage();
                                if (throwables.getMessage().contains("HiveAccessControlException Permission denied")) {
                                    message = message + " See [Hive SQL Exception / HDFS Permissions Issues](https://github.com/cloudera-labs/hms-mirror#hive-sql-exception--hdfs-permissions-issues)";
                                }
                                if (throwables.getMessage().contains("AvroSerdeException")) {
                                    message = message + ". It's possible the `avro.schema.url` referenced file doesn't exist at the target. " +
                                            "Use the `-asm` option and hms-mirror will attempt to copy it to the new cluster.";
                                }
                                tblMirror.getEnvironmentTable(environment).addError(message);
                                rtn = Boolean.FALSE;
                            }
                        }
                    } catch (SQLException throwables) {
                        tblMirror.getEnvironmentTable(environment).addError("Connecting: " + throwables.getMessage());
                        log.error("{}:{}", environment.toString(), throwables.getMessage(), throwables);
                        rtn = Boolean.FALSE;
                    }
                }
            }
        }
        return rtn;
    }


}
