/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.connections.ConnectionException;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hms.mirror.datastrategy.HybridAcidDowngradeInPlaceDataStrategy;
import com.cloudera.utils.hms.mirror.datastrategy.HybridDataStrategy;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

import static com.cloudera.utils.hms.mirror.MessageCode.DISTCP_FOR_SO_ACID;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class TransferService {
    // Constants
    private static final String DB_SUFFIX = ".db";
    private static final String PATH_SEPARATOR = "/";

    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");
    private final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");
    @NonNull
    private final ConfigService configService;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final ExecuteSessionService executeSessionService;
    @NonNull
    private final TableService tableService;
    @NonNull
    private final DatabaseService databaseService;
    @NonNull
    private final WarehouseService warehouseService;
    @NonNull
    private final DataStrategyService dataStrategyService;
    @NonNull
    private final HybridDataStrategy hybridDataStrategy;
    @NonNull
    private final HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy;
    @NonNull
    private final DBMirrorRepository dbMirrorRepository;
    @NonNull
    private final TableMirrorRepository tableMirrorRepository;

    @Async("jobThreadPool")
    public CompletableFuture<ReturnStatus> build(ConversionResult conversionResult, String databaseName, String tableName) {
        ReturnStatus rtn = new ReturnStatus();
        // Set these for the new thread.
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        rtn.setDatabaseName(databaseName);
        rtn.setTableName(tableName);
        RunStatus runStatus = conversionResult.getRunStatus();
        Warehouse warehouse = null;

        DBMirror dbMirror = null;
        try {
            dbMirror = getDbMirrorRepository().findByName(conversionResult.getKey(), databaseName).orElseThrow(() ->
                    new IllegalStateException("Couldn't locate DBMirror "+ databaseName + " for Conversion Key: " + conversionResult.getKey()));
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        TableMirror tableMirror = null;
        try {
            tableMirror = getTableMirrorRepository().findByName(conversionResult.getKey(), databaseName, tableName)
                    .orElseThrow(() -> new IllegalStateException("Couldn't locate table " + databaseName + "." + tableName +
                            " for Conversion Key: " + conversionResult.getKey()));
            // Ensure the table isn't in an ERROR phase before continuing.
            if (tableMirror.getPhaseState() == PhaseState.ERROR) {
                rtn.setStatus(ReturnStatus.Status.INCOMPLETE);
                return CompletableFuture.completedFuture(rtn);
            }
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        try {
            Date start = new Date();
            log.info("Building migration for {}.{}", dbMirror.getName(), tableMirror.getName());
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

            // Set Database to Transfer DB.
            tableMirror.setPhaseState(PhaseState.CALCULATING_SQL);
            tableMirror.incPhase();
            tableMirror.addStep("Build TRANSFER", job.getStrategy().toString());
            try {
                DataStrategy dataStrategy = null;
                switch (job.getStrategy()) {
                    case HYBRID:
                        if (TableUtils.isACID(let) && config.getMigrateACID().isInplace()) {
                            if (hybridAcidDowngradeInPlaceDataStrategy.build(dbMirror, tableMirror)) {
                                rtn.setStatus(ReturnStatus.Status.SUCCESS);
                            } else {
                                rtn.setStatus(ReturnStatus.Status.ERROR);
                                runStatus.getOperationStatistics().getIssues().incrementTables();
                            }
                        } else {
                            if (hybridDataStrategy.build(dbMirror, tableMirror)) {
                                rtn.setStatus(ReturnStatus.Status.SUCCESS);
                            } else {
                                rtn.setStatus(ReturnStatus.Status.ERROR);
                            }
                        }
                        break;
                    default:
                        dataStrategy = getDataStrategyService().getDefaultDataStrategy(job.getStrategy());
                        if (dataStrategy.build(dbMirror, tableMirror)) {
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);
                        } else {
                            rtn.setStatus(ReturnStatus.Status.ERROR);
                        }
                        break;
                }
                // Build out DISTCP workplans.
                boolean consolidateSourceTables = config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp();

                if (rtn.getStatus() == ReturnStatus.Status.SUCCESS && config.getTransfer().getStorageMigration().isDistcp()) {
                    warehouse = warehouseService.getWarehousePlan(dbMirror.getName());

                    // Route to appropriate distcp handler based on configuration
                    if (!isBlank(job.getIntermediateStorage())) {
                        // Intermediate storage scenario: LEFT -> INTERMEDIATE -> RIGHT
                        handleIntermediateStorageDistcp(conversionResult, config, job, dbMirror, tableMirror,
                                let, set, ret, warehouse, consolidateSourceTables, rtn);
                    } else if (job.getStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        // Common push scenario: LEFT -> RIGHT
                        handleCommonPushDistcp(conversionResult, config, dbMirror, tableMirror,
                                let, tet, set, ret, warehouse, consolidateSourceTables, rtn);
                    } else {
                        // Right pull scenario: RIGHT pulls from LEFT
                        handleRightPullDistcp(conversionResult, config, job, dbMirror, tableMirror,
                                let, tet, ret, warehouse, consolidateSourceTables, rtn);
                    }
                }

                if (rtn.getStatus() == ReturnStatus.Status.SUCCESS) {
                    tableMirror.setPhaseState(PhaseState.CALCULATED_SQL);
                } else if (rtn.getStatus() == ReturnStatus.Status.INCOMPLETE) {
                    tableMirror.setPhaseState(PhaseState.CALCULATED_SQL_WARNING);
                } else {
                    tableMirror.setPhaseState(PhaseState.ERROR);
                    // Increase Unsuccessful Table Count.
                    runStatus.getUnSuccessfulTableCount().incrementAndGet();
                }
            } catch (ConnectionException ce) {
                tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + ce.getMessage());
                log.error("Connection Error", ce);
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(ce);
            } catch (RuntimeException rte) {
                tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + rte.getMessage());
                log.error("Transfer Error", rte);
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(rte);
            }

            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tableMirror.setStageDuration(diff);
            log.info("Migration complete for {}.{} in {}ms", dbMirror.getName(), tableMirror.getName(), diff);
        } catch (MissingDataPointException  mde) {
            rtn.setStatus(ReturnStatus.Status.FATAL);
            rtn.setException(mde);
        } finally {
            // Reset the thread context.
            // Need to save any changes to Table Mirror.
            try {
                getTableMirrorRepository().save(tableMirror);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            getExecutionContextService().reset();
        }
        return CompletableFuture.completedFuture(rtn);
    }

    @Async("jobThreadPool")
    public CompletableFuture<ReturnStatus> execute(ConversionResult conversionResult, String databaseName, String tableName) {
        ReturnStatus rtn = new ReturnStatus();
        rtn.setTableName(tableName);
        getExecutionContextService().reset();
        getExecutionContextService().setConversionResult(conversionResult);
        RunStatus runStatus = conversionResult.getRunStatus();
        getExecutionContextService().setRunStatus(runStatus);

        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        Date start = new Date();
        log.info("Processing migration for {}.{}", databaseName, tableName);

        DBMirror dbMirror = null;
        try {
            dbMirror = getDbMirrorRepository().findByName(conversionResult.getKey(), databaseName).orElseThrow(() ->
                    new IllegalStateException("Couldn't locate database + " + databaseName + " for Conversion Key: " + conversionResult.getKey()));
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        TableMirror tableMirror = null;
        try {
            tableMirror = getTableMirrorRepository().findByName(conversionResult.getKey(), databaseName, tableName)
                    .orElseThrow(() -> new IllegalStateException("Couldn't locate table: " + databaseName + "." +
                            tableName + " for Conversion Key: " + conversionResult.getKey()));
            if (tableMirror.getPhaseState() == PhaseState.ERROR) {
                rtn.setStatus(ReturnStatus.Status.INCOMPLETE);
                return CompletableFuture.completedFuture(rtn);
            }
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Set Database to Transfer DB.
        tableMirror.setPhaseState(PhaseState.APPLYING_SQL);

//        tableMirror.setStrategy(job.getStrategy());

        tableMirror.incPhase();
        tableMirror.addStep("Processing TRANSFER", job.getStrategy().toString());
        try {
            DataStrategy dataStrategy = null;
            switch (job.getStrategy()) {
                case HYBRID:
                    if (TableUtils.isACID(let) && config.getMigrateACID().isInplace()) {
                        if (hybridAcidDowngradeInPlaceDataStrategy.execute(dbMirror, tableMirror)) {
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);
                        } else {
                            rtn.setStatus(ReturnStatus.Status.ERROR);
                            runStatus.getOperationStatistics().getIssues().incrementTables();
                        }
                    } else {
                        if (hybridDataStrategy.execute(dbMirror, tableMirror)) {
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);
                        } else {
                            rtn.setStatus(ReturnStatus.Status.ERROR);
                        }
                    }
                    break;
                default:
                    dataStrategy = getDataStrategyService().getDefaultDataStrategy(job.getStrategy());
                    if (dataStrategy.execute(dbMirror, tableMirror)) {
                        rtn.setStatus(ReturnStatus.Status.SUCCESS);
                    } else {
                        rtn.setStatus(ReturnStatus.Status.ERROR);
                    }
                    break;
            }
            if (rtn.getStatus() == ReturnStatus.Status.SUCCESS) {
                tableMirror.setPhaseState(PhaseState.PROCESSED);
            } else {
                tableMirror.setPhaseState(PhaseState.ERROR);
                // Increment unsuccessful table count.
                runStatus.getUnSuccessfulTableCount().incrementAndGet();
            }

            try {
                tableMirror = getTableMirrorRepository().save(tableMirror);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }

        } catch (ConnectionException ce) {
            tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + ce.getMessage());
            log.error("Connection Error", ce);
            rtn.setStatus(ReturnStatus.Status.FATAL);
            rtn.setException(ce);
        } catch (RuntimeException rte) {
            tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + rte.getMessage());
            log.error("Transfer Error", rte);
            rtn.setStatus(ReturnStatus.Status.FATAL);
            rtn.setException(rte);
        }

        Date end = new Date();
        Long diff = end.getTime() - start.getTime();
        tableMirror.setStageDuration(diff);
        log.info("Migration processing complete for {}.{} in {}ms", dbMirror.getName(), tableMirror.getName(), diff);

        return CompletableFuture.completedFuture(rtn);
    }

    /**
     * Builds the default location path for a table when no explicit location is provided.
     * Format: {targetNamespace}/{externalDirectory}/{resolvedDB}.db/{tableName}
     *
     * @param conversionResult The conversion result context
     * @param warehouse The warehouse plan
     * @param dbMirror The database mirror
     * @param tableMirror The table mirror
     * @return The constructed default location path
     */
    private String buildDefaultLocation(ConversionResult conversionResult, Warehouse warehouse,
                                        DBMirror dbMirror, TableMirror tableMirror) {
        return new StringBuilder()
                .append(conversionResult.getTargetNamespace())
                .append(warehouse.getExternalDirectory())
                .append(PATH_SEPARATOR)
                .append(getConversionResultService().getResolvedDB(dbMirror.getName()))
                .append(DB_SUFFIX)
                .append(PATH_SEPARATOR)
                .append(tableMirror.getName())
                .toString();
    }

    /**
     * Builds the intermediate storage path for a table.
     * Format: {intermediateStorage}/{remoteWorkingDir}/{conversionKey}/{dbName}.db/{tableName}
     *
     * @param intermediateStorage The base intermediate storage location
     * @param config The configuration
     * @param conversionResult The conversion result context
     * @param dbMirror The database mirror
     * @param tableMirror The table mirror
     * @return The constructed intermediate storage path
     */
    private String buildIntermediateStoragePath(String intermediateStorage, ConfigLiteDto config,
                                                ConversionResult conversionResult, DBMirror dbMirror,
                                                TableMirror tableMirror) {
        // Normalize intermediate storage path (remove trailing slash if present)
        String normalizedIS = intermediateStorage.endsWith(PATH_SEPARATOR) ?
                intermediateStorage.substring(0, intermediateStorage.length() - 1) :
                intermediateStorage;

        return new StringBuilder()
                .append(normalizedIS)
                .append(PATH_SEPARATOR)
                .append(config.getTransfer().getRemoteWorkingDirectory())
                .append(PATH_SEPARATOR)
                .append(conversionResult.getKey())
                .append(PATH_SEPARATOR)
                .append(dbMirror.getName())
                .append(DB_SUFFIX)
                .append(PATH_SEPARATOR)
                .append(tableMirror.getName())
                .toString();
    }

    /**
     * Gets the location for a table, providing a default if blank and metadata details are loaded.
     *
     * @param tableName The table name
     * @param tableDefinition The table definition
     * @param conversionResult The conversion result
     * @param warehouse The warehouse plan
     * @param dbMirror The database mirror
     * @param tableMirror The table mirror
     * @param config The configuration
     * @return The table location (either from definition or default)
     */
    private String getLocationWithDefault(String tableName, java.util.List<String> tableDefinition,
                                         ConversionResult conversionResult, Warehouse warehouse,
                                         DBMirror dbMirror, TableMirror tableMirror,
                                         ConfigLiteDto config) {
        String location = TableUtils.getLocation(tableName, tableDefinition);
        if (isBlank(location) && config.loadMetadataDetails()) {
            location = buildDefaultLocation(conversionResult, warehouse, dbMirror, tableMirror);
        }
        return location;
    }

    /**
     * Handles distcp translation setup when using intermediate storage.
     * Creates translations for LEFT -> INTERMEDIATE and INTERMEDIATE -> RIGHT.
     */
    private void handleIntermediateStorageDistcp(ConversionResult conversionResult, ConfigLiteDto config,
                                                 JobDto job, DBMirror dbMirror, TableMirror tableMirror,
                                                 EnvironmentTable let, EnvironmentTable set, EnvironmentTable ret,
                                                 Warehouse warehouse, boolean consolidateSourceTables,
                                                    ReturnStatus rtn   ) {
        String intermediatePath = buildIntermediateStoragePath(
                job.getIntermediateStorage(), config, conversionResult, dbMirror, tableMirror);

        // LEFT PUSH to INTERMEDIATE (skip for ACID in-place)
        boolean isAcid = TableUtils.isACID(let);
        if (!isAcid) {
            conversionResult.getTranslator().addTranslation(
                    dbMirror.getName(),
                    Environment.LEFT,
                    TableUtils.getLocation(tableMirror.getName(), let.getDefinition()),
                    intermediatePath,
                    1,
                    consolidateSourceTables);
        }
        // This shouldn't happen now.  Tables are filtered out under this condition.
//        else {
//                tableMirror.addError(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
//                tableMirror.setPhaseState(PhaseState.ERROR);
//                rtn.setStatus(ReturnStatus.Status.ERROR);
//                return;
//        }

        // RIGHT PULL from INTERMEDIATE
        String finalLocation;
        if (!set.getDefinition().isEmpty()) {
            finalLocation = TableUtils.getLocation(ret.getName(), set.getDefinition());
        } else {
            finalLocation = getLocationWithDefault(
                    tableMirror.getName(), ret.getDefinition(),
                    conversionResult, warehouse, dbMirror, tableMirror, config);
        }

        conversionResult.getTranslator().addTranslation(
                dbMirror.getName(),
                Environment.RIGHT,
                intermediatePath,
                finalLocation,
                1,
                consolidateSourceTables);
    }

    /**
     * Handles distcp translation setup for common push scenario (LEFT -> RIGHT).
     */
    private void handleCommonPushDistcp(ConversionResult conversionResult, ConfigLiteDto config,
                                       DBMirror dbMirror, TableMirror tableMirror,
                                       EnvironmentTable let, EnvironmentTable tet,
                                       EnvironmentTable set, EnvironmentTable ret,
                                       Warehouse warehouse, boolean consolidateSourceTables,
                                        ReturnStatus rtn) {
        // Determine source location based on whether table is ACID
        boolean isAcid = TableUtils.isACID(let);
        String originalLocation = isAcid ?
                TableUtils.getLocation(let.getName(), tet.getDefinition()) :
                TableUtils.getLocation(let.getName(), let.getDefinition());

        // Determine target location
        String newLocation;
        if (isAcid) {
            if (config.getMigrateACID().isDowngrade()) {
                newLocation = TableUtils.getLocation(ret.getName(), ret.getDefinition());
            } else {
                // ACID and DISTCP, no good.
                rtn.setStatus(ReturnStatus.Status.ERROR);
                tableMirror.addError(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
                tableMirror.setPhaseState(PhaseState.ERROR);
                return;
//                newLocation = TableUtils.getLocation(ret.getName(), set.getDefinition());
            }
        } else {
            newLocation = TableUtils.getLocation(ret.getName(), ret.getDefinition());
        }

        // Apply default if location is blank
        if (isBlank(newLocation) && config.loadMetadataDetails()) {
            newLocation = buildDefaultLocation(conversionResult, warehouse, dbMirror, tableMirror);
        }

        conversionResult.getTranslator().addTranslation(
                dbMirror.getName(),
                Environment.LEFT,
                originalLocation,
                newLocation,
                1,
                consolidateSourceTables);
    }

    /**
     * Handles distcp translation setup for right pull scenario (RIGHT pulls from LEFT).
     */
    private void handleRightPullDistcp(ConversionResult conversionResult, ConfigLiteDto config,
                                      JobDto job, DBMirror dbMirror, TableMirror tableMirror,
                                      EnvironmentTable let, EnvironmentTable tet, EnvironmentTable ret,
                                      Warehouse warehouse, boolean consolidateSourceTables,
                                      ReturnStatus rtn) {
        boolean isAcid = TableUtils.isACID(let);
        boolean isDowngrade = config.getMigrateACID().isDowngrade();
        boolean isStorageMigration = job.getStrategy() == DataStrategyEnum.STORAGE_MIGRATION;

        // Check if this is an unsupported scenario
        if (isAcid && !isDowngrade && !isStorageMigration) {
            tableMirror.addError(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
            tableMirror.setPhaseState(PhaseState.ERROR);
            rtn.setStatus(ReturnStatus.Status.ERROR);
            return;
        }

        // Determine source and target locations
        String sourceLocation;
        if (isAcid && isDowngrade) {
            sourceLocation = TableUtils.getLocation(tableMirror.getName(), tet.getDefinition());
        } else {
            sourceLocation = TableUtils.getLocation(tableMirror.getName(), let.getDefinition());
        }

        String targetLocation = getLocationWithDefault(
                tableMirror.getName(), ret.getDefinition(),
                conversionResult, warehouse, dbMirror, tableMirror, config);

        conversionResult.getTranslator().addTranslation(
                dbMirror.getName(),
                Environment.RIGHT,
                sourceLocation,
                targetLocation,
                1,
                consolidateSourceTables);
    }
}