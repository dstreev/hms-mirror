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
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
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

    @Async("jobThreadPool")
    public CompletableFuture<ReturnStatus> build(ConversionResult conversionResult, DBMirror dbMirror, TableMirror tableMirror) {
        ReturnStatus rtn = new ReturnStatus();
        // Set these for the new thread.
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        rtn.setTableMirror(tableMirror);
        RunStatus runStatus = conversionResult.getRunStatus();
        Warehouse warehouse = null;
        try {
            Date start = new Date();
            log.info("Building migration for {}.{}", dbMirror.getName(), tableMirror.getName());
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

            // Set Database to Transfer DB.
            tableMirror.setPhaseState(PhaseState.CALCULATING_SQL);
            tableMirror.setStrategy(job.getStrategy());
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
                    // Build distcp reports.
                    // Build when intermediate and NOT ACID with isInPlace.
                    if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                        // The Transfer Table should be available.
                        String isLoc = config.getTransfer().getIntermediateStorage();
                        // Deal with extra '/'
                        isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                        isLoc = isLoc + "/" +
                                config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                conversionResult.getKey() + "/" +
                                dbMirror.getName() + ".db/" +
                                tableMirror.getName();
                        if (TableUtils.isACID(let) && config.getMigrateACID().isInplace()) {
                            // skip the LEFT because the TRANSFER table used to downgrade was placed in the intermediate location.
                        } else {
                            // LEFT PUSH INTERMEDIATE
                            conversionResult.getTranslator().addTranslation(dbMirror.getName(), Environment.LEFT,
                                    TableUtils.getLocation(tableMirror.getName(), let.getDefinition()),
                                    isLoc, 1, consolidateSourceTables);
                        }

                        // RIGHT PULL from INTERMEDIATE
                        String fnlLoc = null;
                        if (!set.getDefinition().isEmpty()) {
                            fnlLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                        } else {
                            fnlLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (isBlank(fnlLoc) && config.loadMetadataDetails()) {
                                String sbDir = conversionResult.getTargetNamespace() +
                                        warehouse.getExternalDirectory() + "/" +
                                        getConversionResultService().getResolvedDB(dbMirror.getName()) + ".db" + "/" + tableMirror.getName();
                                fnlLoc = sbDir;
                            }
                        }
                        conversionResult.getTranslator().addTranslation(dbMirror.getName(), Environment.RIGHT,
                                isLoc,
                                fnlLoc, 1, consolidateSourceTables);
                    } else if (!isBlank(config.getTransfer().getTargetNamespace())
                            && job.getStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        // LEFT PUSH COMMON
                        String origLoc = TableUtils.isACID(let) ?
                                TableUtils.getLocation(let.getName(), tet.getDefinition()) :
                                TableUtils.getLocation(let.getName(), let.getDefinition());
                        String newLoc = null;
                        if (TableUtils.isACID(let)) {
                            if (config.getMigrateACID().isDowngrade()) {
                                newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                            } else {
                                newLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                            }
                        } else {
                            newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                        }
                        if (isBlank(newLoc) && config.loadMetadataDetails()) {
                            String sbDir = config.getTransfer().getTargetNamespace() +
                                    warehouse.getExternalDirectory() + "/" +
                                    getConversionResultService().getResolvedDB(dbMirror.getName()) + ".db" + "/" + tableMirror.getName();
                            newLoc = sbDir;
                        }
                        conversionResult.getTranslator().addTranslation(dbMirror.getName(), Environment.LEFT,
                                origLoc, newLoc, 1, consolidateSourceTables);
                    } else {
                        // RIGHT PULL
                        if (TableUtils.isACID(let)
                                && !config.getMigrateACID().isDowngrade()
                                && !(job.getStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
                            tableMirror.addError(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
                            rtn.setStatus(ReturnStatus.Status.INCOMPLETE);
//                            successful = Boolean.FALSE;
                        } else if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (isBlank(rLoc) && config.loadMetadataDetails()) {
                                String sbDir = conversionResult.getTargetNamespace() +
                                        warehouse.getExternalDirectory() + "/" +
                                        getConversionResultService().getResolvedDB(dbMirror.getName()) + ".db" + "/" + tableMirror.getName();
                                rLoc = sbDir;
                            }
                            conversionResult.getTranslator().addTranslation(dbMirror.getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), tet.getDefinition()),
                                    rLoc, 1, consolidateSourceTables);
                        } else {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (isBlank(rLoc) && config.loadMetadataDetails()) {
                                String sbDir = conversionResult.getTargetNamespace() +
                                        warehouse.getExternalDirectory() + "/" +
                                        getConversionResultService().getResolvedDB(dbMirror.getName()) + ".db" + "/" + tableMirror.getName();
                                rLoc = sbDir;
                            }
                            conversionResult.getTranslator().addTranslation(dbMirror.getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), let.getDefinition())
                                    , rLoc, 1, consolidateSourceTables);
                        }
                    }
                }

                if (rtn.getStatus() == ReturnStatus.Status.SUCCESS) {
                    tableMirror.setPhaseState(PhaseState.CALCULATED_SQL);
                } else if (rtn.getStatus() == ReturnStatus.Status.INCOMPLETE) {
                    tableMirror.setPhaseState(PhaseState.CALCULATED_SQL_WARNING);
                } else {
                    tableMirror.setPhaseState(PhaseState.ERROR);
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
        } catch (MissingDataPointException | RequiredConfigurationException mde) {
            rtn.setStatus(ReturnStatus.Status.FATAL);
            rtn.setException(mde);
        } finally {
            // Reset the thread context.
            getExecutionContextService().reset();
        }
        return CompletableFuture.completedFuture(rtn);
    }

    @Async("jobThreadPool")
    public CompletableFuture<ReturnStatus> execute(ConversionResult conversionResult, DBMirror dbMirror, TableMirror tableMirror) {
        ReturnStatus rtn = new ReturnStatus();
        rtn.setTableMirror(tableMirror);
        getExecutionContextService().reset();
        getExecutionContextService().setConversionResult(conversionResult);
        RunStatus runStatus = conversionResult.getRunStatus();
        getExecutionContextService().setRunStatus(runStatus);

        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        Date start = new Date();
        log.info("Processing migration for {}.{}", dbMirror.getName(), tableMirror.getName());

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Set Database to Transfer DB.
        tableMirror.setPhaseState(PhaseState.APPLYING_SQL);

        tableMirror.setStrategy(job.getStrategy());

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
            if (rtn.getStatus() == ReturnStatus.Status.SUCCESS)
                tableMirror.setPhaseState(PhaseState.PROCESSED);
            else
                tableMirror.setPhaseState(PhaseState.ERROR);
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
}