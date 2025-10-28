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

package com.cloudera.utils.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.CopySpec;
import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class SQLDataStrategy extends DataStrategyBase {

    private final ConfigService configService;
    private final SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy;
    private final TableService tableService;
    private final IntermediateDataStrategy intermediateDataStrategy;

    public SQLDataStrategy(@NonNull ConversionResultService conversionResultService,
                           @NonNull ExecutionContextService executionContextService,
                           @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                           @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                           ConfigService configService, SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy,
                           TableService tableService, IntermediateDataStrategy intermediateDataStrategy) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.configService = configService;
        this.sqlAcidInPlaceDataStrategy = sqlAcidInPlaceDataStrategy;
        this.tableService = tableService;
        this.intermediateDataStrategy = intermediateDataStrategy;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {}.{} buildout SQL Definition", dbMirror.getName(), tableMirror.getName());

        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        EnvironmentTable set = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        // Different transfer technique.  Staging location.
        if (!isBlank(config.getTransfer().getIntermediateStorage()) ||
                !isBlank(config.getTransfer().getTargetNamespace()) ||
                TableUtils.isACID(let)) {
            return getIntermediateDataStrategy().buildOutDefinition(dbMirror, tableMirror);
        }

        if (ret.isExists()) {
            if (let.isExists()) {
                if (job.isSync() && config.isCreateIfNotExists()) {
                    // sync with overwrite.
                    ret.addIssue(SQL_SYNC_W_CINE.getDesc());
                    ret.setCreateStrategy(CreateStrategy.CREATE);
                } else {
                    ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                    ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION.getDesc());
                    String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), dbMirror.getName(), tableMirror.getName(),
                            SCHEMA_EXISTS_NO_ACTION.getDesc());
                    log.error(msg);
                    return Boolean.FALSE;
                }
            } else {
                ret.addIssue(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
                if (job.isSync()) {
                    ret.setCreateStrategy(CreateStrategy.DROP);
                } else {
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                }
                return Boolean.TRUE;
            }
        } else {
            ret.addIssue(SCHEMA_WILL_BE_CREATED.getDesc());
            ret.setCreateStrategy(CreateStrategy.CREATE);
        }

        if (!isBlank(conversionResult.getTargetNamespace())) {
            // If the temp cluster doesn't exist, create it as a clone of the LEFT.
            if (isNull(conversionResult.getConnection(Environment.SHADOW))) {
                ConnectionDto shadowCluster = null;
                try {
                    shadowCluster = conversionResult.getConnection(Environment.LEFT).clone();
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                }
                conversionResult.getConnections().put(Environment.SHADOW, shadowCluster);
            }

            CopySpec shadowSpec = null;

            // Create a 'shadow' table definition on right cluster pointing to the left data.
            shadowSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.SHADOW);

            if (conversionResult.convertManaged())
                shadowSpec.setUpgrade(Boolean.TRUE);

            // Don't claim data.  It will be in the LEFT cluster, so the LEFT owns it.
            shadowSpec.setTakeOwnership(Boolean.FALSE);

            // Create table with alter name in RIGHT cluster.
            shadowSpec.setTableNamePrefix(config.getTransfer().getShadowPrefix());

            // Build Shadow from Source.
            rtn = buildTableSchema(shadowSpec, dbMirror);
        }

        // Create final table in right.
        CopySpec rightSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);

        // Swap out the namespace of the LEFT with the RIGHT.
        rightSpec.setReplaceLocation(Boolean.TRUE);
        if (TableUtils.isManaged(let) && conversionResult.convertManaged()) {
            rightSpec.setUpgrade(Boolean.TRUE);
        } else {
            rightSpec.setMakeExternal(Boolean.TRUE);
        }
        if (job.isReadOnly()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        } else if (TableUtils.isManaged(let)) {
            rightSpec.setTakeOwnership(Boolean.TRUE);
        }
        if (job.isNoPurge()) {
            rightSpec.setTakeOwnership(Boolean.FALSE);
        }

        // Rebuild Target from Source.
        rtn = buildTableSchema(rightSpec, dbMirror);

        return rtn;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        log.debug("Table: {} buildout SQL SQL", tableMirror.getName());

        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        if (config.getTransfer().getIntermediateStorage() != null ||
                config.getTransfer().getTargetNamespace() != null) {
            return getIntermediateDataStrategy().buildOutSql(dbMirror, tableMirror);
        }

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        if (TableUtils.isACID(let)) {
            rtn = Boolean.FALSE;
            // TODO: Hum... Not sure this is right.
            tableMirror.addIssue(Environment.LEFT, "Shouldn't get an ACID table here.");
        } else {
            database = getConversionResultService().getResolvedDB(dbMirror.getName());
            useDb = MessageFormat.format(MirrorConf.USE, database);

            ret.addSql(TableUtils.USE_DESC, useDb);

            String dropStmt = null;
            // Create RIGHT Shadow Table
            if (!set.getDefinition().isEmpty()) {
                // Drop any previous SHADOW table, if it exists.
                dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);

                String shadowCreateStmt = tableService.getCreateStatement(tableMirror, Environment.SHADOW);
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
//                 Repair Partitions
                // TODO: Need to add ALTER partitions here if we know them.
                if (let.getPartitioned()) {
                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
                }
            }

            // RIGHT Final Table
            switch (ret.getCreateStrategy()) {
                case NOTHING:
                case LEAVE:
                    // Do Nothing.
                    break;
                case DROP:
                    if (TableUtils.isView(ret))
                        dropStmt = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                    else
                        dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    break;
                case REPLACE:
                    if (TableUtils.isView(ret))
                        dropStmt = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                    else
                        dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, dropStmt);
                    String createStmt = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                    //tableMirror.getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt);
                    break;
                case CREATE:
                    String createStmt2 = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                    //tableMirror.getCreateStatement(Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                    if (!conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive() && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
                        String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, ret.getName(), let.getOwner());
                        ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                    }
                    break;
            }
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        if (isACIDInPlace(tableMirror, Environment.LEFT)) {
            rtn = getSqlAcidInPlaceDataStrategy().build(dbMirror, tableMirror);
        } else if (!isBlank(config.getTransfer().getIntermediateStorage())
                || !isBlank(config.getTransfer().getTargetNamespace())
                || (TableUtils.isACID(let)
                && config.getMigrateACID().isOn())) {
            if (TableUtils.isACID(let)) {
                tableMirror.setStrategy(DataStrategyEnum.ACID);
            }
            rtn = getIntermediateDataStrategy().build(dbMirror, tableMirror);
        } else {

            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);

            // We should not get ACID tables in this routine.
            try {
                rtn = buildOutDefinition(dbMirror, tableMirror);
            } catch (RequiredConfigurationException e) {
                let.addError("Failed to build out definition: " + e.getMessage());
                rtn = Boolean.FALSE;
            }

            if (rtn)
                rtn = getConversionResultService().AVROCheck(tableMirror);

            if (rtn) {
                try {
                    rtn = buildOutSql(dbMirror, tableMirror);
                } catch (MissingDataPointException e) {
                    let.addError("Failed to build out SQL: " + e.getMessage());
                    rtn = Boolean.FALSE;
                }
            }

            // Construct Transfer SQL
            if (rtn) {
                // TODO: Double check this...
                rtn = buildMigrationSql(dbMirror, tableMirror, Environment.LEFT, Environment.SHADOW, Environment.RIGHT);

            }
        }
        return rtn;
    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        log.info("SQLDataStrategy -> Table: {} execute", tableMirror.getName());

        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

        Boolean rtn = Boolean.FALSE;
        rtn = tableService.runTableSql(tableMirror, Environment.LEFT);
        if (rtn) {
            rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
        }
        // Run Cleanup Scripts on both sides.
        if (!config.isSaveWorkingTables()) {
            // Run the Cleanup Scripts
            boolean CleanupRtn = tableService.runTableSql(tableMirror.getEnvironmentTable(Environment.LEFT).getCleanUpSql(), tableMirror, Environment.LEFT);
            if (!CleanupRtn) {
                tableMirror.addIssue(Environment.LEFT, "Failed to run cleanup SQL.");
            }
            CleanupRtn = tableService.runTableSql(tableMirror.getEnvironmentTable(Environment.RIGHT).getCleanUpSql(), tableMirror, Environment.RIGHT);
            if (!CleanupRtn) {
                tableMirror.addIssue(Environment.RIGHT, "Failed to run cleanup SQL.");
            }
        }
        return rtn;
    }

}
