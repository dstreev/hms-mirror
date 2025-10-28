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
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
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
import static com.cloudera.utils.hms.mirror.SessionVars.SET_TEZ_AS_EXECUTION_ENGINE;
import static com.cloudera.utils.hms.mirror.SessionVars.TEZ_EXECUTION_DESC;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class IntermediateDataStrategy extends DataStrategyBase {

    private final ConfigService configService;
    private final TableService tableService;

    public IntermediateDataStrategy(@NonNull ConversionResultService conversionResultService,
                                    @NonNull ExecutionContextService executionContextService,
                                    @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                                    @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                                    ConfigService configService, TableService tableService) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.configService = configService;
        this.tableService = tableService;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto configLite = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        BuildWhat buildWhat = whatToBuild(dbMirror, tableMirror);

        log.debug("Table: {} buildout Intermediate Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (buildWhat.rightTable) {
            CopySpec rightSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);

            if (ret.isExists()) {
                if (let.isExists()) {
                    if (!TableUtils.isACID(ret) && configLite.isCreateIfNotExists() && conversionResult.getJob().isSync()) {
                        ret.addIssue(CINE_WITH_EXIST.getDesc());
                        ret.setCreateStrategy(CreateStrategy.CREATE);
                    } else if (TableUtils.isACID(ret) && job.isSync()) {
                        ret.addIssue(SCHEMA_EXISTS_SYNC_ACID.getDesc());
                        ret.setCreateStrategy(CreateStrategy.REPLACE);
                    } else {
                        // Already exists, no action.
                        ret.addIssue(SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        ret.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        ret.setCreateStrategy(CreateStrategy.NOTHING);
                        String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), dbMirror.getName(), tableMirror.getName(),
                                SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
                        log.error(msg);
                        return Boolean.FALSE;
                    }
                } else {
                    // Left doesn't exist, but the right does.  Report and do nothing.
                    ret.setCreateStrategy(CreateStrategy.LEAVE);
                    ret.addIssue(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
                    return Boolean.TRUE;
                }
            } else {
                ret.setCreateStrategy(CreateStrategy.CREATE);
            }

            if (!TableUtils.isACID(let) && TableUtils.isManaged(let)) {
                // Managed to EXTERNAL
                rightSpec.setUpgrade(Boolean.TRUE);
                rightSpec.setReplaceLocation(Boolean.TRUE);
            } else if (TableUtils.isACID(let)) {
                // ACID
                if (configLite.getMigrateACID().isDowngrade()) {
                    if (isBlank(configLite.getTransfer().getTargetNamespace())) {
                        if (configLite.getTransfer().getStorageMigration().isDistcp()) {
                            rightSpec.setReplaceLocation(Boolean.TRUE);
                        } else {
                            rightSpec.setStripLocation(Boolean.TRUE);
                        }
                    } else {
                        rightSpec.setReplaceLocation(Boolean.TRUE);
                    }
                    rightSpec.setMakeExternal(Boolean.TRUE);
                    // Strip the Transactional Elements
                    rightSpec.setMakeNonTransactional(Boolean.TRUE);
                    // Set Purge Flag
                    rightSpec.setTakeOwnership(Boolean.TRUE);
                } else {
                    // Use the system default location when converting.
                    rightSpec.setStripLocation(Boolean.TRUE);
                }
            } else {
                // External
                rightSpec.setReplaceLocation(Boolean.TRUE);
            }

            // Build Target from Source.
            rtn = buildTableSchema(rightSpec, dbMirror);
        }

        if (rtn && buildWhat.isTransferTable()) {
            // Build Transfer Spec.
            // When the source is ACID and we need to downgrade the dataset or Intermediate Storage is used.
            CopySpec transferSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.TRANSFER);
            // The Transfer Table should OWN the data, since we need to clean it up after the migration.
            transferSpec.setMakeNonTransactional(Boolean.TRUE);
            transferSpec.setMakeExternal(Boolean.TRUE);
            transferSpec.setTakeOwnership(Boolean.TRUE);

            transferSpec.setTableNamePrefix(configLite.getTransfer().getTransferPrefix());
            transferSpec.setReplaceLocation(Boolean.TRUE);

            // Build transfer table.
            rtn = buildTableSchema(transferSpec, dbMirror);
        }

        // Build Shadow Spec (don't build when using commonStorage)
        // If acid and ma.isOn
        // if not downgrade

        if (buildWhat.isShadowTable()) {
            CopySpec shadowSpec = null;
            // If we built a transfer table, use it as the source for the shadow table.
            if (TableUtils.isACID(let) || !isBlank(configLite.getTransfer().getIntermediateStorage())) {
                shadowSpec = new CopySpec(tableMirror, Environment.TRANSFER, Environment.SHADOW);
            } else {
                shadowSpec = new CopySpec(tableMirror, Environment.LEFT, Environment.SHADOW);
            }
            shadowSpec.setUpgrade(Boolean.TRUE);
            shadowSpec.setMakeExternal(Boolean.TRUE);
            shadowSpec.setTakeOwnership(Boolean.FALSE);
            shadowSpec.setTableNamePrefix(configLite.getTransfer().getShadowPrefix());

            rtn = buildTableSchema(shadowSpec, dbMirror);
        }

        return rtn;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult();

        BuildWhat buildWhat = whatToBuild(dbMirror, tableMirror);

        log.debug("Table: {} buildout Intermediate SQL", tableMirror.getName());

        String useDb = null;
        String database = null;
        String createTbl = null;

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);

        ret.getSql().clear();

        // LEFT Transfer Table
//        database = HmsMirrorConfigUtil.getResolvedDB(dbMirror.getName(), config);
        String originalDatabase = dbMirror.getName();
        useDb = MessageFormat.format(MirrorConf.USE, originalDatabase);


        // Manage Transfer Table.  Should only make this if the TRANSFER table is defined.
        if (buildWhat.isTransferTable()) {
            String transferCreateStmt = tableService.getCreateStatement(tableMirror, Environment.TRANSFER);
            if (!isBlank(transferCreateStmt)) {
                let.addSql(TableUtils.USE_DESC, useDb);
                // Drop any previous TRANSFER table, if it exists.
                if (!isBlank(tet.getName())) {
                    String transferDropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, tet.getName());
                    let.addSql(TableUtils.DROP_DESC, transferDropStmt);
                }
                let.addSql(TableUtils.CREATE_TRANSFER_DESC, transferCreateStmt);
            }
        }

        database = getConversionResultService().getResolvedDB(dbMirror.getName());

        useDb = MessageFormat.format(MirrorConf.USE, database);
        ret.addSql(TableUtils.USE_DESC, useDb);

        // RIGHT SHADOW Table
        if (buildWhat.shadowTable) {
            String shadowCreateStmt = getTableService().getCreateStatement(tableMirror, Environment.SHADOW);
            if (!isBlank(shadowCreateStmt)) {
                // Drop any previous SHADOW table, if it exists.
                String dropStmt = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
                ret.addSql(TableUtils.DROP_DESC, dropStmt);
                // Create Shadow Table
                ret.addSql(TableUtils.CREATE_SHADOW_DESC, shadowCreateStmt);
                // Repair Partitions for Shadow Table
                if (TableUtils.isPartitioned(let)) {
                    String shadowMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, set.getName());
                    ret.addSql(TableUtils.REPAIR_DESC, shadowMSCKStmt);
                }
            }
        }

        // RIGHT Final Table
        if (buildWhat.rightTable) {
            String rightDrop = null;
            switch (ret.getCreateStrategy()) {
                case NOTHING:
                case LEAVE:
                    // Do Nothing.
                    break;
                case DROP:
                    if (TableUtils.isView(ret))
                        rightDrop = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                    else
                        rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, rightDrop);
                    break;
                case REPLACE:
                    if (TableUtils.isView(ret))
                        rightDrop = MessageFormat.format(MirrorConf.DROP_VIEW, ret.getName());
                    else
                        rightDrop = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                    ret.addSql(TableUtils.DROP_DESC, rightDrop);
                    String createStmt = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt);
                    break;
                case CREATE:
                    String createStmt2 = tableService.getCreateStatement(tableMirror, Environment.RIGHT);
                    ret.addSql(TableUtils.CREATE_DESC, createStmt2);
                    if (!conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()
                            && conversionResult.getConfig().getOwnershipTransfer().isTable() && let.getOwner() != null) {
                        String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, ret.getName(), let.getOwner());
                        ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                    }
                    // Don't need this for final table..
                    // Unless we are using 'distcp' to copy the data.
                    // Partitioned, non-acid, w/ distcp.
                    if (let.getPartitioned() && conversionResult.getConfig().getTransfer().getStorageMigration().isDistcp()
                            && !TableUtils.isACID(ret)) {
                        String rightMSCKStmt = MessageFormat.format(MirrorConf.MSCK_REPAIR_TABLE, ret.getName());
                        ret.addSql(TableUtils.REPAIR_DESC, rightMSCKStmt);
                    }

                    break;
            }
        }
        rtn = Boolean.TRUE;
        return rtn;
    }


    @Override
    public BuildWhat whatToBuild(DBMirror dbMirror, TableMirror tableMirror) {
        BuildWhat buildWhat = new BuildWhat();
        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto configLite = conversionResult.getConfig();
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Build the RIGHT table definition.
        // TODO: Do we need to check for the existence of the RIGHT table?

        // Assumes Clusters are linked.
        // acid
        // intermediate
        // transfer YES (downgrade to intermediate namespace)
        // shadow YES (convert back from intermediate namespace)
        // no intermediate
        // transfer YES (downgrade on source namespace)
        // shadow YES (convert back from source namespace)
        // non-acid
        // intermediate
        // transfer YES (transfer to intermediate namespace)
        // shadow YES (convert back from intermediate namespace)
        // no intermediate
        // transfer NO
        // shadow YES (convert back from source namespace)
        if (TableUtils.isACID(let)) {
            buildWhat.transferTable = true;
            buildWhat.transferSql = true;
            buildWhat.shadowTable = true;
            buildWhat.shadowSql = true;
        } else {
            if (!isBlank(configLite.getTransfer().getIntermediateStorage())) {
                buildWhat.transferTable = true;
                buildWhat.transferSql = true;
                buildWhat.shadowTable = true;
                buildWhat.shadowSql = true;
            } else {
                buildWhat.shadowTable = true;
                buildWhat.shadowSql = true;
            }
        }

        // Build the RIGHT table definition.
        buildWhat.rightTable = true;

        return buildWhat;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult();

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        BuildWhat buildWhat = whatToBuild(dbMirror, tableMirror);

        // ================================
        try {
            rtn = buildOutDefinition(dbMirror, tableMirror);
        } catch (RequiredConfigurationException e) {
            let.addError("Failed to build out definition: " + e.getMessage());
            rtn = Boolean.FALSE;
        }

        if (rtn) {
            try {
                rtn = buildOutSql(dbMirror, tableMirror);
            } catch (MissingDataPointException e) {
                let.addError("Failed to build out SQL: " + e.getMessage());
                rtn = Boolean.FALSE;
            }
        }

        // Construct Transfer SQL
        if (rtn && buildWhat.isTransferSql()) {
            if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()) {
                // We need to ensure that 'tez' is the execution engine.
                let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
            }

            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
            let.addCleanUpSql(cleanUp);

            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
            let.addCleanUpSql(leftUsePair);

            rtn = buildMigrationSql(dbMirror, tableMirror, Environment.LEFT, Environment.LEFT, Environment.TRANSFER);
            //tableMirror.transferSql(let, set, ret, config);
        }

        if (rtn && buildWhat.isShadowSql()) {
            rtn = buildMigrationSql(dbMirror, tableMirror, Environment.LEFT, Environment.SHADOW, Environment.RIGHT);
        }

        return rtn;
    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult();
        ConfigLiteDto configLite = conversionResult.getConfig();

        rtn = getTableService().runTableSql(tableMirror, Environment.LEFT);
        if (rtn) {
            rtn = getTableService().runTableSql(tableMirror, Environment.RIGHT);
        }
        // Run Cleanup Scripts on both sides.
        if (!configLite.isSaveWorkingTables()) {
            // Run the Cleanup Scripts
            boolean CleanupRtn = getTableService().runTableSql(tableMirror.getEnvironmentTable(Environment.LEFT).getCleanUpSql(), tableMirror, Environment.LEFT);
            if (!CleanupRtn) {
                tableMirror.addIssue(Environment.LEFT, "Failed to run cleanup SQL.");
            }
            CleanupRtn = getTableService().runTableSql(tableMirror.getEnvironmentTable(Environment.RIGHT).getCleanUpSql(), tableMirror, Environment.RIGHT);
            if (!CleanupRtn) {
                tableMirror.addIssue(Environment.RIGHT, "Failed to run cleanup SQL.");
            }
        }
        return rtn;
    }
}
