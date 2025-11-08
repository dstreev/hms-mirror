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
import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.MirrorConf.DB_LOCATION;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Slf4j
@Getter
public class ExportImportDataStrategy extends DataStrategyBase {

    private final ExportCircularResolveService exportCircularResolveService;
    private final ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;
    private final ConfigService configService;
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final WarehouseService warehouseService;

    public ExportImportDataStrategy(@NonNull ConversionResultService conversionResultService,
                                    @NonNull ExecutionContextService executionContextService,
                                    @NonNull StatsCalculatorService statsCalculatorService, @NonNull CliEnvironment cliEnvironment,
                                    @NonNull TranslatorService translatorService, @NonNull FeatureService featureService,
                                    ExportCircularResolveService exportCircularResolveService,
                                    ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy,
                                    ConfigService configService, DatabaseService databaseService, TableService tableService,
                                    WarehouseService warehouseService) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment, translatorService, featureService);
        this.exportCircularResolveService = exportCircularResolveService;
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
        this.configService = configService;
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.warehouseService = warehouseService;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) throws RequiredConfigurationException {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();

        log.debug("Table: {} buildout EXPORT_IMPORT Definition", tableMirror.getName());
        EnvironmentTable let = null;
        EnvironmentTable ret = null;
        CopySpec copySpec = null;

        let = tableMirror.getEnvironmentTable(Environment.LEFT);
        ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        if (TableUtils.isACID(let) &&
                !conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive()
                        == conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
            let.addError("Can't process ACID tables with EXPORT_IMPORT between 'legacy' and 'non-legacy' hive environments.  The processes aren't compatible.");
            return Boolean.FALSE;
        }

        if (!TableUtils.isHiveNative(let)) {
            let.addError("Can't process ACID tables, VIEWs, or Non Native Hive Tables with this strategy.");
            return Boolean.FALSE;
        }

        copySpec = new CopySpec(tableMirror, Environment.LEFT, Environment.RIGHT);
        // Swap out the namespace of the LEFT with the RIGHT.
        copySpec.setReplaceLocation(Boolean.TRUE);
        if (getConversionResultService().convertManaged())
            copySpec.setUpgrade(Boolean.TRUE);
        if (!job.isReadOnly() || !job.isSync()) {
            copySpec.setTakeOwnership(Boolean.TRUE);
        }
        if (job.isReadOnly()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }
        if (job.isNoPurge()) {
            copySpec.setTakeOwnership(Boolean.FALSE);
        }

        if (ret.isExists()) {
            // Already exists, no action.
            ret.addIssue("Schema exists already, no action.  If you wish to rebuild the schema, " +
                    "drop it first and try again. <b>Any following messages MAY be irrelevant about schema adjustments.</b>");
            ret.setCreateStrategy(CreateStrategy.LEAVE);
        } else {
            ret.addIssue("Schema will be created");
            ret.setCreateStrategy(CreateStrategy.CREATE);
            rtn = Boolean.TRUE;
        }
        if (rtn)
            // Build Target from Source.
            rtn = buildTableSchema(copySpec, dbMirror);
        return rtn;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.TRUE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        log.debug("Database: {} buildout EXPORT_IMPORT SQL", tableMirror.getName());

        Warehouse dbWarehouse = warehouseService.getWarehousePlan(dbMirror.getName());

        String database = null;
        database = getConversionResultService().getResolvedDB(dbMirror.getName());

        EnvironmentTable let = getConversionResultService().getEnvironmentTable(Environment.LEFT, tableMirror);
        String leftNamespace = NamespaceUtils.getNamespace(TableUtils.getLocation(let.getName(), let.getDefinition()));

        EnvironmentTable ret = getConversionResultService().getEnvironmentTable(Environment.RIGHT, tableMirror);

        if (let.getPartitions().size() > job.getHybrid().getExportImportPartitionLimit() &&
                job.getHybrid().getExportImportPartitionLimit() > 0) {
            tableMirror.setPhaseState(PhaseState.ERROR);
            // The partition limit has been exceeded.  The process will need to be done manually.
            let.addError("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                    "limit (hybrid->exportImportPartitionLimit) of "
                    + job.getHybrid().getExportImportPartitionLimit() +
                    ".  This value is used to abort migrations that have a high potential for failure.  " +
                    "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ep'.");
            // Return without further processing.
            return Boolean.FALSE;
        }

        try {

            boolean dropRight = Boolean.FALSE;
            if (ret.isExists() && job.isSync() && let.isExists()) {
                // The Table exists on both sides.
                // Compare the schemas and determine what next.
                boolean withoutCreate = false;
                // If we are Downgrading from ACID, take that into account.
                if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                    withoutCreate = true;
                }
                if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT, withoutCreate)) {
                    // Same, nothing to do.
                    ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
                    return Boolean.TRUE;
                } else {
                    // They don't match.  So we need to drop the table and recreate it.
                    if (job.isSync()) {
                        ret.addIssue(SCHEMA_EXISTS_NOT_MATCH_DROP.getDesc());
                        ret.setCreateStrategy(CreateStrategy.REPLACE);
                        dropRight = Boolean.TRUE;
                    }
                }
            }

            // LEFT Export to directory
            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            String exportLoc = null;

            if (!isBlank(job.getIntermediateStorage())) {
                String isLoc = job.getIntermediateStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" +
                        config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        conversionResult.getKey() + "/" +
                        dbMirror.getName() + "/" +
                        tableMirror.getName();
            } else if (!isBlank(job.getTargetNamespace())) {
                String isLoc = job.getTargetNamespace();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        conversionResult.getKey() + "/" +
                        dbMirror.getName() + "/" +
                        tableMirror.getName();
            } else {
                exportLoc = leftNamespace + config.getTransfer().getExportBaseDirPrefix()
                        + dbMirror.getName() + "/" + let.getName();
            }
            String origTableName = let.getName();
            if (isACIDInPlace(tableMirror, Environment.LEFT)) {
                // Rename original table.
                // Remove property (if exists) to prevent rename from happening.
                if (TableUtils.hasTblProperty(TRANSLATED_TO_EXTERNAL, let)) {
                    String unSetSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, origTableName, TRANSLATED_TO_EXTERNAL);
                    let.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, unSetSql);
                }
                String newTblName = let.getName() + "_archive";
                String renameSql = MessageFormat.format(MirrorConf.RENAME_TABLE, origTableName, newTblName);
                TableUtils.changeTableName(let, newTblName);
                let.addSql(TableUtils.RENAME_TABLE, renameSql);
            }

            String exportSql = MessageFormat.format(MirrorConf.EXPORT_TABLE, let.getName(), exportLoc);
            let.addSql(TableUtils.EXPORT_TABLE, exportSql);

            // RIGHT IMPORT from Directory
            if (!isACIDInPlace(tableMirror, Environment.LEFT)) {
                String useRightDb = MessageFormat.format(MirrorConf.USE, database);
                ret.addSql(TableUtils.USE_DESC, useRightDb);
            }

            String importLoc = exportLoc;

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = getTranslatorService().translateTableLocation(dbMirror, tableMirror, sourceLocation, 1, null);
            String importSql;
            boolean withLocation = Boolean.FALSE;
            if (TableUtils.isACID(let)) {
                if (!config.getMigrateACID().isDowngrade()) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_TABLE, let.getName(), importLoc);
                } else {
                    if (config.getMigrateACID().isInplace()) {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, origTableName, importLoc);
                    } else {
                        importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                    }
                }
            } else {
                if (config.loadMetadataDetails()) {
                    targetLocation = getConversionResultService().getTargetNamespace()
                            + dbWarehouse.getExternalDirectory() +
                            "/" + getConversionResultService().getResolvedDB(dbMirror.getName()) + ".db/"
                            + tableMirror.getName();
                }
                if (config.isForceExternalLocation() || config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE) {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                    withLocation = Boolean.TRUE;
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE, let.getName(), importLoc);
                }

            }

            if (dropRight) {
                String dropExistingTable = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
                ret.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
            }

            if (isACIDInPlace(tableMirror, Environment.LEFT)) {
                let.addSql(TableUtils.IMPORT_TABLE, importSql);
            } else {
                if (withLocation && !targetLocation.startsWith(dbMirror.getEnvironmentProperty(Environment.RIGHT, DB_LOCATION))) {
                    if (config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.ALIGNED) {
                        String errMsg = MessageFormat.format(TABLE_LOCATION_NOT_ALIGNED_WITH_DB.getDesc(), sourceLocation, config.getTransfer().getStorageMigration().getTranslationType());
                        tableMirror.addError(Environment.RIGHT, errMsg);
                        tableMirror.setPhaseState(PhaseState.ERROR);
                        return Boolean.FALSE;
                    } else if (config.isForceExternalLocation() || config.getTransfer().getStorageMigration().getTranslationType() == TranslationTypeEnum.RELATIVE){
                        String errMsg = MessageFormat.format(TABLE_LOCATION_NOT_ALIGNED_WITH_DB.getDesc(), sourceLocation, config.getTransfer().getStorageMigration().getTranslationType());
                        tableMirror.addIssue(Environment.RIGHT, errMsg);
                    }
                }
                ret.addSql(TableUtils.IMPORT_TABLE, importSql);
                if (!conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()
                        && config.getOwnershipTransfer().isTable() && let.getOwner() != null) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                }
            }

        } catch (Throwable t) {
            log.error("Error executing EXPORT_IMPORT", t);
            tableMirror.setPhaseState(PhaseState.ERROR);
            let.addError(t.getMessage());
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

        // Setting resolved strategy
        if (tableMirror.getStrategy() == null) {
            tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
        }

        if (ret.isExists() && !job.isSync() && let.isExists()) {
            ret.addIssue(MessageCode.SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//            let.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//            String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), dbMirror.getName(), tableMirror.getName(),
//                    SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//            log.warn(msg);
            return Boolean.TRUE;
        } else if (ret.isExists() && !let.isExists()) {
            ret.addIssue(MessageCode.SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
            return Boolean.TRUE;
        }

//        if (ret.isExists()) {
//            if (!job.isSync() && let.isExists()) {
//                let.addIssue(MessageCode.SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//                let.addSql(SKIPPED.getDesc(), "-- " + SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//                String msg = MessageFormat.format(TABLE_ISSUE.getDesc(), dbMirror.getName(), tableMirror.getName(),
//                        SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//                log.error(msg);
//                return Boolean.FALSE;
//            } else {
//                // The Table exists on both sides.
//                // Compare the schemas and determine what next.
//                boolean withoutCreate = false;
//                // If we are Downgrading from ACID, take that into account.
//                if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
//                    withoutCreate = true;
//                }
//                if (tableMirror.schemasEqual(Environment.LEFT, Environment.RIGHT, withoutCreate)) {
//                    // Same, nothing to do.
//                    ret.addIssue(SCHEMA_EXISTS_NO_ACTION.getDesc());
//                } else {
//                    // They don't match.  So we need to drop the table and recreate it.
//                    if (job.isSync()) {
//                        // Need to Drop table on the RIGHT.
//                        // First check to see if the table has been set to purge on the right.
//                        //    we don't want to drop data, only the table.
//                        //    AH, but for EXPORT_IMPORT, this is a bootstrap....
//                        ret.addIssue(SCHEMA_EXISTS_NOT_MATCH_DROP.getDesc());
//                        String dropExistingTable = MessageFormat.format(MirrorConf.DROP_TABLE, ret.getName());
//                    }
//                }
////                ret.addIssue(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc());
////                ret.setCreateStrategy(CreateStrategy.LEAVE);
////                return Boolean.TRUE;
//            }
//        }

        if (isACIDInPlace(tableMirror, Environment.LEFT)) {
            rtn = getExportImportAcidDowngradeInPlaceDataStrategy().build(dbMirror, tableMirror);//doEXPORTIMPORTACIDInplaceDowngrade();
        } else {
            if (TableUtils.isACID(let)) {
                if (conversionResult.getConnection(Environment.LEFT).getPlatformType().isLegacyHive() !=
                        conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
                    rtn = Boolean.FALSE;
                    tableMirror.addIssue(Environment.LEFT, "ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive.");
                } else {
                    try {
                        rtn = buildOutSql(dbMirror, tableMirror); //tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
                    } catch (MissingDataPointException e) {
                        let.addError("Failed to build out SQL: " + e.getMessage());
                        rtn = Boolean.FALSE;
                    }
                }

            } else {
                try {
                    rtn = buildOutSql(dbMirror, tableMirror); //tableMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
                } catch (MissingDataPointException e) {
                    let.addError("Failed to build out SQL: " + e.getMessage());
                    rtn = Boolean.FALSE;
                }

                if (rtn)
                    rtn = getConversionResultService().AVROCheck(tableMirror);
            }
        }

        return rtn;

    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        Boolean rtn = Boolean.FALSE;
        // If EXPORT_IMPORT, need to run LEFT queries.
        rtn = tableService.runTableSql(tableMirror, Environment.LEFT);

        // Execute the RIGHT sql if config.execute.
        if (rtn) {
            rtn = tableService.runTableSql(tableMirror, Environment.RIGHT);
        }
        return rtn;
    }

}
