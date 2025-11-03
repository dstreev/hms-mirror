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
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyBase;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;

import static com.cloudera.utils.hms.mirror.MessageCode.EXPORT_IMPORT_SYNC;
import static com.cloudera.utils.hms.mirror.TablePropertyVars.TRANSLATED_TO_EXTERNAL;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service class that extends {@link DataStrategyBase} and provides functionality
 * for resolving circular data dependencies when exporting data across environments.
 * This service is structured to handle the intricacies of managing export/import
 * operations, supports SQL generation for data migration, and facilitates
 * metadata translations and configurations.
 * <p>
 * It depends on various services such as {@link ConfigService}, {@link ExecuteSessionService},
 * {@link DatabaseService}, {@link TableService}, {@link TranslatorService}, and {@link WarehouseService}
 * for performing its internal operations and orchestrating data migration tasks.
 * <p>
 * This service handles the following key responsibilities:
 * <p>
 * 1. Utilizing configuration properties for customizing export/import logic.
 * 2. Building export/import SQL scripts to facilitate data movement across environments.
 * 3. Resolving tables and their associated metadata, ensuring proper export/import behavior.
 * 4. Managing location paths for intermediate storage, target namespace, and final destinations.
 * 5. Determining and executing steps for renaming, exporting, and importing tables.
 * <p>
 * Additional functionality includes advanced handling for ACID tables, managing
 * ownership transfer, and error handling for issues such as exceeding partition limits.
 * <p>
 * This class is primarily intended for use in a system orchestrated around
 * migrating or syncing data between different data environments using custom logic
 * and predefined strategies.
 */
@Service
@Slf4j
@Getter
public class ExportCircularResolveService extends DataStrategyBase {

    @NonNull
    private final WarehouseService warehouseService;

    public ExportCircularResolveService(@NonNull ConversionResultService conversionResultService,
                                        @NonNull ExecutionContextService executionContextService,
                                        @NonNull StatsCalculatorService statsCalculatorService,
                                        @NonNull CliEnvironment cliEnvironment,
                                        @NonNull TranslatorService translatorService,
                                        @NonNull FeatureService featureService,
                                        @NonNull WarehouseService warehouseService) {
        super(conversionResultService, executionContextService, statsCalculatorService, cliEnvironment,
                translatorService, featureService);
        this.warehouseService = warehouseService;
    }

    @Override
    public Boolean buildOutDefinition(DBMirror dbMirror, TableMirror tableMirror) {
        return null;
    }

    public Boolean buildOutExportImportSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("No ConversionResult found in the execution context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

//        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        log.debug("Database: {} buildout EXPORT_IMPORT SQL", tableMirror.getName());
        String database = null;
        database = getConversionResultService().getResolvedDB(dbMirror.getName());
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        String leftNamespace = TableUtils.getLocation(let.getName(), let.getDefinition());
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        Warehouse warehouse = getWarehouseService().getWarehousePlan(dbMirror.getName());
        try {
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
                exportLoc = config.getTransfer().getExportBaseDirPrefix()
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

            String importLoc = null;
            if (!isBlank(job.getIntermediateStorage())
                    || !isBlank(job.getTargetNamespace())) {
                importLoc = exportLoc;
            } else {
                // We'll use the LEFT Namespace for the EXPORT location to ensure consistency with the EXPORT's
                //   regardless of the original locations namespace.  This is important for the IMPORT.
                // checked..
                importLoc = leftNamespace + exportLoc;
            }

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = getTranslatorService().translateTableLocation(dbMirror, tableMirror, sourceLocation, 1, null);
            String importSql;
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
                    targetLocation = conversionResult.getTargetNamespace()
                            + warehouse.getExternalDirectory() +
                            "/" + getConversionResultService().getResolvedDB(dbMirror.getName()) + ".db/"
                            + tableMirror.getName();
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                }
            }

            if (ret.isExists()) {
                if (job.isSync()) {
                    // Need to Drop table first.
                    String dropExistingTable = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
                    if (isACIDInPlace(tableMirror, Environment.LEFT)) {
                        let.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        let.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    } else {
                        ret.addSql(MirrorConf.DROP_TABLE_DESC, dropExistingTable);
                        ret.addIssue(EXPORT_IMPORT_SYNC.getDesc());
                    }
                }
            }
            if (isACIDInPlace(tableMirror, Environment.LEFT)) {
                let.addSql(TableUtils.IMPORT_TABLE, importSql);
            } else {
                ret.addSql(TableUtils.IMPORT_TABLE, importSql);
                if (!conversionResult.getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()
                        && config.getOwnershipTransfer().isTable() && !isBlank(let.getOwner())) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                }
            }

            if (let.getPartitions().size() > job.getHybrid().getExportImportPartitionLimit() &&
                    job.getHybrid().getExportImportPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addError("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->exportImportPartitionLimit) of "
                        + job.getHybrid().getExportImportPartitionLimit() +
                        ".  This value is used to abort migrations that have a high potential for failure.  " +
                        "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ep'.");
                rtn = Boolean.FALSE;
            } else {
                rtn = Boolean.TRUE;
            }
        } catch (Throwable t) {
            log.error("Error executing EXPORT_IMPORT", t);
            // handle exception...
        }
        return rtn;
    }

    @Override
    public Boolean buildOutSql(DBMirror dbMirror, TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean build(DBMirror dbMirror, TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean execute(DBMirror dbMirror, TableMirror tableMirror) {
        return null;
    }

}