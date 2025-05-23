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

import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyBase;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
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

    private final ConfigService configService;
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final WarehouseService warehouseService;

    // All dependencies are injected via the constructor
    public ExportCircularResolveService(StatsCalculatorService statsCalculatorService,
                                        ExecuteSessionService executeSessionService,
                                        TranslatorService translatorService,
                                        ConfigService configService,
                                        DatabaseService databaseService,
                                        TableService tableService,
                                        WarehouseService warehouseService) {
        super(statsCalculatorService, executeSessionService,translatorService);
        this.configService = configService;
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.warehouseService = warehouseService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    public Boolean buildOutExportImportSql(TableMirror tableMirror) throws MissingDataPointException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        log.debug("Database: {} buildout EXPORT_IMPORT SQL", tableMirror.getName());
        String database = null;
        database = HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config);
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        String leftNamespace = TableUtils.getLocation(let.getName(), let.getDefinition());
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        Warehouse warehouse = warehouseService.getWarehousePlan(tableMirror.getParent().getName());
        try {
            // LEFT Export to directory
            String useLeftDb = MessageFormat.format(MirrorConf.USE, tableMirror.getParent().getName());
            let.addSql(TableUtils.USE_DESC, useLeftDb);
            String exportLoc = null;

            if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                String isLoc = config.getTransfer().getIntermediateStorage();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" +
                        config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        config.getRunMarker() + "/" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else if (!isBlank(config.getTransfer().getTargetNamespace())) {
                String isLoc = config.getTransfer().getTargetNamespace();
                // Deal with extra '/'
                isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                exportLoc = isLoc + "/" + config.getTransfer().getRemoteWorkingDirectory() + "/" +
                        config.getRunMarker() + "/" +
                        tableMirror.getParent().getName() + "/" +
                        tableMirror.getName();
            } else {
                exportLoc = config.getTransfer().getExportBaseDirPrefix()
                        + tableMirror.getParent().getName() + "/" + let.getName();
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
            if (!isBlank(config.getTransfer().getIntermediateStorage())
                    || !isBlank(config.getTransfer().getTargetNamespace())) {
                importLoc = exportLoc;
            } else {
                // We'll use the LEFT Namespace for the EXPORT location to ensure consistency with the EXPORT's
                //   regardless of the original locations namespace.  This is important for the IMPORT.
                // checked..
                importLoc = leftNamespace + exportLoc;
            }

            String sourceLocation = TableUtils.getLocation(let.getName(), let.getDefinition());
            String targetLocation = getTranslatorService().translateTableLocation(tableMirror, sourceLocation, 1, null);
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
                    targetLocation = config.getTargetNamespace()
                            + warehouse.getExternalDirectory() +
                            "/" + HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config) + ".db/"
                            + tableMirror.getName();
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                } else {
                    importSql = MessageFormat.format(MirrorConf.IMPORT_EXTERNAL_TABLE_LOCATION, let.getName(), importLoc, targetLocation);
                }
            }

            if (ret.isExists()) {
                if (config.isSync()) {
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
                if (!config.getCluster(Environment.RIGHT).isLegacyHive()
                        && config.getOwnershipTransfer().isTable() && !isBlank(let.getOwner())) {
                    String ownerSql = MessageFormat.format(MirrorConf.SET_TABLE_OWNER, let.getName(), let.getOwner());
                    ret.addSql(MirrorConf.SET_TABLE_OWNER_DESC, ownerSql);
                }
            }

            if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                    config.getHybrid().getExportImportPartitionLimit() > 0) {
                // The partition limit has been exceeded.  The process will need to be done manually.
                let.addError("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
                        "limit (hybrid->exportImportPartitionLimit) of "
                        + config.getHybrid().getExportImportPartitionLimit() +
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
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean build(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        return null;
    }

}