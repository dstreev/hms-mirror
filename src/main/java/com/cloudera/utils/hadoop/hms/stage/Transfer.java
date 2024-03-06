/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.DISTCP_FOR_SO_ACID;

@Slf4j
public class Transfer implements Callable<ReturnStatus> {
//    private static final Logger log = LoggerFactory.getLogger(Transfer.class);
    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tableMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Transfer(Config config, DBMirror dbMirror, TableMirror tableMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tableMirror = tableMirror;
    }


    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();

        try {
            Date start = new Date();
            log.info("Migrating " + dbMirror.getName() + "." + tableMirror.getName());

            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

            // Set Database to Transfer DB.
            tableMirror.setPhaseState(PhaseState.STARTED);

            tableMirror.setStrategy(config.getDataStrategy());
//            tblMirror.setResolvedDbName(config.getResolvedDB(tblMirror.getParent().getName()));

            tableMirror.incPhase();
            tableMirror.addStep("TRANSFER", config.getDataStrategy().toString());
            try {
                DataStrategy dataStrategy = null;
                switch (config.getDataStrategy()) {
//                    case DUMP:
//                        successful = doDump();
//                        break;
//                    case SCHEMA_ONLY:
//                        successful = doSchemaOnly();
//                        break;
//                    case LINKED:
//                        successful = doLinked();
//                        break;
//                    case COMMON:
//                        successful = doCommon();
//                        break;
//                    case EXPORT_IMPORT:
//                        successful = doExportImport();
//                        break;
//                    case SQL:
//                        successful = doSQL();
//                        break;
//                    case CONVERT_LINKED:
//
//                    case STORAGE_MIGRATION:
//                        dataStrategy = config.getDataStrategy().getDataStrategy();
//                        dataStrategy.setConfig(config);
//                        dataStrategy.setDBMirror(dbMirror);
//                        dataStrategy.setTableMirror(tblMirror);
//                        successful = dataStrategy.execute();
//                        break;
                    case HYBRID:
                        if (TableUtils.isACID(let) && config.getMigrateACID().isDowngradeInPlace()) {
                            DataStrategy dsHADI = DataStrategyEnum.HYBRID_ACID_DOWNGRADE_INPLACE.getDataStrategy();
                            dsHADI.setTableMirror(tableMirror);
                            dsHADI.setDBMirror(dbMirror);
                            dsHADI.setConfig(config);
                            successful = dsHADI.execute();// doHYBRIDACIDInplaceDowngrade();
//                            successful = doHYBRIDACIDInplaceDowngrade();
                        } else {
                            DataStrategy dsH = DataStrategyEnum.HYBRID.getDataStrategy();
                            dsH.setTableMirror(tableMirror);
                            dsH.setDBMirror(dbMirror);
                            dsH.setConfig(config);
                            successful = dsH.execute();// doHYBRID();
//                            successful = doHybrid();
                        }
                        break;
                    default:
                        dataStrategy = config.getDataStrategy().getDataStrategy();
                        dataStrategy.setConfig(config);
                        dataStrategy.setDBMirror(dbMirror);
                        dataStrategy.setTableMirror(tableMirror);
                        successful = dataStrategy.execute();
                        break;

                }
                // Build out DISTCP workplans.
                if (successful && config.getTransfer().getStorageMigration().isDistcp()) {
                    // Build distcp reports.
                    if (config.getTransfer().getIntermediateStorage() != null) {
                        // LEFT PUSH INTERMEDIATE
                        // The Transfer Table should be available.
                        String isLoc = config.getTransfer().getIntermediateStorage();
                        // Deal with extra '/'
                        isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                        isLoc = isLoc + "/" +
                                config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                config.getRunMarker() + "/" +
                                dbMirror.getName() + ".db/" +
                                tableMirror.getName();

                        config.getTranslator().addLocation(dbMirror.getName(), Environment.LEFT,
                                TableUtils.getLocation(tableMirror.getName(), let.getDefinition()),
                                isLoc, 1);
                        // RIGHT PULL from INTERMEDIATE
                        String fnlLoc = null;
                        if (set.getDefinition().size() > 0) {
                            fnlLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                        } else {
                            fnlLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (fnlLoc == null && config.getResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (config.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(config.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(config.getResolvedDB(dbMirror.getName())).append(".db").append("/").append(tableMirror.getName());
                                fnlLoc = sbDir.toString();
                            }
                        }
                        config.getTranslator().addLocation(dbMirror.getName(), Environment.RIGHT,
                                isLoc,
                                fnlLoc, 1);
                    } else if (config.getTransfer().getCommonStorage() != null && config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
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
                        if (newLoc == null && config.getResetToDefaultLocation()) {
                            StringBuilder sbDir = new StringBuilder();
                            sbDir.append(config.getTransfer().getCommonStorage());
                            sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                            sbDir.append(config.getResolvedDB(dbMirror.getName())).append(".db").append("/").append(tableMirror.getName());
                            newLoc = sbDir.toString();
                        }
                        config.getTranslator().addLocation(dbMirror.getName(), Environment.LEFT,
                                origLoc, newLoc,1);
                    } else {
                        // RIGHT PULL
                        if (TableUtils.isACID(let)
                                && !config.getMigrateACID().isDowngrade()
                                && !(config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
                            tableMirror.addIssue(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
                            successful = Boolean.FALSE;
                        } else if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (rLoc == null && config.getResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (config.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(config.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(config.getResolvedDB(dbMirror.getName())).append(".db").append("/").append(tableMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            config.getTranslator().addLocation(dbMirror.getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), tet.getDefinition()),
                                    rLoc,1);
                        } else {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (rLoc == null && config.getResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (config.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(config.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(config.getResolvedDB(dbMirror.getName())).append(".db").append("/").append(tableMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            config.getTranslator().addLocation(dbMirror.getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), let.getDefinition())
                                    , rLoc,1);
                        }
                    }
                }

                if (successful)
                    tableMirror.setPhaseState(PhaseState.SUCCESS);
                else
                    tableMirror.setPhaseState(PhaseState.ERROR);
            } catch (ConnectionException ce) {
                tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + ce.getMessage());
                log.error("Connection Error", ce);
                ce.printStackTrace();
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(ce);
            } catch (RuntimeException rte) {
                tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + rte.getMessage());
                log.error("Transfer Error", rte);
                rte.printStackTrace();
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(rte);
            }

            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tableMirror.setStageDuration(diff);
            log.info("Migration complete for " + dbMirror.getName() + "." + tableMirror.getName() + " in " +
                    diff + "ms");
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (Throwable t) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(t);
        }
        return rtn;
    }

//    protected Boolean doSQL() {
//        Boolean rtn = Boolean.FALSE;
//
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//
//        if (tblMirror.isACIDDowngradeInPlace(config, let)) {
//            rtn = doSQLACIDDowngradeInplace();
//        } else if (config.getTransfer().getIntermediateStorage() != null
//                || config.getTransfer().getCommonStorage() != null
//                || (TableUtils.isACID(let)
//                && config.getMigrateACID().isOn())) {
//            if (TableUtils.isACID(let)) {
//                tblMirror.setStrategy(DataStrategyEnum.ACID);
//            }
//            rtn = doIntermediateTransfer();
//        } else {
//
//            EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//            EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
//
//            // We should not get ACID tables in this routine.
//            rtn = tblMirror.buildoutSQLDefinition(config, dbMirror);
//
//            if (rtn)
//                rtn = AVROCheck();
//
//            if (rtn)
//                rtn = tblMirror.buildoutSQLSql(config, dbMirror);
//
//            // Construct Transfer SQL
//            if (rtn) {
//                rtn = tblMirror.buildTransferSql(let, set, ret, config);
//
//                // Execute the RIGHT sql if config.execute.
//                if (rtn) {
//                    config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//                }
//            }
//        }
//        return rtn;
//    }


//    protected Boolean doIntermediateTransfer() {
//        Boolean rtn = Boolean.FALSE;
//
//        rtn = tblMirror.buildoutIntermediateDefinition(config, dbMirror);
//        if (rtn)
//            rtn = tblMirror.buildoutIntermediateSql(config, dbMirror);
//
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
//        EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
//        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//
//        if (rtn) {
//            // Construct Transfer SQL
//            if (config.getCluster(Environment.LEFT).getLegacyHive()) {
//                // We need to ensure that 'tez' is the execution engine.
//                let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
//            }
//
//            Pair cleanUp = new Pair("Post Migration Cleanup", "-- To be run AFTER final RIGHT SQL statements.");
//            let.addCleanUpSql(cleanUp);
//
//            String useLeftDb = MessageFormat.format(MirrorConf.USE, dbMirror.getName());
//            Pair leftUsePair = new Pair(TableUtils.USE_DESC, useLeftDb);
//            let.addCleanUpSql(leftUsePair);
//
//            rtn = tblMirror.buildTransferSql(let, set, ret, config);
//
//            // Execute the LEFT sql if config.execute.
//            if (rtn) {
//                rtn = config.getCluster(Environment.LEFT).runTableSql(tblMirror);
//            }
//
//            // Execute the RIGHT sql if config.execute.
//            if (rtn) {
//                rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//            }
//
//            if (rtn) {
//                // Run the Cleanup Scripts
//                config.getCluster(Environment.LEFT).runTableSql(let.getCleanUpSql(), tblMirror, Environment.LEFT);
//            }
//
//            // RIGHT Shadow table
////            if (set.getDefinition().size() > 0) {
////                List<Pair> rightCleanup = new ArrayList<Pair>();
////
////                String useRightDb = MessageFormat.format(MirrorConf.USE, config.getResolvedDB(dbMirror.getName()));
////                Pair rightUsePair = new Pair(TableUtils.USE_DESC, useRightDb);
////                ret.addCleanUpSql(rightUsePair);
////                String rightDropShadow = MessageFormat.format(MirrorConf.DROP_TABLE, set.getName());
////                Pair rightDropPair = new Pair(TableUtils.DROP_SHADOW_TABLE, rightDropShadow);
////                ret.addCleanUpSql(rightDropPair);
////                tblMirror.addStep("RIGHT ACID Shadow SQL Cleanup", "Built");
////
////                if (rtn) {
////                    // Run the Cleanup Scripts
////                    config.getCluster(Environment.RIGHT).runTableSql(ret.getCleanUpSql(), tblMirror, Environment.RIGHT);
////                }
////            }
//        }
//        return rtn;
//    }

//    protected Boolean doStorageMigrationTransfer() {
//        Boolean rtn = Boolean.FALSE;
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//
//        /*
//        If using distcp, we don't need to go through and rename/recreate the tables.  We just need to change the
//        location of the current tables and partitions.
//         */
//        if (config.getTransfer().getStorageMigration().isDistcp()) {
//
//            String database = dbMirror.getName();
//            String useDb = MessageFormat.format(MirrorConf.USE, database);
//
//            let.addSql(TableUtils.USE_DESC, useDb);
//
//            Boolean noIssues = Boolean.TRUE;
//            String origLocation = TableUtils.getLocation(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT));
//            try {
//                String newLocation = Context.getInstance().getConfig().getTranslator().
//                        translateTableLocation(tblMirror, origLocation, 0, null);
//
//                // Build Alter Statement for Table to change location.
//                String alterTable = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, tblMirror.getEnvironmentTable(Environment.LEFT).getName(), newLocation);
//                Pair alterTablePair = new Pair(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterTable);
//                let.addSql(alterTablePair);
//                if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
//                        config.getTransfer().getWarehouse().getManagedDirectory() != null) {
//                    if (TableUtils.isExternal(tblMirror.getEnvironmentTable(Environment.LEFT))) {
//                        // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
//                        if (!newLocation.startsWith(tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
//                            // Set warning that even though you've specified to warehouse directories, the current configuration
//                            // will NOT place it in that directory.
//                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
//                                    tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
//                                    newLocation);
//                            tblMirror.addIssue(Environment.LEFT, msg);
//                        }
//                    } else {
//                        String location = null;
//                        // Need to make adjustments for hdp3 hive 3.
//                        if (config.getCluster(Environment.LEFT).isHdpHive3()) {
//                            location = tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION);
//                        } else {
//                            location = tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION);
//                        }
//                        if (!newLocation.startsWith(location)) {
//                            // Set warning that even though you've specified to warehouse directories, the current configuration
//                            // will NOT place it in that directory.
//                            String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "table",
//                                    tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
//                                    newLocation);
//                            tblMirror.addIssue(Environment.LEFT, msg);
//                        }
//
//                    }
//                }
//            } catch (RuntimeException rte) {
//                noIssues = Boolean.FALSE;
//                tblMirror.addIssue(Environment.LEFT, rte.getMessage());
//                log.error(rte.getMessage(), rte);
//            }
//
//            // Build Alter Statement for Partitions to change location.
//            if (let.getPartitioned()) {
//                // Loop through partitions in let.getPartitions and build alter statements.
//                for (Map.Entry<String, String> entry: let.getPartitions().entrySet()) {
//                    String partSpec = entry.getKey();
//                    int level = StringUtils.countMatches(partSpec, "/");
//                    // Translate to 'partition spec'.
//                    partSpec = TableUtils.toPartitionSpec(partSpec);
//                    String partLocation = entry.getValue();
//                    try {
//                        String newPartLocation = Context.getInstance().getConfig().getTranslator().
//                                translateTableLocation(tblMirror, partLocation, ++level, entry.getKey());
//                        String addPartSql = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_LOCATION, let.getName(), partSpec, newPartLocation);
//                        String partSpecDesc = MessageFormat.format(MirrorConf.ALTER_TABLE_PARTITION_LOCATION_DESC, partSpec);
//                        let.addSql(partSpecDesc, addPartSql);
//                        if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
//                                config.getTransfer().getWarehouse().getManagedDirectory() != null) {
//                            if (TableUtils.isExternal(tblMirror.getEnvironmentTable(Environment.LEFT))) {
//                                // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
//                                if (!newPartLocation.startsWith(tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
//                                    // Set warning that even though you've specified to warehouse directories, the current configuration
//                                    // will NOT place it in that directory.
//                                    String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
//                                            tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
//                                            newPartLocation);
//                                    tblMirror.addIssue(Environment.LEFT, msg);
//                                }
//                            } else {
//                                String location = null;
//                                // Need to make adjustments for hdp3 hive 3.
//                                if (config.getCluster(Environment.LEFT).isHdpHive3()) {
//                                    location = tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION);
//                                } else {
//                                    location = tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION);
//                                }
//                                if (!newPartLocation.startsWith(location)) {
//                                    // Set warning that even though you've specified to warehouse directories, the current configuration
//                                    // will NOT place it in that directory.
//                                    String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
//                                            tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
//                                            newPartLocation);
//                                    tblMirror.addIssue(Environment.LEFT, msg);
//                                }
//
//                            }
//                        }
//                    } catch (RuntimeException rte) {
//                        noIssues = Boolean.FALSE;
//                        tblMirror.addIssue(Environment.LEFT, rte.getMessage());
//                    }
//                }
//                if (noIssues) {
//                    rtn = Boolean.TRUE;
//                }
//            } else {
//                rtn = Boolean.TRUE;
//            }
//        } else {
//            rtn = tblMirror.buildoutSTORAGEMIGRATIONDefinition(config, dbMirror);
//            if (rtn)
//                rtn = tblMirror.buildoutSTORAGEMIGRATIONSql(config, dbMirror);
//
//            if (rtn) {
//                // Construct Transfer SQL
//                if (config.getCluster(Environment.LEFT).getLegacyHive()) {
//                    // We need to ensure that 'tez' is the execution engine.
//                    let.addSql(new Pair(TEZ_EXECUTION_DESC, SET_TEZ_AS_EXECUTION_ENGINE));
//                }
//                // Set Override Properties.
//                if (config.getOptimization().getOverrides() != null) {
//                    for (String key : config.getOptimization().getOverrides().getLeft().keySet()) {
//                        let.addSql("Setting " + key, "set " + key + "=" + config.getOptimization().getOverrides().getLeft().get(key));
//                    }
//                }
//
//                StatsCalculator.setSessionOptions(config.getCluster(Environment.LEFT), let, let);
//
//                // Need to see if the table has partitions.
//                if (let.getPartitioned()) {
//                    // Check that the partition count doesn't exceed the configuration limit.
//                    // Build Partition Elements.
//                    if (config.getOptimization().getSkip()) {
//                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
//                            let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
//                        }
//                        String partElement = TableUtils.getPartitionElements(let);
//                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
//                                let.getName(), ret.getName(), partElement);
//                        String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
//                        let.addSql(new Pair(transferDesc, transferSql));
//                    } else if (config.getOptimization().getSortDynamicPartitionInserts()) {
//                        // Declarative
//                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
//                            let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=true");
//                            if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
//                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=0");
//                            }
//                        }
//                        String partElement = TableUtils.getPartitionElements(let);
//                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_DECLARATIVE,
//                                let.getName(), ret.getName(), partElement);
//                        String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
//                        let.addSql(new Pair(transferDesc, transferSql));
//                    } else {
//                        // Prescriptive
//                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
//                            let.addSql("Setting " + SORT_DYNAMIC_PARTITION, "set " + SORT_DYNAMIC_PARTITION + "=false");
//                            if (!config.getCluster(Environment.LEFT).getHdpHive3()) {
//                                let.addSql("Setting " + SORT_DYNAMIC_PARTITION_THRESHOLD, "set " + SORT_DYNAMIC_PARTITION_THRESHOLD + "=-1");
//                            }
//                        }
//                        String partElement = TableUtils.getPartitionElements(let);
//                        String distPartElement = StatsCalculator.getDistributedPartitionElements(let);;
//                        String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_WITH_PARTITIONS_PRESCRIPTIVE,
//                                let.getName(), ret.getName(), partElement, distPartElement);
//                        String transferDesc = MessageFormat.format(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, let.getPartitions().size());
//                        let.addSql(new Pair(transferDesc, transferSql));
//                    }
//                    if (TableUtils.isACID(let)) {
//                        if (let.getPartitions().size() > config.getMigrateACID().getPartitionLimit() && config.getMigrateACID().getPartitionLimit() > 0) {
//                            // The partition limit has been exceeded.  The process will need to be done manually.
//                            let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
//                                    "limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
//                                    ".  This value is used to abort migrations that have a high potential for failure.  " +
//                                    "The migration will need to be done manually OR try increasing the limit. Review commandline option '-ap'.");
//                            rtn = Boolean.FALSE;
//                        }
//                    } else {
//                        if (let.getPartitions().size() > config.getHybrid().getSqlPartitionLimit() && config.getHybrid().getSqlPartitionLimit() > 0) {
//                            // The partition limit has been exceeded.  The process will need to be done manually.
//                            let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the configuration " +
//                                    "limit (hybrid->sqlPartitionLimit) of " + config.getHybrid().getSqlPartitionLimit() +
//                                    ".  This value is used to abort migrations that have a high potential for failure.  " +
//                                    "The migration will need to be done manually OR try increasing the limit. Review commandline option '-sp'.");
//                            rtn = Boolean.FALSE;
//                        }
//                    }
//                } else {
//                    // No Partitions
//                    String transferSql = MessageFormat.format(MirrorConf.SQL_DATA_TRANSFER_OVERWRITE, let.getName(), ret.getName());
//                    let.addSql(new Pair(TableUtils.STORAGE_MIGRATION_TRANSFER_DESC, transferSql));
//                }
//            }
//        }
//        if (rtn) {
//            // Run the Transfer Scripts
//            rtn = config.getCluster(Environment.LEFT).runTableSql(let.getSql(), tblMirror, Environment.LEFT);
//        }
//        return rtn;
//    }

//    protected Boolean doHybrid() {
//        Boolean rtn = Boolean.FALSE;
//
//        // Need to look at table.  ACID tables go to doACID()
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//
//        // Acid tables between legacy and non-legacy are forced to intermediate
//        if (TableUtils.isACID(let) && config.legacyMigration()) {
//            tblMirror.setStrategy(DataStrategyEnum.ACID);
//            if (config.getMigrateACID().isOn()) {
//                rtn = doIntermediateTransfer();
//            } else {
//                let.addIssue(TableUtils.ACID_NOT_ON);
//                rtn = Boolean.FALSE;
//            }
//        } else {
//            if (let.getPartitioned()) {
//                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
//                        config.getHybrid().getExportImportPartitionLimit() > 0) {
//                    // SQL
//                    let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
//                            "partition limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
//                            ".  Hence, the SQL method has been selected for the migration.");
//
//                    tblMirror.setStrategy(DataStrategyEnum.SQL);
//                    if (config.getTransfer().getIntermediateStorage() != null ||
//                            config.getTransfer().getCommonStorage() != null) {
//                        rtn = doIntermediateTransfer();
//                    } else {
//                        rtn = doSQL();
//                    }
//                } else {
//                    // EXPORT
//                    tblMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
//                    rtn = doExportImport();
//                }
//            } else {
//                // EXPORT
//                tblMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
//                rtn = doExportImport();
//            }
//        }
//        return rtn;
//    }

//    protected Boolean doDump() {
//        Boolean rtn = Boolean.FALSE;
//
//        rtn = tblMirror.buildoutDUMPDefinition(config, dbMirror);
//        if (rtn) {
//            rtn = tblMirror.buildoutDUMPSql(config, dbMirror);
//        }
//        return rtn;
//    }

//    protected Boolean doSchemaOnly() {
//        Boolean rtn = Boolean.FALSE;
//
//        rtn = tblMirror.buildoutSCHEMA_ONLYDefinition(config, dbMirror);
//
//        if (rtn) {
//            rtn = AVROCheck();
//        }
//        if (rtn) {
//            rtn = tblMirror.buildoutSCHEMA_ONLYSql(config, dbMirror);
//        }
//        if (rtn) {
//            rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//        }
//        return rtn;
//    }

//    protected Boolean doLinked() {
//        Boolean rtn = Boolean.FALSE;
//
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable tet = tblMirror.getEnvironmentTable(Environment.TRANSFER);
//        EnvironmentTable set = tblMirror.getEnvironmentTable(Environment.SHADOW);
//        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//
//        if (TableUtils.isACID(let)) {
//            tblMirror.addIssue(Environment.LEFT, "You can't 'LINK' ACID tables.");
//            rtn = Boolean.FALSE;
//        } else {
//            rtn = tblMirror.buildoutLINKEDDefinition(config, dbMirror);
//        }
//
//        if (rtn) {
//            rtn = tblMirror.buildoutLINKEDSql(config, dbMirror);
//        }
//
//        // Execute the RIGHT sql if config.execute.
//        if (rtn) {
//            rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//        }
//
//        return rtn;
//
//    }

//    protected Boolean doCommon() {
//        Boolean rtn = Boolean.FALSE;
//
//
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//
//        if (TableUtils.isACID(let)) {
//            rtn = Boolean.FALSE;
//            tblMirror.addIssue(Environment.RIGHT,
//                    "Can't transfer SCHEMA reference on COMMON storage for ACID tables.");
//        } else {
//            rtn = tblMirror.buildoutCOMMONDefinition(config, dbMirror);
//        }
//
//        if (rtn) {
//            rtn = tblMirror.buildoutCOMMONSql(config, dbMirror);
//        }
//        // Execute the RIGHT sql if config.execute.
//        if (rtn) {
//            rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//        }
//
//        return rtn;
//    }

//    protected Boolean doExportImport() {
//        Boolean rtn = Boolean.FALSE;
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//        if (ret.getExists()) {
//            if (!config.isSync()) {
//                let.addIssue(MessageCode.SCHEMA_EXISTS_NO_ACTION_DATA.getDesc());
//                return Boolean.FALSE;
//            }
//        }
//
//        if (tblMirror.isACIDDowngradeInPlace(config, let)) {
//            rtn = doEXPORTIMPORTACIDInplaceDowngrade();
//        } else {
//            if (TableUtils.isACID(let)) {
//                if (config.getCluster(Environment.LEFT).getLegacyHive() != config.getCluster(Environment.RIGHT).getLegacyHive()) {
//                    rtn = Boolean.FALSE;
//                    tblMirror.addIssue(Environment.LEFT, "ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive.");
//                } else {
//                    rtn = tblMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
//                }
//
//            } else {
//                rtn = tblMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
//
//                if (rtn)
//                    rtn = AVROCheck();
//            }
//            // If EXPORT_IMPORT, need to run LEFT queries.
//            if (rtn) {
//                rtn = config.getCluster(Environment.LEFT).runTableSql(tblMirror);
//            }
//
//            // Execute the RIGHT sql if config.execute.
//            if (rtn) {
//                rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//            }
//        }
//
//        return rtn;
//    }

//    protected Boolean doConvertLinked() {
//        Boolean rtn = Boolean.FALSE;
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//
////        tblMirror.setResolvedDbName(config.getResolvedDB(tblMirror.getParent().getName()));
//
//        // If RIGHT doesn't exist, run SCHEMA_ONLY.
//        if (ret == null) {
//            tblMirror.addIssue(Environment.RIGHT, "Table doesn't exist.  To transfer, run 'SCHEMA_ONLY'");
//        } else {
//            // Make sure table isn't an ACID table.
//            if (TableUtils.isACID(let)) {
//                tblMirror.addIssue(Environment.LEFT, "ACID tables not eligible for this operation");
//            } else if (tblMirror.isPartitioned(Environment.LEFT)) {
//                // We need to drop the RIGHT and RECREATE.
//                ret.addIssue("Table is partitioned.  Need to change data strategy to drop and recreate.");
//                String useDb = MessageFormat.format(MirrorConf.USE, tblMirror.getParent().getResolvedName());
//                ret.addSql(MirrorConf.USE_DESC, useDb);
//
//                // Make sure the table is NOT set to purge.
//                if (TableUtils.isExternalPurge(ret)) {
//                    String purgeSql = MessageFormat.format(MirrorConf.REMOVE_TABLE_PROP, ret.getName(), EXTERNAL_TABLE_PURGE);
//                    ret.addSql(MirrorConf.REMOVE_TABLE_PROP_DESC, purgeSql);
//                }
//
//                String dropTable = MessageFormat.format(MirrorConf.DROP_TABLE, tblMirror.getName());
//                ret.addSql(MirrorConf.DROP_TABLE_DESC, dropTable);
//                tblMirror.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
//                // Set False that it doesn't exists, which it won't, since we're dropping it.
//                ret.setExists(Boolean.FALSE);
//                rtn = doSchemaOnly();
//            } else {
//                // - AVRO LOCATION
//                if (AVROCheck()) {
//                    String useDb = MessageFormat.format(MirrorConf.USE, tblMirror.getParent().getResolvedName());
//                    ret.addSql(MirrorConf.USE_DESC, useDb);
//                    // Look at the table definition and get.
//                    // - LOCATION
//                    String sourceLocation = TableUtils.getLocation(ret.getName(), ret.getDefinition());
//                    String targetLocation = config.getTranslator().
//                            translateTableLocation(tblMirror, sourceLocation, 1, null);
//                    String alterLocSql = MessageFormat.format(MirrorConf.ALTER_TABLE_LOCATION, ret.getName(), targetLocation);
//                    ret.addSql(MirrorConf.ALTER_TABLE_LOCATION_DESC, alterLocSql);
//                    // TableUtils.updateTableLocation(ret, targetLocation)
//                    // - Check Comments for "legacy.managed" setting.
//                    //    - MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG (if so, set purge flag MirrorConf.EXTERNAL_TABLE_PURGE)
//                    if (TableUtils.isHMSLegacyManaged(ret)) {
//                        // ALTER TABLE x SET TBLPROPERTIES ('purge flag').
//                        String purgeSql = MessageFormat.format(MirrorConf.ADD_TABLE_PROP, ret.getName(), EXTERNAL_TABLE_PURGE, "true");
//                        ret.addSql(MirrorConf.ADD_TABLE_PROP_DESC, purgeSql);
//                    }
//                    rtn = Boolean.TRUE;
//
//                    // Execute the RIGHT sql if config.execute.
//                    if (rtn) {
//                        rtn = config.getCluster(Environment.RIGHT).runTableSql(tblMirror);
//                    }
//                }
//            }
//        }
//
//        return rtn;
//    }

//    protected Boolean doHYBRIDACIDInplaceDowngrade() {
//        Boolean rtn = Boolean.TRUE;
//        /*
//        Check environment is Hive 3.
//            if not, need to do SQLACIDInplaceDowngrade.
//        If table is not partitioned
//            go to export import downgrade inplace
//        else if partitions <= hybrid.exportImportPartitionLimit
//            go to export import downgrade inplace
//        else if partitions <= hybrid.sqlPartitionLimit
//            go to sql downgrade inplace
//        else
//            too many partitions.
//         */
//        if (config.getCluster(Environment.LEFT).getLegacyHive()) {
//            doSQLACIDDowngradeInplace();
//        } else {
//            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//            if (let.getPartitioned()) {
//                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
//                if (let.getPartitions().size() < config.getHybrid().getExportImportPartitionLimit() ||
//                        config.getHybrid().getExportImportPartitionLimit() <= 0) {
//                    doEXPORTIMPORTACIDInplaceDowngrade();
//                } else {
//                    doSQLACIDDowngradeInplace();
//                }
//            } else {
//                // Go with EXPORT_IMPORT
//                doEXPORTIMPORTACIDInplaceDowngrade();
//            }
//        }
//        return rtn;
//    }

//    protected Boolean doSQLACIDDowngradeInplace() {
//        Boolean rtn = Boolean.TRUE;
//        /*
//        rename original table
//        remove artificial bucket in new table def
//        create new external table with original name
//        from original_archive insert overwrite table new external (deal with partitions).
//        write cleanup sql to drop original_archive.
//         */
//        rtn = tblMirror.buildoutSQLACIDDowngradeInplaceDefinition(config, dbMirror);
//
//        if (rtn) {
//            // Build cleanup Queries (drop archive table)
//            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
//            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);
//
//            // Check Partition Counts.
//            if (let.getPartitioned() && let.getPartitions().size() > config.getMigrateACID().getPartitionLimit()) {
//                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the ACID SQL " +
//                        "partition limit (migrateACID->partitionLimit) of " + config.getMigrateACID().getPartitionLimit() +
//                        ".  The queries will NOT be automatically run.");
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        if (rtn) {
//            // Build Transfer SQL
//            rtn = tblMirror.buildoutSQLACIDDowngradeInplaceSQL(config, dbMirror);
//        }
//
//        // run queries.
//        if (rtn) {
//            config.getCluster(Environment.LEFT).runTableSql(tblMirror);
//        }
//
//        return rtn;
//    }

//    protected Boolean doEXPORTIMPORTACIDInplaceDowngrade() {
//        Boolean rtn = Boolean.TRUE;
//        /*
//        rename original to archive
//        export original table
//        import as external to original tablename
//        write cleanup sql to drop original_archive.
//         */
//        // Check Partition Limits before proceeding.
//        rtn = tblMirror.buildoutEXPORT_IMPORTSql(config, dbMirror);
//        if (rtn) {
//            // Build cleanup Queries (drop archive table)
//            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//            String cleanUpArchive = MessageFormat.format(MirrorConf.DROP_TABLE, let.getName());
//            let.addCleanUpSql(TableUtils.DROP_DESC, cleanUpArchive);
//
//            // Check Partition Counts.
//            if (let.getPartitioned() && let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit()) {
//                let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
//                        "partition limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
//                        ".  The queries will NOT be automatically run.");
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        // run queries.
//        if (rtn) {
//            config.getCluster(Environment.LEFT).runTableSql(tblMirror);
//        }
//
//        return rtn;
//    }

//    protected Boolean AVROCheck() {
//        Boolean rtn = Boolean.TRUE;
//        Boolean relative = Boolean.FALSE;
//        // Check for AVRO
//        EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
//        EnvironmentTable ret = tblMirror.getEnvironmentTable(Environment.RIGHT);
//        if (TableUtils.isAVROSchemaBased(let)) {
//            log.info(let.getName() + ": is an AVRO table.");
//            String leftPath = TableUtils.getAVROSchemaPath(let);
//            String rightPath = null;
//            log.debug(let.getName() + ": Original AVRO Schema path: " + leftPath);
//                /* Checks:
//                - Is Path prefixed with a protocol?
//                    - (Y) Does it match the LEFT's hcfsNamespace.
//                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
//                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
//                                 can't guarantee that we can retrieve the file.
//                    - (N) Leave it and copy the file to the same relative path on the RIGHT
//                 */
//            Matcher matcher = protocolNSPattern.matcher(leftPath);
//            // ProtocolNS Found.
//            String cpCmd = null;
//            if (matcher.find()) {
//                log.info(let.getName() + " protocol Matcher found.");
//
//                // Return the whole set of groups.
//                String lns = matcher.group(0);
//
//                // Does it match the "LEFT" hcfsNamespace.
//                String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
//                if (leftNS.endsWith("/")) {
//                    leftNS = leftNS.substring(0, leftNS.length() - 1);
//                }
//                if (lns.startsWith(leftNS)) {
//                    log.info(let.getName() + " table namespace matches LEFT clusters namespace.");
//
//                    // They match, so replace with RIGHT hcfs namespace.
//                    String newNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();
//                    if (newNS.endsWith("/")) {
//                        newNS = newNS.substring(0, newNS.length() - 1);
//                    }
//                    rightPath = leftPath.replace(leftNS, newNS);
//                    log.info(ret.getName() + " table namespace adjusted for RIGHT clusters table to " + rightPath);
//                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
//                } else {
//                    // Protocol found doesn't match configured hcfs namespace for LEFT.
//                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
//                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + config.getCluster(Environment.LEFT).getHcfsNamespace() +
//                            ". Can't determine change, so we'll not do anything.";
//                    ret.addIssue(warning);
//                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
//                    log.warn(warning);
//                }
//            } else {
//                // No Protocol defined.  So we're assuming that its a relative path to the
//                // defaultFS
//                String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
//                log.info(let.getName() + ": " + rpath);
//                ret.addIssue(rpath);
//                rightPath = leftPath;
//                relative = Boolean.TRUE;
//            }
//
//            if (leftPath != null && rightPath != null && config.isCopyAvroSchemaUrls() && config.isExecute()) {
//                // Copy over.
//                log.info(let.getName() + ": Attempting to copy AVRO schema file to target cluster.");
//                HadoopSession session = null;
//                try {
//                    session = config.getCliPool().borrow();
//                    CommandReturn cr = null;
//                    if (relative) {
//                        leftPath = config.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
//                        rightPath = config.getCluster(Environment.RIGHT).getHcfsNamespace() + rightPath;
//                    }
//                    log.info("AVRO Schema COPY from: " + leftPath + " to " + rightPath);
//                    // Ensure the path for the right exists.
//                    matcher = lastDirPattern.matcher(rightPath);
//                    if (matcher.find()) {
//                        String pathEnd = matcher.group(1);
//                        String mkdir = rightPath.substring(0, rightPath.length() - pathEnd.length());
//                        cr = session.processInput("mkdir -p " + mkdir);
//                        if (cr.isError()) {
//                            ret.addIssue("Problem creating directory " + mkdir + ". " + cr.getError());
//                            rtn = Boolean.FALSE;
//                        } else {
//                            cr = session.processInput("cp -f " + leftPath + " " + rightPath);
//                            if (cr.isError()) {
//                                ret.addIssue("Problem copying AVRO schema file from " + leftPath + " to " +
//                                        mkdir + ".\n```" + cr.getError() + "```");
//                                rtn = Boolean.FALSE;
//                            }
//                        }
//                    }
//                } catch (Throwable t) {
//                    log.error(ret.getName() + ": AVRO file copy issue", t);
//                    ret.addIssue(t.getMessage());
//                    rtn = Boolean.FALSE;
//                } finally {
//                    if (session != null)
//                        config.getCliPool().returnSession(session);
//                }
//            } else {
//                log.info(let.getName() + ": did NOT attempt to copy AVRO schema file to target cluster.");
//            }
//            tblMirror.addStep("AVRO", "Checked");
//        } else {
//            // Not AVRO, so no action (passthrough)
//            rtn = Boolean.TRUE;
//        }
//
//        return rtn;
//    }

}
