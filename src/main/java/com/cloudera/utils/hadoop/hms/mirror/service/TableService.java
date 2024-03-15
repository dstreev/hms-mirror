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

package com.cloudera.utils.hadoop.hms.mirror.service;

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.TABLE_EMPTY;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.HMS_STORAGE_MIGRATION_FLAG;
import static com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum.DUMP;
import static com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;

@Service
@Slf4j
public class TableService {

    @Getter
    private ConfigService configService;
    @Getter
    private ConnectionPoolService connectionPoolService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }


    public void getTableDefinition(String database, TableMirror tableMirror, Environment environment) throws SQLException {
        // The connection should already be in the database;
        Config config = getConfigService().getConfig();

        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        // Fetch Table Definition.
        if (config.isLoadingTestData()) {
            // Already loaded from before.
        } else {
            loadSchemaFromCatalog(tableMirror, environment);
        }

        checkTableFilter(tableMirror, environment);

        if (!tableMirror.isRemove() && !config.isLoadingTestData()) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case CONVERT_LINKED:
                case DUMP:
                case LINKED:
                    // These scenario don't require stats.
                    break;
                case SQL:
                case HYBRID:
                case EXPORT_IMPORT:
                case STORAGE_MIGRATION:
                case COMMON:
                case ACID:
                    if (!TableUtils.isView(et) && TableUtils.isHiveNative(et) ) {
                        loadTableStats(et);
                    }
                    break;
            }
        }

        Boolean partitioned = TableUtils.isPartitioned(et);
        if (partitioned && !tableMirror.isRemove() && !config.isLoadingTestData()) {
            /*
            If we are -epl, we need to load the partition metadata for the table. And we need to use the
            metastore_direct connection to do so. Trying to load this through the standard Hive SQL process
            is 'extremely' slow.
             */
            if (config.isEvaluatePartitionLocation() ||
                    (config.getDataStrategy() == STORAGE_MIGRATION && config.getTransfer().getStorageMigration().isDistcp())) {
                loadTablePartitionMetadataDirect(database, et);
            } else {
                loadTablePartitionMetadata(database, et);
            }

        }

        // Check for table partition count filter
        if (config.getFilter().getTblPartitionLimit() != null && config.getFilter().getTblPartitionLimit() > 0) {
            Integer partLimit = config.getFilter().getTblPartitionLimit();
            if (et.getPartitions().size() > partLimit) {
                tableMirror.setRemove(Boolean.TRUE);
                tableMirror.setRemoveReason("The table partition count exceeds the specified table filter partition limit: " +
                        config.getFilter().getTblPartitionLimit() + " < " + et.getPartitions().size());

            }
        }

        log.debug(environment + ":" + database + "." + et.getName() +
                ": Loaded Table Definition");
    }

    public void loadSchemaFromCatalog(TableMirror tableMirror, Environment environment) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        String database = tableMirror.getParent().getName();
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        Config config = getConfigService().getConfig();

        try {

            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);

            if (conn != null) {
                stmt = conn.createStatement();
                log.info(environment + ":" + database + "." + tableMirror.getName() +
                        ": Loading Table Definition");
                String useStatement = MessageFormat.format(MirrorConf.USE, database);
                stmt.execute(useStatement);
                String showStatement = MessageFormat.format(MirrorConf.SHOW_CREATE_TABLE, tableMirror.getName());
                resultSet = stmt.executeQuery(showStatement);
                List<String> tblDef = new ArrayList<String>();
                ResultSetMetaData meta = resultSet.getMetaData();
                if (meta.getColumnCount() >= 1) {
                    while (resultSet.next()) {
                        try {
                            tblDef.add(resultSet.getString(1).trim());
                        } catch (NullPointerException npe) {
                            // catch and continue.
                            log.error(environment + ":" + database + "." + tableMirror.getName() +
                                    ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. " +
                                    "ResultSet record(line) is null. Skipping.");
                        }
                    }
                } else {
                    log.error(environment + ":" + database + "." + tableMirror.getName() +
                            ": Loading Table Definition.  Issue with SHOW CREATE TABLE resultset. No Metadata.");
                }
                et.setDefinition(tblDef);
                et.setName(tableMirror.getName());
                // Identify that the table existed in the Database before other activity.
                et.setExists(Boolean.TRUE);
                tableMirror.addStep(environment.toString(), "Fetched Schema");

                // TODO: Don't do this is table removed from list.
                if (config.isTransferOwnership()) {
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
                                    log.error("Couldn't parse 'owner' value from: " + resultSet.getString(1) +
                                            " for table: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
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

            }
        } catch (SQLException throwables) {
            if (throwables.getMessage().contains("Table not found") || throwables.getMessage().contains("Database does not exist")) {
                // This is ok in the upper cluster where we don't expect the table to exist if the process hadn't run before.
                tableMirror.addStep(environment.toString(), "No Schema");
            } else {
                throwables.printStackTrace();
                et.addIssue(throwables.getMessage());
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

    protected void checkTableFilter(TableMirror tableMirror, Environment environment) {
        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        Config config = getConfigService().getConfig();

        if (environment == Environment.LEFT) {
            if (config.getMigrateVIEW().isOn() && config.getDataStrategy() != DUMP) {
                if (!TableUtils.isView(et)) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("VIEW's only processing selected.");
                }
            } else {
                // Check if ACID for only the LEFT cluster.  If it's the RIGHT cluster, other steps will deal with
                // the conflict.  IE: Rename or exists already.
                if (TableUtils.isManaged(et)
                        && environment == Environment.LEFT) {
                    if (TableUtils.isACID(et)) {
                        // For ACID tables, check that Migrate is ON.
                        if (config.getMigrateACID().isOn()) {
                            tableMirror.addStep("TRANSACTIONAL", Boolean.TRUE);
                        } else {
                            tableMirror.setRemove(Boolean.TRUE);
                            tableMirror.setRemoveReason("ACID table and ACID processing not selected (-ma|-mao).");
                        }
                    } else if (config.getMigrateACID().isOnly()) {
                        // Non ACID Tables should NOT be process if 'isOnly' is set.
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (TableUtils.isHiveNative(et)) {
                    // Non ACID Tables should NOT be process if 'isOnly' is set.
                    if (config.getMigrateACID().isOnly()) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("Non-ACID table and ACID only processing selected `-mao`");
                    }
                } else if (TableUtils.isView(et)) {
                    if (config.getDataStrategy() != DUMP) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("This is a VIEW and VIEW processing wasn't selected.");
                    }
                } else {
                    // Non-Native Tables.
                    if (!config.isMigratedNonNative()) {
                        tableMirror.setRemove(Boolean.TRUE);
                        tableMirror.setRemoveReason("This is a Non-Native hive table and non-native process wasn't " +
                                "selected.");
                    }
                }
            }
        }

        // Check for tables migration flag, to avoid 're-migration'.
        String smFlag = TableUtils.getTblProperty(HMS_STORAGE_MIGRATION_FLAG, et);
        if (smFlag != null) {
            tableMirror.setRemove(Boolean.TRUE);
            tableMirror.setRemoveReason("The table has already gone through the STORAGE_MIGRATION process on " +
                    smFlag + " If this isn't correct, remove the TBLPROPERTY '" + HMS_STORAGE_MIGRATION_FLAG + "' " +
                    "from the table and try again.");
        }

        // Check for table size filter
        if (config.getFilter().getTblSizeLimit() != null && config.getFilter().getTblSizeLimit() > 0) {
            Long dataSize = (Long) et.getStatistics().get(DATA_SIZE);
            if (dataSize != null) {
                if (config.getFilter().getTblSizeLimit() * (1024 * 1024) < dataSize) {
                    tableMirror.setRemove(Boolean.TRUE);
                    tableMirror.setRemoveReason("The table dataset size exceeds the specified table filter size limit: " +
                            config.getFilter().getTblSizeLimit() + "Mb < " + dataSize);
                }
            }
        }

    }

    public void getTables(DBMirror dbMirror, Environment environment) throws SQLException {
        Connection conn = null;
        Config config = getConfigService().getConfig();

        try {
            conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
            if (conn != null) {
                String database = (environment == Environment.LEFT ?
                        dbMirror.getName() : getConfigService().getResolvedDB(dbMirror.getName()));

                log.info(environment + ":" + database + ": Loading tables for database");

                Statement stmt = null;
                ResultSet resultSet = null;
                // Stub out the tables
                try {
                    stmt = conn.createStatement();
                    log.debug(environment + ":" + database + ": Setting Hive DB Session Context");
                    stmt.execute(MessageFormat.format(MirrorConf.USE, database));
                    log.info(environment + ":" + database + ": Getting Table List");
                    List<String> shows = new ArrayList<String>();
                    if (!config.getCluster(environment).isLegacyHive()) {
                        if (config.getMigrateVIEW().isOn()) {
                            shows.add(MirrorConf.SHOW_VIEWS);
                            if (config.getDataStrategy() == DUMP) {
                                shows.add(MirrorConf.SHOW_TABLES);
                            }
                        } else {
                            shows.add(MirrorConf.SHOW_TABLES);
                        }
                    } else {
                        shows.add(MirrorConf.SHOW_TABLES);
                    }
                    for (String show : shows) {
                        resultSet = stmt.executeQuery(show);
                        while (resultSet.next()) {
                            String tableName = resultSet.getString(1);
                            if (tableName.startsWith(config.getTransfer().getTransferPrefix())) {
                                log.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name matches the transfer prefix and is most likely a remnant of a previous " +
                                        "event. If this is a mistake, change the 'transferPrefix' to something more unique.");
                            } else if (tableName.endsWith("storage_migration")) {
                                log.info("Database: " + database + " Table: " + tableName + " was NOT added to list.  " +
                                        "The name is the result of a previous STORAGE_MIGRATION attempt that has not been " +
                                        "cleaned up.");
                            } else {
                                if (config.getFilter().getTblRegEx() == null && config.getFilter().getTblExcludeRegEx() == null) {
                                    TableMirror tableMirror = dbMirror.addTable(tableName);
                                    tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                } else if (config.getFilter().getTblRegEx() != null) {
                                    // Filter Tables
                                    assert (config.getFilter().getTblFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblFilterPattern().matcher(tableName);
                                    if (matcher.matches()) {
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        log.info(database + ":" + tableName + " didn't match table regex filter and " +
                                                "will NOT be added to processing list.");
                                    }
                                } else if (config.getFilter().getTblExcludeRegEx() != null) {
                                    assert (config.getFilter().getTblExcludeFilterPattern() != null);
                                    Matcher matcher = config.getFilter().getTblExcludeFilterPattern().matcher(tableName);
                                    if (!matcher.matches()) { // ANTI-MATCH
                                        TableMirror tableMirror = dbMirror.addTable(tableName);
                                        tableMirror.setMigrationStageMessage("Added to evaluation inventory");
                                    } else {
                                        log.info(database + ":" + tableName + " matched exclude table regex filter and " +
                                                "will NOT be added to processing list.");
                                    }
                                }
                            }
                        }

                    }
                } catch (SQLException se) {
                    log.error(environment + ":" + database + " ", se);
                    // This is helpful if the user running the process doesn't have permissions.
                    dbMirror.addIssue(environment, database + " " + se.getMessage());
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
                }
            }
        } catch (SQLException se) {
            throw se;
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTableOwnership(String database, TableMirror tableMirror) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        EnvironmentTable et = tableMirror.getEnvironmentTable(Environment.LEFT);
        if (getConfig().getTransferOwnership()) {
            try {
                conn = getConnection();
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
                                    log.error("Couldn't parse 'owner' value from: " + resultSet.getString(1) +
                                            " for table: " + tableMirror.getParent().getName() + "." + tableMirror.getName());
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
                    tableMirror.addStep(this.getEnvironment().toString(), "No Schema");
                } else {
                    throwables.printStackTrace();
                    et.addIssue(throwables.getMessage());
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

    protected void loadTablePartitionMetadata(String database, EnvironmentTable envTable) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            if (conn != null) {

                stmt = conn.createStatement();
                log.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                        ": Loading Partitions");

                resultSet = stmt.executeQuery(MessageFormat.format(MirrorConf.SHOW_PARTITIONS, database, envTable.getName()));
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), NOT_SET);
                }
                envTable.setPartitions(partDef);

            }
        } catch (SQLException throwables) {
            envTable.addIssue(throwables.getMessage());
            log.error(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Issue loading Partitions.", throwables);
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

    protected void loadTablePartitionMetadataDirect(String database, EnvironmentTable envTable) {
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
        try {
            conn = getMetastoreDirectConnection();
            log.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Loading Partitions from Metastore Direct Connection.");
            QueryDefinitions queryDefinitions = config.getQueryDefinitions(this.environment);
            if (queryDefinitions != null) {
                String partLocationQuery = queryDefinitions.getQueryDefinition("part_locations").getStatement();
                pstmt = conn.prepareStatement(partLocationQuery);
                pstmt.setString(1, database);
                pstmt.setString(2, envTable.getName());
                resultSet = pstmt.executeQuery();
                Map<String, String> partDef = new HashMap<String, String>();
                while (resultSet.next()) {
                    partDef.put(resultSet.getString(1), resultSet.getString(2));
                }
                envTable.setPartitions(partDef);
            }
            log.debug(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Loaded Partitions from Metastore Direct Connection.");
        } catch (SQLException throwables) {
            envTable.addIssue(throwables.getMessage());
            log.error(getEnvironment() + ":" + database + "." + envTable.getName() +
                    ": Issue loading Partitions from Metastore Direct Connection.", throwables);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    protected void loadTableStats(EnvironmentTable envTable) throws SQLException {
        // Considered only gathering stats for partitioned tables, but decided to gather for all tables to support
        //  smallfiles across the board.
        if (getConfig().getOptimization().getSkipStatsCollection()) {
            log.debug(getEnvironment() + ":" + envTable.getName() + ": Skipping Stats Collection.");
            return;
        }
        switch (getConfig().getDataStrategy()) {
            case DUMP:
            case SCHEMA_ONLY:
                // We don't need stats for these.
                return;
            case STORAGE_MIGRATION:
                if (getConfig().getTransfer().getStorageMigration().isDistcp()) {
                    // We don't need stats for this.
                    return;
                }
                break;
            default:
                break;
        }

        // Determine File sizes in table or partitions.
        /*
        - Get Base location for table
        - Get HadoopSession
        - Do a 'count' of the location.
         */
        String location = TableUtils.getLocation(envTable.getName(), envTable.getDefinition());
        // Only run checks against hdfs and ozone namespaces.
        String[] locationParts = location.split(":");
        String protocol = locationParts[0];
        if (getConfig().getSupportFileSystems().contains(protocol)) {
            HadoopSession cli = null;
            try {
                cli = getConfig().getCliPool().borrow();
                String countCmd = "count " + location;
                CommandReturn cr = cli.processInput(countCmd);
                if (!cr.isError() && cr.getRecords().size() == 1) {
                    // We should only get back one record.
                    List<Object> countRecord = cr.getRecords().get(0);
                    // 0 = Folder Count
                    // 1 = File Count
                    // 2 = Size Summary
                    try {
                        Double avgFileSize = (double) (Long.valueOf(countRecord.get(2).toString()) /
                                Integer.valueOf(countRecord.get(1).toString()));
                        envTable.getStatistics().put(DIR_COUNT, Integer.valueOf(countRecord.get(0).toString()));
                        envTable.getStatistics().put(FILE_COUNT, Integer.valueOf(countRecord.get(1).toString()));
                        envTable.getStatistics().put(DATA_SIZE, Long.valueOf(countRecord.get(2).toString()));
                        envTable.getStatistics().put(AVG_FILE_SIZE, avgFileSize);
                        envTable.getStatistics().put(TABLE_EMPTY, Boolean.FALSE);
                    } catch (ArithmeticException ae) {
                        // Directory is probably empty.
                        envTable.getStatistics().put(TABLE_EMPTY, Boolean.TRUE);
                    }
                } else {
                    // Issue getting count.

                }
            } finally {
                if (cli != null) {
                    getConfig().getCliPool().returnSession(cli);
                }
            }
        }
        // Determine Table File Format
        TableUtils.getSerdeType(envTable);
    }

    public Boolean runTableSql(TableMirror tblMirror) {
        return runTableSql(tblMirror, getEnvironment());
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
        Connection conn = null;
        Boolean rtn = Boolean.TRUE;
        // Skip this if using test data.
        if (!getConfigService().getConfig().isLoadingTestData()) {

            try {
                // conn will be null if config.execute != true.
                conn = getConnection();

                if (conn == null && getConfig().isExecute() && !this.getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    tblMirror.addIssue(getEnvironment(), "Connection missing. This is a bug.");
                }

                if (conn == null && this.getHiveServer2().isDisconnected()) {
                    tblMirror.addIssue(getEnvironment(), "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
                    Statement stmt = null;
                    try {
                        stmt = conn.createStatement();
                        for (Pair pair : sqlList) {
                            log.debug(getEnvironment() + ":SQL:" + pair.getDescription() + ":" + pair.getAction());
                            tblMirror.setMigrationStageMessage("Executing SQL: " + pair.getDescription());
                            if (getConfig().isExecute()) {
                                stmt.execute(pair.getAction());
                                tblMirror.addStep(getEnvironment().toString(), "Sql Run Complete for: " + pair.getDescription());
                            } else {
                                tblMirror.addStep(getEnvironment().toString(), "Sql Run SKIPPED (DRY-RUN) for: " + pair.getDescription());
                            }
                        }
                    } catch (SQLException throwables) {
                        log.error(getEnvironment().toString() + ":" + throwables.getMessage(), throwables);
                        String message = throwables.getMessage();
                        if (throwables.getMessage().contains("HiveAccessControlException Permission denied")) {
                            message = message + " See [Hive SQL Exception / HDFS Permissions Issues](https://github.com/cloudera-labs/hms-mirror#hive-sql-exception--hdfs-permissions-issues)";
                        }
                        if (throwables.getMessage().contains("AvroSerdeException")) {
                            message = message + ". It's possible the `avro.schema.url` referenced file doesn't exist at the target. " +
                                    "Use the `-asm` option and hms-mirror will attempt to copy it to the new cluster.";
                        }
                        tblMirror.getEnvironmentTable(environment).addIssue(message);
                        rtn = Boolean.FALSE;
                    } finally {
                        if (stmt != null) {
                            try {
                                stmt.close();
                            } catch (SQLException sqlException) {
                                // ignore
                            }
                        }
                    }
                }
            } catch (SQLException throwables) {
                tblMirror.getEnvironmentTable(environment).addIssue("Connecting: " + throwables.getMessage());
                log.error(getEnvironment().toString() + ":" + throwables.getMessage(), throwables);
                rtn = Boolean.FALSE;
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException throwables) {
                    //
                }
            }
        }
        return rtn;
    }

}
