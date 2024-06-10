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

//import com.cloudera.utils.hadoop.HadoopSession;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.SourceLocationMap;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.WarehouseMapBuilder;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.util.UrlUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hms.mirror.SessionVars.EXT_DB_LOCATION_PROP;
import static com.cloudera.utils.hms.mirror.SessionVars.LEGACY_DB_LOCATION_PROP;

@Service
@Slf4j
@Getter
public class DatabaseService {

    private ConnectionPoolService connectionPoolService;
    private ExecuteSessionService executeSessionService;
    private QueryDefinitionsService queryDefinitionsService;
    private TranslatorService translatorService;
    private ConfigService configService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setQueryDefinitionsService(QueryDefinitionsService queryDefinitionsService) {
        this.queryDefinitionsService = queryDefinitionsService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }

    public Warehouse addWarehousePlan(String database, String external, String managed) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        hmsMirrorConfig.getDatabases().add(database);
        return warehouseMapBuilder.addWarehousePlan(database, external, managed);
    }

    public Warehouse removeWarehousePlan(String database) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        hmsMirrorConfig.getDatabases().remove(database);
        return warehouseMapBuilder.removeWarehousePlan(database);
    }

    public Warehouse getWarehousePlan(String database) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        return warehouseMapBuilder.getWarehousePlans().get(database);
    }

    public Map<String, Warehouse> getWarehousePlans() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        return warehouseMapBuilder.getWarehousePlans();
    }

    public void clearWarehousePlan() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        warehouseMapBuilder.clearWarehousePlan();
    }

    // Look at the Warehouse Plans and pull the database/table/partition locations the metastore.
    public WarehouseMapBuilder buildDatabaseSources(int consolidationLevelBase, boolean partitionLevelMismatch) throws RequiredConfigurationException {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();

        // Check to see if there are any warehouse plans defined.  If not, skip this process.
        if (warehouseMapBuilder.getWarehousePlans().isEmpty()) {
            log.warn("No Warehouse Plans defined.  Skipping building out the database sources.");
            throw new RequiredConfigurationException("No Warehouse Plans defined.  Skipping building out the database sources.");
        }

        // Need to have this set to ensure we're picking everything up.
        if (!hmsMirrorConfig.isEvaluatePartitionLocation()) {
            throw new RequiredConfigurationException("The 'evaluatePartitionLocation' setting must be set to 'true' to build out the database sources.");
        }

        for (String database : warehouseMapBuilder.getWarehousePlans().keySet()) {
            // Reset the database in the translation map.
            hmsMirrorConfig.getTranslator().removeDatabaseFromTranslationMap(database);
            // Load the database locations.
            loadDatabaseLocationMetadataDirect(database, Environment.LEFT, consolidationLevelBase, partitionLevelMismatch);
        }

        return warehouseMapBuilder;
    }

    public Map<String, SourceLocationMap> getDatabaseSources() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        WarehouseMapBuilder warehouseMapBuilder = hmsMirrorConfig.getTranslator().getWarehouseMapBuilder();
        return warehouseMapBuilder.getSources();
    }

    protected void loadDatabaseLocationMetadataDirect(String database, Environment environment,
                                                      int consolidationLevelBase,
                                                      boolean partitionLevelMismatch) {
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
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        String database = tableMirror.getParent().getName();
//        EnvironmentTable et = tableMirror.getEnvironmentTable(environment);
        try {
            conn = getConnectionPoolService().getMetastoreDirectEnvironmentConnection(environment);
            log.info("Loading Partitions from Metastore Direct Connection {}:{}", environment, database);
            QueryDefinitions queryDefinitions = getQueryDefinitionsService().getQueryDefinitions(environment);
            if (queryDefinitions != null) {
                String dbTableLocationQuery = queryDefinitions.getQueryDefinition("database_table_locations").getStatement();
                pstmt = conn.prepareStatement(dbTableLocationQuery);
                pstmt.setString(1, database);
                resultSet = pstmt.executeQuery();
                while (resultSet.next()) {
                    String table = resultSet.getString(1);
                    String tableType = resultSet.getString(2);
                    String location = resultSet.getString(3);
                    // Filter out some table types. Don't transfer previously moved tables or
                    // interim tables created by hms-mirror.
                    if (!(table.startsWith(hmsMirrorConfig.getTransfer().getTransferPrefix())
                            || table.endsWith("storage_migration"))) {
                        log.warn("Database: {} Table: {} was NOT added to list.  The name matches the transfer prefix " +
                                "and is most likely a remnant of a previous event. If this is a mistake, change the " +
                                "'transferPrefix' to something more unique.", database, table);
                        hmsMirrorConfig.getTranslator().addTableSource(database, table, tableType, location, consolidationLevelBase,
                                partitionLevelMismatch);
                    }
                }
                resultSet.close();
                pstmt.close();
                // Get the Partition Locations
                String dbPartitionLocationQuery = queryDefinitions.getQueryDefinition("database_partition_locations").getStatement();
                pstmt = conn.prepareStatement(dbPartitionLocationQuery);
                pstmt.setString(1, database);
                resultSet = pstmt.executeQuery();
                while (resultSet.next()) {
                    String table = resultSet.getString(1);
                    String tableType = resultSet.getString(2);
                    String partitionSpec = resultSet.getString(3);
                    String tableLocation = resultSet.getString(4);
                    String partitionLocation = resultSet.getString(5);
                    hmsMirrorConfig.getTranslator().addPartitionSource(database, table, tableType, partitionSpec,
                            tableLocation, partitionLocation, consolidationLevelBase, partitionLevelMismatch);
                }
            }


            log.info("Loaded Database Table/Partition Locations from Metastore Direct Connection {}:{}", environment, database);
        } catch (SQLException throwables) {
            log.error("Issue loading Table/Partition Locations from Metastore Direct Connection. {}:{}", environment, database);
            log.error(throwables.getMessage(), throwables);
            throw new RuntimeException(throwables);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
    }

    public boolean loadEnvironmentVars() {
        boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        List<Environment> environments = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment environment : environments) {
            if (hmsMirrorConfig.getCluster(environment) != null) {
                Connection conn = null;
                Statement stmt = null;
                // Clear current variables.
                hmsMirrorConfig.getCluster(environment).getEnvVars().clear();
                log.info("Loading {} Environment Variables", environment);
                try {
                    conn = getConnectionPoolService().getHS2EnvironmentConnection(environment);
                    //getConfig().getCluster(Environment.LEFT).getConnection();
                    if (conn != null) {
                        log.info("Retrieving {} Cluster Connection", environment);
                        stmt = conn.createStatement();
                        // Load Session Environment Variables.
                        ResultSet rs = stmt.executeQuery(MirrorConf.GET_ENV_VARS);
                        while (rs.next()) {
                            String envVarSet = rs.getString(1);
                            hmsMirrorConfig.getCluster(environment).addEnvVar(envVarSet);
                        }
                    }
                } catch (SQLException se) {
                    // Issue
                    rtn = Boolean.FALSE;
                    log.error("Issue getting database connection", se);
                    executeSessionService.getActiveSession().addError(MISC_ERROR, environment + ":Issue getting database connection");
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            log.error("Issue closing LEFT database connection", e);
                            executeSessionService.getActiveSession().addError(MISC_ERROR, environment + ":Issue closing database connection");
                        }
                    }
                }
            }
        }
        return rtn;
    }

    public List<String> listAvailableDatabases(Environment environment) {
        List<String> dbs = new ArrayList<>();
        Connection conn = null;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        try {
            conn = connectionPoolService.getHS2EnvironmentConnection(environment);
            if (conn != null) {
                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    resultSet = stmt.executeQuery(SHOW_DATABASES);
                    while (resultSet.next()) {
                        dbs.add(resultSet.getString(1));
                    }
                } catch (SQLException sql) {
                    log.error("Issue getting database list", sql);
                    throw new RuntimeException(sql);
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
            log.error("Issue getting database connection", se);
            throw new RuntimeException(se);
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException throwables) {
                //
            }
        }
        return dbs;
    }

    public boolean buildDBStatements(DBMirror dbMirror) {
//        Config config = Context.getInstance().getConfig();
        boolean rtn = Boolean.TRUE; // assume all good till we find otherwise.
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        RunStatus runStatus = executeSessionService.getActiveSession().getRunStatus();
        // Start with the LEFT definition.
        Map<String, String> dbDefLeft = dbMirror.getDBDefinition(Environment.LEFT);
        Map<String, String> dbDefRight = dbMirror.getDBDefinition(Environment.RIGHT);
        String database = null;
        String location = null;
        String managedLocation = null;
        String leftNamespace = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace();
        String rightNamespace = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace();

        if (!hmsMirrorConfig.isResetRight()) {
            // Don't buildout RIGHT side with inplace downgrade of ACID tables.
            if (!hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
                switch (hmsMirrorConfig.getDataStrategy()) {
                    case CONVERT_LINKED:
                        // ALTER the 'existing' database to ensure locations are set to the RIGHT hcfsNamespace.
                        database = configService.getResolvedDB(dbMirror.getName());
                        location = dbDefRight.get(DB_LOCATION);
                        managedLocation = dbDefRight.get(DB_MANAGED_LOCATION);

                        if (location != null && !hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                            location = location.replace(leftNamespace, rightNamespace);
                            String alterDB_location = MessageFormat.format(ALTER_DB_LOCATION, database, location);
                            dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDB_location));
                            dbDefRight.put(DB_LOCATION, location);
                        }
                        if (managedLocation != null) {
                            managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
                            if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                                String alterDBMngdLocationSql = MessageFormat.format(ALTER_DB_MNGD_LOCATION, database, managedLocation);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDBMngdLocationSql));
                            } else {
                                String alterDBMngdLocationSql = MessageFormat.format(ALTER_DB_LOCATION, database, managedLocation);
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDBMngdLocationSql));
                                dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                            }
                            dbDefRight.put(DB_MANAGED_LOCATION, managedLocation);
                        }

                        break;
                    default:
                        // Start with the LEFT definition.
                        dbDefLeft = dbMirror.getDBDefinition(Environment.LEFT);
                        database = configService.getResolvedDB(dbMirror.getName());
                        location = dbDefLeft.get(DB_LOCATION);
                        Warehouse dbWarehouse = null;
                        try {
                            dbWarehouse = translatorService.getDatabaseWarehouse(dbMirror.getName());
                        } catch (MissingDataPointException e) {
                            dbMirror.addIssue(Environment.LEFT, "TODO: Missing Warehouse details...");
                            return Boolean.FALSE;
                        }
                        if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
                                location = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace()
                                        + dbWarehouse.getExternalDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            } else {
                                location = hmsMirrorConfig.getTransfer().getCommonStorage()
                                        + dbWarehouse.getExternalDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            }

                        if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                            // Check for Managed Location.
                            managedLocation = dbDefLeft.get(DB_MANAGED_LOCATION);
                        }
                        if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                            if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
                                managedLocation = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace()
                                        + dbWarehouse.getManagedDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            } else {
                                managedLocation = hmsMirrorConfig.getTransfer().getCommonStorage()
                                        + dbWarehouse.getManagedDirectory()
                                        + "/" + dbMirror.getName() + ".db";
                            }
                            // Check is the Managed Location matches the system default.  If it does,
                            //  then we don't need to set it.
                            String envDefaultFS = hmsMirrorConfig.getCluster(Environment.RIGHT).getEnvVars().get(DEFAULT_FS);
                            String envWarehouseDir = hmsMirrorConfig.getCluster(Environment.RIGHT).getEnvVars().get(METASTOREWAREHOUSE);
                            String defaultManagedLocation = envDefaultFS + envWarehouseDir;
                            log.info("Comparing Managed Location: {} to default: {}", managedLocation, defaultManagedLocation);
                            if (managedLocation.startsWith(defaultManagedLocation)) {
                                managedLocation = null;
                                log.info("The location for the DB '{}' is the same as the default FS + warehouse dir.  The database location will NOT be set and will depend on the system default.", database);
                                dbMirror.addIssue(Environment.RIGHT, "The location for the DB '" + database
                                        + "' is the same as the default FS + warehouse dir. The database " +
                                        "location will NOT be set and will depend on the system default.");
                            }
                        }

                        switch (hmsMirrorConfig.getDataStrategy()) {
                            case HYBRID:
                            case EXPORT_IMPORT:
                            case SCHEMA_ONLY:
                            case SQL:
                            case LINKED:
                                if (location != null) {
                                    location = location.replace(leftNamespace, rightNamespace);
                                    // https://github.com/cloudera-labs/hms-mirror/issues/13
                                    // LOCATION to MANAGED LOCATION silent translation for HDP 3 migrations.
                                    if (!hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                                        String locationMinusNS = location.substring(rightNamespace.length());
                                        if (locationMinusNS.startsWith(DEFAULT_MANAGED_BASE_DIR)) {
                                            // Translate to managed.
                                            managedLocation = location;
                                            // Set to null to skip processing.
                                            location = null;
                                            dbMirror.addIssue(Environment.RIGHT, "The LEFT's DB 'LOCATION' element was defined " +
                                                    "as the default 'managed' location in later versions of Hive3.  " +
                                                    "We've adjusted the DB to set the MANAGEDLOCATION setting instead, " +
                                                    "to avoid future conflicts. If your target environment is HDP3, this setting " +
                                                    "will FAIL since the MANAGEDLOCATION property for a Database doesn't exist. " +
                                                    "Fix the source DB's location element to avoid this translation.");
                                        }
                                    }

                                }
                                if (managedLocation != null) {
                                    managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
                                }
                                if (hmsMirrorConfig.getDbPrefix() != null || hmsMirrorConfig.getDbRename() != null) {
                                    // adjust locations.
                                    if (location != null) {
                                        location = UrlUtils.removeLastDirFromUrl(location) + "/"
                                                + configService.getResolvedDB(dbMirror.getName()) + ".db";
                                    }
                                    if (managedLocation != null) {
                                        managedLocation = UrlUtils.removeLastDirFromUrl(managedLocation) + "/"
                                                + configService.getResolvedDB(dbMirror.getName()) + ".db";
                                    }
                                }
                                if (hmsMirrorConfig.isReadOnly() && !hmsMirrorConfig.isLoadingTestData()) {
                                    log.debug("Config set to 'read-only'.  Validating FS before continuing");
                                    CliEnvironment cli = executeSessionService.getCliEnvironment();

                                    // Check that location exists.
                                    String dbLocation = null;
                                    if (location != null) {
                                        dbLocation = location;
                                    } else {
                                        // Get location for DB. If it's not there than:
                                        //     SQL query to get default from Hive.
                                        String defaultDBLocProp = null;
                                        if (hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
                                            defaultDBLocProp = LEGACY_DB_LOCATION_PROP;
                                        } else {
                                            defaultDBLocProp = EXT_DB_LOCATION_PROP;
                                        }

                                        Connection conn = null;
                                        Statement stmt = null;
                                        ResultSet resultSet = null;
                                        try {
                                            conn = connectionPoolService.getHS2EnvironmentConnection(Environment.RIGHT);
                                            //hmsMirrorConfig.getCluster(Environment.RIGHT).getConnection();

                                            stmt = conn.createStatement();

                                            String dbLocSql = "SET " + defaultDBLocProp;
                                            resultSet = stmt.executeQuery(dbLocSql);
                                            if (resultSet.next()) {
                                                String propset = resultSet.getString(1);
                                                String dbLocationPrefix = propset.split("=")[1];
                                                dbLocation = dbLocationPrefix + dbMirror.getName().toLowerCase(Locale.ROOT) + ".db";
                                                log.debug("{} location is: {}", database, dbLocation);

                                            } else {
                                                // Could get property.
                                                throw new RuntimeException("Could not determine DB Location for: " + database);
                                            }
                                        } catch (SQLException throwables) {
                                            log.error("Issue", throwables);
                                            throw new RuntimeException(throwables);
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
                                            if (conn != null) {
                                                try {
                                                    conn.close();
                                                } catch (SQLException throwables) {
                                                    //
                                                }
                                            }
                                        }
                                    }
                                    if (dbLocation != null) {
                                        try {
//                                            CommandReturn cr = cli.processInput("connect");
                                            CommandReturn testCr = cli.processInput("test -d " + dbLocation);
                                            if (testCr.isError()) {
                                                // Doesn't exist.  So we can't create the DB in a "read-only" mode.
                                                runStatus.addError(RO_DB_DOESNT_EXIST, dbLocation,
                                                        testCr.getError(), testCr.getCommand(), dbMirror.getName());
                                                dbMirror.addIssue(Environment.RIGHT, runStatus.getErrorMessage(RO_DB_DOESNT_EXIST));
                                                rtn = Boolean.FALSE;
//                                                throw new RuntimeException(hmsMirrorConfig.getProgression().getErrorMessage(RO_DB_DOESNT_EXIST));
                                            }
                                        } catch (DisabledException e) {
                                            log.warn("Unable to test location {} because the CLI Interface is disabled.", dbLocation);
                                            dbMirror.addIssue(Environment.RIGHT, "Unable to test location " + dbLocation + " because the CLI Interface is disabled. " +
                                                    "This may lead to issues when creating the database in 'read-only' mode.  ");
                                        }
                                    } else {
                                        // Can't determine location.
                                        // TODO: What to do here.
                                        log.error("{}: Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in 'read-only' mode without knowing where it should go and validating existance.", dbMirror.getName());
                                        throw new RuntimeException(dbMirror.getName() + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
                                                "'read-only' mode without knowing where it should go and validating existance.");
                                    }
//                                    } finally {
//                                        config.getCliEnv().returnSession(main);
//                                    }
                                }

                                String createDbL = MessageFormat.format(CREATE_DB, database);
                                StringBuilder sbL = new StringBuilder();
                                sbL.append(createDbL).append("\n");
                                if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                                    sbL.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                                }
                                // TODO: DB Properties.
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(CREATE_DB_DESC, sbL.toString()));

                                if (location != null && !hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                                    String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, database, location);
                                    dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                                    dbDefRight.put(DB_LOCATION, location);
                                }
                                if (managedLocation != null) {
                                    if (!hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                                        String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, database, managedLocation);
                                        dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                        dbDefRight.put(DB_MANAGED_LOCATION, managedLocation);
                                    } else {
                                        String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, database, managedLocation);
                                        dbMirror.getSql(Environment.RIGHT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                        dbMirror.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
                                        dbDefRight.put(DB_LOCATION, managedLocation);
                                    }
                                }

                                break;
                            case DUMP:
                                String createDb = MessageFormat.format(CREATE_DB, database);
                                StringBuilder sb = new StringBuilder();
                                sb.append(createDb).append("\n");
                                if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                                    sb.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                                }
                                if (location != null) {
                                    sb.append(DB_LOCATION).append(" \"").append(location).append("\"\n");
                                }
                                if (managedLocation != null) {
                                    sb.append(DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
                                }
                                // TODO: DB Properties.
                                dbMirror.getSql(Environment.LEFT).add(new Pair(CREATE_DB_DESC, sb.toString()));
                                break;
                            case COMMON:
                                String createDbCom = MessageFormat.format(CREATE_DB, database);
                                StringBuilder sbCom = new StringBuilder();
                                sbCom.append(createDbCom).append("\n");
                                if (dbDefLeft.get(COMMENT) != null && !dbDefLeft.get(COMMENT).trim().isEmpty()) {
                                    sbCom.append(COMMENT).append(" \"").append(dbDefLeft.get(COMMENT)).append("\"\n");
                                }
                                if (location != null) {
                                    sbCom.append(DB_LOCATION).append(" \"").append(location).append("\"\n");
                                }
                                if (managedLocation != null) {
                                    sbCom.append(DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
                                }
                                // TODO: DB Properties.
                                dbMirror.getSql(Environment.RIGHT).add(new Pair(CREATE_DB_DESC, sbCom.toString()));
                                break;
                            case STORAGE_MIGRATION:
//                                StringBuilder sbLoc = new StringBuilder();
//                                sbLoc.append(hmsMirrorConfig.getTransfer().getCommonStorage());
//                                sbLoc.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory());
//                                sbLoc.append("/");
//                                sbLoc.append(database);
//                                sbLoc.append(".db");
                                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                                    String alterDbLoc = MessageFormat.format(ALTER_DB_LOCATION, database, location);
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbLoc));
                                    dbDefRight.put(DB_LOCATION, location);
                                }

//                                StringBuilder sbMngdLoc = new StringBuilder();
//                                sbMngdLoc.append(hmsMirrorConfig.getTransfer().getCommonStorage());
//                                sbMngdLoc.append(hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory());
//                                sbMngdLoc.append("/");
//                                sbMngdLoc.append(database);
//                                sbMngdLoc.append(".db");
                                if (!hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3()) {
                                    String alterDbMngdLoc = MessageFormat.format(ALTER_DB_MNGD_LOCATION, database, managedLocation);
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
                                    dbDefRight.put(DB_MANAGED_LOCATION, managedLocation);
                                } else {
                                    String alterDbMngdLoc = MessageFormat.format(ALTER_DB_LOCATION, database, managedLocation);
                                    dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
                                    dbMirror.addIssue(Environment.LEFT, HDPHIVE3_DB_LOCATION.getDesc());
                                    dbDefRight.put(DB_LOCATION, managedLocation);
                                }

                                dbMirror.addIssue(Environment.LEFT, "This process, when 'executed' will leave the original tables intact in their renamed " +
                                        "version.  They are NOT automatically cleaned up.  Run the produced '" +
                                        dbMirror.getName() + "_LEFT_CleanUp_execute.sql' " +
                                        "file to permanently remove them.  Managed and External/Purge table data will be " +
                                        "removed when dropping these tables.  External non-purge table data will remain in storage.");

                                break;
                        }
                }
            } else {
                // Downgrade in place.
                if (hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null) {
                    // Set the location to the external directory for the database.
                    database = configService.getResolvedDB(dbMirror.getName());
                    location = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() + hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() +
                            "/" + database + ".db";
                    String alterDB_location = MessageFormat.format(ALTER_DB_LOCATION, database, location);
                    dbMirror.getSql(Environment.LEFT).add(new Pair(ALTER_DB_LOCATION_DESC, alterDB_location));
                    dbDefLeft.put(DB_LOCATION, location);
                }
            }
        } else {
            // Reset Right DB.
            database = configService.getResolvedDB(dbMirror.getName());
            String dropDb = MessageFormat.format(DROP_DB, database);
            dbMirror.getSql(Environment.RIGHT).add(new Pair(DROP_DB_DESC, dropDb));
        }
        return rtn;
    }

    public boolean createDatabases() {
        boolean rtn = true;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
        Conversion conversion = executeSessionService.getActiveSession().getConversion();
        for (String database : hmsMirrorConfig.getDatabases()) {
            DBMirror dbMirror = conversion.getDatabase(database);
            rtn = buildDBStatements(dbMirror);

            if (rtn) {
                if (!runDatabaseSql(dbMirror, Environment.LEFT)) {
                    rtn = false;
                }
                if (!runDatabaseSql(dbMirror, Environment.RIGHT)) {
                    rtn = false;
                }
            }
        }
        return rtn;
    }

    public Boolean getDatabase(DBMirror dbMirror, Environment environment) throws SQLException {
        Boolean rtn = Boolean.FALSE;
        Connection conn = null;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        try {
            conn = connectionPoolService.getHS2EnvironmentConnection(environment);//getConnection();
            if (conn != null) {

                String database = (environment == Environment.LEFT ? dbMirror.getName() : configService.getResolvedDB(dbMirror.getName()));

                log.debug("{}:{}: Loading database definition.", environment, database);

                Statement stmt = null;
                ResultSet resultSet = null;
                try {
                    stmt = conn.createStatement();
                    log.debug("{}:{}: Getting Database Definition", environment, database);
                    resultSet = stmt.executeQuery(MessageFormat.format(DESCRIBE_DB, database));
                    //Retrieving the ResultSetMetaData object
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    //getting the column type
                    int column_count = rsmd.getColumnCount();
                    Map<String, String> dbDef = new TreeMap<>();
                    while (resultSet.next()) {
                        for (int i = 0; i < column_count; i++) {
                            String cName = rsmd.getColumnName(i + 1).toUpperCase(Locale.ROOT);
                            String cValue = resultSet.getString(i + 1);
                            // Don't add element if its empty.
                            if (cValue != null && !cValue.trim().isEmpty()) {
                                dbDef.put(cName, cValue);
                            }
                        }
                    }
                    dbMirror.setDBDefinition(environment, dbDef);
                    rtn = Boolean.TRUE;
                } catch (SQLException sql) {
                    // DB Doesn't Exists.
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
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, Environment environment) {
        List<Pair> dbPairs = dbMirror.getSql(environment);
        Boolean rtn = Boolean.TRUE;
        for (Pair pair : dbPairs) {
            if (!runDatabaseSql(dbMirror, pair, environment)) {
                rtn = Boolean.FALSE;
                // don't continue
                break;
            }
        }
        return rtn;
    }

    public Boolean runDatabaseSql(DBMirror dbMirror, Pair dbSqlPair, Environment environment) {
        // Open the connection and ensure we are running this on the "RIGHT" cluster.
        Connection conn = null;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        Boolean rtn = Boolean.TRUE;
        // Skip when running test data.
        if (!hmsMirrorConfig.isLoadingTestData()) {
            try {
                conn = connectionPoolService.getHS2EnvironmentConnection(environment);

                if (conn == null && hmsMirrorConfig.isExecute()
                        && !hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    // this is a problem.
                    rtn = Boolean.FALSE;
                    dbMirror.addIssue(environment, "Connection missing. This is a bug.");
                }

                if (conn == null && hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected()) {
                    dbMirror.addIssue(environment, "Running in 'disconnected' mode.  NO RIGHT operations will be done.  " +
                            "The scripts will need to be run 'manually'.");
                }

                if (conn != null) {
                    if (dbMirror != null)
                        log.debug("{} - {}: {}", environment, dbSqlPair.getDescription(), dbMirror.getName());
                    else
                        log.debug("{} - {}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());

                    Statement stmt = null;
                    try {
                        try {
                            stmt = conn.createStatement();
                        } catch (SQLException throwables) {
                            log.error("Issue building statement", throwables);
                            rtn = Boolean.FALSE;
                        }

                        try {
                            log.debug("{}:{}:{}", environment, dbSqlPair.getDescription(), dbSqlPair.getAction());
                            if (hmsMirrorConfig.isExecute()) // on dry-run, without db, hard to get through the rest of the steps.
                                stmt.execute(dbSqlPair.getAction());
                        } catch (SQLException throwables) {
                            log.error("{}:{}:", environment, dbSqlPair.getDescription(), throwables);
                            dbMirror.addIssue(environment, throwables.getMessage() + " " + dbSqlPair.getDescription() +
                                    " " + dbSqlPair.getAction());
                            rtn = Boolean.FALSE;
                        }

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
                log.error(environment.toString(), throwables);
                throw new RuntimeException(throwables);
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
