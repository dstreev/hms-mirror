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

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.HDPHIVE3_DB_LOCATION;
import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.RO_DB_DOESNT_EXIST;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.EXT_DB_LOCATION_PROP;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.LEGACY_DB_LOCATION_PROP;

@Getter
@Setter
@Slf4j
public class DBMirror {
//    private static final Logger log = LoggerFactory.getLogger(DBMirror.class);

    private String name;
    @JsonIgnore
    private String resolvedName;
//    @JsonIgnore
//    private Config config;

    private Map<Environment, Map<String, String>> dbDefinitions = new TreeMap<Environment, Map<String, String>>();
    @JsonIgnore
    private final Map<Environment, List<String>> issues = new TreeMap<Environment, List<String>>();

    /*
    table - reason
     */
    private final Map<String, String> filteredOut = new TreeMap<String, String>();

    private Map<String, TableMirror> tableMirrors = null;

    @JsonIgnore
    private final Map<Environment, List<Pair>> sql = new TreeMap<Environment, List<Pair>>();

//    public DBMirror(Config config) {
//        this.config = config;
//    }

    /*
    Load a DBMirror instance from a yaml file using the Jackson YAML parser.
     */
    public static DBMirror load(String fileName) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        File dbMirrorFile = new File(fileName);
        DBMirror dbMirror = null;
        String yamlDBMirrorFile = null;
        try {
            yamlDBMirrorFile = IOUtils.toString(dbMirrorFile.toURI(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            dbMirror = mapper.readerFor(DBMirror.class).readValue(yamlDBMirrorFile);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return dbMirror;
    }

//    public void setName(String name) {
//        this.name = name;
//    }

//    public String getName() {
//        return name;
//    }

//    public String getResolvedName() {
//        if (resolvedName == null) {
//            resolvedName = config.getResolvedDB(getName());
//        }
//        return resolvedName;
//    }

    public void setTableMirrors(Map<String, TableMirror> tableMirrors) {
        this.tableMirrors = tableMirrors;
        for (TableMirror tableMirror: tableMirrors.values()) {
            tableMirror.setParent(this);
        }
    }

    @JsonIgnore
    public boolean isThereAnIssue() {
        return issues.size() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public Map<PhaseState, Integer> getPhaseSummary() {
        Map<PhaseState, Integer> rtn = new HashMap<PhaseState, Integer>();
        for (String tableName: getTableMirrors().keySet()) {
            TableMirror tableMirror = getTableMirrors().get(tableName);
            Integer count = rtn.get(tableMirror.getPhaseState());
            if (count != null)
                rtn.put(tableMirror.getPhaseState(), count+1);
            else
                rtn.put(tableMirror.getPhaseState(), 1);
        }
        return rtn;
    }

    @JsonIgnore
    public String getPhaseSummaryString() {
        StringBuilder sb = new StringBuilder();
        Map<PhaseState, Integer> psMap = getPhaseSummary();
        for (PhaseState ps: psMap.keySet()) {
            sb.append(ps).append("(").append(psMap.get(ps)).append(") ");
        }
        return sb.toString();
    }

    public void addIssue(Environment environment, String issue) {
        String scrubbedIssue = issue.replace("\n", "<br/>");
        List<String> issuesList = issues.get(environment);
        if (issuesList == null) {
            issuesList = new ArrayList<String>();
            issues.put(environment, issuesList);
        }
        issuesList.add(scrubbedIssue);
//        getIssues().add(scrubbedIssue);
    }

    public List<String> getIssuesList(Environment environment) {
        return issues.get(environment);
    }

    public Map<String, String> getFilteredOut() {
        return filteredOut;
    }

    public Map<Environment, Map<String, String>> getDBDefinitions() {
        return dbDefinitions;
    }

    public Map<String, String> getDBDefinition(Environment environment) {
        Map<String, String> rtn = dbDefinitions.get(environment);
        if (rtn == null) {
            rtn = new TreeMap<>();
            dbDefinitions.put(environment,rtn);
        }
        return rtn;
    }

    public void setDBDefinition(Environment enviroment, Map<String, String> dbDefinition) {
        dbDefinitions.put(enviroment, dbDefinition);
    }

    public void setDBDefinitions(Map<Environment, Map<String, String>> dbDefinitions) {
        this.dbDefinitions = dbDefinitions;
    }

    public List<Pair> getSql(Environment environment) {
        List<Pair> sqlList = null;
        if (sql.get(environment) == null) {
            sqlList = new ArrayList<Pair>();
            sql.put(environment, sqlList);
        } else {
            sqlList = sql.get(environment);
        }
        return sqlList;
    }

    /*
    Return String[3] for Hive.  0-Create Sql, 1-Location, 2-Mngd Location.
     */
//    public void buildDBStatements() {
////        Config config = Context.getInstance().getConfig();
//        // Start with the LEFT definition.
//        Map<String, String> dbDefLeft = getDBDefinition(Environment.LEFT);//getDBDefinition(Environment.LEFT);
//        Map<String, String> dbDefRight = getDBDefinition(Environment.RIGHT);//getDBDefinition(Environment.LEFT);
//        String database = null; //config.getResolvedDB(getName());
//        String location = null; //dbDef.get(MirrorConf.DB_LOCATION);
//        String managedLocation = null;
//        String leftNamespace = config.getCluster(Environment.LEFT).getHcfsNamespace();
//        String rightNamespace = config.getCluster(Environment.RIGHT).getHcfsNamespace();
//
//        if (!config.getResetRight()) {
//            // Don't buildout RIGHT side with inplace downgrade of ACID tables.
//            if (!config.getMigrateACID().isDowngradeInPlace()) {
//                switch (config.getDataStrategy()) {
//                    case CONVERT_LINKED:
//                        // ALTER the 'existing' database to ensure locations are set to the RIGHT hcfsNamespace.
////                        dbDefRight = getDBDefinition(Environment.RIGHT);
//                        database = config.getResolvedDB(getName());
//                        location = dbDefRight.get(DB_LOCATION);
//                        managedLocation = dbDefRight.get(MirrorConf.DB_MANAGED_LOCATION);
//
//                        if (location != null && !config.getCluster(Environment.RIGHT).isHdpHive3()) {
//                            location = location.replace(leftNamespace, rightNamespace);
//                            String alterDB_location = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, location);
//                            this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDB_location));
//                            dbDefRight.put(DB_LOCATION, location);
//                        }
//                        if (managedLocation != null) {
//                            managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
//                            if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
//                                String alterDBMngdLocationSql = MessageFormat.format(MirrorConf.ALTER_DB_MNGD_LOCATION, database, managedLocation);
//                                this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_MNGD_LOCATION_DESC, alterDBMngdLocationSql));
//                            } else {
//                                String alterDBMngdLocationSql = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, managedLocation);
//                                this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDBMngdLocationSql));
//                                this.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
//                            }
//                            dbDefRight.put(MirrorConf.DB_MANAGED_LOCATION, managedLocation);
//                        }
//
//                        break;
//                    default:
//                        // Start with the LEFT definition.
//                        dbDefLeft = getDBDefinition(Environment.LEFT);
//                        database = config.getResolvedDB(getName());
//                        location = dbDefLeft.get(DB_LOCATION);
//                        if (config.getTransfer().getWarehouse() != null
//                                && config.getTransfer().getWarehouse().getExternalDirectory() != null) {
//                            if (config.getTransfer().getCommonStorage() == null) {
//                                location = config.getCluster(Environment.RIGHT).getHcfsNamespace()
//                                        + config.getTransfer().getWarehouse().getExternalDirectory()
//                                        + "/" + this.getName() + ".db";
//                            } else {
//                                location = config.getTransfer().getCommonStorage()
//                                        + config.getTransfer().getWarehouse().getExternalDirectory()
//                                        + "/" + this.getName() + ".db";
//                            }
//                        }
//
//                        if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
//                            // Check for Managed Location.
//                            managedLocation = dbDefLeft.get(MirrorConf.DB_MANAGED_LOCATION);
//                        }
//                        if (config.getTransfer().getWarehouse() != null
//                                && config.getTransfer().getWarehouse().getManagedDirectory() != null
//                                && !config.getCluster(Environment.RIGHT).getLegacyHive()) {
//                            if (config.getTransfer().getCommonStorage() == null) {
//                                managedLocation = config.getCluster(Environment.RIGHT).getHcfsNamespace()
//                                        + config.getTransfer().getWarehouse().getManagedDirectory()
//                                        + "/" + this.getName() + ".db";
//                            } else {
//                                managedLocation = config.getTransfer().getCommonStorage()
//                                        + config.getTransfer().getWarehouse().getManagedDirectory()
//                                        + "/" + this.getName() + ".db";
//                            }
//                            // Check is the Managed Location matches the system default.  If it does,
//                            //  then we don't need to set it.
//                            String envDefaultFS = config.getCluster(Environment.RIGHT).getEnvVars().get(DEFAULT_FS);
//                            String envWarehouseDir = config.getCluster(Environment.RIGHT).getEnvVars().get(METASTOREWAREHOUSE);
//                            String defaultManagedLocation = envDefaultFS + envWarehouseDir;
//                            log.info("Comparing Managed Location: " + managedLocation + " to default: " + defaultManagedLocation);
//                            if (managedLocation.startsWith(defaultManagedLocation)) {
//                                managedLocation = null;
//                                log.info("The location for the DB '" + database + "' is the same as the default FS + warehouse dir.  " +
//                                        "The database location will NOT be set and will depend on the system default.");
//                                this.addIssue(Environment.RIGHT, "The location for the DB '" + database
//                                        + "' is the same as the default FS + warehouse dir. The database " +
//                                        "location will NOT be set and will depend on the system default.");
//                            }
//                        }
//
//                        switch (config.getDataStrategy()) {
//                            case HYBRID:
//                            case EXPORT_IMPORT:
//                            case SCHEMA_ONLY:
//                            case SQL:
//                            case LINKED:
//                                if (location != null) {
//                                    location = location.replace(leftNamespace, rightNamespace);
//                                    // https://github.com/cloudera-labs/hms-mirror/issues/13
//                                    // LOCATION to MANAGED LOCATION silent translation for HDP 3 migrations.
//                                    if (!config.getCluster(Environment.LEFT).getLegacyHive()) {
//                                        String locationMinusNS = location.substring(rightNamespace.length());
//                                        if (locationMinusNS.startsWith(MirrorConf.DEFAULT_MANAGED_BASE_DIR)) {
//                                            // Translate to managed.
//                                            managedLocation = location;
//                                            // Set to null to skip processing.
//                                            location = null;
//                                            this.addIssue(Environment.RIGHT, "The LEFT's DB 'LOCATION' element was defined " +
//                                                    "as the default 'managed' location in later versions of Hive3.  " +
//                                                    "We've adjusted the DB to set the MANAGEDLOCATION setting instead, " +
//                                                    "to avoid future conflicts. If your target environment is HDP3, this setting " +
//                                                    "will FAIL since the MANAGEDLOCATION property for a Database doesn't exist. " +
//                                                    "Fix the source DB's location element to avoid this translation." );
//                                        }
//                                    }
//
//                                }
//                                if (managedLocation != null) {
//                                    managedLocation = managedLocation.replace(leftNamespace, rightNamespace);
//                                }
//                                if (config.getDbPrefix() != null || config.getDbRename() != null) {
//                                    // adjust locations.
//                                    if (location != null) {
//                                        location = Translator.removeLastDirFromUrl(location) + "/" + config.getResolvedDB(getName()) + ".db";
//                                    }
//                                    if (managedLocation != null) {
//                                        managedLocation = Translator.removeLastDirFromUrl(managedLocation) + "/" + config.getResolvedDB(getName()) + ".db";
//                                    }
//                                }
//                                if (config.isReadOnly() && !config.isLoadingTestData()) {
//                                    log.debug("Config set to 'read-only'.  Validating FS before continuing");
//                                    HadoopSession main = null;
//                                    try {
//                                        main = config.getCliPool().borrow();
//                                        String[] api = {"-api"};
//                                        try {
//                                            main.start(api);
//                                        } catch (Exception e) {
//                                            e.printStackTrace();
//                                        }
//
//                                        CommandReturn cr = main.processInput("connect");
//                                        // Check that location exists.
//                                        String dbLocation = null;
//                                        if (location != null) {
//                                            dbLocation = location;
//                                        } else {
//                                            // Get location for DB. If it's not there than:
//                                            //     SQL query to get default from Hive.
//                                            String defaultDBLocProp = null;
//                                            if (config.getCluster(Environment.RIGHT).getLegacyHive()) {
//                                                defaultDBLocProp = LEGACY_DB_LOCATION_PROP;
//                                            } else {
//                                                defaultDBLocProp = EXT_DB_LOCATION_PROP;
//                                            }
//
//                                            Connection conn = null;
//                                            Statement stmt = null;
//                                            ResultSet resultSet = null;
//                                            try {
//                                                conn = config.getCluster(Environment.RIGHT).getConnection();
//
//                                                stmt = conn.createStatement();
//
//                                                String dbLocSql = "SET " + defaultDBLocProp;
//                                                resultSet = stmt.executeQuery(dbLocSql);
//                                                if (resultSet.next()) {
//                                                    String propset = resultSet.getString(1);
//                                                    String dbLocationPrefix = propset.split("=")[1];
//                                                    dbLocation = dbLocationPrefix + getName().toLowerCase(Locale.ROOT) + ".db";
//                                                    log.debug(database + " location is: " + dbLocation);
//
//                                                } else {
//                                                    // Could get property.
//                                                    throw new RuntimeException("Could not determine DB Location for: " + database);
//                                                }
//
//                                            } catch (SQLException throwables) {
//                                                log.error("Issue", throwables);
//                                                throw new RuntimeException(throwables);
//                                            } finally {
//                                                if (resultSet != null) {
//                                                    try {
//                                                        resultSet.close();
//                                                    } catch (SQLException sqlException) {
//                                                        // ignore
//                                                    }
//                                                }
//                                                if (stmt != null) {
//                                                    try {
//                                                        stmt.close();
//                                                    } catch (SQLException sqlException) {
//                                                        // ignore
//                                                    }
//                                                }
//                                                if (conn != null) {
//                                                    try {
//                                                        conn.close();
//                                                    } catch (SQLException throwables) {
//                                                        //
//                                                    }
//                                                }
//                                            }
//
//
//                                        }
//                                        if (dbLocation != null) {
//                                            CommandReturn testCr = main.processInput("test -d " + dbLocation);
//                                            if (testCr.isError()) {
//                                                // Doesn't exist.  So we can't create the DB in a "read-only" mode.
//                                                config.addError(RO_DB_DOESNT_EXIST, dbLocation,
//                                                        testCr, testCr.getCommand(), getName());
//                                                addIssue(Environment.RIGHT, config.getProgression().getErrorMessage(RO_DB_DOESNT_EXIST));
//                                                throw new RuntimeException(config.getProgression().getErrorMessage(RO_DB_DOESNT_EXIST));
////                                        } else {
////                                            config.getCluster(Environment.RIGHT).databaseSql(config, database, dbCreate[0]);
//                                            }
//                                        } else {
//                                            // Can't determine location.
//                                            // TODO: What to do here.
//                                            log.error(getName() + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
//                                                    "'read-only' mode without knowing where it should go and validating existance.");
//                                            throw new RuntimeException(getName() + ": Couldn't determine DB directory on RIGHT cluster.  Can't CREATE database in " +
//                                                    "'read-only' mode without knowing where it should go and validating existance.");
//                                        }
//                                    } finally {
//                                        config.getCliPool().returnSession(main);
//                                    }
//                                }
//
//                                String createDbL = MessageFormat.format(MirrorConf.CREATE_DB, database);
//                                StringBuilder sbL = new StringBuilder();
//                                sbL.append(createDbL).append("\n");
//                                if (dbDefLeft.get(MirrorConf.COMMENT) != null && !dbDefLeft.get(MirrorConf.COMMENT).trim().isEmpty()) {
//                                    sbL.append(MirrorConf.COMMENT).append(" \"").append(dbDefLeft.get(MirrorConf.COMMENT)).append("\"\n");
//                                }
//                                // TODO: DB Properties.
//                                this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.CREATE_DB_DESC, sbL.toString()));
//
//                                if (location != null && !config.getCluster(Environment.RIGHT).isHdpHive3()) {
//                                    String alterDbLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, location);
//                                    this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbLoc));
//                                    dbDefRight.put(DB_LOCATION, location);
//                                }
//                                if (managedLocation != null) {
//                                    if (!config.getCluster(Environment.RIGHT).isHdpHive3()) {
//                                        String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_MNGD_LOCATION, database, managedLocation);
//                                        this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
//                                        dbDefRight.put(DB_MANAGED_LOCATION, managedLocation);
//                                    } else {
//                                        String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, managedLocation);
//                                        this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
//                                        this.addIssue(Environment.RIGHT, HDPHIVE3_DB_LOCATION.getDesc());
//                                        dbDefRight.put(DB_LOCATION, managedLocation);
//                                    }
//                                }
//
//                                break;
//                            case DUMP:
//                                String createDb = MessageFormat.format(MirrorConf.CREATE_DB, database);
//                                StringBuilder sb = new StringBuilder();
//                                sb.append(createDb).append("\n");
//                                if (dbDefLeft.get(MirrorConf.COMMENT) != null && !dbDefLeft.get(MirrorConf.COMMENT).trim().isEmpty()) {
//                                    sb.append(MirrorConf.COMMENT).append(" \"").append(dbDefLeft.get(MirrorConf.COMMENT)).append("\"\n");
//                                }
//                                if (location != null) {
//                                    sb.append(DB_LOCATION).append(" \"").append(location).append("\"\n");
//                                }
//                                if (managedLocation != null) {
//                                    sb.append(MirrorConf.DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
//                                }
//                                // TODO: DB Properties.
//                                this.getSql(Environment.LEFT).add(new Pair(MirrorConf.CREATE_DB_DESC, sb.toString()));
//                                break;
//                            case COMMON:
//                                String createDbCom = MessageFormat.format(MirrorConf.CREATE_DB, database);
//                                StringBuilder sbCom = new StringBuilder();
//                                sbCom.append(createDbCom).append("\n");
//                                if (dbDefLeft.get(MirrorConf.COMMENT) != null && !dbDefLeft.get(MirrorConf.COMMENT).trim().isEmpty()) {
//                                    sbCom.append(MirrorConf.COMMENT).append(" \"").append(dbDefLeft.get(MirrorConf.COMMENT)).append("\"\n");
//                                }
//                                if (location != null) {
//                                    sbCom.append(DB_LOCATION).append(" \"").append(location).append("\"\n");
//                                }
//                                if (managedLocation != null) {
//                                    sbCom.append(MirrorConf.DB_MANAGED_LOCATION).append(" \"").append(managedLocation).append("\"\n");
//                                }
//                                // TODO: DB Properties.
//                                this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.CREATE_DB_DESC, sbCom.toString()));
//                                break;
//                            case STORAGE_MIGRATION:
//                                StringBuilder sbLoc = new StringBuilder();
//                                sbLoc.append(config.getTransfer().getCommonStorage());
//                                sbLoc.append(config.getTransfer().getWarehouse().getExternalDirectory());
//                                sbLoc.append("/");
//                                sbLoc.append(database);
//                                sbLoc.append(".db");
//                                if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
//                                    String alterDbLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, sbLoc.toString());
//                                    this.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbLoc));
//                                    dbDefRight.put(DB_LOCATION, sbLoc.toString());
//                                }
//
//                                StringBuilder sbMngdLoc = new StringBuilder();
//                                sbMngdLoc.append(config.getTransfer().getCommonStorage());
//                                sbMngdLoc.append(config.getTransfer().getWarehouse().getManagedDirectory());
//                                sbMngdLoc.append("/");
//                                sbMngdLoc.append(database);
//                                sbMngdLoc.append(".db");
//                                if (!config.getCluster(Environment.LEFT).isHdpHive3()) {
//                                    String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_MNGD_LOCATION, database, sbMngdLoc.toString());
//                                    this.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_MNGD_LOCATION_DESC, alterDbMngdLoc));
//                                    dbDefRight.put(DB_MANAGED_LOCATION, sbMngdLoc.toString());
//                                } else {
//                                    String alterDbMngdLoc = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, sbMngdLoc.toString());
//                                    this.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDbMngdLoc));
//                                    this.addIssue(Environment.LEFT, HDPHIVE3_DB_LOCATION.getDesc());
//                                    dbDefRight.put(DB_LOCATION, sbMngdLoc.toString());
//                                }
//
//                                this.addIssue(Environment.LEFT,"This process, when 'executed' will leave the original tables intact in their renamed " +
//                                        "version.  They are NOT automatically cleaned up.  Run the produced '" +
//                                        getName() + "_LEFT_CleanUp_execute.sql' " +
//                                        "file to permanently remove them.  Managed and External/Purge table data will be " +
//                                        "removed when dropping these tables.  External non-purge table data will remain in storage.");
//
//                                break;
//                        }
//                }
//            } else {
//                // Downgrade in place.
//                if (config.getTransfer().getWarehouse().getExternalDirectory() != null) {
//                    // Set the location to the external directory for the database.
//                    database = config.getResolvedDB(getName());
//                    location = config.getCluster(Environment.LEFT).getHcfsNamespace() + config.getTransfer().getWarehouse().getExternalDirectory() +
//                            "/" + database + ".db";
//                    String alterDB_location = MessageFormat.format(MirrorConf.ALTER_DB_LOCATION, database, location);
//                    this.getSql(Environment.LEFT).add(new Pair(MirrorConf.ALTER_DB_LOCATION_DESC, alterDB_location));
//                    dbDefLeft.put(DB_LOCATION, location);
//                }
//            }
//        } else {
//            // Reset Right DB.
//            database = config.getResolvedDB(getName());
//            String dropDb = MessageFormat.format(MirrorConf.DROP_DB, database);
//            this.getSql(Environment.RIGHT).add(new Pair(MirrorConf.DROP_DB_DESC, dropDb));
//        }
//    }

    public TableMirror addTable(String table) {
        if (getTableMirrors().containsKey(table)) {
            log.debug("Table object found in map: " + table);
            return getTableMirrors().get(table);
        } else {
            log.info("Adding table object to map: " + table);
            TableMirror tableMirror = new TableMirror();
            tableMirror.setName(table);
            tableMirror.setParent(this);
            getTableMirrors().put(table, tableMirror);
            return tableMirror;
        }
    }

    public Map<String, TableMirror> getTableMirrors() {
        if (tableMirrors == null) {
            tableMirrors = new TreeMap<String, TableMirror>();
        }
        return tableMirrors;
    }

    public TableMirror getTable(String table) {
        return getTableMirrors().get(table);
    }

    public boolean hasIssues() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasIssues())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasActions() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasActions())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasAddedProperties() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasAddedProperties())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasStatistics() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasStatistics())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

}
