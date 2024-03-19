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
import com.cloudera.utils.hadoop.HadoopSessionFactory;
import com.cloudera.utils.hadoop.HadoopSessionPool;
import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hadoop.hms.mirror.feature.LegacyTranslations;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.cloudera.utils.hadoop.hms.mirror.ConnectionPoolTypes.HYBRID;
import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;

//@Component
@Slf4j
@Getter
@Setter
//@ConfigurationProperties(prefix = "hms-mirror.config")
//@PropertySource("file:${hms-mirror.config.filename}")
@JsonIgnoreProperties({"featureList"})
//@DependsOn("transfer")
public class Config {

//    @JsonIgnore
    /*
    Used to seed and build the config from the config yaml at the startup of the application.
     */
//    @Value("${hms-mirror.config.default-filename}")
//    private String defaultFilename = null;

//    private static final Logger log = LoggerFactory.getLogger(Config.class);
    @Setter
    @Getter
    @JsonIgnore
    private Date initDate = new Date();
    private Acceptance acceptance = new Acceptance();
    @JsonIgnore
    private HadoopSessionPool cliPool;
    //        for (Map.Entry<Environment, Cluster> entry : clusters.entrySet()) {
    //            entry.getValue().setConfig(this);
    //        }
    @Setter
    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();
    private String commandLineOptions = null;
    private boolean copyAvroSchemaUrls = Boolean.FALSE;
    @Getter
    private ConnectionPoolTypes connectionPoolLib = HYBRID; // DBCP2 is Alternate.
    private DataStrategyEnum dataStrategy = DataStrategyEnum.SCHEMA_ONLY;
    private boolean databaseOnly = Boolean.FALSE;
    private boolean dumpTestData = Boolean.FALSE;
    private String loadTestDataFile = null;

    private boolean evaluatePartitionLocation = Boolean.FALSE;
    private Filter filter = new Filter();
    private boolean skipLinkCheck = Boolean.FALSE;
    private String[] databases = null;
    @JsonIgnore
    private String decryptPassword;

    private LegacyTranslations legacyTranslations = new LegacyTranslations();
    @JsonIgnore
    private final String runMarker = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

    /*
   Prefix the DB with this to create an alternate db.
   Good for testing.

   Should leave null for like replication.
    */
    private String dbPrefix = null;
    private String dbRename = null;
    private Environment dumpSource = Environment.LEFT;
    private boolean execute = Boolean.FALSE;
    @JsonIgnore
    private final List<String> flags = new LinkedList<String>();
    /*
    Use 'flip' to switch LEFT and RIGHT cluster definitions.  Allows you to change the direction of the calls.
     */
    private boolean flip = Boolean.FALSE;
    private HybridConfig hybrid = new HybridConfig();
    private IcebergConfig icebergConfig = new IcebergConfig();
    private MigrateACID migrateACID = new MigrateACID();
    private MigrateVIEW migrateVIEW = new MigrateVIEW();
    private boolean migratedNonNative = Boolean.FALSE;
    private Optimization optimization = new Optimization();
    private String outputDirectory = null;

    @JsonIgnore
    private String password;
    @JsonIgnore
    private String passwordKey;

    @JsonIgnore
    private Progression progression;

    private boolean quiet = Boolean.FALSE;
    /*
    Used when a schema is transferred and has 'purge' properties for the table.
    When this is 'true', we'll remove the 'purge' option.
    This is helpful for datasets that are in DR, where the table doesn't
    control the Filesystem and we don't want to mess that up.
     */
    private boolean readOnly = Boolean.FALSE;
    /*
    Like 'readonly' without the restrictions that would invalidate the target filesystem.
     */
    private boolean noPurge = Boolean.FALSE;
    /*
    When Common Storage is used with the SQL Data Strategy, this will 'replace' the original table
    with a table by the same name but who's data lives in the common storage location.
     */
    private boolean replace = Boolean.FALSE;
    @JsonIgnore
    private boolean replay = Boolean.FALSE;
    private boolean resetRight = Boolean.FALSE;

    private boolean resetToDefaultLocation = Boolean.FALSE;

    private boolean skipFeatures = Boolean.FALSE;
    private boolean skipLegacyTranslation = Boolean.FALSE;
    /*
    Always true.  leaving to ensure config serialization compatibility.
     */
    @Deprecated
    private boolean sqlOutput = Boolean.TRUE;

    private final List<String> supportFileSystems = new ArrayList<String>(Arrays.asList(
            "hdfs", "ofs", "s3", "s3a", "s3n", "wasb", "adls", "gf"
    ));

    /*
    Sync is valid for SCHEMA_ONLY, LINKED, and COMMON data strategies.
    This will compare the tables between LEFT and RIGHT to ensure that they are in SYNC.
    SYNC means: If a table on the LEFT:
    - it will be created on the RIGHT
    - exists on the right and has changed (Fields and/or Serde), it will be dropped and recreated
    - missing and exists on the RIGHT, it will be dropped.

    If the -ro option is used, the tables that are changed or dropped will be disconnected from the data before
    the drop to ensure they don't modify the FileSystem.

    This option can NOT be used with -tf (table filter).

    Transactional tables are NOT considered in this process.
     */
    private boolean sync = Boolean.FALSE;

//    @Autowired
    private TransferConfig transfer = new TransferConfig();

    private boolean transferOwnership = Boolean.FALSE;
    @JsonIgnore
    private ScheduledExecutorService transferThreadPool;
    @JsonIgnore
    private ScheduledExecutorService metadataThreadPool;
    //    @JsonIgnore
    private Translator translator = new Translator();

    public Config() {
    }

    /*
        Use this to initialize a default config.
         */
    public static void setup(String configFile) {
        Config config = new Config();
        Scanner scanner = new Scanner(System.in);

        //  prompt for the user's name
        System.out.println("----------------------------------------------------------------");
        System.out.println(".... Default Config not found.  Setup default config.");
        System.out.println("----------------------------------------------------------------");
        Boolean kerb = Boolean.FALSE;
        for (Environment env : Environment.values()) {
            if (env.isVisible()) {
                System.out.println();
                System.out.println("Setup " + env + " cluster....");
                System.out.println();


                // get their input as a String
                // Legacy?
                System.out.print("Is the " + env + " hive instance Hive 1 or Hive 2? (Y/N)");
                String response = scanner.next();
                if (response.equalsIgnoreCase("y")) {
                    config.getCluster(env).setLegacyHive(Boolean.TRUE);
                } else {
                    config.getCluster(env).setLegacyHive(Boolean.FALSE);
                }

                // hcfsNamespace
                System.out.print("What is the namespace for the " + env + " cluster? ");
                response = scanner.next();
                config.getCluster(env).setHcfsNamespace(response);

                // HS2 URI
                System.out.print("What is the JDBC URI for the " + env + " cluster? ");
                response = scanner.next();
                HiveServer2Config hs2Cfg = new HiveServer2Config();
                config.getCluster(env).setHiveServer2(hs2Cfg);
                hs2Cfg.setUri(response);

                // If Kerberized, notify to include hive jar in 'aux_libs'
                if (!kerb && response.contains("principal")) {
                    // appears the connection is kerberized.
                    System.out.println("----------------------------------------------------------------------------------------");
                    System.out.println("The connection appears to be Kerberized.\n\t\tPlace the 'hive standalone' driver in '$HOME/.hms-mirror/aux_libs'");
                    System.out.println("\tSPECIAL RUN INSTRUCTIONS for Legacy Kerberos Connections.");
                    System.out.println("\thttps://github.com/cloudera-labs/hms-mirror#running-against-a-legacy-non-cdp-kerberized-hiveserver2");
                    System.out.println("----------------------------------------------------------------------------------------");
                    kerb = Boolean.TRUE;
                } else if (response.contains("principal")) {
                    System.out.println("----------------------------------------------------------------------------------------");
                    System.out.println("The connection ALSO appears to be Kerberized.\n");
                    System.out.println(" >> Will your Kerberos Ticket be TRUSTED for BOTH JDBC Kerberos Connections? (Y/N)");
                    response = scanner.next();
                    if (!response.equalsIgnoreCase("y")) {
                        throw new RuntimeException("Both JDBC connection must trust your kerberos ticket.");
                    }
                    System.out.println(" >> Are both clusters running the same version of Hadoop/Hive? (Y/N)");
                    response = scanner.next();
                    if (!response.equalsIgnoreCase("y")) {
                        throw new RuntimeException("Both JDBC connections must be running the same version.");
                    }
                } else {
                    //    get jarFile location.
                    //    get username
                    //    get password
                    System.out.println("----------------------------------------------------------------------------------------");
                    System.out.println("What is the location (local) of the 'hive standalone' jar file?");
                    response = scanner.next();
                    hs2Cfg.setJarFile(response);
                    System.out.println("Connection username?");
                    response = scanner.next();
                    hs2Cfg.getConnectionProperties().put("user", response);
                    System.out.println("Connection password?");
                    response = scanner.next();
                    hs2Cfg.getConnectionProperties().put("password", response);
                }
                // Partition Discovery
                // Only on the RIGHT cluster.
                if (env == Environment.RIGHT) {
                    PartitionDiscovery pd = config.getCluster(env).getPartitionDiscovery();
                    if (!config.getCluster(env).isLegacyHive()) {
                        // Can only auto-discover in Hive 3
                        System.out.println("Set created tables to 'auto-discover' partitions?(Y/N)");
                        response = scanner.next();
                        if (response.equalsIgnoreCase("y")) {
                            pd.setAuto(Boolean.TRUE);
                        }
                    }
                    System.out.println("Run 'MSCK' after table creation?(Y/N)");
                    response = scanner.next();
                    if (response.equalsIgnoreCase("y")) {
                        pd.setInitMSCK(Boolean.TRUE);
                    }
                }
            }
        }

        try {
            ObjectMapper mapper;
            mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String configStr = mapper.writeValueAsString(config);
            File cfgFile = new File(configFile);
            FileWriter cfgFileWriter = null;
            try {
                cfgFileWriter = new FileWriter(cfgFile);
                cfgFileWriter.write(configStr);
                log.debug("Default Config 'saved' to: " + cfgFile.getPath());
            } catch (IOException ioe) {
                log.error("Problem 'writing' default config", ioe);
            } finally {
                cfgFileWriter.close();
            }
        } catch (JsonProcessingException e) {
            log.error("Problem 'saving' default config", e);
        } catch (IOException ioe) {
            log.error("Problem 'closing' default config file", ioe);
        }
    }

    public void addWarning(MessageCode code) {
        getProgression().addWarning(code);
    }
    public void addWarning(MessageCode code, Object... args) {
        getProgression().addWarning(code, args);
    }

    public void addError(MessageCode code) {
        getProgression().addError(code);
    }

    public void addError(MessageCode code, Object... args) {
        getProgression().addError(code, args);
    }

    @JsonIgnore
    public Boolean isTranslateLegacy() {
        Boolean rtn = Boolean.FALSE;
        if (!skipLegacyTranslation) {
            // contribs can be in legacy and non-legacy envs.
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public HadoopSessionPool getCliPool() {
        if (cliPool == null) {
            GenericObjectPoolConfig<HadoopSession> hspCfg = new GenericObjectPoolConfig<HadoopSession>();
            hspCfg.setMaxTotal(transfer.getConcurrency());
            this.cliPool = new HadoopSessionPool(new GenericObjectPool<HadoopSession>(new HadoopSessionFactory(), hspCfg));
        }
        return cliPool;
    }

    @JsonIgnore
    public Boolean isLoadingTestData() {
        if (loadTestDataFile != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Boolean isFlip() {
        return flip;
    }

    public void setFlip(Boolean flip) {
        this.flip = flip;
        if (this.flip) {
            Cluster origLeft = getCluster(Environment.LEFT);
            origLeft.setEnvironment(Environment.RIGHT);
            Cluster origRight = getCluster(Environment.RIGHT);
            origRight.setEnvironment(Environment.LEFT);
            getClusters().put(Environment.RIGHT, origLeft);
            getClusters().put(Environment.LEFT, origRight);
        }
    }


    public Boolean isReplace() {
        return replace;
    }

    public boolean convertManaged() {
        if (getCluster(Environment.LEFT).isLegacyHive() && !getCluster(Environment.RIGHT).isLegacyHive()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public void setDataStrategy(DataStrategyEnum dataStrategy) {
        this.dataStrategy = dataStrategy;
        if (this.dataStrategy != null &&
                this.dataStrategy == DataStrategyEnum.DUMP) {
            this.getMigrateACID().setOn(Boolean.TRUE);
            this.getMigrateVIEW().setOn(Boolean.TRUE);
            this.setMigratedNonNative(Boolean.TRUE);
        }
    }

    public boolean isExecute() {
        if (!execute) {
            log.debug("Dry-run: ON");
        }
        return execute;
    }

    public void setGlobalLocationMapKV(String[] extLocs) {
        boolean set = Boolean.TRUE;
        if (extLocs != null) {
            for (String property : extLocs) {
                try {
                    String[] keyValue = property.split("=");
                    if (keyValue.length == 2) {
                        getTranslator().addGlobalLocationMap(keyValue[0], keyValue[1]);
                    }
                } catch (Throwable t) {
                    set = Boolean.FALSE;
                }
            }
        }
    }

//    public String getResolvedDB(String database) {
//        String rtn = null;
//        // Set Local Value for adjustments
//        String lclDb = database;
//        // When dbp, set new value
//        lclDb = (dbPrefix != null ? dbPrefix + lclDb : lclDb);
//        // Rename overrides prefix, otherwise use lclDb as its been set.
//        rtn = (dbRename != null ? dbRename : lclDb);
//        return rtn;
//    }
//
    public ScheduledExecutorService getMetadataThreadPool() {
        if (metadataThreadPool == null) {
            metadataThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
        }
        return metadataThreadPool;
    }

    public ScheduledExecutorService getTransferThreadPool() {
        if (transferThreadPool == null) {
            transferThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
        }
        return transferThreadPool;
    }

//    @JsonIgnore
//    public Boolean isConnectionKerberized() {
//        boolean rtn = Boolean.FALSE;
//        Set<Environment> envs = clusters.keySet();
//        for (Environment env : envs) {
//            Cluster cluster = clusters.get(env);
//            if (cluster.getHiveServer2() != null && cluster.getHiveServer2().isValidUri()
//                    && cluster.getHiveServer2().getUri() != null && cluster.getHiveServer2().getUri().contains("principal")) {
//                rtn = Boolean.TRUE;
//            }
//        }
//        return rtn;
//    }

//    public Boolean canDeriveDistcpPlan() {
//        Boolean rtn = Boolean.FALSE;
//        if (getTransfer().getStorageMigration().isDistcp()) {
//            rtn = Boolean.TRUE;
//        } else {
//            addWarning(DISTCP_OUTPUT_NOT_REQUESTED);
//        }
//        if (rtn && resetToDefaultLocation && getTransfer().getWarehouse().getExternalDirectory() == null) {
//            rtn = Boolean.FALSE;
//        }
//        return rtn;
//    }
//
    /*
    Before processing, validate the config for issues and warn.  A valid configuration will return 'null'.  An invalid
    config will return an array of String representing the issues.
     */
//    public Boolean validate() {
//        Boolean rtn = Boolean.TRUE;
//
//        // Set distcp options.
//        canDeriveDistcpPlan();
//
////        if (getCluster(Environment.RIGHT).isInitialized()) {
//            switch (getDataStrategy()) {
//                case DUMP:
//                case STORAGE_MIGRATION:
//                case ICEBERG_CONVERSION:
//                    break;
//                default:
//                    if (getCluster(Environment.RIGHT).isHdpHive3()) {
//                        this.getTranslator().setForceExternalLocation(Boolean.TRUE);
//                        addWarning(HDP3_HIVE);
//
//                    }
//                    // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
//                    if (!getMigrateACID().isDowngradeInPlace()) {
//                        if (getCluster(Environment.RIGHT).getLegacyHive() && !getCluster(Environment.LEFT).getLegacyHive() &&
//                                !getDumpTestData()) {
//                            addError(NON_LEGACY_TO_LEGACY);
//                            rtn = Boolean.FALSE;
//                        }
//                    }
//            }
////        }
//
//        if (getCluster(Environment.LEFT).isHdpHive3() && getCluster(Environment.LEFT).getLegacyHive()) {
//            addError(LEGACY_AND_HIVE3);
//            rtn = Boolean.FALSE;
//        }
//
//        if (getCluster(Environment.RIGHT).isHdpHive3() && getCluster(Environment.RIGHT).getLegacyHive()) {
//            addError(LEGACY_AND_HIVE3);
//            rtn = Boolean.FALSE;
//        }
//
//        if (getCluster(Environment.LEFT).isHdpHive3() && this.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
//            this.getTranslator().setForceExternalLocation(Boolean.TRUE);
//            if (getMigrateACID().isOn() && !getTransfer().getStorageMigration().isDistcp()) {
//                addError(HIVE3_ON_HDP_ACID_TRANSFERS);
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        if (resetToDefaultLocation) {
//            if (!(dataStrategy == DataStrategyEnum.SCHEMA_ONLY ||
//                    dataStrategy == DataStrategyEnum.STORAGE_MIGRATION ||
//                    dataStrategy == DataStrategyEnum.SQL ||
//                    dataStrategy == DataStrategyEnum.EXPORT_IMPORT ||
//                    dataStrategy == DataStrategyEnum.HYBRID)) {
//                addError(RESET_TO_DEFAULT_LOCATION);
//                rtn = Boolean.FALSE;
//            }
//            if (getTransfer().getWarehouse().getManagedDirectory() == null || getTransfer().getWarehouse().getExternalDirectory() == null) {
//                addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
//                rtn = Boolean.FALSE;
//            }
//            if (getTransfer().getStorageMigration().isDistcp()) {
//                addWarning(RDL_DC_WARNING_TABLE_ALIGNMENT);
////                if (getEvaluatePartitionLocation()) {
//
////                }
//            }
//            if (getTranslator().getForceExternalLocation()) {
//                addWarning(RDL_FEL_OVERRIDES);
//            }
//        }
//
//        if (getDataStrategy() == DataStrategyEnum.LINKED) {
//            if (getMigrateACID().isOn()) {
//                log.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
//                // TODO: Add to errors.
//                throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
//            }
//        }
//
//        // When RIGHT is defined
//        switch (getDataStrategy()) {
//            case SQL:
//            case EXPORT_IMPORT:
//            case HYBRID:
//            case LINKED:
//            case SCHEMA_ONLY:
//            case CONVERT_LINKED:
//                // When the storage on LEFT and RIGHT match, we need to specify both rdl (resetDefaultLocation)
//                //   and use -dbp (db prefix) to identify a new db name (hence a location).
//                if (getCluster(Environment.RIGHT) != null &&
//                        (getCluster(Environment.LEFT).getHcfsNamespace()
//                                .equalsIgnoreCase(getCluster(Environment.RIGHT).getHcfsNamespace()))) {
//                    if (!resetToDefaultLocation) {
//                        addError(SAME_CLUSTER_COPY_WITHOUT_RDL);
//                        rtn = Boolean.FALSE;
//                    }
//                    if (getDbPrefix() == null && getDbRename() == null) {
//                        addError(SAME_CLUSTER_COPY_WITHOUT_DBPR);
//                        rtn = Boolean.FALSE;
//                    }
//                }
//        }
//
//        if (getEvaluatePartitionLocation() && !isLoadingTestData()) {
//            switch (getDataStrategy()) {
//                case SCHEMA_ONLY:
//                case DUMP:
//                    // Check the metastore_direct config on the LEFT.
//                    if (getCluster(Environment.LEFT).getMetastoreDirect() == null) {
//                        addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
//                        rtn = Boolean.FALSE;
//                    }
//                    addWarning(EVALUATE_PARTITION_LOCATION);
//                    break;
//                case STORAGE_MIGRATION:
//                    if (getCluster(Environment.LEFT).getMetastoreDirect() == null) {
//                        addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
//                        rtn = Boolean.FALSE;
//                    }
//                    if (!getTransfer().getStorageMigration().isDistcp()) {
//                        addError(EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION, "LEFT");
//                        rtn = Boolean.FALSE;
//                    }
//                    break;
//                default:
//                    addError(EVALUATE_PARTITION_LOCATION_USE);
//                    rtn = Boolean.FALSE;
//            }
//        }
//
//        // Only allow db rename with a single database.
//        if (getDbRename() != null && getDatabases().length > 1) {
//            addError(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION);
//            rtn = Boolean.FALSE;
//        }
//
//        if (isLoadingTestData()) {
//            if (getFilter().isTableFiltering()) {
//                addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
//            }
//        }
//
//        if (isFlip() && getCluster(Environment.LEFT) == null) {
//            addError(FLIP_WITHOUT_RIGHT);
//            rtn = Boolean.FALSE;
//        }
//
//        if (getTransfer().getConcurrency() > 4 && !isLoadingTestData()) {
//            // We need to pass on a few scale parameters to the hs2 configs so the connection pools can handle the scale requested.
//            if (getCluster(Environment.LEFT) != null) {
//                Cluster cluster = getCluster(Environment.LEFT);
//                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString((Integer) getTransfer().getConcurrency() / 2));
//                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString((Integer) getTransfer().getConcurrency() / 2));
//                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
//                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(getTransfer().getConcurrency()));
//                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
//                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(getTransfer().getConcurrency()));
//                }
//            }
//            if (getCluster(Environment.RIGHT) != null) {
//                Cluster cluster = getCluster(Environment.RIGHT);
//                if (cluster.getHiveServer2() != null) {
//                    cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString((Integer) getTransfer().getConcurrency() / 2));
//                    cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString((Integer) getTransfer().getConcurrency() / 2));
//                    if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
//                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(getTransfer().getConcurrency()));
//                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
//                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(getTransfer().getConcurrency()));
//                    }
//                }
//            }
//        }
//
//        if (getTransfer().getStorageMigration().isDistcp()) {
////            if (resetToDefaultLocation && (getTransfer().getWarehouse().getManagedDirectory() == null || getTransfer().getWarehouse().getExternalDirectory() == null)) {
////                addError(DISTCP_VALID_DISTCP_RESET_TO_DEFAULT_LOCATION);
////                rtn = Boolean.FALSE;
////            }
//            if (getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
//                    || getDataStrategy() == DataStrategyEnum.COMMON
//                    || getDataStrategy() == DataStrategyEnum.DUMP
//                    || getDataStrategy() == DataStrategyEnum.LINKED
//                    || getDataStrategy() == DataStrategyEnum.CONVERT_LINKED
//                    || getDataStrategy() == DataStrategyEnum.HYBRID) {
//                addError(DISTCP_VALID_STRATEGY);
//                rtn = Boolean.FALSE;
//            }
//            if (getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
//                    && getTransfer().getStorageMigration().isDistcp()) {
//                addWarning(STORAGE_MIGRATION_DISTCP_EXECUTE);
//            }
//
//            if (getFilter().isTableFiltering()) {
//                addWarning(DISTCP_W_TABLE_FILTERS);
//            } else {
//                addWarning(DISTCP_WO_TABLE_FILTERS);
//            }
////            if (getDataStrategy() == DataStrategy.STORAGE_MIGRATION
////                    && getMigrateACID().isOn() && !getEvaluatePartitionLocation()) {
////                addError(STORAGE_MIGRATION_DISTCP_ACID);
////                rtn = Boolean.FALSE;
////            }
//            if (getDataStrategy() == DataStrategyEnum.SQL
//                    && getMigrateACID().isOn()
//                    && getMigrateACID().isDowngrade()
//                    && (getTransfer().getWarehouse().getExternalDirectory() == null)) {
//                addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
//                rtn = Boolean.FALSE;
//            }
//            if (getDataStrategy() == DataStrategyEnum.SQL) {
//                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
//                if (getMigrateACID().isOn() || getMigrateACID().isOnly()) {
//                    if (!getMigrateACID().isDowngrade()) {
//                        addError(SQL_DISTCP_ONLY_W_DA_ACID);
//                        rtn = Boolean.FALSE;
//                    }
//                }
//                if (getTransfer().getCommonStorage() != null
////                        || getTransfer().getIntermediateStorage() != null)
//                ) {
//                    addError(SQL_DISTCP_ACID_W_STORAGE_OPTS);
//                    rtn = Boolean.FALSE;
//                }
//            }
//        }
//
//        // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
//        if (getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY && getMigrateACID().isOn() && getMigrateACID().isDowngrade()) {
//            addError(ACID_DOWNGRADE_SCHEMA_ONLY);
//            rtn = Boolean.FALSE;
//        }
//
//        if (getMigrateACID().isDowngradeInPlace()) {
//            if (getDataStrategy() != DataStrategyEnum.SQL) {
//                addError(VALID_ACID_DA_IP_STRATEGIES);
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        if (getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY) {
//            if (!getTransfer().getStorageMigration().isDistcp()) {
//                if (resetToDefaultLocation) {
//                    // requires distcp.
//                    addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL);
//                    rtn = Boolean.FALSE;
//                }
//                if (getTransfer().getIntermediateStorage() != null) {
//                    // requires distcp.
//                    addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS);
//                    rtn = Boolean.FALSE;
//                }
//            }
//        }
//
//        if (resetToDefaultLocation && (getTransfer().getWarehouse().getExternalDirectory() == null)) {
//            addWarning(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
//        }
//
//        if (sync && (getFilter().getTblRegEx() != null || getFilter().getTblExcludeRegEx() != null)) {
//            addWarning(SYNC_TBL_FILTER);
//        }
//        if (sync && !(dataStrategy == DataStrategyEnum.SCHEMA_ONLY || dataStrategy == DataStrategyEnum.LINKED ||
//                dataStrategy == DataStrategyEnum.SQL || dataStrategy == DataStrategyEnum.EXPORT_IMPORT ||
//                dataStrategy == DataStrategyEnum.HYBRID)) {
//            addError(VALID_SYNC_STRATEGIES);
//            rtn = Boolean.FALSE;
//        }
//        if (migrateACID.isOn() && !(dataStrategy == DataStrategyEnum.SCHEMA_ONLY || dataStrategy == DataStrategyEnum.DUMP ||
//                dataStrategy == DataStrategyEnum.EXPORT_IMPORT || dataStrategy == DataStrategyEnum.HYBRID ||
//                dataStrategy == DataStrategyEnum.SQL || dataStrategy == DataStrategyEnum.STORAGE_MIGRATION)) {
//            addError(VALID_ACID_STRATEGIES);
//            rtn = Boolean.FALSE;
//        }
//
//        // DUMP does require Execute.
//        if (isExecute() && dataStrategy == DataStrategyEnum.DUMP) {
//            setExecute(Boolean.FALSE);
//        }
//
//        if (migrateACID.isOn() && migrateACID.isInplace()) {
//            if (!(dataStrategy == DataStrategyEnum.SQL)) {
//                addError(VALID_ACID_DA_IP_STRATEGIES);
//                rtn = Boolean.FALSE;
//            }
//            if (this.getTransfer().getCommonStorage() != null) {
//                addError(COMMON_STORAGE_WITH_DA_IP);
//                rtn = Boolean.FALSE;
//            }
//            if (this.getTransfer().getIntermediateStorage() != null) {
//                addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
//                rtn = Boolean.FALSE;
//            }
//            if (this.getTransfer().getStorageMigration().isDistcp()) {
//                addError(DISTCP_W_DA_IP_ACID);
//                rtn = Boolean.FALSE;
//            }
//            if (getCluster(Environment.LEFT).getLegacyHive()) {
//                addError(DA_IP_NON_LEGACY);
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        if (dataStrategy == DataStrategyEnum.STORAGE_MIGRATION) {
//            // The commonStorage and Storage Migration Namespace are the same thing.
//            if (this.getTransfer().getCommonStorage() == null) {
//                // Use the same namespace, we're assuming that was the intent.
//                this.getTransfer().setCommonStorage(getCluster(Environment.LEFT).getHcfsNamespace());
//                // Force reset to default location.
////                this.setResetToDefaultLocation(Boolean.TRUE);
//                addWarning(STORAGE_MIGRATION_NAMESPACE_LEFT, getCluster(Environment.LEFT).getHcfsNamespace());
//                if (!getResetToDefaultLocation() && getTranslator().getGlobalLocationMap().size() == 0) {
//                    addError(STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM);
//                    rtn = Boolean.FALSE;
//                }
//            }
//            if (this.getTransfer().getWarehouse() == null ||
//                    (this.getTransfer().getWarehouse().getManagedDirectory() == null ||
//                            this.getTransfer().getWarehouse().getExternalDirectory() == null)) {
//                addError(STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS);
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        // Because some just don't get you can't do this...
//        if (this.getTransfer().getWarehouse().getManagedDirectory() != null &&
//                this.getTransfer().getWarehouse().getExternalDirectory() != null) {
//            // Make sure these aren't set to the same location.
//            if (this.getTransfer().getWarehouse().getManagedDirectory().equals(this.getTransfer().getWarehouse().getExternalDirectory())) {
//                addError(WAREHOUSE_DIRS_SAME_DIR, this.getTransfer().getWarehouse().getExternalDirectory()
//                        , this.getTransfer().getWarehouse().getManagedDirectory());
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        if (dataStrategy == DataStrategyEnum.ACID) {
//            addError(ACID_NOT_TOP_LEVEL_STRATEGY);
//            rtn = Boolean.FALSE;
//        }
//
//        // Test to ensure the clusters are LINKED to support underlying functions.
//        switch (dataStrategy) {
//            case LINKED:
//                if (this.getTransfer().getCommonStorage() != null) {
//                    addError(COMMON_STORAGE_WITH_LINKED);
//                    rtn = Boolean.FALSE;
//                }
//                if (this.getTransfer().getIntermediateStorage() != null) {
//                    addError(INTERMEDIATE_STORAGE_WITH_LINKED);
//                    rtn = Boolean.FALSE;
//                }
//            case HYBRID:
//            case EXPORT_IMPORT:
//            case SQL:
//                // Only do link test when NOT using intermediate storage.
//                if (this.getCluster(Environment.RIGHT).getHiveServer2() != null
//                        && !this.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
//                        && this.getTransfer().getIntermediateStorage() == null
//                        && this.getTransfer().getCommonStorage() == null) {
//                    if (!getMigrateACID().isDowngradeInPlace() && !linkTest()) {
//                        addError(LINK_TEST_FAILED);
//                        rtn = Boolean.FALSE;
//
//                    }
//                } else {
//                    addWarning(LINK_TEST_SKIPPED_WITH_IS);
//                }
//                break;
//            case SCHEMA_ONLY:
//                if (this.isCopyAvroSchemaUrls()) {
//                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
//                    if (!linkTest()) {
//                        addError(LINK_TEST_FAILED);
//                        rtn = Boolean.FALSE;
//                    }
//                }
//                break;
//            case DUMP:
//                if (getDumpSource() == Environment.RIGHT) {
//                    addWarning(DUMP_ENV_FLIP);
//                }
//            case COMMON:
//                break;
//            case CONVERT_LINKED:
//                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
//                if (getCluster(Environment.RIGHT).getLegacyHive()) {
//                    addError(LEGACY_HIVE_RIGHT_CLUSTER);
//                    rtn = Boolean.FALSE;
//                }
//                break;
//        }
//
//        // Check the use of downgrades and replace.
//        if (getMigrateACID().isDowngrade()) {
//            if (!getMigrateACID().isOn()) {
//                addError(DOWNGRADE_ONLY_FOR_ACID);
//                rtn = Boolean.FALSE;
//            }
//        }
//
//        if (isReplace()) {
//            if (getDataStrategy() != DataStrategyEnum.SQL) {
//                addError(REPLACE_ONLY_WITH_SQL);
//                rtn = Boolean.FALSE;
//            }
//            if (getMigrateACID().isOn()) {
//                if (!getMigrateACID().isDowngrade()) {
//                    addError(REPLACE_ONLY_WITH_DA);
//                    rtn = Boolean.FALSE;
//                }
//            }
//        }
//
//        if (getCluster(Environment.RIGHT) != null) {
//            if (getDataStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
//            getCluster(Environment.RIGHT).getCreateIfNotExists()) {
//                addWarning(CINE_WITH_DATASTRATEGY);
//            }
//        }
//
//        if (this.getTranslator().getGlobalLocationMap() != null) {
//            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
//            //    For items that match /data/mydir maybe confusing as to which one to adjust.
//            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
//            //      will push longer paths to be evaluated first and once a match is found, skip further checks.
//
//        }
//
//        // TODO: Check the connections.
//        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.
//
//        // Set maxConnections to Concurrency.
//        // Don't validate connections or url's if we're working with test data.
//        if (!isLoadingTestData()) {
//            HiveServer2Config leftHS2 = this.getCluster(Environment.LEFT).getHiveServer2();
//            if (!leftHS2.isValidUri()) {
//                rtn = Boolean.FALSE;
//                addError(LEFT_HS2_URI_INVALID);
//            }
//
//            if (leftHS2.isKerberosConnection() && leftHS2.getJarFile() != null) {
//                rtn = Boolean.FALSE;
//                addError(LEFT_KERB_JAR_LOCATION);
//            }
//
//            HiveServer2Config rightHS2 = this.getCluster(Environment.RIGHT).getHiveServer2();
//
//            if (rightHS2 != null) {
//                // TODO: Add validation for -rid (right-is-disconnected) option.
//                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
//                // -
//                //
//                if (getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION && !rightHS2.isValidUri()) {
//                    if (!this.getDataStrategy().equals(DataStrategyEnum.DUMP)) {
//                        rtn = Boolean.FALSE;
//                        addError(RIGHT_HS2_URI_INVALID);
//                    }
//                } else {
//
//                    if (rightHS2.isKerberosConnection() && rightHS2.getJarFile() != null) {
//                        rtn = Boolean.FALSE;
//                        addError(RIGHT_KERB_JAR_LOCATION);
//                    }
//
//                    if (leftHS2.isKerberosConnection() && rightHS2.isKerberosConnection() &&
//                            (this.getCluster(Environment.LEFT).getLegacyHive() != this.getCluster(Environment.RIGHT).getLegacyHive())) {
//                        rtn = Boolean.FALSE;
//                        addError(KERB_ACROSS_VERSIONS);
//                    }
//                }
//            } else {
//                if (!(getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION || getDataStrategy() == DataStrategyEnum.DUMP)) {
//                    if (!getMigrateACID().isDowngradeInPlace()) {
//                        rtn = Boolean.FALSE;
//                        addError(RIGHT_HS2_DEFINITION_MISSING);
//                    }
//                }
//            }
//        }
//
//        if (rtn) {
//            // Last check for errors.
//            if (progression.getErrors().getReturnCode() != 0) {
//                rtn = Boolean.FALSE;
//            }
//        }
//        return rtn;
//    }

//    protected Boolean linkTest() {
//        Boolean rtn = Boolean.FALSE;
//        if (this.skipLinkCheck || this.isLoadingTestData()) {
//            log.warn("Skipping Link Check.");
//            rtn = Boolean.TRUE;
//        } else {
//            HadoopSession session = null;
//            try {
//                session = getCliPool().borrow();
//                log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
//                // TODO: develop a test to copy data between clusters.
//                String leftHCFSNamespace = this.getCluster(Environment.LEFT).getHcfsNamespace();
//                String rightHCFSNamespace = this.getCluster(Environment.RIGHT).getHcfsNamespace();
//
//                // List User Directories on LEFT
//                String leftlsTestLine = "ls " + leftHCFSNamespace + "/user";
//                String rightlsTestLine = "ls " + rightHCFSNamespace + "/user";
//                log.info("LEFT ls testline: " + leftlsTestLine);
//                log.info("RIGHT ls testline: " + rightlsTestLine);
//
//                CommandReturn lcr = session.processInput(leftlsTestLine);
//                if (lcr.isError()) {
//                    throw new RuntimeException("Link to RIGHT cluster FAILED.\n " + lcr.getError() +
//                            "\nCheck configuration and hcfsNamespace value.  " +
//                            "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
//                }
//                CommandReturn rcr = session.processInput(rightlsTestLine);
//                if (rcr.isError()) {
//                    throw new RuntimeException("Link to LEFT cluster FAILED.\n " + rcr.getError() +
//                            "\nCheck configuration and hcfsNamespace value.  " +
//                            "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
//                }
//                rtn = Boolean.TRUE;
//            } finally {
//                if (session != null)
//                    getCliPool().returnSession(session);
//            }
//        }
//        return rtn;
//    }
//
//    public Boolean checkConnections() {
//        boolean rtn = Boolean.FALSE;
//        Set<Environment> envs = new HashSet<>();
//        if (!(getDataStrategy() == DataStrategyEnum.DUMP || getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
//                getDataStrategy() == DataStrategyEnum.ICEBERG_CONVERSION)) {
//            envs.add(Environment.LEFT);
//            envs.add(Environment.RIGHT);
//        } else {
//            envs.add(Environment.LEFT);
//        }
//
//        for (Environment env : envs) {
//            Cluster cluster = clusters.get(env);
//            if (cluster != null
//                    && cluster.getHiveServer2() != null
//                    && cluster.getHiveServer2().isValidUri()
//                    && !cluster.getHiveServer2().isDisconnected()) {
//                Connection conn = null;
//                try {
//                    conn = cluster.getConnection();
//                    // May not be set for DUMP strategy (RIGHT cluster)
//                    log.debug(env + ":" + ": Checking Hive Connection");
//                    if (conn != null) {
////                        Statement stmt = null;
////                        ResultSet resultSet = null;
////                        try {
////                            stmt = conn.createStatement();
////                            resultSet = stmt.executeQuery("SHOW DATABASES");
////                            resultSet = stmt.executeQuery("SELECT 'HIVE CONNECTION TEST PASSED' AS STATUS");
//                        log.debug(env + ":" + ": Hive Connection Successful");
//                        rtn = Boolean.TRUE;
////                        } catch (SQLException sql) {
//                        // DB Doesn't Exists.
////                            log.error(env + ": Hive Connection check failed.", sql);
////                            rtn = Boolean.FALSE;
////                        } finally {
////                            if (resultSet != null) {
////                                try {
////                                    resultSet.close();
////                                } catch (SQLException sqlException) {
////                                     ignore
////                                }
////                            }
////                            if (stmt != null) {
////                                try {
////                                    stmt.close();
////                                } catch (SQLException sqlException) {
//                        // ignore
////                                }
////                            }
////                        }
//                    } else {
//                        log.error(env + ": Hive Connection check failed.  Connection is null.");
//                        rtn = Boolean.FALSE;
//                    }
//                } catch (SQLException se) {
//                    rtn = Boolean.FALSE;
//                    log.error(env + ": Hive Connection check failed.", se);
//                    se.printStackTrace();
//                } finally {
//                    if (conn != null) {
//                        try {
//                            log.info(env + ": Closing Connection");
//                            conn.close();
//                        } catch (Throwable throwables) {
//                            log.error(env + ": Error closing connection.", throwables);
//                        }
//                    }
//                }
//            }
//        }
//        return rtn;
//    }

    public Cluster getCluster(Environment environment) {
        Cluster cluster = getClusters().get(environment);
        if (cluster == null) {
            cluster = new Cluster();
            cluster.setConfig(this);
            switch (environment) {
                case TRANSFER:
                    cluster.setLegacyHive(getCluster(Environment.LEFT).isLegacyHive());
                    break;
                case SHADOW:
                    cluster.setLegacyHive(getCluster(Environment.RIGHT).isLegacyHive());
                    break;
            }
            getClusters().put(environment, cluster);
        }
        if (cluster.getEnvironment() == null) {
            cluster.setEnvironment(environment);
        }
        return cluster;
    }

//    public Boolean loadPartitionMetadata() {
//        if (getEvaluatePartitionLocation() ||
//                (getDataStrategy() == STORAGE_MIGRATION && getTransfer().getStorageMigration().isDistcp())) {
//            return Boolean.TRUE;
//        } else {
//            return Boolean.FALSE;
//        }
//    }


    /*
    Legacy is when one of the clusters is legacy.
     */
//    public Boolean legacyMigration() {
//        Boolean rtn = Boolean.FALSE;
//        if (getCluster(Environment.LEFT).getLegacyHive() != getCluster(Environment.RIGHT).getLegacyHive()) {
//            if (getCluster(Environment.LEFT).getLegacyHive()) {
//                rtn = Boolean.TRUE;
//            }
//        }
//        return rtn;
//    }
//
//    public Boolean getSkipStatsCollection() {
//        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
//        if (!optimization.isSkipStatsCollection()) {
//            try {
//                switch (getDataStrategy()) {
//                    case DUMP:
//                    case SCHEMA_ONLY:
//                    case EXPORT_IMPORT:
//                        optimization.setSkipStatsCollection(Boolean.TRUE);
//                        break;
//                    case STORAGE_MIGRATION:
//                        if (getTransfer().getStorageMigration().isDistcp()) {
//                            optimization.setSkipStatsCollection(Boolean.TRUE);
//                        }
//                        break;
//                }
//            } catch (NullPointerException npe) {
//                // Ignore: Caused during 'setup' since the context and config don't exist.
//            }
//        }
//        return optimization.isSkipStatsCollection();
//    }


}
