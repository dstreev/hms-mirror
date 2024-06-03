/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.connections.ConnectionPoolTypes;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.feature.LegacyTranslations;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.METASTORE_DIRECT_CONFIG;
import static com.cloudera.utils.hms.mirror.connections.ConnectionPoolTypes.HYBRID;
import static com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties({"featureList"})
public class HmsMirrorConfig implements Cloneable {

    @JsonIgnore
    private final String runMarker = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    @JsonIgnore
    private final List<String> flags = new LinkedList<>();
    @JsonIgnore
    private final List<String> supportFileSystems = new ArrayList<String>(Arrays.asList(
            "hdfs", "ofs", "s3", "s3a", "s3n", "wasb", "adls", "gf", "viewfs"
    ));
    @Setter
    @Getter
    @JsonIgnore
    private Date initDate = new Date();
    private Acceptance acceptance = new Acceptance();

    @Setter
    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();
    private String commandLineOptions = null;
    @JsonIgnore
    private String configFilename = null;

    private boolean copyAvroSchemaUrls = Boolean.FALSE;
    @Getter
    private ConnectionPoolTypes connectionPoolLib = HYBRID; // DBCP2 is Alternate.
    private DataStrategyEnum dataStrategy = DataStrategyEnum.SCHEMA_ONLY;
    private boolean databaseOnly = Boolean.FALSE;
    private boolean dumpTestData = Boolean.FALSE;
    /*
    See ConversionInitialization for more information on this load process.
     */
    private String loadTestDataFile = null;
    private boolean evaluatePartitionLocation = Boolean.FALSE;
    private Filter filter = new Filter();
    private boolean skipLinkCheck = Boolean.FALSE;
    private List<String> databases = new ArrayList<>();
    @JsonIgnore
    private String encryptedPassword;
    private LegacyTranslations legacyTranslations = new LegacyTranslations();
    /*
   Prefix the DB with this to create an alternate db.
   Good for testing.

   Should leave null for like replication.
    */
    private String dbPrefix = null;
    private String dbRename = null;
    private Environment dumpSource = Environment.LEFT;
    private boolean execute = Boolean.FALSE;
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
    private String outputDirectory = System.getProperty("user.home") + System.getProperty("file.separator")
            + ".hms-mirror/reports/";
    @JsonIgnore
    private String password;
    @JsonIgnore
    private String passwordKey;

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
//    @JsonIgnore
//    private ScheduledExecutorService transferThreadPool;
//    @JsonIgnore
//    private ScheduledExecutorService metadataThreadPool;
    //    @JsonIgnore
    private Translator translator = new Translator();
    @JsonIgnore
    private boolean validated = Boolean.FALSE;

    // Handle null databases from config load.
    public List<String> getDatabases() {
        if (databases == null) {
            databases = new ArrayList<>();
        }
        return databases;
    }

    public static boolean save(HmsMirrorConfig config, String configFilename, Boolean overwrite) throws IOException {
        boolean rtn = Boolean.FALSE;
        try {
            ObjectMapper mapper;
            mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String configStr = mapper.writeValueAsString(config);
            File cfgFile = new File(configFilename);
            if (cfgFile.exists() && (overwrite == null || !overwrite)) {
                log.error("Config file already exists.  Use 'overwrite' to replace.");
                throw new IOException("Config file already exists.  Use 'overwrite' to replace.");
//                return rtn;
            }
            FileWriter cfgFileWriter = null;
            try {
                cfgFileWriter = new FileWriter(cfgFile);
                cfgFileWriter.write(configStr);
                rtn = Boolean.TRUE;
                log.debug("Config 'saved' to: {}", cfgFile.getPath());
            } catch (IOException ioe) {
                log.error("Problem 'writing' config", ioe);
                throw new IOException("Problem 'writing' config", ioe);
            } finally {
                assert cfgFileWriter != null;
                cfgFileWriter.close();
            }
        } catch (JsonProcessingException e) {
            log.error("Problem 'saving' default config", e);
            throw new IOException("Problem 'saving' config", e);
        } catch (IOException ioe) {
            log.error("Problem 'closing' config file", ioe);
            throw new IOException("Problem 'closing' config file", ioe);
        }
        return rtn;
    }

    @JsonIgnore
    public Boolean loadPartitionMetadata() {
        if (isEvaluatePartitionLocation() ||
                (getDataStrategy() == STORAGE_MIGRATION &&
                        getTransfer().getStorageMigration().isDistcp())) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @JsonIgnore
    public Boolean isConnectionKerberized() {
        boolean rtn = Boolean.FALSE;

        Set<Environment> envs = getClusters().keySet();
        for (Environment env : envs) {
            Cluster cluster = getClusters().get(env);
            if (cluster.getHiveServer2() != null &&
                    cluster.getHiveServer2().isValidUri() &&
                    cluster.getHiveServer2().getUri() != null &&
                    cluster.getHiveServer2().getUri().contains("principal")) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }


    /*
        Use this to initialize a default config.
        TODO: Need to add back.
         */
    public static void setup(String configFile) throws IOException {
        HmsMirrorConfig hmsMirrorConfig = new HmsMirrorConfig();
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
                    hmsMirrorConfig.getCluster(env).setLegacyHive(Boolean.TRUE);
                } else {
                    hmsMirrorConfig.getCluster(env).setLegacyHive(Boolean.FALSE);
                }

                // hcfsNamespace
                System.out.print("What is the namespace for the " + env + " cluster? ");
                response = scanner.next();
                hmsMirrorConfig.getCluster(env).setHcfsNamespace(response);

                // HS2 URI
                System.out.print("What is the JDBC URI for the " + env + " cluster? ");
                response = scanner.next();
                HiveServer2Config hs2Cfg = new HiveServer2Config();
                hmsMirrorConfig.getCluster(env).setHiveServer2(hs2Cfg);
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
                    PartitionDiscovery pd = hmsMirrorConfig.getCluster(env).getPartitionDiscovery();
                    if (!hmsMirrorConfig.getCluster(env).isLegacyHive()) {
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

        save(hmsMirrorConfig, configFile, Boolean.FALSE);

    }

    @Override
    public HmsMirrorConfig clone() {
        try {
            HmsMirrorConfig clone = (HmsMirrorConfig) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

//    public void addError(MessageCode code) {
//        getRunStatus().addError(code);
//    }
//
//    public void addError(MessageCode code, Object... args) {
//        getRunStatus().addError(code, args);
//    }
//
//    public void addWarning(MessageCode code) {
//        getRunStatus().addWarning(code);
//    }
//
//    public void addWarning(MessageCode code, Object... args) {
//        getRunStatus().addWarning(code, args);
//    }

    public boolean convertManaged() {
        if (getCluster(Environment.LEFT).isLegacyHive() && !getCluster(Environment.RIGHT).isLegacyHive()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Cluster getCluster(Environment environment) {
        Cluster cluster = getClusters().get(environment);
        if (cluster == null) {
            cluster = new Cluster();
//            cluster.setHmsMirrorConfig(this);
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

//    public ScheduledExecutorService getMetadataThreadPool() {
//        if (metadataThreadPool == null) {
//            metadataThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
//        }
//        return metadataThreadPool;
//    }
//
//    public ScheduledExecutorService getTransferThreadPool() {
//        if (transferThreadPool == null) {
//            transferThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
//        }
//        return transferThreadPool;
//    }

    public boolean isExecute() {
        if (!execute) {
            log.trace("Dry-run: ON");
        }
        return execute;
    }

    public Boolean isFlip() {
        return flip;
    }

    @JsonIgnore
    public Boolean isLoadingTestData() {
        if (loadTestDataFile != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Boolean isReplace() {
        return replace;
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

    public void setDataStrategy(DataStrategyEnum dataStrategy) {
        this.dataStrategy = dataStrategy;
        if (this.dataStrategy != null) {
            if (this.dataStrategy == DataStrategyEnum.DUMP) {
                this.getMigrateACID().setOn(Boolean.TRUE);
                this.getMigrateVIEW().setOn(Boolean.TRUE);
                this.setMigratedNonNative(Boolean.TRUE);
            } else if (this.dataStrategy == STORAGE_MIGRATION) {
                getMigrateACID().setOn(Boolean.TRUE);
                setEvaluatePartitionLocation(Boolean.TRUE);
            }
        }
    }

    // Deal with any clean up that changes the state of the config, leaving the original intact.
    // This allow us to 'reset' the config to the original state.
    @JsonIgnore // Needed to prevent recursion.
    public HmsMirrorConfig getResolvedConfig() {
        HmsMirrorConfig resolvedConfig = this.clone();
        if (resolvedConfig.isFlip()) {
            Cluster left = resolvedConfig.getCluster(Environment.LEFT);
            left.setEnvironment(Environment.RIGHT);
            Cluster right = resolvedConfig.getCluster(Environment.RIGHT);
            right.setEnvironment(Environment.LEFT);
            resolvedConfig.getClusters().put(Environment.RIGHT, left);
            resolvedConfig.getClusters().put(Environment.LEFT, right);
            // Need to unset this, so we don't flip again if the config is cloned.
            resolvedConfig.setFlip(Boolean.FALSE);
        }
        return resolvedConfig;
    }

    public void setFlip(Boolean flip) {
        this.flip = flip;
//        if (this.flip) {
//            Cluster origLeft = getCluster(Environment.LEFT);
//            origLeft.setEnvironment(Environment.RIGHT);
//            Cluster origRight = getCluster(Environment.RIGHT);
//            origRight.setEnvironment(Environment.LEFT);
//            getClusters().put(Environment.RIGHT, origLeft);
//            getClusters().put(Environment.LEFT, origRight);
//        }
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

    public static HmsMirrorConfig loadConfig(String configFilename) {
        HmsMirrorConfig hmsMirrorConfig;
        log.info("Initializing Config.");
        // Check if absolute path.
        if (!configFilename.startsWith("/")) {
            // If filename contain a file.separator, assume the location is
            // relative to the working directory.
            if ((configFilename.contains(File.separator))) {
                // Load relative to the working directory.
                File workingDir = new File(".");
                configFilename = workingDir.getAbsolutePath() + File.separator + configFilename;
            } else {
                // Assume it's in the default config directory.
                configFilename = System.getProperty("user.home") + File.separator + ".hms-mirror/cfg"
                        + File.separator + configFilename;
            }
        }
        log.info("Loading config: {}", configFilename);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        URL cfgUrl = null;
        try {
            // Load file from classpath and convert to string
            File cfgFile = new File(configFilename);
            if (!cfgFile.exists()) {
                // Try loading from resource (classpath).  Mostly for testing.
                cfgUrl = mapper.getClass().getResource(configFilename);
                if (cfgUrl == null) {
                    throw new RuntimeException("Couldn't locate configuration file: " + configFilename);
                }
                log.info("Using 'classpath' config: {}", configFilename);
            } else {
                log.info("Using filesystem config: {}", configFilename);
                try {
                    cfgUrl = cfgFile.toURI().toURL();
                } catch (MalformedURLException mfu) {
                    throw new RuntimeException("Couldn't locate configuration file: "
                            + configFilename, mfu);
                }
            }

            String yamlCfgFile = IOUtils.toString(cfgUrl, StandardCharsets.UTF_8);
            hmsMirrorConfig = mapper.readerFor(HmsMirrorConfig.class).readValue(yamlCfgFile);

            hmsMirrorConfig.setConfigFilename(configFilename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Config loaded.");
//        log.info("Transfer Concurrency: {}", hmsMirrorConfig.getTransfer().getConcurrency());
        return hmsMirrorConfig;

    }


}
