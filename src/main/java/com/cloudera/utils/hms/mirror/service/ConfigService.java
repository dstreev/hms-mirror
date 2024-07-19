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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.*;
import com.cloudera.utils.hms.mirror.domain.support.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.STORAGE_MIGRATION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class ConfigService {

    private org.springframework.core.env.Environment springEnv;

//    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setSpringEnv(org.springframework.core.env.Environment springEnv) {
        this.springEnv = springEnv;
    }

//    @Autowired
//    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
//        this.executeSessionService = executeSessionService;
//    }

    public boolean doWareHousePlansExist(ExecuteSession session) {
        HmsMirrorConfig config = session.getConfig();
        WarehouseMapBuilder warehouseMapBuilder = config.getTranslator().getWarehouseMapBuilder();
        Warehouse warehouse = config.getTransfer().getWarehouse();
        if ((nonNull(warehouse)
                && nonNull(warehouse.getExternalDirectory())
                && nonNull(warehouse.getManagedDirectory()))
                || !warehouseMapBuilder.getWarehousePlans().isEmpty()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public boolean canDeriveDistcpPlan(ExecuteSession session) {
        boolean rtn = Boolean.FALSE;
//        ExecuteSession executeSession = executeSessionService.getActiveSession();
        HmsMirrorConfig config = session.getConfig();

        if (config.getTransfer().getStorageMigration().isDistcp()) {
            if (rtn) {
                rtn = doWareHousePlansExist(session);
            }
            // We need to get partition location to support partitioned tables and distcp
            if (!config.isEvaluatePartitionLocation()) {
                if (config.getDataStrategy() == STORAGE_MIGRATION) {
                    // This is an ERROR condition since we can NOT build the correct ALTER TABLE statements without this
                    // information.
                    rtn = Boolean.FALSE;
                    session.addError(DISTCP_REQUIRES_EPL);
                } else {
                    session.addWarning(DISTCP_REQUIRES_EPL);
                }
            }
            rtn = Boolean.TRUE;
        } else {
            session.addWarning(DISTCP_OUTPUT_NOT_REQUESTED);
        }


        return rtn;
    }

    public Boolean getSkipStatsCollection(HmsMirrorConfig config) {
//        HmsMirrorConfig config = executeSessionService.getActiveSession().getConfig();

        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (!config.getOptimization().isSkipStatsCollection()) {
            try {
                switch (config.getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        config.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        break;
                    case STORAGE_MIGRATION:
                        // TODO: This might be helpful to get so we can be more clear with the distcp process. EG: Mapper count.
                        if (config.getTransfer().getStorageMigration().isDistcp()) {
                            config.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return config.getOptimization().isSkipStatsCollection();
    }

//    @JsonIgnore
//    public Boolean isConnectionKerberized() {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        return hmsMirrorConfig.isConnectionKerberized();
//    }

    public Boolean legacyMigration(HmsMirrorConfig config) {
        Boolean rtn = Boolean.FALSE;
//        HmsMirrorConfig config = executeSessionService.getActiveSession().getConfig();

        if (config.getCluster(Environment.LEFT).isLegacyHive() != config.getCluster(Environment.RIGHT).isLegacyHive()) {
            if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    protected Boolean linkTest(ExecuteSession session, CliEnvironment cli) throws DisabledException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig config = session.getConfig();

        if (isNull(cli)) {
            return Boolean.TRUE;
        }

        if (config.isSkipLinkCheck() || config.isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {
//            CliEnvironment cli = executeSessionService.getCliEnvironment();

            log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
            // TODO: develop a test to copy data between clusters.
            String leftHCFSNamespace = config.getCluster(Environment.LEFT).getHcfsNamespace();
            String rightHCFSNamespace = config.getCluster(Environment.RIGHT).getHcfsNamespace();

            // List User Directories on LEFT
            String leftlsTestLine = "ls " + leftHCFSNamespace + "/user";
            String rightlsTestLine = "ls " + rightHCFSNamespace + "/user";
            log.info("LEFT ls testline: {}", leftlsTestLine);
            log.info("RIGHT ls testline: {}", rightlsTestLine);

            CommandReturn lcr = cli.processInput(leftlsTestLine);
            if (lcr.isError()) {
                throw new RuntimeException("Link to RIGHT cluster FAILED.\n " + lcr.getError() +
                        "\nCheck configuration and hcfsNamespace value.  " +
                        "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
            }
            CommandReturn rcr = cli.processInput(rightlsTestLine);
            if (rcr.isError()) {
                throw new RuntimeException("Link to LEFT cluster FAILED.\n " + rcr.getError() +
                        "\nCheck configuration and hcfsNamespace value.  " +
                        "Check the documentation about Linking clusters: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
            }
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

//    public Boolean loadPartitionMetadata() {
//        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();
//        return hmsMirrorConfig.loadPartitionMetadata();
//    }

    /*
    Load a config for the default config directory.
    Check that it is valid, if not, revert to the previous config.

    TODO: Need to return an error that can be shown via the REST API.
     */
    public HmsMirrorConfig loadConfig(String configFileName) {
        HmsMirrorConfig rtn = HmsMirrorConfig.loadConfig(configFileName);
        return rtn;
    }

    public boolean saveConfig(HmsMirrorConfig config, String configFileName, Boolean overwrite) throws IOException {
        return HmsMirrorConfig.save(config, configFileName, overwrite);
    }

    public void overlayConfig(HmsMirrorConfig config, HmsMirrorConfig overlay) {
//        config.overlay(overlay);
        List<Environment> envs = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment env : envs) {
            if (nonNull(config.getCluster(env))) {
                if (nonNull(overlay.getCluster(env))) {
                    if (nonNull(config.getCluster(env).getHiveServer2())) {
                        if (nonNull(overlay.getCluster(env).getHiveServer2())) {
                            config.getCluster(env).setHiveServer2(overlay.getCluster(env).getHiveServer2());
                        }
                    }
                    if (nonNull(config.getCluster(env).getMetastoreDirect())) {
                        if (nonNull(overlay.getCluster(env).getMetastoreDirect())) {
                            config.getCluster(env).setMetastoreDirect(overlay.getCluster(env).getMetastoreDirect());
                        }
                    }
                }
            }
        }

        // Set Encrypted Status of Passwords.
        config.setEncryptedPasswords(overlay.isEncryptedPasswords());

        setDefaultsForDataStrategy(config);
    }

    /*
    Apply a set of rules based on a hierarchy of settings and make adjustments to other configuration elements.
     */
    public void fixConfigSettings(HmsMirrorConfig config) {
        // Making sure the Metastore Direct is available under certain conditions.
        if (config.isEvaluatePartitionLocation()
                || config.getTransfer().getStorageMigration().isDistcp()) {
            // Ensure the LEFT Metastore exists.
            if (nonNull(config.getCluster(Environment.LEFT)) && isNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                config.getCluster(Environment.LEFT).setMetastoreDirect(new DBStore());
            }
        }

        // If the RIGHT Metastore Direct is set, remove it.  Not needed by any process.
        if (nonNull(config.getCluster(Environment.RIGHT)) &&
                nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
            config.getCluster(Environment.RIGHT).setMetastoreDirect(null);
        }

        switch (config.getDataStrategy()) {
            case DUMP:
                break;
            case STORAGE_MIGRATION:
                break;
            case SCHEMA_ONLY:
                // Sync process is read-only and no-purge to ensure data is not deleted.
                // Because we'll assume that the data is being copied through other processes.
                if (config.isSync()) {
                    config.setReadOnly(Boolean.TRUE);
                    config.setNoPurge(Boolean.TRUE);
                }
                break;
            case SQL:
                break;
            case EXPORT_IMPORT:
                break;
            case HYBRID:
                break;
            case COMMON:
                break;
            case LINKED:
                // Sync process is read-only and no-purge to ensure data is not deleted.
                // Because we'll assume that the data is being copied through other processes.
                if (config.isSync()) {
                    config.setReadOnly(Boolean.TRUE);
                    config.setNoPurge(Boolean.TRUE);
                }
                break;
            default:
                break;

        }
    }

    public void setDefaultsForDataStrategy(HmsMirrorConfig config) {
        // Set Attribute for the config.
        switch (config.getDataStrategy()) {
            case DUMP:
                config.setEvaluatePartitionLocation(Boolean.FALSE);
                break;
            case STORAGE_MIGRATION:
                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.DISTCP);
                break;
            case SCHEMA_ONLY:
                config.setEvaluatePartitionLocation(Boolean.FALSE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                break;
            case SQL:
                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case EXPORT_IMPORT:
                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.EXPORT_IMPORT);
                break;
            case HYBRID:
                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.HYBRID);
                break;
            case COMMON:
                config.setEvaluatePartitionLocation(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.SQL);
                break;
            case LINKED:
                config.setEvaluatePartitionLocation(Boolean.FALSE);
                // Set to ensure 'dr' doesn't delete LINKED tables.
                config.setNoPurge(Boolean.TRUE);
                config.setReadOnly(Boolean.TRUE);
                config.getTransfer().getStorageMigration().setDataMovementStrategy(DataMovementStrategyEnum.MANUAL);
                config.getMigrateACID().setOn(Boolean.FALSE);
                break;
            default:
                break;
        }

    }

    public HmsMirrorConfig createForDataStrategy(DataStrategyEnum dataStrategy) {
        HmsMirrorConfig rtn = new HmsMirrorConfig();
        rtn.setDataStrategy(dataStrategy);

        switch (dataStrategy) {
            case DUMP:
                rtn.getMigrateACID().setOn(Boolean.TRUE);
                Cluster leftDump = new Cluster();
                leftDump.setEnvironment(Environment.LEFT);
                leftDump.setHiveServer2(new HiveServer2Config());
                leftDump.setMetastoreDirect(new DBStore());
                leftDump.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.LEFT, leftDump);
                break;
            case STORAGE_MIGRATION:
                rtn.getMigrateACID().setOn(Boolean.TRUE);
                Cluster leftSM = new Cluster();
                leftSM.setEnvironment(Environment.LEFT);
                leftSM.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.LEFT, leftSM);
                leftSM.setMetastoreDirect(new DBStore());
                leftSM.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                rtn.getTransfer().setCommonStorage("ofs://NEED_TO_SET_THIS");
                leftSM.setHiveServer2(new HiveServer2Config());
                break;
            case SCHEMA_ONLY:
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
                Cluster leftT = new Cluster();
                leftT.setEnvironment(Environment.LEFT);
                leftT.setLegacyHive(Boolean.TRUE);
                rtn.getClusters().put(Environment.LEFT, leftT);
                leftT.setMetastoreDirect(new DBStore());
                leftT.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                leftT.setHiveServer2(new HiveServer2Config());
                Cluster rightT = new Cluster();
                rightT.setEnvironment(Environment.RIGHT);
                rightT.setHiveServer2(new HiveServer2Config());
                rightT.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.RIGHT, rightT);
                break;
            case COMMON:
                Cluster leftC = new Cluster();
                leftC.setEnvironment(Environment.LEFT);
                leftC.setLegacyHive(Boolean.TRUE);
                rtn.getClusters().put(Environment.LEFT, leftC);
                leftC.setMetastoreDirect(new DBStore());
                leftC.getMetastoreDirect().setType(DBStore.DB_TYPE.MYSQL);
                rtn.getTransfer().setCommonStorage("hdfs|s3a|ofs://NEED_TO_SET_THIS");
                leftC.setHiveServer2(new HiveServer2Config());
                Cluster rightC = new Cluster();
                rightC.setEnvironment(Environment.RIGHT);
                rightC.setLegacyHive(Boolean.FALSE);
                rtn.getClusters().put(Environment.RIGHT, rightC);
                break;
//            case ICEBERG_CONVERSION:
//                break;
            default:
                break;
        }

        setDefaultsForDataStrategy(rtn);
        return rtn;
    }

//    public void setupGSS() {
//        try {
//            String CURRENT_USER_PROP = "current.user";
//
//            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
//            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};
//
//            // Get a value that over rides the default, if nothing then use default.
//            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");
//
//            // Set a default
//            if (hadoopConfDirProp == null)
//                hadoopConfDirProp = "/etc/hadoop/conf";
//
//            Configuration hadoopConfig = new Configuration(true);
//
//            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
//            for (String file : HADOOP_CONF_FILES) {
//                File f = new File(hadoopConfDir, file);
//                if (f.exists()) {
//                    log.debug("Adding conf resource: '{}'", f.getAbsolutePath());
//                    try {
//                        // I found this new Path call failed on the Squadron Clusters.
//                        // Not sure why.  Anyhow, the above seems to work the same.
//                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
//                    } catch (Throwable t) {
//                        // This worked for the Squadron Cluster.
//                        // I think it has something to do with the Docker images.
//                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
//                    }
//                }
//            }
//
//            // hadoop.security.authentication
//            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
//                try {
//                    UserGroupInformation.setConfiguration(hadoopConfig);
//                } catch (Throwable t) {
//                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
//                    log.error("Failed GSS Init.  Attempting different Group Mapping");
//                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
//                    UserGroupInformation.setConfiguration(hadoopConfig);
//                }
//            }
//        } catch (Throwable t) {
//            log.error("Issue initializing Kerberos", t);
//            throw t;
//        }
//    }

    public Boolean validateForConnections(ExecuteSession session) {
        Boolean rtn = Boolean.TRUE;

        HmsMirrorConfig config = session.getConfig();

        RunStatus runStatus = session.getRunStatus();

        if (config.isEncryptedPasswords()) {
            runStatus.addWarning(PASSWORDS_ENCRYPTED);
            if (isNull(config.getPasswordKey()) || config.getPasswordKey().isEmpty()) {
                runStatus.addError(PKEY_PASSWORD_CFG);
                rtn = Boolean.FALSE;
            }
        }

        List<Environment> envList = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        Set<Environment> envSet = new TreeSet<>(envList);
        // Validate that the cluster properties are set for the data strategy.
        switch (config.getDataStrategy()) {
            // Need both clusters defined and HS2 configs set.
            case SQL:
                // Inplace Downgrade is a single cluster effort
                if (config.getMigrateACID().isDowngradeInPlace()) {
                    envSet.remove(Environment.RIGHT);
                    // Drop the Right cluster to prevent confusion.
                    config.getClusters().remove(Environment.RIGHT);
                }
            case SCHEMA_ONLY:
            case EXPORT_IMPORT:
            case HYBRID:
            case COMMON:
                for (Environment env : envSet) {
                    if (isNull(config.getCluster(env))) {
                        runStatus.addError(CLUSTER_NOT_DEFINED_OR_CONFIGURED, env);
                        rtn = Boolean.FALSE;
                    } else {
                        if (isNull(config.getCluster(env).getHiveServer2())) {
                            if (!config.isLoadingTestData()) {
                                runStatus.addError(HS2_NOT_DEFINED_OR_CONFIGURED, env);
                                rtn = Boolean.FALSE;
                            }
                        } else {
                            // Check the config values.
                            /*
                            Minimum Values:
                            - uri
                            - driverClassName
                            - username (if not kerberized)

                             */
                            HiveServer2Config hs2 = config.getCluster(env).getHiveServer2();
                            if (!config.isLoadingTestData()) {
                                if (isBlank(hs2.getUri())) {
                                    runStatus.addError(MISSING_PROPERTY, "uri", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                                if (isBlank(hs2.getDriverClassName())) {
                                    runStatus.addError(MISSING_PROPERTY, "driverClassName", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                                if (!hs2.isKerberosConnection() && isBlank(hs2.getConnectionProperties().getProperty("user"))) {
                                    runStatus.addError(MISSING_PROPERTY, "user", "HiveServer2", env);
                                    rtn = Boolean.FALSE;
                                }
                            }
                        }
                    }
                    // If evaluate partition locations is set, we need metastore_direct set on LEFT.
                    if (env == Environment.LEFT) {
                        if (config.isEvaluatePartitionLocation()) {
                            if (isNull(config.getCluster(env).getMetastoreDirect())) {
                                if (!config.isLoadingTestData()) {
                                    runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, env);
                                    rtn = Boolean.FALSE;
                                }
                            } else {
                                // Check the config values;
                                    /* Minimum Values:
                                    - type
                                    - uri
                                    - username
                                    - password
                                     */
                                if (!config.isLoadingTestData()) {
                                    DBStore dbStore = config.getCluster(env).getMetastoreDirect();
                                    if (isNull(dbStore.getType())) {
                                        runStatus.addError(MISSING_PROPERTY, "type", "Metastore Direct", env);
                                        rtn = Boolean.FALSE;
                                    }
                                    if (isBlank(dbStore.getUri())) {
                                        runStatus.addError(MISSING_PROPERTY, "uri", "Metastore Direct", env);
                                        rtn = Boolean.FALSE;
                                    }
                                    if (isBlank(dbStore.getConnectionProperties().getProperty("user"))) {
                                        runStatus.addError(MISSING_PROPERTY, "user", "Metastore Direct", env);
                                        rtn = Boolean.FALSE;
                                    }
                                    if (isBlank(dbStore.getConnectionProperties().getProperty("password"))) {
                                        runStatus.addError(MISSING_PROPERTY, "password", "Metastore Direct", env);
                                        rtn = Boolean.FALSE;
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            // Need Left cluster defined with HS2 config.
            case STORAGE_MIGRATION:
                // Check for Metastore Direct on LEFT.
            case DUMP:
                // Drop the Right cluster to prevent confusion.
                config.getClusters().remove(Environment.RIGHT);
                break;

        }

        if (config.isEvaluatePartitionLocation() && !config.isLoadingTestData()) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case DUMP:
                case STORAGE_MIGRATION:
                    // Check the metastore_direct config on the LEFT.
                    if (isNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                        runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    runStatus.addWarning(EVALUATE_PARTITION_LOCATION);
                    break;
//                    if (isNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
//                        runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
//                        rtn = Boolean.FALSE;
//                    }
//                    if (!config.getTransfer().getStorageMigration().isDistcp()) {
//                        runStatus.addError(EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION, "LEFT");
//                        rtn = Boolean.FALSE;
//                    }
//                    break;
                default:
                    runStatus.addError(EVALUATE_PARTITION_LOCATION_USE);
                    rtn = Boolean.FALSE;
            }
        }

        // Set maxConnections to Concurrency.
        // Don't validate connections or url's if we're working with test data.
        if (!config.isLoadingTestData()) {
            HiveServer2Config leftHS2 = config.getCluster(Environment.LEFT).getHiveServer2();
            if (!leftHS2.isValidUri()) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_HS2_URI_INVALID);
            }

            if (leftHS2.isKerberosConnection() && !isBlank(leftHS2.getJarFile())) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_KERB_JAR_LOCATION);
            }


            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                // TODO: Add validation for -rid (right-is-disconnected) option.
                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
                // -
                //
                HiveServer2Config rightHS2 = config.getCluster(Environment.RIGHT).getHiveServer2();

                if (config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION
                        && !rightHS2.isValidUri()) {
                    if (!config.getDataStrategy().equals(DataStrategyEnum.DUMP)) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_URI_INVALID);
                    }
                } else {

                    if (rightHS2.isKerberosConnection()
                            && rightHS2.getJarFile() != null) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_KERB_JAR_LOCATION);
                    }

                    if (leftHS2.isKerberosConnection()
                            && rightHS2.isKerberosConnection()
                            && (config.getCluster(Environment.LEFT).isLegacyHive() != config.getCluster(Environment.RIGHT).isLegacyHive())) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(KERB_ACROSS_VERSIONS);
                    }
                }
            } else {
                if (!(config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                        || config.getDataStrategy() == DataStrategyEnum.DUMP)) {
                    if (!config.getMigrateACID().isDowngradeInPlace()) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_DEFINITION_MISSING);
                    }
                }
            }
        }

        return rtn;
    }

    public Boolean validate(ExecuteSession session, CliEnvironment cli, boolean connectionCheckOnly) {
        Boolean rtn = Boolean.TRUE;

        HmsMirrorConfig config = session.getConfig();

        RunStatus runStatus = session.getRunStatus();

        rtn = validateForConnections(session);
        if (connectionCheckOnly) {
            return rtn;
        }

        // Reset the config validated flag.
//        runStatus.setConfigValidated(Boolean.FALSE);

        // Set distcp options.
        canDeriveDistcpPlan(session);

        switch (config.getDataStrategy()) {
            case DUMP:
                break;
            case STORAGE_MIGRATION:
                // Ensure we have the metastore_direct config set.
                if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                    runStatus.addError(STORAGE_MIGRATION_NOT_AVAILABLE_FOR_LEGACY, Environment.LEFT);
                    rtn = Boolean.FALSE;
                }
                if (isNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                    runStatus.addError(METASTORE_DIRECT_CONFIG, Environment.LEFT);
                    rtn = Boolean.FALSE;
                }
                break;
            case ICEBERG_CONVERSION:
                break;
            default:
                if (nonNull(config.getCluster(Environment.RIGHT)) && config.getCluster(Environment.RIGHT).isHdpHive3()) {
                    config.getTranslator().setForceExternalLocation(Boolean.TRUE);
                    runStatus.addWarning(HDP3_HIVE);

                }
                // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
                if (!config.getMigrateACID().isDowngradeInPlace()) {
                    if (config.getCluster(Environment.RIGHT).isLegacyHive() &&
                            !config.getCluster(Environment.LEFT).isLegacyHive() &&
                            !config.isDumpTestData()) {
                        runStatus.addError(NON_LEGACY_TO_LEGACY);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (config.isEvaluatePartitionLocation() && !config.isLoadingTestData()) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case DUMP:
                case STORAGE_MIGRATION:
                    if (isNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                        runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
//                    } else if (!config.getTransfer().getStorageMigration().isDistcp()) {
//                        runStatus.addError(EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION, "LEFT");
//                        rtn = Boolean.FALSE;
                    }
                    runStatus.addWarning(EVALUATE_PARTITION_LOCATION);
                    break;
                default:
                    runStatus.addError(EVALUATE_PARTITION_LOCATION_USE);
                    rtn = Boolean.FALSE;
            }
        }


        if (config.getCluster(Environment.LEFT).isHdpHive3() &&
                config.getCluster(Environment.LEFT).isLegacyHive()) {
            runStatus.addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (nonNull(config.getCluster(Environment.RIGHT)) && config.getCluster(Environment.RIGHT).isHdpHive3() &&
                config.getCluster(Environment.RIGHT).isLegacyHive()) {
            runStatus.addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (config.getCluster(Environment.LEFT).isHdpHive3() &&
                config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            config.getTranslator().setForceExternalLocation(Boolean.TRUE);
            if (config.getMigrateACID().isOn() &&
                    !config.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addError(HIVE3_ON_HDP_ACID_TRANSFERS);
                rtn = Boolean.FALSE;
            }
        }

        if (config.isResetToDefaultLocation()) {
            if (!(config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY ||
                    config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                    config.getDataStrategy() == DataStrategyEnum.SQL ||
                    config.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                    config.getDataStrategy() == DataStrategyEnum.HYBRID ||
                    config.getDataStrategy() == DataStrategyEnum.COMMON)) {
                runStatus.addError(RESET_TO_DEFAULT_LOCATION, config.getDataStrategy());
                rtn = Boolean.FALSE;
            }
            switch (config.getDataStrategy()) {
                case STORAGE_MIGRATION:
                    // Need to check that we've set some database Warehouse Plans.
                    if (nonNull(config.getTransfer().getWarehouse())) {
                        if (isBlank(config.getTransfer().getWarehouse().getManagedDirectory()) ||
                                isBlank(config.getTransfer().getWarehouse().getExternalDirectory())) {
                            if (config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
                                runStatus.addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                                rtn = Boolean.FALSE;
                            }
                        }
                    } else {
                        // Are there any Warehouse Plans.
                        if (nonNull(config.getTranslator().getWarehouseMapBuilder())) {
                            if (isNull(config.getTranslator().getWarehouseMapBuilder().getWarehousePlans())
                                    || config.getTranslator().getWarehouseMapBuilder().getWarehousePlans().isEmpty()) {
                                runStatus.addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                                rtn = Boolean.FALSE;
                            }
                        } else {
                            runStatus.addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                            rtn = Boolean.FALSE;
                        }
                    }
                    break;
                default:
                    // Check for warehouse settings.  This will be a warning now that we have the Warehouse Plans
                    // available for a database that is more specific.
                    if (nonNull(config.getTransfer().getWarehouse())) {
                        if (isBlank(config.getTransfer().getWarehouse().getManagedDirectory()) ||
                                isBlank(config.getTransfer().getWarehouse().getExternalDirectory())) {
                            runStatus.addWarning(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                        }
                    } else {
                        runStatus.addWarning(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                    }
                    break;
            }
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addWarning(RDL_DC_WARNING_TABLE_ALIGNMENT);
            }
            if (config.getTranslator().isForceExternalLocation()) {
                runStatus.addWarning(RDL_FEL_OVERRIDES);
            }
        }

        if (config.getDataStrategy() == DataStrategyEnum.LINKED) {
            if (config.getMigrateACID().isOn()) {
                log.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
                // TODO: Add to errors.
                throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
            }
        }

        if (nonNull(config.getCluster(Environment.LEFT)) &&
                isNull(config.getCluster(Environment.LEFT).getHcfsNamespace())) {
            runStatus.addError(LEFT_NAMESPACE_NOT_DEFINED);
            rtn = Boolean.FALSE;
        }

        // When RIGHT is defined
        switch (config.getDataStrategy()) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
                // When the storage on LEFT and RIGHT match, we need to specify both rdl (resetDefaultLocation)
                //   and use -dbp (db prefix) to identify a new db name (hence a location).
                try {
                    if (nonNull(config.getCluster(Environment.RIGHT)) &&
                            isNull(config.getCluster(Environment.RIGHT).getHcfsNamespace())) {
                        runStatus.addError(RIGHT_NAMESPACE_NOT_DEFINED);
                        rtn = Boolean.FALSE;
                    } else {
                        if (nonNull(config.getCluster(Environment.RIGHT))
                                && nonNull(config.getCluster(Environment.LEFT).getHcfsNamespace())
                                && (config.getCluster(Environment.LEFT).getHcfsNamespace()
                                .equalsIgnoreCase(config.getCluster(Environment.RIGHT).getHcfsNamespace()))) {
                            if (!config.isResetToDefaultLocation()) {
                                runStatus.addError(SAME_CLUSTER_COPY_WITHOUT_RDL);
                                rtn = Boolean.FALSE;
                            }
                            if (isBlank(config.getDbPrefix()) &&
                                    isBlank(config.getDbRename())) {
                                runStatus.addError(SAME_CLUSTER_COPY_WITHOUT_DBPR);
                                rtn = Boolean.FALSE;
                            }
                        }
                    }
                } catch (NullPointerException npe) {
                    // Ignore and continue.  NPE causing fields would be caught elsewhere
                }
        }

        // Only allow db rename with a single database.
        if (config.getDbRename() != null &&
                config.getDatabases().size() > 1) {
            runStatus.addError(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION);
            rtn = Boolean.FALSE;
        }

        if (config.isLoadingTestData()) {
            if (config.getFilter().isTableFiltering()) {
                runStatus.addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
            }
        }

        if (config.getTransfer().getStorageMigration().isDistcp()) {
            if (config.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                    || config.getDataStrategy() == DataStrategyEnum.COMMON
                    || config.getDataStrategy() == DataStrategyEnum.DUMP
                    || config.getDataStrategy() == DataStrategyEnum.LINKED
                    || config.getDataStrategy() == DataStrategyEnum.CONVERT_LINKED
                    || config.getDataStrategy() == DataStrategyEnum.HYBRID) {
                runStatus.addError(DISTCP_VALID_STRATEGY, config.getDataStrategy());
                rtn = Boolean.FALSE;
            }
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                    && config.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addWarning(STORAGE_MIGRATION_DISTCP_EXECUTE);
            }

            if (config.getFilter().isTableFiltering()) {
                runStatus.addWarning(DISTCP_W_TABLE_FILTERS);
            } else {
                runStatus.addWarning(DISTCP_WO_TABLE_FILTERS);
            }
            if (config.getDataStrategy() == DataStrategyEnum.SQL
                    && config.getMigrateACID().isOn()
                    && config.getMigrateACID().isDowngrade()
                    && (!doWareHousePlansExist(session))) {
                runStatus.addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn = Boolean.FALSE;
            }
            if (config.getDataStrategy() == DataStrategyEnum.SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (config.getMigrateACID().isOn() ||
                        config.getMigrateACID().isOnly()) {
                    if (!config.getMigrateACID().isDowngrade()) {
                        runStatus.addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn = Boolean.FALSE;
                    }
                }
                if (!isBlank(config.getTransfer().getCommonStorage())) {
                    runStatus.addError(SQL_DISTCP_ACID_W_STORAGE_OPTS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
        if (config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY &&
                config.getMigrateACID().isOn() &&
                config.getMigrateACID().isDowngrade()) {
            runStatus.addError(ACID_DOWNGRADE_SCHEMA_ONLY);
            rtn = Boolean.FALSE;
        }

        if (config.getMigrateACID().isDowngradeInPlace()) {
            if (config.getDataStrategy() != DataStrategyEnum.SQL) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
        }

        if (config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY) {
            if (!config.getTransfer().getStorageMigration().isDistcp()) {
                if (config.isResetToDefaultLocation()) {
                    // requires distcp.
                    runStatus.addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL);
                    rtn = Boolean.FALSE;
                }
                if (config.getTransfer().getIntermediateStorage() != null) {
                    // requires distcp.
                    runStatus.addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (config.isSync()
                && (config.getFilter().getTblRegEx() != null
                || config.getFilter().getTblExcludeRegEx() != null)) {
            runStatus.addWarning(SYNC_TBL_FILTER);
        }
        if (config.isSync()
                && !(config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || config.getDataStrategy() == DataStrategyEnum.LINKED ||
                config.getDataStrategy() == DataStrategyEnum.SQL ||
                config.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                config.getDataStrategy() == DataStrategyEnum.HYBRID)) {
            runStatus.addError(VALID_SYNC_STRATEGIES);
            rtn = Boolean.FALSE;
        }
        if (config.getMigrateACID().isOn()
                && !(config.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || config.getDataStrategy() == DataStrategyEnum.DUMP
                || config.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                || config.getDataStrategy() == DataStrategyEnum.HYBRID
                || config.getDataStrategy() == DataStrategyEnum.SQL
                || config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            runStatus.addError(VALID_ACID_STRATEGIES);
            rtn = Boolean.FALSE;
        }

        // DUMP does require Execute.
        if (config.isExecute()
                && config.getDataStrategy() == DataStrategyEnum.DUMP) {
            config.setExecute(Boolean.FALSE);
        }

        if (config.getMigrateACID().isOn()
                && config.getMigrateACID().isInplace()) {
            if (!(config.getDataStrategy() == DataStrategyEnum.SQL)) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
            if (config.getTransfer().getCommonStorage() != null) {
                runStatus.addError(COMMON_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (config.getTransfer().getIntermediateStorage() != null) {
                runStatus.addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (config.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addError(DISTCP_W_DA_IP_ACID);
                rtn = Boolean.FALSE;
            }
            if (config.getCluster(Environment.LEFT).isLegacyHive()) {
                runStatus.addError(DA_IP_NON_LEGACY);
                rtn = Boolean.FALSE;
            }
        }

        if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            // The commonStorage and Storage Migration Namespace are the same thing.
            if (isBlank(config.getTransfer().getCommonStorage())) {
                // Use the same namespace, we're assuming that was the intent.
                config.getTransfer().setCommonStorage(config.getCluster(Environment.LEFT).getHcfsNamespace());
                // Force reset to default location.
//                this.setResetToDefaultLocation(Boolean.TRUE);
                runStatus.addWarning(STORAGE_MIGRATION_NAMESPACE_LEFT, config.getCluster(Environment.LEFT).getHcfsNamespace());
                if (!config.isResetToDefaultLocation()
                        && config.getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                    runStatus.addError(STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM);
                    rtn = Boolean.FALSE;
                }
            }
            // If the warehouses aren't set and there are no GLM entries...
            if (isNull(config.getTransfer().getWarehouse()) ||
                    (isNull(config.getTransfer().getWarehouse().getManagedDirectory()) ||
                            isNull(config.getTransfer().getWarehouse().getExternalDirectory()))) {
                if (!doWareHousePlansExist(session)) {
                    runStatus.addError(STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS);
                    rtn = Boolean.FALSE;
                } else {
                    // Check if there are any GLM's defined.
                    if (config.getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                        runStatus.addError(STORAGE_MIGRATION_GLMS_NOT_BUILT);
                        rtn = Boolean.FALSE;
                    }
                }
            }
        }

        // Because some just don't get you can't do this...
        if (nonNull(config.getTransfer().getWarehouse())) {
            if ((config.getTransfer().getWarehouse().getManagedDirectory() != null) &&
                    (config.getTransfer().getWarehouse().getExternalDirectory() != null)) {
                // Make sure these aren't set to the same location.
                if (config.getTransfer().getWarehouse().getManagedDirectory().equals(config.getTransfer().getWarehouse().getExternalDirectory())) {
                    runStatus.addError(WAREHOUSE_DIRS_SAME_DIR, config.getTransfer().getWarehouse().getExternalDirectory()
                            , config.getTransfer().getWarehouse().getManagedDirectory());
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (config.getDataStrategy() == DataStrategyEnum.ACID) {
            runStatus.addError(ACID_NOT_TOP_LEVEL_STRATEGY);
            rtn = Boolean.FALSE;
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (config.getDataStrategy()) {
            case LINKED:
                if (config.getTransfer().getCommonStorage() != null) {
                    runStatus.addError(COMMON_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
                if (config.getTransfer().getIntermediateStorage() != null) {
                    runStatus.addError(INTERMEDIATE_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                // Downgrade inplace is a single cluster effort.
                if (!config.getMigrateACID().isDowngradeInPlace()) {
                    if (config.getCluster(Environment.RIGHT).getHiveServer2() != null
                            && !config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                            && isBlank(config.getTransfer().getIntermediateStorage())
                            && isBlank(config.getTransfer().getCommonStorage())) {

                        try {
                            if (!config.getMigrateACID().isDowngradeInPlace() && !linkTest(session, cli)) {
                                runStatus.addError(LINK_TEST_FAILED);
                                rtn = Boolean.FALSE;
                            }
                        } catch (DisabledException e) {
                            log.error("Link Test Skipped because the CLI Interface is disabled.");
                            runStatus.addError(LINK_TEST_FAILED);
                            rtn = Boolean.FALSE;
                        }
                    } else {
                        runStatus.addWarning(LINK_TEST_SKIPPED_WITH_IS);
                    }
                }
                break;
            case SCHEMA_ONLY:
                if (config.isCopyAvroSchemaUrls()) {
                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    try {
                        if (!linkTest(session, cli)) {
                            runStatus.addError(LINK_TEST_FAILED);
                            rtn = Boolean.FALSE;
                        }
                    } catch (DisabledException e) {
                        log.error("Link Test Skipped because the CLI Interface is disabled.");
                        runStatus.addError(LINK_TEST_FAILED);
                        rtn = Boolean.FALSE;
                    }
                }
                break;
            case DUMP:
                if (config.getDumpSource() == Environment.RIGHT) {
                    runStatus.addWarning(DUMP_ENV_FLIP);
                }
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (config.getCluster(Environment.RIGHT).isLegacyHive()) {
                    runStatus.addError(LEGACY_HIVE_RIGHT_CLUSTER);
                    rtn = Boolean.FALSE;
                }
                break;
        }

        // Check the use of downgrades and replace.
        // Removing.  If you use -ma, you'll also be processing non-ACID tables.
        // Logic further down will check for this.
//        if (getConfig().getMigrateACID().isDowngrade()) {
//            if (!getConfig().getMigrateACID().isOn()) {
//                getConfig().addError(DOWNGRADE_ONLY_FOR_ACID);
//                rtn = Boolean.FALSE;
//            }
//        }

        if (config.isReplace()) {
            if (config.getDataStrategy() != DataStrategyEnum.SQL) {
                runStatus.addError(REPLACE_ONLY_WITH_SQL);
                rtn = Boolean.FALSE;
            }
            if (config.getMigrateACID().isOn()) {
                if (!config.getMigrateACID().isDowngrade()) {
                    runStatus.addError(REPLACE_ONLY_WITH_DA);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (config.isReadOnly()) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                case SQL:
                    break;
                default:
                    runStatus.addError(RO_VALID_STRATEGIES);
                    rtn = Boolean.FALSE;
            }
        }

        if (config.getCluster(Environment.RIGHT) != null) {
            if (config.getDataStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
                    config.getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                runStatus.addWarning(CINE_WITH_DATASTRATEGY);
            }
        }

        if (config.getTranslator().getOrderedGlobalLocationMap() != null) {
            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
            //    For items that match /data/mydir maybe confusing as to which one to adjust.
            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
            //      will push longer paths to be evaluated first and once a match is found, skip further checks.

        }

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.


        if (rtn) {
            // Last check for errors.
            if (runStatus.hasErrors()) {
                rtn = Boolean.FALSE;
            }
        }
//        runStatus.setConfigValidated(rtn);
        return rtn;
    }

    public HmsMirrorConfig flipConfig(HmsMirrorConfig config) {
        if (config != null) {
            Cluster leftClone = config.getCluster(Environment.LEFT).clone();
            leftClone.setEnvironment(Environment.RIGHT);
            Cluster rightClone = config.getCluster(Environment.RIGHT).clone();
            rightClone.setEnvironment(Environment.LEFT);
            config.getClusters().put(Environment.RIGHT, leftClone);
            config.getClusters().put(Environment.LEFT, rightClone);
        }
        return config;
    }

}
