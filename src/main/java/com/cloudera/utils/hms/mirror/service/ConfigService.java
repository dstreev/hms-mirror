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
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HiveServer2Config;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Set;

import static com.cloudera.utils.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;

@Service
@Slf4j
@Getter
public class ConfigService {

    private org.springframework.core.env.Environment springEnv;

    private ExecuteSessionService executeSessionService;

    @Autowired
    public void setSpringEnv(org.springframework.core.env.Environment springEnv) {
        this.springEnv = springEnv;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }


    public Boolean canDeriveDistcpPlan() {
        Boolean rtn = Boolean.FALSE;
        ExecuteSession executeSession = executeSessionService.getActiveSession();
        HmsMirrorConfig hmsMirrorConfig = executeSession.getResolvedConfig();

        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
            // We need to get partition location to support partitioned tables and distcp
            if (!hmsMirrorConfig.isEvaluatePartitionLocation()) {
                if (hmsMirrorConfig.getDataStrategy() == STORAGE_MIGRATION) {
                    // This is an ERROR condition since we can NOT build the correct ALTER TABLE statements without this
                    // information.
                    rtn = Boolean.FALSE;
                    executeSession.addError(DISTCP_REQUIRES_EPL);
                } else {
                    executeSession.addWarning(DISTCP_REQUIRES_EPL);
                }
            }
            rtn = Boolean.TRUE;
        } else {
            executeSession.addWarning(DISTCP_OUTPUT_NOT_REQUESTED);
        }
        if (rtn && hmsMirrorConfig.isResetToDefaultLocation() &&
                hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() == null) {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public String getResolvedDB(String database) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        String rtn = null;
        // Set Local Value for adjustments
        String lclDb = database;
        // When dbp, set new value
        lclDb = (hmsMirrorConfig.getDbPrefix() != null ? hmsMirrorConfig.getDbPrefix() + lclDb : lclDb);
        // Rename overrides prefix, otherwise use lclDb as its been set.
        rtn = (hmsMirrorConfig.getDbRename() != null ? hmsMirrorConfig.getDbRename() : lclDb);
        return rtn;
    }

    public Boolean getSkipStatsCollection() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (!hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
            try {
                switch (hmsMirrorConfig.getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        hmsMirrorConfig.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        break;
                    case STORAGE_MIGRATION:
                        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                            hmsMirrorConfig.getOptimization().setSkipStatsCollection(Boolean.TRUE);
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return hmsMirrorConfig.getOptimization().isSkipStatsCollection();
    }

    @JsonIgnore
    public Boolean isConnectionKerberized() {
        boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        Set<Environment> envs = hmsMirrorConfig.getClusters().keySet();
        for (Environment env : envs) {
            Cluster cluster = hmsMirrorConfig.getClusters().get(env);
            if (cluster.getHiveServer2() != null &&
                    cluster.getHiveServer2().isValidUri() &&
                    cluster.getHiveServer2().getUri() != null &&
                    cluster.getHiveServer2().getUri().contains("principal")) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public Boolean legacyMigration() {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive() != hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
            if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    protected Boolean linkTest() throws DisabledException {
        Boolean rtn = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        if (hmsMirrorConfig.isSkipLinkCheck() || hmsMirrorConfig.isLoadingTestData()) {
            log.warn("Skipping Link Check.");
            rtn = Boolean.TRUE;
        } else {
            CliEnvironment cli = executeSessionService.getCliEnvironment();

            log.info("Performing Cluster Link Test to validate cluster 'hcfsNamespace' availability.");
            // TODO: develop a test to copy data between clusters.
            String leftHCFSNamespace = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace();
            String rightHCFSNamespace = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace();

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

    public Boolean loadPartitionMetadata() {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        if (hmsMirrorConfig.isEvaluatePartitionLocation() ||
                (hmsMirrorConfig.getDataStrategy() == STORAGE_MIGRATION &&
                        hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp())) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    /*
    Load a config for the default config directory.
    Check that it is valid, if not, revert to the previous config.

    TODO: Need to return an error that can be shown via the REST API.
     */
    public HmsMirrorConfig loadConfig(String configFileName) {
        HmsMirrorConfig rtn = HmsMirrorConfig.loadConfig(configFileName);
        return rtn;
    }

    public boolean saveConfig(HmsMirrorConfig config, String configFileName, Boolean overwrite) {
        return HmsMirrorConfig.save(config, configFileName, overwrite);
    }

    public void setupGSS() {
        try {
            String CURRENT_USER_PROP = "current.user";

            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

            // Get a value that over rides the default, if nothing then use default.
            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

            // Set a default
            if (hadoopConfDirProp == null)
                hadoopConfDirProp = "/etc/hadoop/conf";

            Configuration hadoopConfig = new Configuration(true);

            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(hadoopConfDir, file);
                if (f.exists()) {
                    log.debug("Adding conf resource: '{}'", f.getAbsolutePath());
                    try {
                        // I found this new Path call failed on the Squadron Clusters.
                        // Not sure why.  Anyhow, the above seems to work the same.
                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
                    } catch (Throwable t) {
                        // This worked for the Squadron Cluster.
                        // I think it has something to do with the Docker images.
                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
                    }
                }
            }

            // hadoop.security.authentication
            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                try {
                    UserGroupInformation.setConfiguration(hadoopConfig);
                } catch (Throwable t) {
                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
                    log.error("Failed GSS Init.  Attempting different Group Mapping");
                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
                    UserGroupInformation.setConfiguration(hadoopConfig);
                }
            }
        } catch (Throwable t) {
            log.error("Issue initializing Kerberos", t);
            throw t;
        }
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;

        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getActiveSession().getResolvedConfig();

        RunStatus runStatus = executeSessionService.getActiveSession().getRunStatus();
        // Reset the config validated flag.
        runStatus.setConfigValidated(Boolean.FALSE);

        // Set distcp options.
        canDeriveDistcpPlan();

        switch (hmsMirrorConfig.getDataStrategy()) {
            case DUMP:
            case STORAGE_MIGRATION:
            case ICEBERG_CONVERSION:
                break;
            default:
                if (hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3()) {
                    hmsMirrorConfig.getTranslator().setForceExternalLocation(Boolean.TRUE);
                    runStatus.addWarning(HDP3_HIVE);

                }
                // Check for INPLACE DOWNGRADE, in which case no RIGHT needs to be defined or check.
                if (!hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
                    if (hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive() &&
                            !hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive() &&
                            !hmsMirrorConfig.isDumpTestData()) {
                        runStatus.addError(NON_LEGACY_TO_LEGACY);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3() &&
                hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
            runStatus.addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (hmsMirrorConfig.getCluster(Environment.RIGHT).isHdpHive3() &&
                hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
            runStatus.addError(LEGACY_AND_HIVE3);
            rtn = Boolean.FALSE;
        }

        if (hmsMirrorConfig.getCluster(Environment.LEFT).isHdpHive3() &&
                hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            hmsMirrorConfig.getTranslator().setForceExternalLocation(Boolean.TRUE);
            if (hmsMirrorConfig.getMigrateACID().isOn() &&
                    !hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addError(HIVE3_ON_HDP_ACID_TRANSFERS);
                rtn = Boolean.FALSE;
            }
        }

        if (hmsMirrorConfig.isResetToDefaultLocation()) {
            if (!(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY ||
                    hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION ||
                    hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SQL ||
                    hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                    hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.HYBRID)) {
                runStatus.addError(RESET_TO_DEFAULT_LOCATION);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() == null ||
                    hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() == null) {
                runStatus.addError(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addWarning(RDL_DC_WARNING_TABLE_ALIGNMENT);
            }
            if (hmsMirrorConfig.getTranslator().isForceExternalLocation()) {
                runStatus.addWarning(RDL_FEL_OVERRIDES);
            }
        }

        if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.LINKED) {
            if (hmsMirrorConfig.getMigrateACID().isOn()) {
                log.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
                // TODO: Add to errors.
                throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
            }
        }

        // When RIGHT is defined
        switch (hmsMirrorConfig.getDataStrategy()) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
            case LINKED:
            case SCHEMA_ONLY:
            case CONVERT_LINKED:
                // When the storage on LEFT and RIGHT match, we need to specify both rdl (resetDefaultLocation)
                //   and use -dbp (db prefix) to identify a new db name (hence a location).
                if (hmsMirrorConfig.getCluster(Environment.RIGHT) != null &&
                        (hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace()
                                .equalsIgnoreCase(hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace()))) {
                    if (!hmsMirrorConfig.isResetToDefaultLocation()) {
                        runStatus.addError(SAME_CLUSTER_COPY_WITHOUT_RDL);
                        rtn = Boolean.FALSE;
                    }
                    if (hmsMirrorConfig.getDbPrefix() == null &&
                            hmsMirrorConfig.getDbRename() == null) {
                        runStatus.addError(SAME_CLUSTER_COPY_WITHOUT_DBPR);
                        rtn = Boolean.FALSE;
                    }
                }
        }

        if (hmsMirrorConfig.isEvaluatePartitionLocation() && !hmsMirrorConfig.isLoadingTestData()) {
            switch (hmsMirrorConfig.getDataStrategy()) {
                case SCHEMA_ONLY:
                case DUMP:
                    // Check the metastore_direct config on the LEFT.
                    if (hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                        runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    runStatus.addWarning(EVALUATE_PARTITION_LOCATION);
                    break;
                case STORAGE_MIGRATION:
                    if (hmsMirrorConfig.getCluster(Environment.LEFT).getMetastoreDirect() == null) {
                        runStatus.addError(EVALUATE_PARTITION_LOCATION_CONFIG, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    if (!hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                        runStatus.addError(EVALUATE_PARTITION_LOCATION_STORAGE_MIGRATION, "LEFT");
                        rtn = Boolean.FALSE;
                    }
                    break;
                default:
                    runStatus.addError(EVALUATE_PARTITION_LOCATION_USE);
                    rtn = Boolean.FALSE;
            }
        }

        // Only allow db rename with a single database.
        if (hmsMirrorConfig.getDbRename() != null &&
                hmsMirrorConfig.getDatabases().length > 1) {
            runStatus.addError(DB_RENAME_ONLY_WITH_SINGLE_DB_OPTION);
            rtn = Boolean.FALSE;
        }

        if (hmsMirrorConfig.isLoadingTestData()) {
            if (hmsMirrorConfig.getFilter().isTableFiltering()) {
                runStatus.addWarning(IGNORING_TBL_FILTERS_W_TEST_DATA);
            }
        }

        if (hmsMirrorConfig.isFlip() &&
                hmsMirrorConfig.getCluster(Environment.LEFT) == null) {
            runStatus.addError(FLIP_WITHOUT_RIGHT);
            rtn = Boolean.FALSE;
        }

        if (hmsMirrorConfig.getTransfer().getConcurrency() > 4 &&
                !hmsMirrorConfig.isLoadingTestData()) {
            // We need to pass on a few scale parameters to the hs2 configs so the connection pools can handle the scale requested.
            if (hmsMirrorConfig.getCluster(Environment.LEFT) != null) {
                Cluster cluster = hmsMirrorConfig.getCluster(Environment.LEFT);
                cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency() / 2));
                cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency() / 2));
                if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency()));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                    cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency()));
                }
            }
            if (hmsMirrorConfig.getCluster(Environment.RIGHT) != null) {
                Cluster cluster = hmsMirrorConfig.getCluster(Environment.RIGHT);
                if (cluster.getHiveServer2() != null) {
                    cluster.getHiveServer2().getConnectionProperties().setProperty("initialSize", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency() / 2));
                    cluster.getHiveServer2().getConnectionProperties().setProperty("minIdle", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency() / 2));
                    if (cluster.getHiveServer2().getDriverClassName().equals(HiveServer2Config.APACHE_HIVE_DRIVER_CLASS_NAME)) {
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxIdle", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency()));
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxWaitMillis", "10000");
                        cluster.getHiveServer2().getConnectionProperties().setProperty("maxTotal", Integer.toString(hmsMirrorConfig.getTransfer().getConcurrency()));
                    }
                }
            }
        }

        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                    || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.COMMON
                    || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP
                    || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.LINKED
                    || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.CONVERT_LINKED
                    || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.HYBRID) {
                runStatus.addError(DISTCP_VALID_STRATEGY);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                    && hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addWarning(STORAGE_MIGRATION_DISTCP_EXECUTE);
            }

            if (hmsMirrorConfig.getFilter().isTableFiltering()) {
                runStatus.addWarning(DISTCP_W_TABLE_FILTERS);
            } else {
                runStatus.addWarning(DISTCP_WO_TABLE_FILTERS);
            }
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SQL
                    && hmsMirrorConfig.getMigrateACID().isOn()
                    && hmsMirrorConfig.getMigrateACID().isDowngrade()
                    && (hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() == null)) {
                runStatus.addError(SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SQL) {
                // For SQL, we can only migrate ACID tables with `distcp` if we're downgrading of them.
                if (hmsMirrorConfig.getMigrateACID().isOn() ||
                        hmsMirrorConfig.getMigrateACID().isOnly()) {
                    if (!hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                        runStatus.addError(SQL_DISTCP_ONLY_W_DA_ACID);
                        rtn = Boolean.FALSE;
                    }
                }
                if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                    runStatus.addError(SQL_DISTCP_ACID_W_STORAGE_OPTS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        // Because the ACID downgrade requires some SQL transformation, we can't do this via SCHEMA_ONLY.
        if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY &&
                hmsMirrorConfig.getMigrateACID().isOn() &&
                hmsMirrorConfig.getMigrateACID().isDowngrade()) {
            runStatus.addError(ACID_DOWNGRADE_SCHEMA_ONLY);
            rtn = Boolean.FALSE;
        }

        if (hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
            if (hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.SQL) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
        }

        if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY) {
            if (!hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                if (hmsMirrorConfig.isResetToDefaultLocation()) {
                    // requires distcp.
                    runStatus.addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL);
                    rtn = Boolean.FALSE;
                }
                if (hmsMirrorConfig.getTransfer().getIntermediateStorage() != null) {
                    // requires distcp.
                    runStatus.addError(DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (hmsMirrorConfig.isResetToDefaultLocation()
                && (hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() == null)) {
            runStatus.addWarning(RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS);
        }

        if (hmsMirrorConfig.isSync()
                && (hmsMirrorConfig.getFilter().getTblRegEx() != null
                || hmsMirrorConfig.getFilter().getTblExcludeRegEx() != null)) {
            runStatus.addWarning(SYNC_TBL_FILTER);
        }
        if (hmsMirrorConfig.isSync()
                && !(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.LINKED ||
                hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SQL ||
                hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT ||
                hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.HYBRID)) {
            runStatus.addError(VALID_SYNC_STRATEGIES);
            rtn = Boolean.FALSE;
        }
        if (hmsMirrorConfig.getMigrateACID().isOn()
                && !(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SCHEMA_ONLY
                || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP
                || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.EXPORT_IMPORT
                || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.HYBRID
                || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SQL
                || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
            runStatus.addError(VALID_ACID_STRATEGIES);
            rtn = Boolean.FALSE;
        }

        // DUMP does require Execute.
        if (hmsMirrorConfig.isExecute()
                && hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP) {
            hmsMirrorConfig.setExecute(Boolean.FALSE);
        }

        if (hmsMirrorConfig.getMigrateACID().isOn()
                && hmsMirrorConfig.getMigrateACID().isInplace()) {
            if (!(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.SQL)) {
                runStatus.addError(VALID_ACID_DA_IP_STRATEGIES);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                runStatus.addError(COMMON_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getTransfer().getIntermediateStorage() != null) {
                runStatus.addError(INTERMEDIATE_STORAGE_WITH_DA_IP);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                runStatus.addError(DISTCP_W_DA_IP_ACID);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
                runStatus.addError(DA_IP_NON_LEGACY);
                rtn = Boolean.FALSE;
            }
        }

        if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            // The commonStorage and Storage Migration Namespace are the same thing.
            if (hmsMirrorConfig.getTransfer().getCommonStorage() == null) {
                // Use the same namespace, we're assuming that was the intent.
                hmsMirrorConfig.getTransfer().setCommonStorage(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace());
                // Force reset to default location.
//                this.setResetToDefaultLocation(Boolean.TRUE);
                runStatus.addWarning(STORAGE_MIGRATION_NAMESPACE_LEFT, hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace());
                if (!hmsMirrorConfig.isResetToDefaultLocation()
                        && hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
                    runStatus.addError(STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM);
                    rtn = Boolean.FALSE;
                }
            }
            if (hmsMirrorConfig.getTransfer().getWarehouse() == null ||
                    (hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() == null ||
                            hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() == null)) {
                runStatus.addError(STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS);
                rtn = Boolean.FALSE;
            }
        }

        // Because some just don't get you can't do this...
        if (hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() != null &&
                hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null) {
            // Make sure these aren't set to the same location.
            if (hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory().equals(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory())) {
                runStatus.addError(WAREHOUSE_DIRS_SAME_DIR, hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()
                        , hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory());
                rtn = Boolean.FALSE;
            }
        }

        if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.ACID) {
            runStatus.addError(ACID_NOT_TOP_LEVEL_STRATEGY);
            rtn = Boolean.FALSE;
        }

        // Test to ensure the clusters are LINKED to support underlying functions.
        switch (hmsMirrorConfig.getDataStrategy()) {
            case LINKED:
                if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                    runStatus.addError(COMMON_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
                if (hmsMirrorConfig.getTransfer().getIntermediateStorage() != null) {
                    runStatus.addError(INTERMEDIATE_STORAGE_WITH_LINKED);
                    rtn = Boolean.FALSE;
                }
            case HYBRID:
            case EXPORT_IMPORT:
            case SQL:
                // Only do link test when NOT using intermediate storage.
                if (hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2() != null
                        && !hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()
                        && hmsMirrorConfig.getTransfer().getIntermediateStorage() == null
                        && hmsMirrorConfig.getTransfer().getCommonStorage() == null) {

                    try {
                        if (!hmsMirrorConfig.getMigrateACID().isDowngradeInPlace() && !linkTest()) {
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
                break;
            case SCHEMA_ONLY:
                if (hmsMirrorConfig.isCopyAvroSchemaUrls()) {
                    log.info("CopyAVROSchemaUrls is TRUE, so the cluster must be linked to do this.  Testing...");
                    try {
                        if (!linkTest()) {
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
                if (hmsMirrorConfig.getDumpSource() == Environment.RIGHT) {
                    runStatus.addWarning(DUMP_ENV_FLIP);
                }
            case COMMON:
                break;
            case CONVERT_LINKED:
                // Check that the RIGHT cluster is NOT a legacy cluster.  No testing done in this scenario.
                if (hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive()) {
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

        if (hmsMirrorConfig.isReplace()) {
            if (hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.SQL) {
                runStatus.addError(REPLACE_ONLY_WITH_SQL);
                rtn = Boolean.FALSE;
            }
            if (hmsMirrorConfig.getMigrateACID().isOn()) {
                if (!hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                    runStatus.addError(REPLACE_ONLY_WITH_DA);
                    rtn = Boolean.FALSE;
                }
            }
        }

        if (hmsMirrorConfig.isReadOnly()) {
            switch (hmsMirrorConfig.getDataStrategy()) {
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

        if (hmsMirrorConfig.getCluster(Environment.RIGHT) != null) {
            if (hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.SCHEMA_ONLY &&
                    hmsMirrorConfig.getCluster(Environment.RIGHT).isCreateIfNotExists()) {
                runStatus.addWarning(CINE_WITH_DATASTRATEGY);
            }
        }

        if (hmsMirrorConfig.getTranslator().getOrderedGlobalLocationMap() != null) {
            // Validate that none of the 'from' maps overlap.  IE: can't have /data and /data/mydir as from locations.
            //    For items that match /data/mydir maybe confusing as to which one to adjust.
            //   OR we move this to a TreeMap and supply a custom comparator the sorts by length, then natural.  This
            //      will push longer paths to be evaluated first and once a match is found, skip further checks.

        }

        // TODO: Check the connections.
        // If the environments are mix of legacy and non-legacy, check the connections for kerberos or zk.

        // Set maxConnections to Concurrency.
        // Don't validate connections or url's if we're working with test data.
        if (!hmsMirrorConfig.isLoadingTestData()) {
            HiveServer2Config leftHS2 = hmsMirrorConfig.getCluster(Environment.LEFT).getHiveServer2();
            if (!leftHS2.isValidUri()) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_HS2_URI_INVALID);
            }

            if (leftHS2.isKerberosConnection() && leftHS2.getJarFile() != null) {
                rtn = Boolean.FALSE;
                runStatus.addError(LEFT_KERB_JAR_LOCATION);
            }

            HiveServer2Config rightHS2 = hmsMirrorConfig.getCluster(Environment.RIGHT).getHiveServer2();

            if (rightHS2 != null) {
                // TODO: Add validation for -rid (right-is-disconnected) option.
                // - Only applies to SCHEMA_ONLY, SQL, EXPORT_IMPORT, and HYBRID data strategies.
                // -
                //
                if (hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION
                        && !rightHS2.isValidUri()) {
                    if (!hmsMirrorConfig.getDataStrategy().equals(DataStrategyEnum.DUMP)) {
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
                            && (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive() != hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive())) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(KERB_ACROSS_VERSIONS);
                    }
                }
            } else {
                if (!(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION
                        || hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.DUMP)) {
                    if (!hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
                        rtn = Boolean.FALSE;
                        runStatus.addError(RIGHT_HS2_DEFINITION_MISSING);
                    }
                }
            }
        }

        if (rtn) {
            // Last check for errors.
            if (runStatus.getErrors().getReturnCode() != 0) {
                rtn = Boolean.FALSE;
            }
        }
        runStatus.setConfigValidated(rtn);
        return rtn;
    }

}
