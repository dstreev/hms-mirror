/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.connections.ConnectionPools;
import com.cloudera.utils.hms.mirror.domain.core.*;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.FileSystems;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.isBlank;

/*
For RocksDB persistence, save this into its own column family 'conversionResult'.
 */
@Getter
@Setter
@Slf4j
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConversionResult {

    private static final DateTimeFormatter KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");

    // This would be the top level Key for the RocksDB columnFamily.
    @Schema(description = "Primary key used for RocksDB storage. Automatically generated with timestamp and UUID suffix. " +
            "This serves as the unique identifier in the RocksDB column family for conversion result persistence",
            accessMode = Schema.AccessMode.READ_ONLY,
            example = "20250115_143045123_a7f9")
    private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);
    // This would the value of the key about.  This can't be null.

    @Schema(description = "Timestamp when this conversion result was first created",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;

    @Schema(description = "Timestamp when this conversion result was last modified",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;

    @Schema(description = "List of supported Hadoop-compatible file systems for the migration. " +
            "These represent the supported URI schemes (e.g., hdfs://, s3a://, wasb://) that can be processed",
            accessMode = Schema.AccessMode.READ_ONLY)
    private final List<String> supportedFileSystems = new ArrayList<String>(Arrays.asList(
            "hdfs", "ofs", "s3", "s3a", "s3n", "wasb", "adls", "gf", "viewfs", "maprfs", "gs"
    ));

    @Schema(description = "Flag used to identify that this process was seeded via a Test Dataset")
    private boolean mockTestDataset = Boolean.FALSE;

//    @JsonIgnore
    @Schema(description = "Base output directory for migration reports and artifacts. " +
            "Defaults to user home directory under .hms-mirror/reports/",
            example = "/Users/username/.hms-mirror/reports/")
    private String outputDirectory = System.getProperty("user.home") + FileSystems.getDefault().getSeparator()
            + ".hms-mirror/reports/";
//    @JsonIgnore
    @Schema(description = "Flag indicating whether the output directory was explicitly set by the user",
            defaultValue = "false")
    private boolean userSetOutputDirectory = Boolean.FALSE;
//    @JsonIgnore
    @Schema(description = "Final resolved output directory path after all processing. " +
            "This is the actual directory where reports will be written",
            example = "/Users/username/.hms-mirror/reports/20250115_143045123")
    private String finalOutputDirectory = null;

    @JsonIgnore
    private boolean connected;

    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/config', with the value being the yaml string.
     */
    @Schema(description = "Configuration settings for this conversion. " +
            "Contains migration policies, optimization settings, and feature flags used during the conversion process",
            implementation = ConfigLiteDto.class)
    private ConfigLiteDto config;
    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/config', with the value being the yaml string.
     */
    @Schema(description = "Dataset specification for this conversion. " +
            "Defines which databases and tables are included in the migration scope",
            implementation = DatasetDto.class)
    private DatasetDto dataset;
    /*
    Resolved Map of connections by Environment (LEFT/RIGHT).
     */
    @Schema(description = "Map of cluster connections by environment (LEFT for source cluster, RIGHT for target cluster). " +
            "Each connection contains HCFS namespace, HiveServer2 settings, and metastore direct configuration",
            example = "{\"LEFT\": {...}, \"RIGHT\": {...}}")
    private Map<Environment, ConnectionDto> connections = new HashMap<>();

    @JsonIgnore
    private ConnectionPoolType connectionPoolLib; // DBCP2 is Alternate.

    /*
    Build this from the ConnectionDto's.  Not persisted, this is the actual connection pools.
     */
    @JsonIgnore
    private ConnectionPools connectionPools;

    /*
    This is a copy of the JobDto for this conversion.
     */
    @Schema(description = "Job definition for this conversion. " +
            "Contains strategy, references to dataset and config, connection references, and disaster recovery settings",
            implementation = JobDto.class)
    private JobDto job;
    /*
    RunStatus tracks the progress and status of the conversion process.
      This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/runStatus', with the value being the yaml string.
     */
    @JsonIgnore
    private RunStatus runStatus;

    @Schema(description = "Job execution settings controlling whether this is a dry run or actual execution. " +
            "When dryRun is true, no changes are made to the target cluster",
            implementation = JobExecution.class)
    private JobExecution jobExecution;

    /*
    Each key in this structure is the name of a database. And will serve as the first part of the
    key that builds on the main key. EG: /database/Finance or /database/HR.

     */
    @JsonIgnore
    private Map<String, DBMirror> databases = new TreeMap<>();

    /*
    Used to store the working translations (locations).
    This will be stored separately from the main conversion result.
    The key will be: /translator
    */
    @JsonIgnore
    private Translator translator = new Translator();

    public boolean convertManaged() {
        if (getConnection(Environment.LEFT).getPlatformType().isLegacyHive() && !getConnection(Environment.RIGHT).getPlatformType().isLegacyHive()) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public JobExecution getJobExecution() {
        if (jobExecution == null) {
            jobExecution = new JobExecution();
        }
        return jobExecution;
    }

    /**
     * The Target Namespace should be the hcfsNamespace on the Target Cluster.  When the Data Strategy is STORAGE_MIGRATION
     * then pull it from the JobDto.
     * @return
     * @throws RequiredConfigurationException
     */
    @JsonIgnore
    public String getTargetNamespace() {
        String rtn = null;
        // Pull from the Job.
        if (job.getStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
            rtn = job.getTargetNamespace();
        } else if (config.getMigrateACID().isOnly() && config.getMigrateACID().isInplace()) {
            // Downgrade Inplace only has LEFT connection
            ConnectionDto left = getConnection(Environment.LEFT);
            if (left != null) {
                rtn = left.getHcfsNamespace();
                if (isBlank(rtn)) {
                    // TODO: Try to pull from the Environment Values of the Right Cluster.
//                    right.getHs2EnvSets()
                }
            }
        } else {
            // Get it from the Right.
            ConnectionDto right = getConnection(Environment.RIGHT);
            if (right != null) {
                rtn = right.getHcfsNamespace();
                if (isBlank(rtn)) {
                    // TODO: Try to pull from the Environment Values of the Right Cluster.
//                    right.getHs2EnvSets()
                }
            }
        }
        return rtn;
    }

    /**
     * @deprecated Use {@link com.cloudera.utils.hms.mirror.service.ConversionResultService#addDatabase(ConversionResult, String)} instead.
     * This method will be removed in a future release. Business logic should not reside in domain objects.
    @Deprecated(since = "4.0", forRemoval = true)
    public DBMirror addDatabase(String database) {
        if (databases.containsKey(database)) {
            return databases.get(database);
        } else {
            DBMirror dbs = new DBMirror();
            dbs.setName(database);
            databases.put(database, dbs);
            return dbs;
        }
    }
     */

    /**
     * @deprecated Use {@link com.cloudera.utils.hms.mirror.service.ConversionResultService#getDatabase(ConversionResult, String)} instead.
     * This method will be removed in a future release. Business logic should not reside in domain objects.
     *
     * Moved to ConversionResultService.
     */

    /**
     * Get ConnectionDto for a specific environment.
     *
     * @param environment The environment (LEFT or RIGHT)
     * @return ConnectionDto for the specified environment
     */
    public ConnectionDto getConnection(Environment environment) {
        return connections.get(environment);
    }

    /**
     * Set ConnectionDto for a specific environment.
     *
     * @param environment The environment (LEFT or RIGHT)
     * @param connection The ConnectionDto to set
     */
    public void setConnection(Environment environment, ConnectionDto connection) {
        connections.put(environment, connection);
    }

    /**
     * Get RunStatus, creating it if it doesn't exist.
     * This provides lazy initialization similar to ExecuteSession.
     *
     * @return RunStatus object, never null
     */
    public RunStatus getRunStatus() {
        if (runStatus == null) {
            this.runStatus = new RunStatus();
            try {
                this.runStatus.setKey(this.getKey());
                this.runStatus.setAppVersion(com.jcabi.manifests.Manifests.read("HMS-Mirror-Version"));
            } catch (IllegalArgumentException iae) {
                this.runStatus.setAppVersion("Unknown");
            }
        }
        return runStatus;
    }
}