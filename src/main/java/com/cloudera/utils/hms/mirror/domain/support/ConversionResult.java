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
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/*
For RocksDB persistence, save this into its own column family 'conversionResult'.
 */
@Getter
@Setter
@Slf4j
public class ConversionResult {

    private static final DateTimeFormatter KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");

    // This would be the top level Key for the RocksDB columnFamily.
    private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);
    // This would the value of the key about.  This can't be null.

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;

    private final List<String> supportedFileSystems = new ArrayList<String>(Arrays.asList(
            "hdfs", "ofs", "s3", "s3a", "s3n", "wasb", "adls", "gf", "viewfs", "maprfs", "gs"
    ));

    @Schema(description = "Flag used to identify that this process was seeded via a Test Dataset")
    private boolean mockTestDataset = Boolean.FALSE;

    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/config', with the value being the yaml string.
     */
    private ConfigLiteDto config;
    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/config', with the value being the yaml string.
     */
    private DatasetDto dataset;
    /*
    Resolved Map of connections by Environment (LEFT/RIGHT).
     */
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
    private JobDto job;
    /*
    RunStatus tracks the progress and status of the conversion process.
      This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/runStatus', with the value being the yaml string.
     */
    @JsonIgnore
    private RunStatus runStatus;

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

    public String getTargetNamespace() throws RequiredConfigurationException {
        String rtn = null;
        if (nonNull(config.getTransfer()) && !isBlank(config.getTransfer().getTargetNamespace())) {
            rtn = config.getTransfer().getTargetNamespace();
        } else if (nonNull(getConnection(Environment.RIGHT))
                && !isBlank(getConnection(Environment.RIGHT).getHcfsNamespace())) {
            log.warn("Using RIGHT 'hcfsNamespace' for 'targetNamespace'.");
            rtn = getConnection(Environment.RIGHT).getHcfsNamespace();
        }
        if (isBlank(rtn)) {
            throw new RequiredConfigurationException("Target Namespace is required.  Please set 'targetNamespace'.");
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
//    @Deprecated(since = "4.0", forRemoval = true)
//    public DBMirror getDatabase(String database) {
//        return databases.get(database);
//    }

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