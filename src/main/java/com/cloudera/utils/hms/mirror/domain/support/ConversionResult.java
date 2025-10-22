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

import com.cloudera.utils.hms.mirror.Marker;
import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.reporting.ReportingConf;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/*
For RocksDB persistence, save this into its own column family 'conversionResult'.
 */
@Getter
@Setter
@Slf4j
public class ConversionResult implements Cloneable {

    static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // This would be the top level Key for the RocksDB columnFamily.
    private String key = df.format(new Date());
    // This would the value of the key about.  This can't be null.
    private String name;

    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/config', with the value being the yaml string.
     */
    private ConfigLiteDto configLite;
    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/config', with the value being the yaml string.
     */
    private DatasetDto dataset;
    /*
    Map of connections by Environment (LEFT/RIGHT).
    Each connection should be saved as a yaml in RocksDB. The key for this should build on the above key plus
      '/connection/{environment}', with the value being the yaml string.
     */
    private Map<Environment, ConnectionDto> connections = new HashMap<>();
    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/job', with the value being the yaml string.
     */
    private JobDto job;
    /*
    RunStatus tracks the progress and status of the conversion process.
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/runStatus', with the value being the yaml string.
     */
    private RunStatus runStatus;

    /*
    Each key in this structure is the name of a database. And will serve as the first part of the
    key that builds on the main key. EG: /database/Finance or /database/HR.

     */
    private Map<String, DBMirror> databases = new TreeMap<>();

    /**
     * @deprecated Use {@link com.cloudera.utils.hms.mirror.service.ConversionResultService#addDatabase(ConversionResult, String)} instead.
     * This method will be removed in a future release. Business logic should not reside in domain objects.
     */
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

    /**
     * @deprecated Use {@link com.cloudera.utils.hms.mirror.service.ConversionResultService#getDatabase(ConversionResult, String)} instead.
     * This method will be removed in a future release. Business logic should not reside in domain objects.
     */
    @Deprecated(since = "4.0", forRemoval = true)
    public DBMirror getDatabase(String database) {
        return databases.get(database);
    }

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
                this.runStatus.setAppVersion(com.jcabi.manifests.Manifests.read("HMS-Mirror-Version"));
            } catch (IllegalArgumentException iae) {
                this.runStatus.setAppVersion("Unknown");
            }
        }
        return runStatus;
    }
}