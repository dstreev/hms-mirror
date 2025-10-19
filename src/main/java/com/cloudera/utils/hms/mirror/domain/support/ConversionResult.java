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
    // This would the the value of the key about.  This can't be null.
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
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/dataset', with the value being the yaml string.
     */
    private ConnectionDto leftConnection;
    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/connection/left', with the value being the yaml string.
     */
    private ConnectionDto rightConnection;
    /*
    This should be saved as a yaml in RocksDB.  The key for this should build on the above key plus
      '/job', with the value being the yaml string.
     */
    private JobDto job;

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
}