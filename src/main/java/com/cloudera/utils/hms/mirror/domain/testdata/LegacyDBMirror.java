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

package com.cloudera.utils.hms.mirror.domain.testdata;

import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.cloudera.utils.hms.mirror.MirrorConf.DB_LOCATION;
import static com.cloudera.utils.hms.mirror.MirrorConf.DB_MANAGED_LOCATION;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Getter
@Setter
@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
public class LegacyDBMirror {

    @JsonIgnore
    private String key;
    @Schema(description = "The name of the database that will be created during migration.")
    private String name;

    private final Map<Environment, List<String>> issues = new TreeMap<>();
    /*
    table - reason
     */
    private final Map<String, String> filteredOut = new TreeMap<>();
    //    @JsonIgnore
    private final Map<Environment, List<Pair>> sql = new TreeMap<>();

//    @JsonIgnore
    private final Map<Environment, Map<String, String>> problemSQL = new TreeMap<>();

    @Schema(description = "The name of the database that will be created during migration. If no prefix or rename was specified" +
            "in the Dataset.DatabasesSpec, this will be the original name of the database.  If either of these are set on the input " +
            "this name represents those adjustements.")
    private String resolvedName;

    private Map<Environment, Map<String, String>> properties = new TreeMap<>();

    private Map<String, TableMirror> tableMirrors = null;

    private Map<Environment, Map<String, Number>> environmentStatistics = new TreeMap<>();

    public Map<String, String> getFilteredOut() {
        return filteredOut;
    }



}
