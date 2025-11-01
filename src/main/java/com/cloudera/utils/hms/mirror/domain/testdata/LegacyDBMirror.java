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
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
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

    @JsonIgnore
    public boolean isThereAnIssue() {
        return !issues.isEmpty() ? Boolean.TRUE : Boolean.FALSE;
    }

    public List<String> getIssuesList(Environment environment) {
        return issues.get(environment);
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

    public boolean hasIssues() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasIssues())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public boolean hasErrors() {
        boolean rtn = Boolean.FALSE;
        for (Map.Entry<String, TableMirror> entry : getTableMirrors().entrySet()) {
            if (entry.getValue().hasErrors())
                rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Map<PhaseState, Integer> getPhaseSummary() {
        Map<PhaseState, Integer> rtn = new HashMap<>();
        for (String tableName : getTableMirrors().keySet()) {
            TableMirror tableMirror = getTableMirrors().get(tableName);
            Integer count = rtn.get(tableMirror.getPhaseState());
            if (nonNull(count))
                rtn.put(tableMirror.getPhaseState(), count + 1);
            else
                rtn.put(tableMirror.getPhaseState(), 1);
        }
        return rtn;
    }

    @JsonIgnore
    public String getPhaseSummaryString() {
        StringBuilder sb = new StringBuilder();
        Map<PhaseState, Integer> psMap = getPhaseSummary();
        for (PhaseState ps : psMap.keySet()) {
            sb.append(ps).append("(").append(psMap.get(ps)).append(") ");
        }
        return sb.toString();
    }

    public List<Pair> getSql(Environment environment) {
        List<Pair> sqlList = null;
        if (isNull(sql.get(environment))) {
            sqlList = new ArrayList<>();
            sql.put(environment, sqlList);
        } else {
            sqlList = sql.get(environment);
        }
        return sqlList;
    }
    public static LegacyDBMirror fromDBMirror(DBMirror dbMirror) {
        if (dbMirror == null) {
            return null;
        }

        LegacyDBMirror legacy = new LegacyDBMirror();

        // Copy simple fields
        legacy.setKey(dbMirror.getKey());
        legacy.setName(dbMirror.getName());
        legacy.setResolvedName(dbMirror.getResolvedName());

        // Deep copy issues map (final field)
        legacy.issues.clear();
        for (Map.Entry<Environment, List<String>> entry : dbMirror.getIssues().entrySet()) {
            if (entry.getValue() != null) {
                legacy.issues.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            } else {
                legacy.issues.put(entry.getKey(), null);
            }
        }

        // Deep copy filteredOut map (final field)
        legacy.filteredOut.clear();
        legacy.filteredOut.putAll(dbMirror.getFilteredOut());

        // Deep copy sql map (final field)
        legacy.sql.clear();
        for (Map.Entry<Environment, List<Pair>> entry : dbMirror.getSql().entrySet()) {
            if (entry.getValue() != null) {
                // Pair is immutable, so we can use ArrayList copy constructor
                legacy.sql.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            } else {
                legacy.sql.put(entry.getKey(), null);
            }
        }

        // Deep copy problemSQL map (final field)
        legacy.problemSQL.clear();
        for (Map.Entry<Environment, Map<String, String>> entry : dbMirror.getProblemSQL().entrySet()) {
            if (entry.getValue() != null) {
                legacy.problemSQL.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            } else {
                legacy.problemSQL.put(entry.getKey(), null);
            }
        }

        // Deep copy properties map
        if (dbMirror.getProperties() != null) {
            legacy.properties = new TreeMap<>();
            for (Map.Entry<Environment, Map<String, String>> entry : dbMirror.getProperties().entrySet()) {
                if (entry.getValue() != null) {
                    legacy.properties.put(entry.getKey(), new TreeMap<>(entry.getValue()));
                } else {
                    legacy.properties.put(entry.getKey(), null);
                }
            }
        }

        // Deep copy tableMirrors map
        if (dbMirror.getTableMirrors() != null) {
            legacy.tableMirrors = new TreeMap<>();
            for (Map.Entry<String, TableMirror> entry : dbMirror.getTableMirrors().entrySet()) {
                if (entry.getValue() != null) {
                    // Deep clone TableMirror objects
                    TableMirror clonedTableMirror = entry.getValue().clone();
                    legacy.tableMirrors.put(entry.getKey(), clonedTableMirror);
                } else {
                    legacy.tableMirrors.put(entry.getKey(), null);
                }
            }
        }

        // Deep copy environmentStatistics map
        if (dbMirror.getEnvironmentStatistics() != null) {
            legacy.environmentStatistics = new TreeMap<>();
            for (Map.Entry<Environment, Map<String, Number>> entry : dbMirror.getEnvironmentStatistics().entrySet()) {
                if (entry.getValue() != null) {
                    legacy.environmentStatistics.put(entry.getKey(), new TreeMap<>(entry.getValue()));
                } else {
                    legacy.environmentStatistics.put(entry.getKey(), null);
                }
            }
        }

        return legacy;
    }

}
