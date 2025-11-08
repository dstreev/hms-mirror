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

package com.cloudera.utils.hms.mirror.domain.core;

import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
public class DBMirror implements Cloneable {

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

    private Map<Environment, Map<String, String>> properties = new TreeMap<>();

    /*
    Setting this to Ignore because we don't want it to be serialized as this could be a large object.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Map<String, TableMirror> tableMirrors = null;

    private Map<Environment, Map<String, Number>> environmentStatistics = new TreeMap<>();

    public void addIssue(Environment environment, String issue) {
        String scrubbedIssue = issue.replace("\n", "<br/>");
        List<String> issuesList = issues.get(environment);
        if (isNull(issuesList)) {
            issuesList = new ArrayList<>();
            issues.put(environment, issuesList);
        }
        issuesList.add(scrubbedIssue);
    }

    public void addProblemSQL(Environment environment, String sql, String reason) {
        Map<String, String> sqlList = problemSQL.get(environment);
        if (isNull(sqlList)) {
            sqlList = new TreeMap<>();
            problemSQL.put(environment, sqlList);
        }
        sqlList.put(sql, reason);
    }

    public void setProperty(Environment environment, String dbProperty, String value) {
        Map<String, String> dbDefinition = getEnvironmentProperties(environment);
        if (isNull(dbDefinition)) {
            dbDefinition = new TreeMap<>();
            properties.put(environment, dbDefinition);
        }
        dbDefinition.put(dbProperty, value);
    }

    @JsonIgnore
    public String getEnvironmentProperty(Environment environment, String dbProperty) {
        String rtn = null;
        if (nonNull(getEnvironmentProperties(environment))) {
            rtn = getEnvironmentProperties(environment).get(dbProperty);
        }
        return rtn;
    }

    @JsonIgnore
    public String getLocationDirectory(Environment environment) {
        String location = null;
        location = getEnvironmentProperty(environment, DB_LOCATION);
        if (nonNull(location)) {
            location = NamespaceUtils.getLastDirectory(location);
        } else {
            location = getName() + ".db"; // Set to the database name.
        }
        return location;
    }

    @JsonIgnore
    public String getManagedLocationDirectory(Environment environment) {
        String location = null;
        location = getEnvironmentProperty(environment, DB_MANAGED_LOCATION);
        if (nonNull(location)) {
            location = NamespaceUtils.getLastDirectory(location);
        } else {
            location = getName() + ".db"; // Set to the database name.
        }
        return location;
    }

    public Map<String, String> getEnvironmentProperties(Environment environment) {
        Map<String, String> rtn = properties.get(environment);
//        if (isNull(rtn)) {
//            rtn = new TreeMap<>();
//            properties.put(environment, rtn);
//        }
        return rtn;
    }

    public Map<String, String> getFilteredOut() {
        return filteredOut;
    }

    public List<String> getIssuesList(Environment environment) {
        return issues.get(environment);
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


    public void setProperty(Environment enviroment, Map<String, String> dbProperties) {
        properties.put(enviroment, dbProperties);
    }

    // Used to strip 'work' recorded in DBMirror.  Things like SQL, issues, etc.
    public void stripWork() {
        this.getSql().clear();
        this.getIssues().clear();
        this.getFilteredOut().clear();
        /*
        TODO: This might need to be externalized.
        for (TableMirror tableMirror : getTableMirrors().values()) {
            // Leave LEFT because it is the source.
            EnvironmentTable let = tableMirror.getEnvironments().get(Environment.LEFT);
            let.getIssues().clear();
            let.getSql().clear();
            let.getCleanUpSql().clear();
//            let.getStatistics().clear();
            tableMirror.getEnvironments().remove(Environment.SHADOW);
            tableMirror.getEnvironments().remove(Environment.TRANSFER);
            tableMirror.getEnvironments().remove(Environment.RIGHT);
//            tableMirror.getIssues().clear();
//            tableMirror.getSql().clear();
//            tableMirror.getStatistics().clear();
            tableMirror.getSteps().clear();
        }
        */
    }

    @Override
    public DBMirror clone() {
        try {
            DBMirror clone = (DBMirror) super.clone();
            
            // Clone the final Maps by clearing and repopulating them
            // Note: We can't reassign final fields, but we can clear and repopulate them
            
            // Clone issues map (final field)
            clone.issues.clear();
            for (Map.Entry<Environment, List<String>> entry : this.issues.entrySet()) {
                clone.issues.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            
            // Clone filteredOut map (final field)
            clone.filteredOut.clear();
            clone.filteredOut.putAll(this.filteredOut);
            
            // Clone sql map (final field)
            clone.sql.clear();
            for (Map.Entry<Environment, List<Pair>> entry : this.sql.entrySet()) {
                List<Pair> clonedList = new ArrayList<>();
                for (Pair pair : entry.getValue()) {
                    // Pair is immutable, so we can share the reference
                    clonedList.add(pair);
                }
                clone.sql.put(entry.getKey(), clonedList);
            }
            
            // Clone problemSQL map (final field)
            clone.problemSQL.clear();
            for (Map.Entry<Environment, Map<String, String>> entry : this.problemSQL.entrySet()) {
                clone.problemSQL.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            }
            
            // Clone non-final fields
            if (this.properties != null) {
                clone.properties = new TreeMap<>();
                for (Map.Entry<Environment, Map<String, String>> entry : this.properties.entrySet()) {
                    if (entry.getValue() != null) {
                        clone.properties.put(entry.getKey(), new TreeMap<>(entry.getValue()));
                    } else {
                        clone.properties.put(entry.getKey(), null);
                    }
                }
            }

            /*
            // Clone tableMirrors map and its contents
            if (this.tableMirrors != null) {
                clone.tableMirrors = new TreeMap<>();
                for (Map.Entry<String, TableMirror> entry : this.tableMirrors.entrySet()) {
                    if (entry.getValue() != null) {
                        // Deep clone TableMirror objects
                        TableMirror clonedTableMirror = entry.getValue().clone();
                        clonedTableMirror.setParent(clone);  // Set the cloned table's parent to the clone
                        clone.tableMirrors.put(entry.getKey(), clonedTableMirror);
                    } else {
                        clone.tableMirrors.put(entry.getKey(), null);
                    }
                }
            }
            */

            // Clone environmentStatistics map
            if (this.environmentStatistics != null) {
                clone.environmentStatistics = new TreeMap<>();
                for (Map.Entry<Environment, Map<String, Number>> entry : this.environmentStatistics.entrySet()) {
                    if (entry.getValue() != null) {
                        clone.environmentStatistics.put(entry.getKey(), new TreeMap<>(entry.getValue()));
                    } else {
                        clone.environmentStatistics.put(entry.getKey(), null);
                    }
                }
            }
            
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Clone not supported for DBMirror", e);
        }
    }

}
