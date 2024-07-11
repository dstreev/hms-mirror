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

import com.cloudera.utils.hms.mirror.EnvironmentMap;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.StringLengthComparator;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties({"dbLocationMap"})
public class Translator implements Cloneable {
//    private int consolidationLevelBase = 1;
//    @Schema(description = "If the Partition Spec doesn't match the partition hierarchy, then set this to true.")
//    private boolean partitionLevelMismatch = Boolean.FALSE;

    @JsonIgnore
    private final Map<String, EnvironmentMap> translationMap = new TreeMap<>();

    /*
    Use this to force the location element in the external table create statements and
    not rely on the database 'location' element.
     */
    private boolean forceExternalLocation = Boolean.FALSE;
    private Map<String, String> globalLocationMap = null;

    @JsonIgnore
    private Map<String, String> orderedGlobalLocationMap = null;

    private WarehouseMapBuilder warehouseMapBuilder = null;

    public void addGlobalLocationMap(String from, String to) {
        getOrderedGlobalLocationMap().put(from, to);
    }

    // Needed to handle npe when loaded from json
    public WarehouseMapBuilder getWarehouseMapBuilder() {
        if (warehouseMapBuilder == null)
            warehouseMapBuilder = new WarehouseMapBuilder();
        return warehouseMapBuilder;
    }

    @Override
    public Translator clone() {
        try {
            Translator clone = (Translator) super.clone();
            if (globalLocationMap != null)
                clone.globalLocationMap = new HashMap<>(globalLocationMap);
            if (orderedGlobalLocationMap != null)
                clone.orderedGlobalLocationMap = new TreeMap<>(orderedGlobalLocationMap);
            if (warehouseMapBuilder != null)
                clone.warehouseMapBuilder = (WarehouseMapBuilder)warehouseMapBuilder.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public String removeGlobalLocationMap(String from) {
        return getOrderedGlobalLocationMap().remove(from);
    }

    public List<String> removeGlobalLocationMap(List<String> fromList) {
        List<String> rtn = new ArrayList<>();
        for (String from : fromList)
            rtn.add(getOrderedGlobalLocationMap().remove(from));
        return rtn;
    }

    public void addTableSource(String database, String table, String tableType, String source, int consolidationLevelBase,
                               boolean partitionLevelMismatch) {
        if (warehouseMapBuilder == null)
            warehouseMapBuilder = new WarehouseMapBuilder();
        try {
            TableType type = TableType.valueOf(tableType);
            warehouseMapBuilder.addSourceLocation(database, table, type, null, source, null,
                    consolidationLevelBase, partitionLevelMismatch);
        } catch (IllegalArgumentException iae) {
            log.info("Not a supported table type: {}", tableType);
        }
    }

    public void addPartitionSource(String database, String table, String tableType, String partitionSpec,
                                   String tableSource, String partitionSource, int consolidationLevelBase,
                                   boolean partitionLevelMismatch) {
        if (warehouseMapBuilder == null)
            warehouseMapBuilder = new WarehouseMapBuilder();
        TableType type = TableType.valueOf(tableType);
        warehouseMapBuilder.addSourceLocation(database, table, type, partitionSpec, tableSource, partitionSource,
                consolidationLevelBase, partitionLevelMismatch);
    }

    public void removeDatabaseFromTranslationMap(String database) {
        translationMap.remove(database);
    }

    public synchronized void addTranslation(String database, Environment environment, String originalLocation, String newLocation, int level) {
        EnvironmentMap environmentMap = translationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        environmentMap.addTranslationLocation(environment, originalLocation, newLocation, level);
//        getDbLocationMap(database, environment).put(originalLocation, newLocation);
    }

    public synchronized Set<EnvironmentMap.TranslationLevel> getTranslationMap(String database, Environment environment) {
        EnvironmentMap envMap = translationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        return envMap.getTranslationSet(environment);
    }

    // Needed to ensure the return is the ordered map.
    public Map<String, String> getGlobalLocationMap() {
        return getOrderedGlobalLocationMap();
    }

    @JsonIgnore
    // This set is ordered by the length of the key in descending order
    // to ensure that the longest path is replaced first.
    public Map<String, String> getOrderedGlobalLocationMap() {
        if (orderedGlobalLocationMap == null) {
            orderedGlobalLocationMap = new TreeMap<String, String>(new StringLengthComparator());
            // Add the global location map to the ordered map.
            if (globalLocationMap != null)
                orderedGlobalLocationMap.putAll(globalLocationMap);
        }
        return orderedGlobalLocationMap;
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;
        return rtn;
    }

}
