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

package com.cloudera.utils.hms.mirror.domain.dto;

import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.TableType;
import com.cloudera.utils.hms.mirror.domain.support.WarehouseSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Dataset DTO for HMS Mirror Dataset management.
 * A dataset defines a collection of databases and their associated tables
 * for processing by HMS Mirror. Each database can contain either a specific
 * list of tables or filter criteria to determine which tables to include.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Dataset configuration containing databases and table specifications")
public class DatasetDto implements Cloneable {

    @Schema(description = "Unique name for the dataset", required = true, example = "production-analytics")
    private String name;
    
    @Schema(description = "Optional description of the dataset", example = "Production analytics databases for migration")
    private String description;

    @Schema(description = "List of databases included in this dataset")
    private List<DatabaseSpec> databases = new ArrayList<>();
    
    @Schema(description = "Creation timestamp")
    private String createdDate;
    
    @Schema(description = "Last modification timestamp")
    private String modifiedDate;

    public DatabaseSpec getDatabase(String databaseName) {
        if (databaseName != null && databases != null) {
            for (DatabaseSpec dbSpec : databases) {
                if (dbSpec != null && databaseName.equals(dbSpec.getDatabaseName())) {
                    return dbSpec;
                }
            }
        }
        return null;
    }

//    /**
//     * Create a deep clone of this DatasetDto.
//     * All nested objects and collections are cloned to avoid shared references.
//     *
//     * @return A deep clone of this DatasetDto
//     */
//    /**
//     * Get database names as a Set for backward compatibility.
//     *
//     * @return Set of database names
//     */
//    public java.util.Set<String> getDatabases() {
//        java.util.Set<String> dbNames = new java.util.TreeSet<>();
//        if (databases != null) {
//            for (DatabaseSpec dbSpec : databases) {
//                if (dbSpec != null && dbSpec.getDatabaseName() != null) {
//                    dbNames.add(dbSpec.getDatabaseName());
//                }
//            }
//        }
//        return dbNames;
//    }
//
//    /**
//     * Set databases from a collection of database names.
//     * Creates DatabaseSpec entries for each name.
//     *
//     * @param databaseNames Collection of database names
//     */
//    public void setDatabases(java.util.Collection<String> databaseNames) {
//        if (this.databases == null) {
//            this.databases = new ArrayList<>();
//        } else {
//            this.databases.clear();
//        }
//
//        if (databaseNames != null) {
//            for (String dbName : databaseNames) {
//                DatabaseSpec spec = new DatabaseSpec();
//                spec.setDatabaseName(dbName);
//                this.databases.add(spec);
//            }
//        }
//    }
//
//    /**
//     * Get the internal list of DatabaseSpec objects.
//     * This method provides direct access to the DatabaseSpec list for code that needs
//     * full database specifications rather than just database names.
//     *
//     * @return List of DatabaseSpec objects
//     */
//    public List<DatabaseSpec> getDatabaseSpecs() {
//        return databases != null ? databases : new ArrayList<>();
//    }
//
//    /**
//     * Get the Filter from the dataset.
//     * For backward compatibility, returns the filter from the first database spec if available.
//     *
//     * @return Filter object or null
//     */
//    public com.cloudera.utils.hms.mirror.domain.core.Filter getFilter() {
//        if (databases != null && !databases.isEmpty() && databases.get(0) != null) {
//            TableFilter tableFilter = databases.get(0).getFilter();
//            if (tableFilter != null) {
//                // Convert TableFilter to Filter
//                com.cloudera.utils.hms.mirror.domain.core.Filter filter = new com.cloudera.utils.hms.mirror.domain.core.Filter();
//                filter.setTblRegEx(tableFilter.getIncludePattern());
//                filter.setTblExcludeRegEx(tableFilter.getExcludePattern());
//                if (tableFilter.getMaxSizeBytes() > 0) {
//                    filter.setTblSizeLimit(tableFilter.getMaxSizeBytes());
//                }
//                if (tableFilter.getMaxPartitions() > 0) {
//                    filter.setTblPartitionLimit((int) tableFilter.getMaxPartitions());
//                }
//                return filter;
//            }
//        }
//        return new com.cloudera.utils.hms.mirror.domain.core.Filter();
//    }

    public DatasetDto clone() throws CloneNotSupportedException {
        DatasetDto clone = (DatasetDto) super.clone();

        // Copy primitive and immutable fields
        clone.name = this.name;
        clone.description = this.description;
        clone.createdDate = this.createdDate;
        clone.modifiedDate = this.modifiedDate;

        // Deep clone databases list
        if (this.databases != null) {
            clone.databases = new ArrayList<>();
            for (DatabaseSpec dbSpec : this.databases) {
                if (dbSpec != null) {
                    clone.databases.add(dbSpec.deepClone());
                }
            }
        } else {
            clone.databases = new ArrayList<>();
        }

        return clone;
    }

    /**
     * Specification for a database within a dataset.
     * Contains either a specific list of tables OR filter criteria (not both).
     */
    @Getter
    @Setter
    @Schema(description = "Database specification within a dataset")
    public static class DatabaseSpec {
        
        @Schema(description = "Database name", required = true, example = "analytics_db")
        private String databaseName;

        /**
         * User-provided global location map for custom translations.
         * Key: source path.
         * Value: Map from TableType to target location.
         */
        @Schema(description = "User-provided global location map for custom translations")
        private Map<String, Map<TableType, String>> userGlobalLocationMap = null;

        @Schema(description = "Optional database prefix to use for the database name that will be appended during migration.")
        private String dbPrefix = null;

        @Schema(description = "Optional database rename to use when migrating the database.")
        private String dbRename = null;

        @Schema(description = "Specific list of tables to include (if not using filters)")
        private List<String> tables = new ArrayList<>();
        
        @Schema(description = "Table filter configuration (if not specifying explicit table list)")
        private TableFilter filter;
        
        @Schema(description = "Warehouse configuration for the database with managed and external directories")
        private Warehouse warehouse = new Warehouse(WarehouseSource.PLAN, null, null);

        /**
         * Create a deep clone of this DatabaseSpec.
         * All nested objects and collections are cloned to avoid shared references.
         *
         * @return A deep clone of this DatabaseSpec
         */
        public DatabaseSpec deepClone() {
            DatabaseSpec clone = new DatabaseSpec();

            // Copy primitive and immutable fields
            clone.databaseName = this.databaseName;
            clone.dbPrefix = this.dbPrefix;
            clone.dbRename = this.dbRename;

            // Deep clone userGlobalLocationMap (nested maps)
            if (this.userGlobalLocationMap != null) {
                clone.userGlobalLocationMap = new HashMap<>();
                for (Map.Entry<String, Map<TableType, String>> entry : this.userGlobalLocationMap.entrySet()) {
                    if (entry.getValue() != null) {
                        clone.userGlobalLocationMap.put(entry.getKey(), new HashMap<>(entry.getValue()));
                    }
                }
            }

            // Deep clone tables list
            if (this.tables != null) {
                clone.tables = new ArrayList<>(this.tables);
            } else {
                clone.tables = new ArrayList<>();
            }

            // Deep clone filter
            if (this.filter != null) {
                clone.filter = this.filter.deepClone();
            }

            // Deep clone warehouse
            try {
                if (this.warehouse != null) {
                    clone.warehouse = this.warehouse.clone();
                } else {
                    clone.warehouse = new Warehouse(WarehouseSource.PLAN, null, null);
                }
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException("Failed to clone Warehouse", e);
            }

            return clone;
        }
    }

    /**
     * Table filter configuration based on pattern matching.
     * Similar to the existing HMS Mirror filter patterns.
     */
    @Getter
    @Setter
    @Schema(description = "Filter criteria for selecting tables within a database")
    public static class TableFilter {
        
        @Schema(description = "Regular expression pattern for table names to include", 
               example = "fact_.*|dim_.*")
        private String includeRegEx;
        
        @Schema(description = "Regular expression pattern for table names to exclude", 
               example = ".*_temp|.*_staging")
        private String excludeRegEx;
        
        @Schema(description = "Filter for table types", 
               example = "EXTERNAL_TABLE,MANAGED_TABLE")
        private List<String> tableTypes = new ArrayList<>();
        
        @Schema(description = "Minimum number of partitions (0 = no minimum)", example = "0")
        private int minPartitions = 0;
        
        @Schema(description = "Maximum number of partitions (0 = no maximum)", example = "0")  
        private int maxPartitions = 0;
        
        @Schema(description = "Minimum table size in bytes (0 = no minimum)", example = "0")
        private long minSizeMb = 0L;
        
        @Schema(description = "Maximum table size in bytes (0 = no maximum)", example = "0")
        private long maxSizeMb = 0L;

        @JsonIgnore
        private Pattern includeRegExPattern = null;
        @JsonIgnore
        private Pattern excludeRegExPattern = null;

        public Pattern getIncludeRegExPattern() {
            // If null, create Pattern object
            if (this.includeRegExPattern == null && this.includeRegEx != null) {
                this.includeRegExPattern = Pattern.compile(this.includeRegEx);
            }
            return this.includeRegExPattern;
        }

        public Pattern getExcludeRegExPattern() {
            // If null, create Pattern object
            if (this.excludeRegExPattern == null && this.excludeRegEx != null) {
                this.excludeRegExPattern = Pattern.compile(this.excludeRegEx);
            }
            return this.excludeRegExPattern;
        }

        /**
         * Create a deep clone of this TableFilter.
         * All nested objects and collections are cloned to avoid shared references.
         *
         * @return A deep clone of this TableFilter
         */
        public TableFilter deepClone() {
            TableFilter clone = new TableFilter();

            // Copy primitive and immutable fields
            clone.includeRegEx = this.includeRegEx;
            clone.excludeRegEx = this.excludeRegEx;
            clone.minPartitions = this.minPartitions;
            clone.maxPartitions = this.maxPartitions;
            clone.minSizeMb = this.minSizeMb;
            clone.maxSizeMb = this.maxSizeMb;

            // Deep clone tableTypes list
            if (this.tableTypes != null) {
                clone.tableTypes = new ArrayList<>(this.tableTypes);
            } else {
                clone.tableTypes = new ArrayList<>();
            }

            return clone;
        }
    }
}