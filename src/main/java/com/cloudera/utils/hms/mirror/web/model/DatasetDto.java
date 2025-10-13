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

package com.cloudera.utils.hms.mirror.web.model;

import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.WarehouseSource;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

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
public class DatasetDto {

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
        
        @Schema(description = "Specific list of tables to include (if not using filters)")
        private List<String> tables = new ArrayList<>();
        
        @Schema(description = "Table filter configuration (if not specifying explicit table list)")
        private TableFilter filter;
        
        @Schema(description = "Warehouse configuration for the database with managed and external directories")
        private Warehouse warehouse = new Warehouse(WarehouseSource.PLAN, null, null);
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
        private String includePattern;
        
        @Schema(description = "Regular expression pattern for table names to exclude", 
               example = ".*_temp|.*_staging")
        private String excludePattern;
        
        @Schema(description = "Filter for table types", 
               example = "EXTERNAL_TABLE,MANAGED_TABLE")
        private List<String> tableTypes = new ArrayList<>();
        
        @Schema(description = "Minimum number of partitions (0 = no minimum)", example = "0")
        private int minPartitions = 0;
        
        @Schema(description = "Maximum number of partitions (0 = no maximum)", example = "0")  
        private int maxPartitions = 0;
        
        @Schema(description = "Minimum table size in bytes (0 = no minimum)", example = "0")
        private long minSizeBytes = 0L;
        
        @Schema(description = "Maximum table size in bytes (0 = no maximum)", example = "0")
        private long maxSizeBytes = 0L;
    }
}