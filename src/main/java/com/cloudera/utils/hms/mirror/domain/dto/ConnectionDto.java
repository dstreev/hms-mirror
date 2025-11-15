/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import com.cloudera.utils.hms.mirror.domain.support.WarehouseSource;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Data Transfer Object for HMS-Mirror connection configuration.
 * Supports backward/forward compatibility by ignoring unknown JSON fields during deserialization.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectionDto implements Cloneable {

    private static final DateTimeFormatter KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");

    // This would be the top level Key for the RocksDB columnFamily.
    @Schema(description = "Primary key used for RocksDB storage. If not explicitly set, defaults to the connection name. " +
            "This serves as the unique identifier in the RocksDB column family for connection persistence",
            accessMode = Schema.AccessMode.READ_WRITE,
            example = "prod-left-cluster")
    private String key = null;
    //    private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);

    @Schema(description = "Unique name for the connection. Used as the primary identifier and fallback key for RocksDB storage. " +
            "This name is referenced by jobs and datasets to identify which connection to use",
            required = true,
            example = "prod-left-cluster")
    private String name;

    @Schema(description = "Optional description explaining the purpose and environment of this connection",
            example = "Production left cluster - CDP 7.1.9")
    private String description;

    @Schema(description = "Environment classification for this connection. Used for organizing connections by deployment stage",
            example = "PROD",
            allowableValues = {"DEV", "TEST", "PROD", "UAT", "STAGING"})
    private Environment environment;

    // Platform configuration
    @Schema(description = "Platform type of the Hadoop/Hive cluster. Determines version-specific behavior and compatibility settings. " +
            "Common values include CDP, HDP, CDH for various Cloudera and Hortonworks distributions",
            required = true,
            example = "CDP")
    private PlatformType platformType;

    // Core configuration
    @Schema(description = "Hadoop Compatible File System (HCFS) namespace for the cluster. " +
            "This is the base URL for the distributed file system (e.g., hdfs://namenode:8020 or s3a://bucket). " +
            "Used for validating and accessing data locations",
            required = true,
            example = "hdfs://namenode.example.com:8020")
    private String hcfsNamespace;

    // HiveServer2 configuration (flattened)
    @Schema(description = "Flag indicating whether HiveServer2 connection should be established for this cluster. " +
            "When true, hs2Uri and credentials must be provided. HiveServer2 is used for executing Hive queries",
            defaultValue = "false",
            example = "true")
    private boolean hs2Connected = Boolean.FALSE;

    @Schema(description = "JDBC connection URI for HiveServer2. Must start with 'jdbc:hive2://'. " +
            "May include Kerberos principal and other connection parameters. Required when hs2Connected is true",
            example = "jdbc:hive2://hiveserver.example.com:10000/default;principal=hive/_HOST@EXAMPLE.COM")
    private String hs2Uri;

    @Schema(description = "Username for HiveServer2 authentication. Not required for Kerberos-based authentication",
            example = "hive_admin")
    private String hs2Username;

    @Schema(description = "Password for HiveServer2 authentication. Stored securely and can be encrypted. " +
            "Not required for Kerberos-based authentication",
            accessMode = Schema.AccessMode.WRITE_ONLY,
            example = "********")
    private String hs2Password;

    @Schema(description = "Additional JDBC connection properties for HiveServer2. " +
            "Key-value pairs passed to the JDBC driver (e.g., useSSL=true, socketTimeout=60000)",
            example = "{\"useSSL\": \"true\", \"socketTimeout\": \"60000\"}")
    private Map<String, String> hs2ConnectionProperties;

    @Schema(description = "Hive version for HiveServer2 connection. Used for version-specific SQL dialect and feature support",
            example = "3.1.3000.7.1.9.0-387")
    private String hs2Version;

    @Schema(description = "List of Hive SET commands to execute at the start of each HiveServer2 session. " +
            "Used for session-level configuration (e.g., 'SET hive.exec.dynamic.partition.mode=nonstrict')",
            example = "[\"SET hive.exec.dynamic.partition.mode=nonstrict\", \"SET hive.vectorized.execution.enabled=true\"]")
    private List<String> hs2EnvSets = new ArrayList<>();

    // Metastore Direct configuration (flattened, optional)
    @Schema(description = "Enable direct JDBC connection to the Hive metastore database. " +
            "When enabled, metadata operations bypass HiveServer2 for improved performance. " +
            "Requires direct network access to the metastore database",
            defaultValue = "false",
            example = "true")
    private boolean metastoreDirectEnabled;

    @Schema(description = "JDBC connection URI for direct metastore database access. " +
            "Format depends on database type (MySQL, MariaDB, PostgreSQL, Oracle). Required when metastoreDirectEnabled is true",
            example = "jdbc:mysql://metastore-db.example.com:3306/hive_metastore")
    private String metastoreDirectUri;

    @Schema(description = "Type of metastore database backend. Determines JDBC driver and SQL dialect",
            allowableValues = {"MYSQL", "POSTGRES", "ORACLE", "MSSQL", "DERBY"},
            example = "MYSQL")
    private String metastoreDirectType;

    @Schema(description = "Username for metastore database authentication",
            example = "hive_metastore_user")
    private String metastoreDirectUsername;

    @Schema(description = "Password for metastore database authentication. Stored securely and can be encrypted",
            accessMode = Schema.AccessMode.WRITE_ONLY,
            example = "********")
    private String metastoreDirectPassword;

    @Schema(description = "Minimum number of connections to maintain in the metastore database connection pool. " +
            "Lower values reduce resource usage but may increase latency",
            defaultValue = "3",
            example = "5")
    private Integer metastoreDirectMinConnections;

    @Schema(description = "Maximum number of connections allowed in the metastore database connection pool. " +
            "Higher values support more concurrent operations but consume more database resources",
            defaultValue = "10",
            example = "20")
    private Integer metastoreDirectMaxConnections;

    @Schema(description = "Additional JDBC connection properties for metastore database connection. " +
            "Key-value pairs passed to the JDBC driver (e.g., serverTimezone=UTC, useSSL=true)",
            example = "{\"serverTimezone\": \"UTC\", \"useSSL\": \"true\"}")
    private Map<String, String> metastoreDirectConnectionProperties;

    @Schema(description = "Version of the metastore database schema. Used for compatibility checks",
            example = "3.1.0")
    private String metastoreDirectVersion;

    @JsonIgnore
    private DBStore metastoreDirectDBStore;

    // Partition discovery settings (flattened)
    @Schema(description = "Enable automatic partition discovery for tables. " +
            "When enabled, partitions are automatically detected and added to the metastore without explicit ALTER TABLE commands",
            defaultValue = "false",
            example = "true")
    private boolean partitionDiscoveryAuto;

    @Schema(description = "Run MSCK REPAIR TABLE during initialization to discover and add missing partitions. " +
            "Useful for tables with many pre-existing partitions on the file system",
            defaultValue = "false",
            example = "true")
    private boolean partitionDiscoveryInitMSCK;

    @Schema(description = "Maximum number of partitions to process per table in a single operation. " +
            "Prevents memory issues when working with heavily partitioned tables. " +
            "Set to null for unlimited (use with caution on large tables)",
            example = "1000")
    private Integer partitionBucketLimit;

    // Additional cluster settings
    @Schema(description = "Create databases and tables with IF NOT EXISTS clause to prevent errors if they already exist. " +
            "Recommended for idempotent migrations and disaster recovery scenarios",
            defaultValue = "true",
            example = "true")
    private boolean createIfNotExists;

    @Schema(description = "Enable automatic computation of table-level statistics (e.g., row count, total size) after table creation. " +
            "Improves query optimization but increases migration time",
            defaultValue = "false",
            example = "true")
    private boolean enableAutoTableStats;

    @Schema(description = "Enable automatic computation of column-level statistics (e.g., min/max values, distinct counts) after table creation. " +
            "Provides more granular optimization but significantly increases migration time",
            defaultValue = "false",
            example = "false")
    private boolean enableAutoColumnStats;

    // TODO: Need to populate this somehow.
    @Schema(description = "The Hive Environment Values for this connection.")
    private Map<String, String> envVars = new HashMap<>();

    // TODO: Need to populate this somehow.
    @Schema(description = "Warehouse configuration for connection with managed and external directories")
    private Warehouse warehouse = new Warehouse(WarehouseSource.ENVIRONMENT, null, null);

    // System fields - Individual test results for each component
    @Schema(description = "HCFS Namespace connection test results")
    private ConnectionTestResults hcfsTestResults;

    @Schema(description = "HiveServer2 connection test results")
    private ConnectionTestResults hs2TestResults;

    @Schema(description = "Metastore Direct connection test results")
    private ConnectionTestResults metastoreDirectTestResults;

    @Schema(description = "Timestamp when this connection configuration was first created",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;

    @Schema(description = "Timestamp when this connection configuration was last modified",
            accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;

    public String getKey() {
        if (key == null) {
            if (name == null) {
                throw new IllegalStateException("name is required");
            } else {
                key = name;
            }
        }
        return key;
    }

    @JsonIgnore
    public DBStore getMetastoreDirectDBStore() {
        // If this hasn't been set, construct one based on the values in this
        // object.
        if (metastoreDirectDBStore == null) {
            metastoreDirectDBStore = new DBStore();
            metastoreDirectDBStore.setType(DBStore.DB_TYPE.valueOf(metastoreDirectType));
            metastoreDirectDBStore.setUri(metastoreDirectUri);
            if (metastoreDirectConnectionProperties != null) {
                metastoreDirectConnectionProperties = new HashMap<>(metastoreDirectConnectionProperties);
            }
            // Add the username and password to the connection properties.
            metastoreDirectDBStore.getConnectionProperties().put("user", metastoreDirectUsername);
            metastoreDirectDBStore.getConnectionProperties().put("password", metastoreDirectPassword);
            // TODO: Add initSql support.

        }
        return metastoreDirectDBStore;
    }

    public void reset() {
        // Reset test results to null (will be re-tested)
        hcfsTestResults = null;
        hs2TestResults = null;
        metastoreDirectTestResults = null;
    }

    @JsonIgnore
    public boolean isHs2KerberosConnection() {
        if (!isBlank(getHs2Uri()) && getHs2Uri().contains("principal")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @JsonIgnore
    public boolean isValidHs2Uri() {
        Boolean rtn = Boolean.TRUE;
        if (isHs2Connected()) {
            if (isBlank(getHs2Uri()) || !getHs2Uri().startsWith("jdbc:hive2://")) {
                rtn = Boolean.FALSE;
            }
        }
        return rtn;
    }

    /**
     * Create a deep clone of this ConnectionDto.
     * All nested objects and collections are cloned to avoid shared references.
     * This properly implements the Cloneable interface contract.
     *
     * @return A deep clone of this ConnectionDto
     * @throws CloneNotSupportedException if cloning is not supported
     */
    @Override
    public ConnectionDto clone() {
        // Start with a shallow clone (copies all primitive and immutable fields)
        ConnectionDto clone = null;
        try {
            clone = (ConnectionDto) super.clone();

            // Now deep clone mutable objects to avoid shared references

            // Deep clone hs2ConnectionProperties map
            if (this.hs2ConnectionProperties != null) {
                clone.hs2ConnectionProperties = new HashMap<>(this.hs2ConnectionProperties);
            }

            // Deep clone metastoreDirectConnectionProperties map
            if (this.metastoreDirectConnectionProperties != null) {
                clone.metastoreDirectConnectionProperties = new HashMap<>(this.metastoreDirectConnectionProperties);
            }

            // Deep clone hcfsTestResults
            if (this.hcfsTestResults != null) {
                clone.hcfsTestResults = this.hcfsTestResults.clone();
            }

            // Deep clone hs2TestResults
            if (this.hs2TestResults != null) {
                clone.hs2TestResults = this.hs2TestResults.clone();
            }

            // Deep clone metastoreDirectTestResults
            if (this.metastoreDirectTestResults != null) {
                clone.metastoreDirectTestResults = this.metastoreDirectTestResults.clone();
            }
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("Clone not supported", e);
        }

        return clone;
    }

    public enum Environment {
        DEV, TEST, PROD, UAT, STAGING
    }

    /**
     * Nested class for connection test results.
     * Supports backward/forward compatibility by ignoring unknown JSON fields during deserialization.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConnectionTestResults implements Cloneable {
        @Schema(description = "Status of the connection test",
                allowableValues = {"SUCCESS", "FAILED", "NEVER_TESTED"},
                example = "SUCCESS")
        private TestStatus status;

        @Schema(description = "Timestamp when the connection was last tested",
                accessMode = Schema.AccessMode.READ_ONLY)
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
        private LocalDateTime lastTested;

        @Schema(description = "Duration of the connection test in seconds",
                example = "1.234")
        private Double duration;

        @Schema(description = "Error message if the connection test failed. Null if test succeeded",
                example = "Connection refused: connect")
        private String errorMessage;

        @Schema(description = "Additional details about the connection test results. " +
                "May include diagnostic information, warnings, or configuration insights",
                example = "Successfully connected to HiveServer2 at hiveserver.example.com:10000")
        private String details;

        public enum TestStatus {
            SUCCESS, FAILED, NEVER_TESTED
        }

        /**
         * Create a deep clone of this ConnectionTestResults.
         *
         * @return A deep clone of this ConnectionTestResults
         */
        @Override
        public ConnectionTestResults clone() {
            ConnectionTestResults clone = new ConnectionTestResults();
            clone.status = this.status; // enum is immutable
            clone.lastTested = this.lastTested; // LocalDateTime is immutable
            clone.duration = this.duration; // Double is immutable
            clone.errorMessage = this.errorMessage; // String is immutable
            clone.details = this.details; // String is immutable
            return clone;
        }
    }
}