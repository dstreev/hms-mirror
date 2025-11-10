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
import com.cloudera.utils.hms.mirror.domain.support.ConnectionStatus;
import com.cloudera.utils.hms.mirror.domain.support.DriverType;
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
    private String key = null;
    //    private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);
    private String name;
    private String description;
    private Environment environment;

    // Platform configuration
    private PlatformType platformType;

    // Core configuration
    private String hcfsNamespace;

    // HiveServer2 configuration (flattened)
    private boolean hs2Connected = Boolean.FALSE;
    private String hs2Uri;
    private String hs2Username;
    private String hs2Password;
    private Map<String, String> hs2ConnectionProperties;
    private String hs2Version;
    private List<String> hs2EnvSets = new ArrayList<>();

    // Metastore Direct configuration (flattened, optional)
    private boolean metastoreDirectEnabled;
    private String metastoreDirectUri;
    private String metastoreDirectType;
    private String metastoreDirectUsername;
    private String metastoreDirectPassword;
    private Integer metastoreDirectMinConnections;
    private Integer metastoreDirectMaxConnections;
    private Map<String, String> metastoreDirectConnectionProperties;
    private String metastoreDirectVersion;
    @JsonIgnore
    private DBStore metastoreDirectDBStore;

    // Partition discovery settings (flattened)
    private boolean partitionDiscoveryAuto;
    private boolean partitionDiscoveryInitMSCK;
    private Integer partitionBucketLimit;

    // Additional cluster settings
    private boolean createIfNotExists;
    private boolean enableAutoTableStats;
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

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;

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
        if (isHs2Connected()) {
            if (!isBlank(getHs2Uri()) && getHs2Uri().contains("principal")) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
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
        private TestStatus status;

        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
        private LocalDateTime lastTested;

        private Double duration;
        private String errorMessage;
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