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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectionDto {

    private String id;
    private String name;
    private String description;
    private Environment environment;
    
    // Platform configuration
    private String platformType;
    
    // Core configuration
    private String hcfsNamespace;
    
    // HiveServer2 configuration (flattened)
    private String hs2Uri;
    private String hs2Username;
    private String hs2Password;
    private Map<String, String> hs2ConnectionProperties;

    // Metastore Direct configuration (flattened, optional)
    private boolean metastoreDirectEnabled;
    private String metastoreDirectUri;
    private String metastoreDirectType;
    private String metastoreDirectUsername;
    private String metastoreDirectPassword;
    private Integer metastoreDirectMinConnections;
    private Integer metastoreDirectMaxConnections;
    
    // Partition discovery settings (flattened)
    private boolean partitionDiscoveryAuto;
    private boolean partitionDiscoveryInitMSCK;
    private Integer partitionBucketLimit;

    // Additional cluster settings
    private boolean createIfNotExists;
    private boolean enableAutoTableStats;
    private boolean enableAutoColumnStats;

    // System fields
    private ConnectionTestResults testResults;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime created;
    
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modified;
    
    private boolean isDefault;

    public enum Environment {
        DEV, TEST, PROD, UAT, STAGING
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConnectionTestResults {
        private TestStatus status;
        
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
        private LocalDateTime lastTested;
        
        private Double duration;
        private String errorMessage;
        private String details;

        public enum TestStatus {
            SUCCESS, FAILED, NEVER_TESTED
        }
    }
}