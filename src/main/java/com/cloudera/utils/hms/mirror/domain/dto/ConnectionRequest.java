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

import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
import lombok.Data;

import java.util.Map;

@Data
public class ConnectionRequest {
    private String name;
    private String description;
    private String environment;
    private ConfigRequest config;

    @Data
    public static class ConfigRequest {
        private PlatformType platformType;
        private String hcfsNamespace;
        private HiveServer2Request hiveServer2;
        private MetastoreDirectRequest metastoreDirect;
        private String connectionPoolLib;
        private PartitionDiscoveryRequest partitionDiscovery;
        private boolean createIfNotExists;
        private boolean enableAutoTableStats;
        private boolean enableAutoColumnStats;
    }

    @Data
    public static class HiveServer2Request {
        private String uri;
        private Map<String, String> connectionProperties;
        private String driverClassName;
        private String jarFile;
        private boolean disconnected;
    }

    @Data
    public static class MetastoreDirectRequest {
        private String uri;
        private String type;
        private Map<String, String> connectionProperties;
        private ConnectionPoolRequest connectionPool;
    }

    @Data
    public static class ConnectionPoolRequest {
        private int min;
        private int max;
    }

    @Data
    public static class PartitionDiscoveryRequest {
        private boolean auto;
        private boolean initMSCK;
        private int partitionBucketLimit;
    }

    /**
     * Convert this request DTO to a flattened Connection entity
     */
    public ConnectionDto toConnection() {
        ConnectionDto connectionDto = new ConnectionDto();
        
        // Basic information
        connectionDto.setName(this.name);
        connectionDto.setDescription(this.description);
        if (this.environment != null) {
            connectionDto.setEnvironment(ConnectionDto.Environment.valueOf(this.environment));
        }
        
        if (this.config != null) {
            // Platform configuration
            connectionDto.setPlatformType(this.config.platformType);
            connectionDto.setHcfsNamespace(this.config.hcfsNamespace);
            connectionDto.setCreateIfNotExists(this.config.createIfNotExists);
            connectionDto.setEnableAutoTableStats(this.config.enableAutoTableStats);
            connectionDto.setEnableAutoColumnStats(this.config.enableAutoColumnStats);
            
            // HiveServer2 configuration (flattened)
            if (this.config.hiveServer2 != null) {
                HiveServer2Request hs2 = this.config.hiveServer2;
                connectionDto.setHs2Uri(hs2.uri);
//                connectionDto.setHs2DriverClassName(hs2.driverClassName);
//                connectionDto.setHs2JarFile(hs2.jarFile);
//                connectionDto.setHs2Disconnected(hs2.disconnected);
                connectionDto.setHs2ConnectionProperties(hs2.connectionProperties);
                
                // Extract username and password from connection properties
                if (hs2.connectionProperties != null) {
                    connectionDto.setHs2Username(hs2.connectionProperties.get("user"));
                    connectionDto.setHs2Password(hs2.connectionProperties.get("password"));
                }
            }
            
            // Metastore Direct configuration (flattened)
            if (this.config.metastoreDirect != null) {
                MetastoreDirectRequest msd = this.config.metastoreDirect;
                connectionDto.setMetastoreDirectEnabled(true);
                connectionDto.setMetastoreDirectUri(msd.uri);
                connectionDto.setMetastoreDirectType(msd.type);
                
                if (msd.connectionProperties != null) {
                    connectionDto.setMetastoreDirectUsername(msd.connectionProperties.get("user"));
                    connectionDto.setMetastoreDirectPassword(msd.connectionProperties.get("password"));
                }
                
                if (msd.connectionPool != null) {
                    connectionDto.setMetastoreDirectMinConnections(msd.connectionPool.min);
                    connectionDto.setMetastoreDirectMaxConnections(msd.connectionPool.max);
                }
            } else {
                connectionDto.setMetastoreDirectEnabled(false);
            }
            
            // Partition discovery configuration (flattened)
            if (this.config.partitionDiscovery != null) {
                PartitionDiscoveryRequest pd = this.config.partitionDiscovery;
                connectionDto.setPartitionDiscoveryAuto(pd.auto);
                connectionDto.setPartitionDiscoveryInitMSCK(pd.initMSCK);
                connectionDto.setPartitionBucketLimit(pd.partitionBucketLimit);
            } else {
                // Set defaults
                connectionDto.setPartitionDiscoveryAuto(true);
                connectionDto.setPartitionDiscoveryInitMSCK(true);
                connectionDto.setPartitionBucketLimit(100);
            }
        }
        
        return connectionDto;
    }
}