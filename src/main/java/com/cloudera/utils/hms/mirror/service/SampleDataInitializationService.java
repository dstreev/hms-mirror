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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.config.SampleDataProperties;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.core.HybridConfig;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "hms-mirror.rocksdb.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
@Slf4j
public class SampleDataInitializationService {

    private final SampleDataProperties sampleDataProperties;
    private final ConnectionService connectionService;
    private final DatasetManagementService datasetManagementService;
    private final ConfigurationManagementService configurationManagementService;

    @PostConstruct
    public void initializeSampleData() {
        if (!sampleDataProperties.isCreate()) {
            log.debug("Sample data creation is disabled");
            return;
        }

        log.info("Initializing sample data for HMS Mirror");
        
        try {
            createSampleConnections();
            createSampleDatasets();
            createSampleConfigurations();
            log.info("Sample data initialization completed successfully");
        } catch (Exception e) {
            log.error("Failed to initialize sample data", e);
        }
    }

    private void createSampleConnections() {
        log.info("Creating 4 demo connections");
        
        ConnectionDto[] connectionDtos = {
            ConnectionDto.builder()
                .name("CDP-Dev-Cluster")
                .description("Development CDP cluster for testing and prototyping")
                .environment(ConnectionDto.Environment.DEV)
                .platformType("CDP")
                .hcfsNamespace("hdfs://dev-cluster")
                .hs2Uri("jdbc:hive2://dev-hiveserver:10000/default")
                .hs2Username("hive")
                .hs2DriverClassName("org.apache.hive.jdbc.HiveDriver")
                .metastoreDirectEnabled(true)
                .metastoreDirectUri("thrift://dev-metastore:9083")
                .metastoreDirectType("MYSQL")
                .partitionDiscoveryAuto(true)
                .enableAutoTableStats(true)
                .created(LocalDateTime.now())
                .modified(LocalDateTime.now())
                .isDefault(true)
                .build(),
                
            ConnectionDto.builder()
                .name("CDP-Prod-Cluster")
                .description("Production CDP cluster for live workloads")
                .environment(ConnectionDto.Environment.PROD)
                .platformType("CDP")
                .hcfsNamespace("hdfs://prod-cluster")
                .hs2Uri("jdbc:hive2://prod-hiveserver:10000/default")
                .hs2Username("hive")
                .hs2DriverClassName("org.apache.hive.jdbc.HiveDriver")
                .metastoreDirectEnabled(true)
                .metastoreDirectUri("thrift://prod-metastore:9083")
                .metastoreDirectType("POSTGRESQL")
                .partitionDiscoveryAuto(true)
                .enableAutoTableStats(true)
                .enableAutoColumnStats(true)
                .created(LocalDateTime.now())
                .modified(LocalDateTime.now())
                .build(),
                
            ConnectionDto.builder()
                .name("Legacy-HDP-Cluster")
                .description("Legacy HDP cluster for migration source")
                .environment(ConnectionDto.Environment.PROD)
                .platformType("HDP")
                .hcfsNamespace("hdfs://legacy-cluster")
                .hs2Uri("jdbc:hive2://legacy-hiveserver:10000/default")
                .hs2Username("hive")
                .hs2DriverClassName("org.apache.hive.jdbc.HiveDriver")
                .metastoreDirectEnabled(false)
                .partitionDiscoveryAuto(false)
                .created(LocalDateTime.now())
                .modified(LocalDateTime.now())
                .build(),
                
            ConnectionDto.builder()
                .name("Test-Environment")
                .description("Testing environment for validation and QA")
                .environment(ConnectionDto.Environment.TEST)
                .platformType("CDP")
                .hcfsNamespace("hdfs://test-cluster")
                .hs2Uri("jdbc:hive2://test-hiveserver:10000/default")
                .hs2Username("hive")
                .hs2DriverClassName("org.apache.hive.jdbc.HiveDriver")
                .metastoreDirectEnabled(true)
                .metastoreDirectUri("thrift://test-metastore:9083")
                .metastoreDirectType("MYSQL")
                .partitionDiscoveryAuto(true)
                .created(LocalDateTime.now())
                .modified(LocalDateTime.now())
                .build()
        };

        for (ConnectionDto connectionDto : connectionDtos) {
            try {
                connectionService.createConnection(connectionDto);
                log.debug("Created sample connection: {}", connectionDto.getName());
            } catch (Exception e) {
                if (e.getMessage().contains("already exists")) {
                    log.debug("Connection {} already exists, skipping", connectionDto.getName());
                } else {
                    log.warn("Failed to create connection: {}", connectionDto.getName(), e);
                }
            }
        }
    }

    private void createSampleDatasets() {
        log.info("Creating 6 demo datasets");
        
        DatasetDto[] datasets = {
            createDataset("Analytics-Warehouse", "Production analytics warehouse tables",
                new String[]{"analytics_db"}, new String[]{"fact_sales", "fact_orders", "dim_customer", "dim_product"}),
                
            createDataset("Financial-Reports", "Financial reporting databases",
                new String[]{"finance_db", "reporting_db"}, new String[]{"monthly_reports", "quarterly_reports"}),
                
            createDataset("Customer-360", "Customer 360 view databases with filters",
                new String[]{"customer_db"}, null, "customer_.*", ".*_temp"),
                
            createDataset("Operational-Data", "Operational data sources",
                new String[]{"operations_db", "logs_db"}, new String[]{"system_logs", "audit_logs", "metrics"}),
                
            createDataset("Marketing-Analytics", "Marketing and campaign data",
                new String[]{"marketing_db"}, null, "campaign_.*|email_.*", ".*_staging"),
                
            createDataset("Data-Lake-Raw", "Raw data lake ingestion",
                new String[]{"raw_db", "staging_db"}, null, ".*", ".*_backup|.*_archive")
        };

        for (DatasetDto dataset : datasets) {
            try {
                Map<String, Object> result = datasetManagementService.saveDataset(dataset.getName(), dataset);
                if ("SUCCESS".equals(result.get("status"))) {
                    log.debug("Created sample dataset: {}", dataset.getName());
                } else {
                    log.warn("Failed to create dataset {}: {}", dataset.getName(), result.get("message"));
                }
            } catch (Exception e) {
                log.warn("Failed to create dataset: {}", dataset.getName(), e);
            }
        }
    }

    private void createSampleConfigurations() {
        log.info("Creating 5 demo configurations");
        
        try {
            // Create 5 sample configurations using ConfigLiteDto
            ConfigLiteDto[] configurations = {
                createSchemaOnlyConfiguration(),
                createHybridConfiguration(),
                createSqlConfiguration(),
                createLinkedConfiguration(),
                createStorageMigrationConfiguration()
            };

            for (ConfigLiteDto config : configurations) {
                try {
                    Map<String, Object> result = configurationManagementService.saveConfiguration(config.getName(), config);
                    if ("SUCCESS".equals(result.get("status"))) {
                        log.debug("Created sample configuration: {}", config.getName());
                    } else {
                        log.warn("Failed to create configuration {}: {}", config.getName(), result.get("message"));
                    }
                } catch (Exception e) {
                    log.warn("Failed to create configuration: {}", config.getName(), e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to create sample configurations", e);
        }
    }

    private DatasetDto createDataset(String name, String description, String[] databases, String[] tables) {
        return createDataset(name, description, databases, tables, null, null);
    }

    private DatasetDto createDataset(String name, String description, String[] databases, String[] tables, 
                                   String includePattern, String excludePattern) {
        DatasetDto dataset = new DatasetDto();
        dataset.setName(name);
        dataset.setDescription(description);
        
        for (String dbName : databases) {
            DatasetDto.DatabaseSpec dbSpec = new DatasetDto.DatabaseSpec();
            dbSpec.setDatabaseName(dbName);
            
            if (tables != null) {
                dbSpec.setTables(Arrays.asList(tables));
            } else if (includePattern != null || excludePattern != null) {
                DatasetDto.TableFilter filter = new DatasetDto.TableFilter();
                filter.setIncludePattern(includePattern);
                filter.setExcludePattern(excludePattern);
                filter.setTableTypes(Arrays.asList("MANAGED_TABLE", "EXTERNAL_TABLE"));
                dbSpec.setFilter(filter);
            }
            
            dataset.getDatabases().add(dbSpec);
        }
        
        return dataset;
    }

    private ConfigLiteDto createSchemaOnlyConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("Schema-Only-Migration");
        config.setComment("Schema Only Migration Configuration - metadata only transfer");
        config.setMigrateNonNative(true);

        return config;
    }

    private ConfigLiteDto createHybridConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("Hybrid-Data-Migration");
        config.setComment("Hybrid Data Migration Configuration - combines schema and limited data movement");
        config.setMigrateNonNative(true);

        return config;
    }

    private ConfigLiteDto createSqlConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("SQL-Export-Import");
        config.setComment("SQL Export Import Configuration - uses SQL-based data transfer");
        config.setMigrateNonNative(true);
        
        return config;
    }

    private ConfigLiteDto createLinkedConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("Linked-Tables-Setup");
        config.setComment("Linked Tables Configuration - creates external tables pointing to original data");
        config.setMigrateNonNative(false);

        return config;
    }

    private ConfigLiteDto createStorageMigrationConfiguration() {
        ConfigLiteDto config = new ConfigLiteDto("Storage-Migration-Plan");
        config.setComment("Storage Migration Configuration - in-place storage format migrations");
        config.setMigrateNonNative(true);

        return config;
    }
}