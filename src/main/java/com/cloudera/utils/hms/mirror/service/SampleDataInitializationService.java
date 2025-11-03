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
import com.cloudera.utils.hms.mirror.domain.core.IcebergConversion;
import com.cloudera.utils.hms.mirror.domain.core.MigrateACID;
import com.cloudera.utils.hms.mirror.domain.core.Optimization;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.support.PlatformType;
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
    private final ConnectionManagementService connectionManagementService;
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
            createSystemConfigurations();
            log.info("Sample data initialization completed successfully");
        } catch (Exception e) {
            log.error("Failed to initialize sample data", e);
        }
    }

    private void createSampleConnections() {
        log.info("Creating 4 demo connections");

        ConnectionDto[] connectionDtos = {
                // 1. Legacy HDP2 Production Cluster
                ConnectionDto.builder()
                        .name("DEMO HDP2-Production")
                        .description("Legacy HDP 2.6.5 production cluster - source for migration")
                        .environment(ConnectionDto.Environment.PROD)
                        .platformType(PlatformType.HDP2)
                        .hcfsNamespace("hdfs://hdp2-prod-nn:8020")
                        .hs2Uri("jdbc:hive2://hdp2-prod-hs2:10000/default")
                        .hs2Username("hive")
                        .hs2Password("")
                        .hs2ConnectionProperties(Map.of(
                                "transportMode", "binary",
                                "ssl", "false"
                        ))
                        .metastoreDirectEnabled(true)
                        .metastoreDirectUri("jdbc:mysql://hdp2-mysql:3306/hive")
                        .metastoreDirectType("MYSQL")
                        .metastoreDirectUsername("hive")
                        .metastoreDirectPassword("hive_password")
                        .metastoreDirectMinConnections(2)
                        .metastoreDirectMaxConnections(10)
                        .partitionDiscoveryAuto(true)
                        .partitionDiscoveryInitMSCK(false)
                        .partitionBucketLimit(100)
                        .createIfNotExists(false)
                        .enableAutoTableStats(false)
                        .enableAutoColumnStats(false)
                        .created(LocalDateTime.now())
                        .isDefault(false)
                        .build(),

                // 2. CDH6 Production Cluster
                ConnectionDto.builder()
                        .name("DEMO CDH6-Production")
                        .description("CDH 6.3.4 production cluster with Kerberos")
                        .environment(ConnectionDto.Environment.PROD)
                        .platformType(PlatformType.CDH6)
                        .hcfsNamespace("hdfs://cdh6-prod-nameservice")
                        .hs2Uri("jdbc:hive2://cdh6-prod-hs2:10000/default;principal=hive/_HOST@REALM.COM")
                        .hs2Username("")
                        .hs2Password("")
                        .hs2ConnectionProperties(Map.of(
                                "transportMode", "http",
                                "httpPath", "cliservice",
                                "ssl", "true",
                                "sslTrustStore", "/opt/cloudera/security/jks/truststore.jks"
                        ))
                        .metastoreDirectEnabled(true)
                        .metastoreDirectUri("jdbc:postgresql://cdh6-postgres:5432/metastore")
                        .metastoreDirectType("POSTGRES")
                        .metastoreDirectUsername("hive")
                        .metastoreDirectPassword("hive_password")
                        .metastoreDirectMinConnections(3)
                        .metastoreDirectMaxConnections(15)
                        .partitionDiscoveryAuto(true)
                        .partitionDiscoveryInitMSCK(true)
                        .partitionBucketLimit(200)
                        .createIfNotExists(true)
                        .enableAutoTableStats(true)
                        .enableAutoColumnStats(false)
                        .created(LocalDateTime.now())
                        .isDefault(false)
                        .build(),

                // 3. CDP Private Cloud Base 7.1.9 (Default)
                ConnectionDto.builder()
                        .name("DEMO CDP-Base-7.1.9")
                        .description("CDP Private Cloud Base 7.1.9 target cluster - default connection")
                        .environment(ConnectionDto.Environment.PROD)
                        .platformType(PlatformType.CDP7_1_9_SP1)
                        .hcfsNamespace("hdfs://cdp7-nameservice")
                        .hs2Uri("jdbc:hive2://cdp7-hs2-lb:10000/default;principal=hive/_HOST@REALM.COM;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")
                        .hs2Username("")
                        .hs2Password("")
                        .hs2ConnectionProperties(Map.of(
                                "transportMode", "http",
                                "httpPath", "cliservice",
                                "ssl", "true",
                                "sslTrustStore", "/opt/cloudera/security/jks/truststore.jks",
                                "retries", "3"
                        ))
                        .metastoreDirectEnabled(true)
                        .metastoreDirectUri("jdbc:postgresql://cdp7-rds.aws.com:5432/hive_metastore")
                        .metastoreDirectType("POSTGRES")
                        .metastoreDirectUsername("hive_admin")
                        .metastoreDirectPassword("secure_password")
                        .metastoreDirectMinConnections(5)
                        .metastoreDirectMaxConnections(20)
                        .partitionDiscoveryAuto(true)
                        .partitionDiscoveryInitMSCK(true)
                        .partitionBucketLimit(500)
                        .createIfNotExists(true)
                        .enableAutoTableStats(true)
                        .enableAutoColumnStats(true)
                        .created(LocalDateTime.now())
                        .isDefault(true)
                        .build(),

                // 4. CDP Public Cloud Development
                ConnectionDto.builder()
                        .name("CDP-DataHub-Dev")
                        .description("CDP Public Cloud DataHub development environment with S3")
                        .environment(ConnectionDto.Environment.DEV)
                        .platformType(PlatformType.CDP7_2)
                        .hcfsNamespace("s3a://cdp-dev-bucket")
                        .hs2Uri("jdbc:hive2://dev-gateway.cloudera.site:443/default;transportMode=http;httpPath=cdp-proxy-api/hive;ssl=true")
                        .hs2Username("dev_user")
                        .hs2Password("dev_token")
                        .hs2ConnectionProperties(Map.of(
                                "transportMode", "http",
                                "httpPath", "cdp-proxy-api/hive",
                                "ssl", "true",
                                "retries", "5"
                        ))
                        .metastoreDirectEnabled(false)
                        .metastoreDirectUri(null)
                        .metastoreDirectType(null)
                        .metastoreDirectUsername(null)
                        .metastoreDirectPassword(null)
                        .metastoreDirectMinConnections(2)
                        .metastoreDirectMaxConnections(10)
                        .partitionDiscoveryAuto(true)
                        .partitionDiscoveryInitMSCK(false)
                        .partitionBucketLimit(100)
                        .createIfNotExists(true)
                        .enableAutoTableStats(false)
                        .enableAutoColumnStats(false)
                        .created(LocalDateTime.now())
                        .isDefault(false)
                        .build()
        };

        for (ConnectionDto connectionDto : connectionDtos) {
            try {
                Map<String, Object> result = connectionManagementService.save(connectionDto);
                if ("SUCCESS".equals(result.get("status"))) {
                    log.debug("Created sample connection: {}", connectionDto.getName());
                } else if (result.get("message") != null && result.get("message").toString().contains("already exists")) {
                    log.debug("Connection {} already exists, skipping", connectionDto.getName());
                } else {
                    log.warn("Failed to create connection: {} - {}", connectionDto.getName(), result.get("message"));
                }
            } catch (Exception e) {
                log.warn("Failed to create connection: {}", connectionDto.getName(), e);
            }
        }
    }

    private void createSampleDatasets() {
        log.info("Creating 6 demo datasets");
        
        DatasetDto[] datasets = {
            createDataset("DEMO Analytics-Warehouse", "Production analytics warehouse tables",
                new String[]{"analytics_db"}, new String[]{"fact_sales", "fact_orders", "dim_customer", "dim_product"}),
                
            createDataset("DEMO Financial-Reports", "Financial reporting databases",
                new String[]{"finance_db", "reporting_db"}, new String[]{"monthly_reports", "quarterly_reports"}),
                
            createDataset("DEMO Customer-360", "Customer 360 view databases with filters",
                new String[]{"customer_db"}, null, "customer_.*", ".*_temp"),
                
            createDataset("DEMO Operational-Data", "Operational data sources",
                new String[]{"operations_db", "logs_db"}, new String[]{"system_logs", "audit_logs", "metrics"}),
                
            createDataset("DEMO Marketing-Analytics", "Marketing and campaign data",
                new String[]{"marketing_db"}, null, "campaign_.*|email_.*", ".*_staging"),
                
            createDataset("DEMO Data-Lake-Raw", "Raw data lake ingestion",
                new String[]{"raw_db", "staging_db"}, null, ".*", ".*_backup|.*_archive")
        };

        for (DatasetDto dataset : datasets) {
            try {
                Map<String, Object> result = datasetManagementService.save(dataset);
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

    private void createSystemConfigurations() {
        log.info("Creating 5 demo configurations");

        try {
            // Create 5 sample configurations using ConfigLiteDto
            ConfigLiteDto[] configurations = {
                    // 1. Basic Schema-Only Migration
                    createSchemaOnlyConfig(),

                    // 2. ACID Downgrade with SQL Strategy
                    createAcidDowngradeConfig(),

                    // 3. Hybrid Migration with Statistics
                    createHybridMigrationConfig(),

                    // 4. Storage Migration with Warehouse Plan
                    createStorageMigrationConfig(),

                    // 5. Iceberg Conversion
                    createIcebergConversionConfig()
            };

            for (ConfigLiteDto config : configurations) {
                try {
                    Map<String, Object> result = configurationManagementService.save(config);
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

    private ConfigLiteDto createSchemaOnlyConfig() {
        ConfigLiteDto config = new ConfigLiteDto("DEMO Schema-Only-Migration");
        config.setDescription("Basic schema-only migration from HDP2 to CDP7 without data movement");
//        config.setExecute(false); // Dry-run by default
        config.setCreateIfNotExists(true);
        config.setEnableAutoTableStats(false);
        config.setEnableAutoColumnStats(false);

        // Basic VIEW migration
        config.getMigrateVIEW().setOn(true);

        return config;
    }

    private ConfigLiteDto createAcidDowngradeConfig() {
        ConfigLiteDto config = new ConfigLiteDto("DEMO ACID-Downgrade-SQL");
        config.setDescription("ACID table downgrade using SQL strategy for legacy platform compatibility");
//        config.setExecute(false); // Dry-run by default
        config.setCreateIfNotExists(true);

        // Configure ACID migration - downgrade to external tables
        MigrateACID acidConfig = config.getMigrateACID();
        acidConfig.setOn(true);
        acidConfig.setDowngrade(true);
        acidConfig.setInplace(false);
        acidConfig.setPartitionLimit(500);
        acidConfig.setArtificialBucketThreshold(4);

        // VIEW migration
        config.getMigrateVIEW().setOn(true);

        return config;
    }

    private ConfigLiteDto createHybridMigrationConfig() {
        ConfigLiteDto config = new ConfigLiteDto("DEMO Hybrid-Migration-Stats");
        config.setDescription("Hybrid migration with automatic statistics collection and optimization");
//        config.setExecute(false); // Dry-run by default
        config.setCreateIfNotExists(true);
        config.setEnableAutoTableStats(true);
        config.setEnableAutoColumnStats(true);

        // Optimization settings
        Optimization optimization = config.getOptimization();
        optimization.setSkip(false);
        optimization.setSortDynamicPartitionInserts(true);
        optimization.setAutoTune(true);
        optimization.setCompressTextOutput(true);

        // VIEW migration
        config.getMigrateVIEW().setOn(true);

        return config;
    }

    private ConfigLiteDto createStorageMigrationConfig() {
        ConfigLiteDto config = new ConfigLiteDto("DEMO Storage-Migration");
        config.setDescription("Storage migration with namespace translation");
//        config.setExecute(false); // Dry-run by default
        config.setCreateIfNotExists(true);
        config.setForceExternalLocation(true);
        config.setEnableAutoTableStats(false);
        config.setEnableAutoColumnStats(false);

        return config;
    }

    private ConfigLiteDto createIcebergConversionConfig() {
        ConfigLiteDto config = new ConfigLiteDto("DEMO Iceberg-Conversion");
        config.setDescription("Convert Hive tables to Iceberg format for improved performance");
//        config.setExecute(false); // Dry-run by default
        config.setCreateIfNotExists(true);
        config.setMigrateNonNative(true); // Required for Iceberg

        // Iceberg conversion settings
        IcebergConversion icebergConversion = config.getIcebergConversion();
        icebergConversion.setVersion(2); // Iceberg format version

        // Optimization for Iceberg
        Optimization optimization = config.getOptimization();
        optimization.setSkip(false);
        optimization.setAutoTune(true);
        optimization.setCompressTextOutput(true);

        return config;
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
                filter.setIncludeRegEx(includePattern);
                filter.setExcludeRegEx(excludePattern);
                filter.setTableTypes(Arrays.asList("MANAGED_TABLE", "EXTERNAL_TABLE"));
                dbSpec.setFilter(filter);
            }
            
            dataset.getDatabases().add(dbSpec);
        }
        
        return dataset;
    }

}