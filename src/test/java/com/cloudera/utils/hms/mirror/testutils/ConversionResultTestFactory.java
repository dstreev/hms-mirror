/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.testutils;

import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.domain.core.*;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;

import java.util.*;

/**
 * Test utility factory for creating ConversionResult test objects.
 * Provides 5 pre-configured ConversionResult scenarios with full test data:
 *
 * 1. Simple SCHEMA_ONLY migration
 * 2. ACID table migration with partitions and data movement
 * 3. VIEW migration with dependent tables
 * 4. HYBRID migration with mixed table types
 * 5. EXPORT_IMPORT with complex configuration and multiple databases
 *
 * All objects are fully populated with test data for all non-@JsonIgnore fields.
 */
public class ConversionResultTestFactory {

    /**
     * Scenario 1: Simple SCHEMA_ONLY migration
     * - Single database with external tables
     * - No data movement, metadata only
     * - Minimal configuration
     */
    public static ConversionResult createSimpleSchemaOnlyConversion() {
        ConversionResult result = new ConversionResult();
        result.setKey("schema-only-" + System.currentTimeMillis());

        // Basic Configuration
        ConfigLiteDto config = new ConfigLiteDto();
        config.setMigrateACID(new MigrateACID());
        config.getMigrateACID().setOn(false);
        config.setMigrateVIEW(new MigrateVIEW());
        config.getMigrateVIEW().setOn(false);
        config.setMigrateNonNative(false);
        config.setCreateIfNotExists(true);
        config.setOptimization(new Optimization());
        config.getOptimization().setSkipStatsCollection(true);
        // Create Some Overrides
        config.getOptimization().getOverrides().addProperty("key1", "value1", SideType.BOTH);
        config.getOptimization().getOverrides().addProperty("tez.queue.name", "marketing", SideType.LEFT);
        config.getOptimization().getOverrides().addProperty("tez.queue.name", "finance", SideType.RIGHT);

        config.setTransfer(new TransferConfig());
        config.getTransfer().setTargetNamespace("hdfs://target-cluster:8020");
        config.setOwnershipTransfer(new TransferOwnership());
        result.setConfig(config);

        // Dataset
        DatasetDto dataset = new DatasetDto();
        dataset.setName("simple-schema-dataset");
        dataset.setDescription("Simple schema-only migration dataset");
        dataset.setDatabases(new ArrayList<>());

        DatasetDto.DatabaseSpec dbSpec = new DatasetDto.DatabaseSpec();
        dbSpec.setDatabaseName("sales_db");
        DatasetDto.TableFilter filter = new DatasetDto.TableFilter();
        filter.setMaxSizeMb(0);
        filter.setMaxPartitions(0);
        dbSpec.setFilter(filter);
        dataset.getDatabases().add(dbSpec);
        result.setDataset(dataset);

        // Connections
        ConnectionDto leftConn = createConnection("left-cdh6",
            "jdbc:hive2://cdh6-cluster:10000", PlatformType.CDH6, "hdfs://cdh6:8020");
        ConnectionDto rightConn = createConnection("right-cdp7",
            "jdbc:hive2://cdp7-cluster:10000", PlatformType.CDP7_3, "hdfs://cdp7:8020");
        result.setConnection(Environment.LEFT, leftConn);
        result.setConnection(Environment.RIGHT, rightConn);

        // Job
        JobDto job = new JobDto();
        job.setName("simple-schema-migration");
        job.setDescription("Migrate schema only for sales database");
        job.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
        job.setSync(false);
        job.setConfigReference("simple-config");
        job.setDatasetReference("simple-schema-dataset");
        job.setLeftConnectionReference("left-cdh6");
        job.setRightConnectionReference("right-cdp7");
        result.setJob(job);

        // JobExecution
        JobExecution jobExec = new JobExecution();
        jobExec.setDryRun(false);
        result.setJobExecution(jobExec);

        // Database with tables
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("sales_db");

        // Add external table
        TableMirror table1 = createExternalTable("customers",
            Arrays.asList("id INT", "name STRING", "email STRING"),
            "hdfs://cdh6:8020/warehouse/sales_db.db/customers",
            false);
        dbMirror.getTableMirrors().put("customers", table1);

        TableMirror table2 = createExternalTable("orders",
            Arrays.asList("order_id INT", "customer_id INT", "order_date DATE", "amount DECIMAL(10,2)"),
            "hdfs://cdh6:8020/warehouse/sales_db.db/orders",
            false);
        dbMirror.getTableMirrors().put("orders", table2);

        result.getDatabases().put("sales_db", dbMirror);

        return result;
    }

    /**
     * Scenario 2: ACID table migration with partitions and data movement
     * - ACID managed tables with partitions
     * - Data movement enabled
     * - Statistics collection
     */
    public static ConversionResult createAcidMigrationConversion() {
        ConversionResult result = new ConversionResult();
        result.setKey("acid-migration-" + System.currentTimeMillis());

        // ACID Configuration
        ConfigLiteDto config = new ConfigLiteDto();
        config.setMigrateACID(new MigrateACID());
        config.getMigrateACID().setOn(true);
        config.getMigrateACID().setDowngrade(false);
        config.setMigrateVIEW(new MigrateVIEW());
        config.getMigrateVIEW().setOn(false);
        config.setMigrateNonNative(false);
        config.setCreateIfNotExists(true);
        config.setOptimization(new Optimization());
        config.getOptimization().setSkipStatsCollection(false);
        config.getOptimization().setCompressTextOutput(true);
        config.setTransfer(new TransferConfig());
        config.getTransfer().setTargetNamespace("hdfs://cdp-target:8020");
        config.getTransfer().setIntermediateStorage("s3a://migration-bucket/staging");
        config.setOwnershipTransfer(new TransferOwnership());
        config.getOwnershipTransfer().setTable(true);
        result.setConfig(config);

        // Dataset with partition limits
        DatasetDto dataset = new DatasetDto();
        dataset.setName("acid-dataset");
        dataset.setDescription("ACID tables with partitions");
        dataset.setDatabases(new ArrayList<>());

        DatasetDto.DatabaseSpec dbSpec = new DatasetDto.DatabaseSpec();
        dbSpec.setDatabaseName("transaction_db");
        DatasetDto.TableFilter filter = new DatasetDto.TableFilter();
        filter.setMaxSizeMb(5000);
        filter.setMaxPartitions(1000);
        filter.setIncludeRegEx("txn_.*");
        dbSpec.setFilter(filter);
        dataset.getDatabases().add(dbSpec);
        result.setDataset(dataset);

        // Connections
        ConnectionDto leftConn = createConnection("left-hdp3",
            "jdbc:hive2://hdp3-cluster:10000", PlatformType.HDP3, "hdfs://hdp3:8020");
        ConnectionDto rightConn = createConnection("right-cdp7",
            "jdbc:hive2://cdp7-cluster:10000", PlatformType.CDP7_3, "s3a://cdp-data/");
        result.setConnection(Environment.LEFT, leftConn);
        result.setConnection(Environment.RIGHT, rightConn);

        // Job
        JobDto job = new JobDto();
        job.setName("acid-migration");
        job.setDescription("Migrate ACID tables to CDP");
        job.setStrategy(DataStrategyEnum.ACID);
        job.setSync(false);
        job.setConfigReference("acid-config");
        job.setDatasetReference("acid-dataset");
        job.setLeftConnectionReference("left-hdp3");
        job.setRightConnectionReference("right-cdp7");
        result.setJob(job);

        // JobExecution
        JobExecution jobExec = new JobExecution();
        jobExec.setDryRun(false);
        result.setJobExecution(jobExec);

        // Database with ACID tables
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("transaction_db");

        // ACID partitioned table
        TableMirror table1 = createAcidTable("txn_history",
            Arrays.asList("txn_id BIGINT", "customer_id INT", "amount DECIMAL(18,2)", "status STRING"),
            "hdfs://hdp3:8020/warehouse/tablespace/managed/hive/transaction_db.db/txn_history",
            true,
            "txn_date");

        // Add partitions
        EnvironmentTable leftEnv = table1.getEnvironmentTable(Environment.LEFT);
        Map<String, String> partitions = new LinkedHashMap<>();
        partitions.put("txn_date=2024-01-01", "hdfs://hdp3:8020/warehouse/tablespace/managed/hive/transaction_db.db/txn_history/txn_date=2024-01-01");
        partitions.put("txn_date=2024-01-02", "hdfs://hdp3:8020/warehouse/tablespace/managed/hive/transaction_db.db/txn_history/txn_date=2024-01-02");
        partitions.put("txn_date=2024-01-03", "hdfs://hdp3:8020/warehouse/tablespace/managed/hive/transaction_db.db/txn_history/txn_date=2024-01-03");
        leftEnv.setPartitions(partitions);

        // Add statistics
        leftEnv.getStatistics().put("data_size", 1024L * 1024 * 500); // 500 MB
        leftEnv.getStatistics().put("file_count", 150);
        leftEnv.getStatistics().put("avg_file_size", 1024.0 * 1024 * 3.33); // ~3.33 MB

        // Add SQL statements
        List<Pair> sqlStatements = new ArrayList<>();
        sqlStatements.add(new Pair("Create Database", "CREATE DATABASE IF NOT EXISTS transaction_db"));
        sqlStatements.add(new Pair("Create Table", "CREATE TABLE transaction_db.txn_history (txn_id BIGINT, customer_id INT, amount DECIMAL(18,2), status STRING) PARTITIONED BY (txn_date DATE) STORED AS ORC TBLPROPERTIES ('transactional'='true')"));
        sqlStatements.add(new Pair("Add Partitions", "ALTER TABLE transaction_db.txn_history ADD IF NOT EXISTS PARTITION (txn_date='2024-01-01')"));
        leftEnv.setSql(sqlStatements);

        dbMirror.getTableMirrors().put("txn_history", table1);

        result.getDatabases().put("transaction_db", dbMirror);

        return result;
    }

    /**
     * Scenario 3: VIEW migration with dependent tables
     * - Multiple VIEWs
     * - External tables that VIEWs depend on
     * - VIEW-only migration mode
     */
    public static ConversionResult createViewMigrationConversion() {
        ConversionResult result = new ConversionResult();
        result.setKey("view-migration-" + System.currentTimeMillis());

        // VIEW Configuration
        ConfigLiteDto config = new ConfigLiteDto();
        config.setMigrateACID(new MigrateACID());
        config.getMigrateACID().setOn(false);
        config.setMigrateVIEW(new MigrateVIEW());
        config.getMigrateVIEW().setOn(true);
        config.setMigrateNonNative(false);
        config.setCreateIfNotExists(true);
        config.setOptimization(new Optimization());
        config.getOptimization().setSkipStatsCollection(true);
        config.setTransfer(new TransferConfig());
        config.getTransfer().setTargetNamespace("hdfs://target:8020");
        config.setOwnershipTransfer(new TransferOwnership());
        result.setConfig(config);

        // Dataset
        DatasetDto dataset = new DatasetDto();
        dataset.setName("view-dataset");
        dataset.setDescription("Views migration dataset");
        dataset.setDatabases(new ArrayList<>());

        DatasetDto.DatabaseSpec dbSpec = new DatasetDto.DatabaseSpec();
        dbSpec.setDatabaseName("analytics_db");
        DatasetDto.TableFilter filter = new DatasetDto.TableFilter();
        filter.setIncludeRegEx(".*_view");
        dbSpec.setFilter(filter);
        dataset.getDatabases().add(dbSpec);
        result.setDataset(dataset);

        // Connections
        ConnectionDto leftConn = createConnection("left-cdp71",
            "jdbc:hive2://cdp71-cluster:10000", PlatformType.CDP7_1, "hdfs://cdp71:8020");
        ConnectionDto rightConn = createConnection("right-cdp73",
            "jdbc:hive2://cdp73-cluster:10000", PlatformType.CDP7_3, "hdfs://cdp73:8020");
        result.setConnection(Environment.LEFT, leftConn);
        result.setConnection(Environment.RIGHT, rightConn);

        // Job
        JobDto job = new JobDto();
        job.setName("view-migration");
        job.setDescription("Migrate analytical views");
        job.setStrategy(DataStrategyEnum.DUMP);
        job.setSync(false);
        job.setConfigReference("view-config");
        job.setDatasetReference("view-dataset");
        job.setLeftConnectionReference("left-cdp71");
        job.setRightConnectionReference("right-cdp73");
        result.setJob(job);

        // JobExecution
        JobExecution jobExec = new JobExecution();
        jobExec.setDryRun(true);
        result.setJobExecution(jobExec);

        // Database with VIEWs
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("analytics_db");

        // Base table (not a view)
        TableMirror baseTable = createExternalTable("sales_fact",
            Arrays.asList("sale_id BIGINT", "product_id INT", "customer_id INT", "sale_date DATE", "amount DECIMAL(10,2)"),
            "hdfs://cdp71:8020/warehouse/tablespace/external/hive/analytics_db.db/sales_fact",
            false);
        dbMirror.getTableMirrors().put("sales_fact", baseTable);

        // VIEW 1
        TableMirror view1 = createView("daily_sales_view",
            "CREATE VIEW daily_sales_view AS SELECT sale_date, SUM(amount) as total_sales FROM sales_fact GROUP BY sale_date");
        dbMirror.getTableMirrors().put("daily_sales_view", view1);

        // VIEW 2
        TableMirror view2 = createView("customer_summary_view",
            "CREATE VIEW customer_summary_view AS SELECT customer_id, COUNT(*) as purchase_count, SUM(amount) as lifetime_value FROM sales_fact GROUP BY customer_id");
        dbMirror.getTableMirrors().put("customer_summary_view", view2);

        result.getDatabases().put("analytics_db", dbMirror);

        return result;
    }

    /**
     * Scenario 4: HYBRID migration with mixed table types
     * - Mix of ACID, external, and managed tables
     * - Multiple databases
     * - Complex hybrid configuration
     */
    public static ConversionResult createHybridMigrationConversion() {
        ConversionResult result = new ConversionResult();
        result.setKey("hybrid-migration-" + System.currentTimeMillis());

        // HYBRID Configuration
        ConfigLiteDto config = new ConfigLiteDto();
        config.setMigrateACID(new MigrateACID());
        config.getMigrateACID().setOn(true);
        config.setMigrateVIEW(new MigrateVIEW());
        config.getMigrateVIEW().setOn(true);
        config.setMigrateNonNative(true);
        config.setCreateIfNotExists(true);
        config.setOptimization(new Optimization());
        config.getOptimization().setSkipStatsCollection(false);
        config.getOptimization().setCompressTextOutput(true);
        config.getOptimization().setAutoTune(true);
        config.setTransfer(new TransferConfig());
        config.getTransfer().setTargetNamespace("s3a://target-bucket/warehouse");
        config.getTransfer().setIntermediateStorage("s3a://staging-bucket/intermediate");
        config.setOwnershipTransfer(new TransferOwnership());
        config.getOwnershipTransfer().setTable(true);
        config.getOwnershipTransfer().setDatabase(true);
        result.setConfig(config);

        // Dataset with multiple databases
        DatasetDto dataset = new DatasetDto();
        dataset.setName("hybrid-dataset");
        dataset.setDescription("Mixed table types across multiple databases");
        dataset.setDatabases(new ArrayList<>());

        DatasetDto.DatabaseSpec dbSpec1 = new DatasetDto.DatabaseSpec();
        dbSpec1.setDatabaseName("operational_db");
        DatasetDto.TableFilter filter1 = new DatasetDto.TableFilter();
        filter1.setMaxSizeMb(10000);
        dbSpec1.setFilter(filter1);
        dataset.getDatabases().add(dbSpec1);

        DatasetDto.DatabaseSpec dbSpec2 = new DatasetDto.DatabaseSpec();
        dbSpec2.setDatabaseName("reporting_db");
        DatasetDto.TableFilter filter2 = new DatasetDto.TableFilter();
        filter2.setMaxSizeMb(5000);
        dbSpec2.setFilter(filter2);
        dataset.getDatabases().add(dbSpec2);

        result.setDataset(dataset);

        // Connections with Kerberos
        ConnectionDto leftConn = createConnection("left-hdp25",
            "jdbc:hive2://hdp25-cluster:10000/default;principal=hive/_HOST@REALM.COM",
            PlatformType.HDP2, "hdfs://hdp25:8020");
        ConnectionDto rightConn = createConnection("right-cdp73",
            "jdbc:hive2://cdp73-cluster:10000/default;principal=hive/_HOST@REALM.COM",
            PlatformType.CDP7_3, "s3a://cdp-data/");
        result.setConnection(Environment.LEFT, leftConn);
        result.setConnection(Environment.RIGHT, rightConn);

        // Job with hybrid config
        JobDto job = new JobDto();
        job.setName("hybrid-migration");
        job.setDescription("Complex hybrid migration with mixed table types");
        job.setStrategy(DataStrategyEnum.HYBRID);
        job.setSync(false);
        job.setHybrid(new HybridConfig());
        job.getHybrid().setExportImportPartitionLimit(500);
        job.getHybrid().setSqlPartitionLimit(100);
        job.setConfigReference("hybrid-config");
        job.setDatasetReference("hybrid-dataset");
        job.setLeftConnectionReference("left-hdp25");
        job.setRightConnectionReference("right-cdp73");
        result.setJob(job);

        // JobExecution
        JobExecution jobExec = new JobExecution();
        jobExec.setDryRun(false);
        result.setJobExecution(jobExec);

        // Database 1: Operational with ACID tables
        DBMirror db1 = new DBMirror();
        db1.setName("operational_db");

        TableMirror acidTable = createAcidTable("inventory",
            Arrays.asList("item_id INT", "warehouse_id INT", "quantity INT", "last_updated TIMESTAMP"),
            "hdfs://hdp25:8020/apps/hive/warehouse/operational_db.db/inventory",
            true,
            null);
        db1.getTableMirrors().put("inventory", acidTable);

        TableMirror externalTable = createExternalTable("suppliers",
            Arrays.asList("supplier_id INT", "name STRING", "country STRING"),
            "hdfs://hdp25:8020/data/suppliers",
            false);
        db1.getTableMirrors().put("suppliers", externalTable);

        result.getDatabases().put("operational_db", db1);

        // Database 2: Reporting with views
        DBMirror db2 = new DBMirror();
        db2.setName("reporting_db");

        TableMirror reportTable = createExternalTable("sales_summary",
            Arrays.asList("year INT", "month INT", "total_sales DECIMAL(18,2)", "order_count BIGINT"),
            "hdfs://hdp25:8020/warehouse/reporting/sales_summary",
            false);
        db2.getTableMirrors().put("sales_summary", reportTable);

        TableMirror reportView = createView("quarterly_report_view",
            "CREATE VIEW quarterly_report_view AS SELECT year, FLOOR((month-1)/3)+1 as quarter, SUM(total_sales) as quarterly_sales FROM sales_summary GROUP BY year, FLOOR((month-1)/3)+1");
        db2.getTableMirrors().put("quarterly_report_view", reportView);

        result.getDatabases().put("reporting_db", db2);

        return result;
    }

    /**
     * Scenario 5: EXPORT_IMPORT with complex configuration and multiple databases
     * - Multiple databases with various table types
     * - Export/Import strategy
     * - Disaster recovery settings
     * - Complex warehouse planning
     */
    public static ConversionResult createExportImportConversion() {
        ConversionResult result = new ConversionResult();
        result.setKey("export-import-" + System.currentTimeMillis());

        // Complex Configuration
        ConfigLiteDto config = new ConfigLiteDto();
        config.setMigrateACID(new MigrateACID());
        config.getMigrateACID().setOn(true);
        config.getMigrateACID().setDowngrade(true);
        config.setMigrateVIEW(new MigrateVIEW());
        config.getMigrateVIEW().setOn(true);
        config.setMigrateNonNative(true);
        config.setCreateIfNotExists(true);
        config.setOptimization(new Optimization());
        config.getOptimization().setSkipStatsCollection(false);
        config.getOptimization().setCompressTextOutput(true);
        config.getOptimization().setAutoTune(true);
        config.getOptimization().setSortDynamicPartitionInserts(true);
        config.setTransfer(new TransferConfig());
        config.getTransfer().setTargetNamespace("hdfs://production-cluster:8020");
        config.getTransfer().setIntermediateStorage("hdfs://staging-cluster:8020/intermediate");
        Warehouse warehouse = new Warehouse();
        warehouse.setManagedDirectory("/warehouse/tablespace/managed/hive");
        warehouse.setExternalDirectory("/warehouse/tablespace/external/hive");
        config.getTransfer().setWarehouse(warehouse);
        config.setOwnershipTransfer(new TransferOwnership());
        config.getOwnershipTransfer().setTable(true);
        config.getOwnershipTransfer().setDatabase(true);
        result.setConfig(config);

        // Complex Dataset with warehouse plans
        DatasetDto dataset = new DatasetDto();
        dataset.setName("export-import-dataset");
        dataset.setDescription("Full production migration with export/import");
        dataset.setDatabases(new ArrayList<>());

        // Database 1: Finance
        DatasetDto.DatabaseSpec dbSpec1 = new DatasetDto.DatabaseSpec();
        dbSpec1.setDatabaseName("finance_db");
        dbSpec1.setDbPrefix("prod_");
        DatasetDto.TableFilter filter1 = new DatasetDto.TableFilter();
        filter1.setMaxSizeMb(50000);
        filter1.setMaxPartitions(2000);
        filter1.setExcludeRegEx(".*_temp|.*_backup");
        dbSpec1.setFilter(filter1);
        Warehouse warehouse1 = new Warehouse();
        warehouse1.setManagedDirectory("hdfs://production-cluster:8020/warehouse/managed/finance");
        warehouse1.setExternalDirectory("hdfs://production-cluster:8020/warehouse/external/finance");
        dbSpec1.setWarehouse(warehouse1);
        dataset.getDatabases().add(dbSpec1);

        // Database 2: HR
        DatasetDto.DatabaseSpec dbSpec2 = new DatasetDto.DatabaseSpec();
        dbSpec2.setDatabaseName("hr_db");
        DatasetDto.TableFilter filter2 = new DatasetDto.TableFilter();
        filter2.setMaxSizeMb(10000);
        dbSpec2.setFilter(filter2);
        dataset.getDatabases().add(dbSpec2);

        // Database 3: Marketing
        DatasetDto.DatabaseSpec dbSpec3 = new DatasetDto.DatabaseSpec();
        dbSpec3.setDatabaseName("marketing_db");
        DatasetDto.TableFilter filter3 = new DatasetDto.TableFilter();
        filter3.setMaxSizeMb(20000);
        filter3.setIncludeRegEx("campaign_.*|customer_.*");
        dbSpec3.setFilter(filter3);
        dataset.getDatabases().add(dbSpec3);

        result.setDataset(dataset);

        // Production-grade connections
        ConnectionDto leftConn = createConnection("source-prod-cluster",
            "jdbc:hive2://source-prod:10000/default;ssl=true;sslTrustStore=/path/to/truststore.jks;principal=hive/_HOST@PROD.REALM.COM",
            PlatformType.CDP7_1, "hdfs://source-prod:8020");
        ConnectionDto rightConn = createConnection("target-prod-cluster",
            "jdbc:hive2://target-prod:10000/default;ssl=true;sslTrustStore=/path/to/truststore.jks;principal=hive/_HOST@PROD.REALM.COM",
            PlatformType.CDP7_3, "hdfs://target-prod:8020");
        result.setConnection(Environment.LEFT, leftConn);
        result.setConnection(Environment.RIGHT, rightConn);

        // Job with disaster recovery
        JobDto job = new JobDto();
        job.setName("production-migration");
        job.setDescription("Full production migration with disaster recovery setup");
        job.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
        job.setSync(true);
        job.setDisasterRecovery(true);
        job.setConfigReference("export-import-config");
        job.setDatasetReference("export-import-dataset");
        job.setLeftConnectionReference("source-prod-cluster");
        job.setRightConnectionReference("target-prod-cluster");
        result.setJob(job);

        // JobExecution
        JobExecution jobExec = new JobExecution();
        jobExec.setDryRun(false);
        result.setJobExecution(jobExec);

        // Database 1: Finance with complex tables
        DBMirror financeDb = new DBMirror();
        financeDb.setName("finance_db");

        // ACID table with many partitions
        TableMirror transactions = createAcidTable("transactions",
            Arrays.asList("txn_id BIGINT", "account_id INT", "amount DECIMAL(18,2)",
                         "type STRING", "status STRING", "created_at TIMESTAMP"),
            "hdfs://source-prod:8020/warehouse/managed/finance_db.db/transactions",
            true,
            "year, month");

        EnvironmentTable txnLeftEnv = transactions.getEnvironmentTable(Environment.LEFT);
        Map<String, String> txnPartitions = new LinkedHashMap<>();
        for (int year = 2023; year <= 2024; year++) {
            for (int month = 1; month <= 12; month++) {
                String partKey = String.format("year=%d/month=%02d", year, month);
                String partPath = String.format("hdfs://source-prod:8020/warehouse/managed/finance_db.db/transactions/%s", partKey);
                txnPartitions.put(partKey, partPath);
            }
        }
        txnLeftEnv.setPartitions(txnPartitions);
        txnLeftEnv.getStatistics().put("data_size", 1024L * 1024 * 1024 * 25); // 25 GB
        txnLeftEnv.getStatistics().put("file_count", 2400);
        financeDb.getTableMirrors().put("transactions", transactions);

        // External reference table
        TableMirror accounts = createExternalTable("accounts",
            Arrays.asList("account_id INT", "customer_id INT", "account_type STRING",
                         "balance DECIMAL(18,2)", "opened_date DATE"),
            "hdfs://source-prod:8020/warehouse/external/finance_db.db/accounts",
            false);
        financeDb.getTableMirrors().put("accounts", accounts);

        result.getDatabases().put("finance_db", financeDb);

        // Database 2: HR
        DBMirror hrDb = new DBMirror();
        hrDb.setName("hr_db");

        TableMirror employees = createAcidTable("employees",
            Arrays.asList("emp_id INT", "name STRING", "department STRING",
                         "salary DECIMAL(12,2)", "hire_date DATE"),
            "hdfs://source-prod:8020/warehouse/managed/hr_db.db/employees",
            false,
            null);
        hrDb.getTableMirrors().put("employees", employees);

        result.getDatabases().put("hr_db", hrDb);

        // Database 3: Marketing
        DBMirror marketingDb = new DBMirror();
        marketingDb.setName("marketing_db");

        TableMirror campaigns = createExternalTable("campaign_metrics",
            Arrays.asList("campaign_id INT", "campaign_name STRING", "impressions BIGINT",
                         "clicks BIGINT", "conversions INT", "cost DECIMAL(10,2)", "date DATE"),
            "hdfs://source-prod:8020/warehouse/external/marketing_db.db/campaign_metrics",
            true);

        EnvironmentTable campLeftEnv = campaigns.getEnvironmentTable(Environment.LEFT);
        Map<String, String> campPartitions = new LinkedHashMap<>();
        campPartitions.put("date=2024-01-01", "hdfs://source-prod:8020/warehouse/external/marketing_db.db/campaign_metrics/date=2024-01-01");
        campPartitions.put("date=2024-01-02", "hdfs://source-prod:8020/warehouse/external/marketing_db.db/campaign_metrics/date=2024-01-02");
        campLeftEnv.setPartitions(campPartitions);

        marketingDb.getTableMirrors().put("campaign_metrics", campaigns);

        TableMirror customerView = createView("customer_lifetime_value_view",
            "CREATE VIEW customer_lifetime_value_view AS SELECT customer_id, SUM(conversions * value_per_conversion) as lifetime_value FROM campaign_metrics GROUP BY customer_id");
        marketingDb.getTableMirrors().put("customer_lifetime_value_view", customerView);

        result.getDatabases().put("marketing_db", marketingDb);

        return result;
    }

    // ===========================
    // Helper Methods
    // ===========================

    private static ConnectionDto createConnection(String name, String uri,
                                                  PlatformType platformType, String namespace) {
        ConnectionDto conn = new ConnectionDto();
        conn.setName(name);
        conn.setHs2Uri(uri);
        conn.setPlatformType(platformType);
        conn.setHcfsNamespace(namespace);
        return conn;
    }

    private static TableMirror createExternalTable(String name, List<String> columns,
                                                   String location, boolean partitioned) {
        TableMirror table = new TableMirror();
        table.setName(name);
        table.setStrategy(DataStrategyEnum.SCHEMA_ONLY);

        EnvironmentTable leftEnv = new EnvironmentTable(table);
        leftEnv.setName(name);
        leftEnv.setExists(true);

        List<String> definition = new ArrayList<>();
        definition.add("CREATE EXTERNAL TABLE " + name + " (");
        for (int i = 0; i < columns.size(); i++) {
            definition.add("  " + columns.get(i) + (i < columns.size() - 1 ? "," : ""));
        }
        definition.add(")");
        if (partitioned) {
            definition.add("PARTITIONED BY (date DATE)");
        }
        definition.add("STORED AS PARQUET");
        definition.add("LOCATION '" + location + "'");
        leftEnv.setDefinition(definition);

        table.getEnvironments().put(Environment.LEFT, leftEnv);

        return table;
    }

    private static TableMirror createAcidTable(String name, List<String> columns,
                                              String location, boolean partitioned,
                                              String partitionColumns) {
        TableMirror table = new TableMirror();
        table.setName(name);
        table.setStrategy(DataStrategyEnum.ACID);

        EnvironmentTable leftEnv = new EnvironmentTable(table);
        leftEnv.setName(name);
        leftEnv.setExists(true);

        List<String> definition = new ArrayList<>();
        definition.add("CREATE TABLE " + name + " (");
        for (int i = 0; i < columns.size(); i++) {
            definition.add("  " + columns.get(i) + (i < columns.size() - 1 ? "," : ""));
        }
        definition.add(")");
        if (partitioned && partitionColumns != null) {
            definition.add("PARTITIONED BY (" + partitionColumns + ")");
        }
        definition.add("STORED AS ORC");
        definition.add("LOCATION '" + location + "'");
        definition.add("TBLPROPERTIES (");
        definition.add("  'transactional'='true',");
        definition.add("  'transactional_properties'='default'");
        definition.add(")");
        leftEnv.setDefinition(definition);

        table.getEnvironments().put(Environment.LEFT, leftEnv);
        table.addStep("TRANSACTIONAL", true);

        return table;
    }

    private static TableMirror createView(String name, String viewDef) {
        TableMirror table = new TableMirror();
        table.setName(name);
        table.setStrategy(DataStrategyEnum.DUMP);

        EnvironmentTable leftEnv = new EnvironmentTable(table);
        leftEnv.setName(name);
        leftEnv.setExists(true);

        List<String> definition = Arrays.asList(viewDef);
        leftEnv.setDefinition(definition);

        table.getEnvironments().put(Environment.LEFT, leftEnv);

        return table;
    }
}
