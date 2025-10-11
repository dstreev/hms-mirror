| Property | SCHEMA_ONLY | SQL | EXPORT_IMPORT | HYBRID | DUMP | LINKED | COMMON | STORAGE_MIGRATION |
|----------|-------------|----|---------------|--------|------|--------|--------|-------------------|
| **GENERAL SETTINGS** |
| `comment` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `copyAvroSchemaUrls` | ✅ | ✅ | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | ✅ |
| `dataStrategy` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `databaseOnly` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `dumpTestData` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `loadTestDataFile` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `skipLinkCheck` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ❌ | ❌ | ❌ |
| `encryptedPasswords` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| **DATABASE SELECTION** |
| `databases` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `dbPrefix` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `dbRename` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | 🟡 |
| `dumpSource` | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| **LEFT CLUSTER** |
| `clusters.LEFT.environment` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `clusters.LEFT.createIfNotExists` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `clusters.LEFT.platformType` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `clusters.LEFT.hcfsNamespace` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `clusters.LEFT.hiveServer2.*` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `clusters.LEFT.partitionDiscovery.auto` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `clusters.LEFT.partitionDiscovery.initMSCK` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `clusters.LEFT.enableAutoTableStats` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `clusters.LEFT.enableAutoColumnStats` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `clusters.LEFT.metastore_direct.*` | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 | 🟡 |
| **RIGHT CLUSTER** |
| `clusters.RIGHT.environment` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ❌ |
| `clusters.RIGHT.createIfNotExists` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ❌ |
| `clusters.RIGHT.platformType` | ✅ | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ❌ |
| `clusters.RIGHT.hcfsNamespace` | ✅ | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ❌ |
| `clusters.RIGHT.hiveServer2.*` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `clusters.RIGHT.partitionDiscovery.auto` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `clusters.RIGHT.partitionDiscovery.initMSCK` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `clusters.RIGHT.enableAutoTableStats` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ❌ |
| `clusters.RIGHT.enableAutoColumnStats` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ❌ |
| `clusters.RIGHT.metastore_direct.*` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | ❌ |
| **FILTER** |
| `filter.dbRegEx` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `filter.dbPropertySkipList` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `filter.tblExcludeRegEx` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `filter.tblRegEx` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `filter.tblSizeLimit` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `filter.tblPartitionLimit` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| **LEGACY TRANSLATIONS** |
| `legacyTranslations.rowSerde` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **EXECUTION CONTROL** |
| `execute` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `readOnly` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `noPurge` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| `replace` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | ❌ | ❌ | ❌ |
| `resetRight` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | ❌ |
| `saveWorkingTables` | ⚪ | ✅ | ✅ | ✅ | ❌ | ⚪ | ⚪ | ✅ |
| `skipFeatures` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `skipLegacyTranslation` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `sync` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ❌ |
| **HYBRID STRATEGY** |
| `hybrid.exportImportPartitionLimit` | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| `hybrid.sqlPartitionLimit` | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| `hybrid.sqlSizeLimit` | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **ICEBERG CONVERSION** |
| `icebergConversion.enable` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `icebergConversion.fileTypeTranslation` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `icebergConversion.version` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `icebergConversion.tableProperties` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `icebergConversion.inplace` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **ACID MIGRATION** |
| `migrateACID.on` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | 🟡 |
| `migrateACID.only` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | 🟡 |
| `migrateACID.artificialBucketThreshold` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | 🟡 |
| `migrateACID.partitionLimit` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | 🟡 |
| `migrateACID.downgrade` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | 🟡 |
| `migrateACID.inplace` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | 🟡 |
| **VIEW MIGRATION** |
| `migrateVIEW.on` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **NON-NATIVE TABLES** |
| `migrateNonNative` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **OPTIMIZATION** |
| `optimization.sortDynamicPartitionInserts` | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| `optimization.skip` | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| `optimization.autoTune` | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| `optimization.compressTextOutput` | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| `optimization.skipStatsCollection` | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |
| `optimization.overrides.properties` | ⚪ | ✅ | ✅ | ✅ | ❌ | ⚪ | ⚪ | ✅ |
| `optimization.buildShadowStatistics` | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| **OWNERSHIP TRANSFER** |
| `ownershipTransfer.database` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ❌ |
| `ownershipTransfer.table` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ❌ |
| **TRANSFER SETTINGS** |
| `transfer.transferPrefix` | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| `transfer.shadowPrefix` | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| `transfer.storageMigrationPostfix` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `transfer.exportBaseDirPrefix` | ❌ | ❌ | ✅ | 🟡 | ❌ | ❌ | ❌ | ❌ |
| `transfer.remoteWorkingDirectory` | ⚪ | ⚪ | ⚪ | ⚪ | ❌ | ⚪ | ⚪ | ⚪ |
| `transfer.intermediateStorage` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | ❌ | ❌ | ❌ |
| `transfer.targetNamespace` | ✅ | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ✅ |
| **STORAGE MIGRATION** |
| `transfer.storageMigration.translationType` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | ✅ |
| `transfer.storageMigration.dataMovementStrategy` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `transfer.storageMigration.dataFlow` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | 🟡 |
| `transfer.storageMigration.skipDatabaseLocationAdjustments` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `transfer.storageMigration.createArchive` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| `transfer.storageMigration.consolidateTablesForDistcp` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | 🟡 |
| `transfer.storageMigration.strict` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| **WAREHOUSE SETTINGS** |
| `transfer.warehouse.source` | ✅ | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ✅ |
| `transfer.warehouse.externalDirectory` | ✅ | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ✅ |
| `transfer.warehouse.managedDirectory` | ✅ | ✅ | ✅ | ✅ | 🟡 | ✅ | ✅ | ✅ |
| **TRANSLATOR** |
| `translator.forceExternalLocation` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | 🟡 |
| `translator.autoGlobalLocationMap` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | 🟡 |
| `translator.userGlobalLocationMap` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | 🟡 |
| `translator.warehouseMapBuilder.sources` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | 🟡 |
| `translator.warehouseMapBuilder.inSync` | 🟡 | 🟡 | 🟡 | 🟡 | ❌ | 🟡 | 🟡 | 🟡 |
| `translator.warehouseMapBuilder.warehousePlans` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |

---

## Legend

The following symbols are used to indicate property availability and applicability across different data strategies:

| Symbol | Meaning | Description |
|--------|---------|-------------|
| ✅ | **Required/Supported** | This property is required or actively supported for this data strategy. Setting this property will have a direct effect on the migration behavior. |
| 🟡 | **Optional/Conditional** | This property is optional or conditionally supported. It may be used in specific scenarios or configurations within this data strategy. |
| ❌ | **Not Applicable** | This property is not applicable or not supported for this data strategy. Setting this property will have no effect or may be ignored. |
| ⚪ | **Universal/Common** | This property applies to all data strategies uniformly. It represents common configuration options that work across all migration types. |

### Data Strategy Types

- **SCHEMA_ONLY**: Migrates only metadata (table and database definitions) without moving any actual data
- **SQL**: Uses Hive SQL INSERT statements to move data between clusters
- **EXPORT_IMPORT**: Uses Hive's native EXPORT/IMPORT functionality for data migration
- **HYBRID**: Combines multiple strategies (SQL and EXPORT_IMPORT) based on table characteristics
- **DUMP**: Creates a dump of metadata and data for analysis or backup purposes
- **LINKED**: Creates external tables that point to data in the source location without moving data
- **COMMON**: Properties that apply across all strategies (not a standalone strategy)
- **STORAGE_MIGRATION**: Migrates data to different storage systems or formats within the same cluster

### Property Categories

- **GENERAL SETTINGS**: Basic configuration options that affect overall migration behavior
- **DATABASE SELECTION**: Controls which databases and how they are selected for migration
- **LEFT/RIGHT CLUSTER**: Configuration for source (LEFT) and target (RIGHT) cluster connections
- **FILTER**: Options to filter which databases and tables to include/exclude
- **EXECUTION CONTROL**: Settings that control how the migration is executed
- **OPTIMIZATION**: Performance and optimization settings for data movement
- **TRANSFER SETTINGS**: Configuration for data transfer processes and storage locations
- **WAREHOUSE SETTINGS**: Warehouse directory and location configurations
- **TRANSLATOR**: Settings for translating storage locations between clusters