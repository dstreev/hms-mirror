# Data Strategies

## SQL
Purpose: Use Hive SQL to move data between clusters via INSERT statements

### Connection Selection
- LEFT connection
	Select from pre-configured connection profiles
- RIGHT connection
	Select from pre-configured connection profiles

### General Behavior
- databaseOnly
	Note: Migrate only database definitions, skip tables
- copyAvroSchemaUrls
	Note: Copy AVRO schema files between clusters
- skipLinkCheck
	Note: Skip cluster storage accessibility check

### Cluster Connectivity
- transfer.intermediateStorage
	Note: Required if skipLinkCheck=true. Shared storage location accessible by both clusters for data transit.

### Migration Type Selection
- ACID Tables (Full Support)
	- migrateACID.on
		Note: Include ACID tables with other tables
	- migrateACID.only
		Note: Migrate ONLY ACID tables
	- migrateACID.artificialBucketThreshold
		Note: Default=2. Remove bucket definitions if count ≤ threshold
	- migrateACID.partitionLimit
		Note: Default=500. Skip ACID tables exceeding this partition count
	- migrateACID.downgrade
		Note: Convert ACID to EXTERNAL/PURGE tables
	- migrateACID.inplace
		Note: Downgrade on same cluster
- Views
	- migrateVIEW.on
		Note: Migrate views after underlying tables exist
- Non-Native Tables
	- migrateNonNative
		Note: Migrate HBase, Kafka, JDBC tables

### Iceberg Conversion (Optional)
- beta
	Note: Must be set to true to enable Iceberg conversion features
- icebergConversion.enable
	Note: Set to true to convert Hive tables to Iceberg format during migration
- icebergConversion.version
	Note: 1 or 2. Default=2 (recommended). Iceberg table format version.
- icebergConversion.fileTypeTranslation
	Note: STANDARD. File type translation strategy for Iceberg conversion.
- icebergConversion.inplace
	Note: true=Convert on same cluster (LEFT only). false=Convert while migrating to different cluster.
- icebergConversion.tableProperties
	Note: Iceberg-specific table properties as key-value pairs. Example: write.format.default=parquet
- Requirements
	Note: RIGHT cluster must support Iceberg (CDP 7.3.1+, CDW 1.5.1+, Public Cloud Aug 2023+). Review CDP Iceberg limitations. Conversion is typically one-way.

### SQL Strategy Limits
- hybrid.sqlPartitionLimit
	Note: Default=3000. Skip tables with more partitions than this limit

### Optimization (SQL Data Movement)
- Optimization Approach
	- optimization.autoTune
		Note: RECOMMENDED. Automatically optimize based on file statistics and table characteristics
	- optimization.sortDynamicPartitionInserts
		Note: Use sorted dynamic partition inserts. Single writer per partition.
	- optimization.skip
		Note: No optimizations. Use when data is already well-organized.
- Additional Settings
	- optimization.compressTextOutput
		Note: Compress TEXT format files during data movement
	- optimization.skipStatsCollection
		Note: Skip filesystem statistics gathering. Use for slow storage like S3.
	- optimization.buildShadowStatistics
		Note: Build statistics on shadow/transfer tables
	- optimization.overrides.properties
		Note: Hive property overrides as key-value pairs. Example: tez.queue.name=migration
	- saveWorkingTables
		Note: Keep shadow/transfer tables after migration for debugging

### Location Translation Strategy
- transfer.storageMigration.translationType
	- RELATIVE
		Note: Simple namespace replacement
	- ALIGNED
		Note: Use warehouse plans for organized layout
- transfer.targetNamespace
	Note: Target storage namespace
- If ALIGNED
	- transfer.warehouse
		Note: Global warehouse configuration
	- translator.warehouseMapBuilder.warehousePlans
		Note: Per-database warehouse plans
	- translator.forceExternalLocation
		Note: Force LOCATION clause for external tables

### Transfer Settings
- transfer.transferPrefix
	Note: Default=hms_mirror_transfer_. Prefix for transfer tables created during migration.
- transfer.shadowPrefix
	Note: Default=hms_mirror_shadow_. Prefix for shadow tables that reference LEFT data.
- transfer.remoteWorkingDirectory
	Note: Working directory on remote cluster for temporary files

### Testing Options
- Is this a test migration?
	- dbPrefix
		Note: Prepend to database names for testing
	- dbRename
		Note: Rename single database

### Ownership Transfer
- ownershipTransfer.database
	Note: Transfer database ownership
- ownershipTransfer.table
	Note: Transfer table ownership

### Optional Features
- skipFeatures
	Note: Skip automatic schema corrections
- skipLegacyTranslation
	Note: Skip SerDe translations
- quiet
	Note: Reduce output verbosity

### Execution Mode (Final Step)
- execute
	Note: false=DRY RUN, true=Execute
- readOnly
	Note: Create tables without PURGE flag
- noPurge
	Note: Force no PURGE flag
- sync
	Note: Synchronize schemas (drop/recreate if different)
- resetRight
	Note: DROP CASCADE databases on RIGHT first
- saveWorkingTables
	Note: Keep shadow/transfer tables

## EXPORT_IMPORT
Purpose: Use Hive EXPORT/IMPORT to package and transfer schemas and data

### Connection Selection
- LEFT connection
	Select from pre-configured connection profiles
- RIGHT connection
	Select from pre-configured connection profiles

### General Behavior
- databaseOnly
	Note: Migrate only database definitions
- copyAvroSchemaUrls
	Note: Copy AVRO schema files
- skipLinkCheck
	Note: Skip storage accessibility check

### Cluster Connectivity
- transfer.intermediateStorage
	Note: Shared storage for export packages if clusters not linked

### Export/Import Settings
- transfer.exportBaseDirPrefix
	Note: Default=/apps/hive/warehouse/export_. Directory for EXPORT packages. Must be accessible by both clusters.

### Migration Type Selection
- ACID Tables (Limited Support)
	- migrateACID.on
		Note: Include ACID tables. Cannot migrate ACIDv1 to ACIDv2 (Hive 1/2 to 3).
	- migrateACID.only
		Note: Migrate only ACID tables
	- migrateACID.artificialBucketThreshold
		Note: Remove bucket definitions threshold
	- migrateACID.partitionLimit
		Note: Partition count limit for ACID tables
	- migrateACID.downgrade
		Note: Downgrade ACID to EXTERNAL/PURGE
- Views
	- migrateVIEW.on
		Note: Migrate views
- Non-Native Tables
	- migrateNonNative
		Note: Migrate non-native tables

### Iceberg Conversion (Optional)
- beta
	Note: Must be set to true to enable Iceberg conversion features
- icebergConversion.enable
	Note: Set to true to convert Hive tables to Iceberg format during migration
- icebergConversion.version
	Note: 1 or 2. Default=2 (recommended). Iceberg table format version.
- icebergConversion.fileTypeTranslation
	Note: STANDARD. File type translation strategy for Iceberg conversion.
- icebergConversion.inplace
	Note: true=Convert on same cluster (LEFT only). false=Convert while migrating to different cluster.
- icebergConversion.tableProperties
	Note: Iceberg-specific table properties as key-value pairs. Example: write.format.default=parquet
- Requirements
	Note: RIGHT cluster must support Iceberg (CDP 7.3.1+, CDW 1.5.1+, Public Cloud Aug 2023+). Review CDP Iceberg limitations. Conversion is typically one-way.

### Optimization (Hive Engine Data Movement)
- optimization.autoTune
	Note: Auto-optimize based on statistics
- optimization.sortDynamicPartitionInserts
	Note: Use sorted dynamic partition inserts
- optimization.skip
	Note: Skip optimizations
- optimization.compressTextOutput
	Note: Compress TEXT output
- optimization.skipStatsCollection
	Note: Skip filesystem stats collection
- optimization.buildShadowStatistics
	Note: Build shadow table statistics
- optimization.overrides.properties
	Note: Hive property overrides
- saveWorkingTables
	Note: Keep working tables for debugging

### Location Translation Strategy
Note: Same as SCHEMA_ONLY

### Transfer Settings
Note: Same as SQL

### Testing Options
Note: Same as SQL

### Ownership Transfer
Note: Same as SQL

### Optional Features
Note: Same as SQL

### Execution Mode (Final Step)
Note: Same as SQL

### Limitations
Note: Slow with partitioned tables. Default limit 100 partitions. Not suitable for ACIDv1→ACIDv2 migration.

## HYBRID
Purpose: Automatically choose between SQL and EXPORT_IMPORT based on table characteristics

### Connection Selection
- LEFT connection
	Select from pre-configured connection profiles
- RIGHT connection
	Select from pre-configured connection profiles

### General Behavior
- databaseOnly
	Note: Migrate only database definitions
- copyAvroSchemaUrls
	Note: Copy AVRO schema files
- skipLinkCheck
	Note: Skip storage accessibility check

### Cluster Connectivity
- transfer.intermediateStorage
	Note: Shared storage if clusters not linked

### Hybrid Decision Thresholds
- hybrid.exportImportPartitionLimit
	Note: Default=100. Tables with ≤ this many partitions use EXPORT_IMPORT
- hybrid.sqlPartitionLimit
	Note: Default=3000. Tables between exportImport and this limit use SQL. Above this = SKIPPED.
- hybrid.sqlSizeLimit
	Note: Default=1073741824 (1GB). Size consideration for SQL selection

### Export Settings
- transfer.exportBaseDirPrefix
	Note: Only used when EXPORT_IMPORT is selected by HYBRID logic

### Migration Type Selection
Note: Same as SQL (Full ACID Support)

### Iceberg Conversion (Optional)
- beta
	Note: Must be set to true to enable Iceberg conversion features
- icebergConversion.enable
	Note: Set to true to convert Hive tables to Iceberg format during migration
- icebergConversion.version
	Note: 1 or 2. Default=2 (recommended). Iceberg table format version.
- icebergConversion.fileTypeTranslation
	Note: STANDARD. File type translation strategy for Iceberg conversion.
- icebergConversion.inplace
	Note: true=Convert on same cluster (LEFT only). false=Convert while migrating to different cluster.
- icebergConversion.tableProperties
	Note: Iceberg-specific table properties as key-value pairs. Example: write.format.default=parquet
- Requirements
	Note: RIGHT cluster must support Iceberg (CDP 7.3.1+, CDW 1.5.1+, Public Cloud Aug 2023+). Review CDP Iceberg limitations. Conversion is typically one-way.

### Optimization (For SQL Portion)
Note: Same as SQL

### Location Translation Strategy
Note: Same as SCHEMA_ONLY

### Transfer Settings
Note: Same as SQL

### Testing Options
Note: Same as SQL

### Ownership Transfer
Note: Same as SQL

### Optional Features
Note: Same as SQL

### Execution Mode (Final Step)
Note: Same as SQL

### Decision Logic
Note: ACIDv1→ACIDv2 = SQL. Partitions ≤ exportImportPartitionLimit = EXPORT_IMPORT. Partitions ≤ sqlPartitionLimit = SQL. Otherwise = SKIP.

## STORAGE_MIGRATION
Purpose: Move data within single cluster to different storage (HDFS→Ozone, HDFS→S3, etc.)

### Connection Selection
- Cluster connection
	Note: Only ONE cluster needed. Migration within same cluster.

### General Behavior
- copyAvroSchemaUrls
	Note: Copy AVRO schema files if moving to new namespace
- databaseOnly
	Note: Migrate only database definitions

### Target Storage (Required)
- transfer.targetNamespace
	Note: REQUIRED. New storage namespace. Example: ofs://ozone-vol/bucket or s3a://new-bucket

### Data Movement Strategy (Required)
- transfer.storageMigration.dataMovementStrategy
	- SQL
		Note: Use Hive SQL via engine to move data. Triggers optimization settings and allows Iceberg conversion.
	- DISTCP
		Note: Generate distcp plans/scripts for data movement
	- MANUAL
		Note: Only migrate metadata. You handle data movement separately.

### If DISTCP Selected
- transfer.storageMigration.dataFlow
	Note: PULL or PUSH. Which cluster runs distcp commands.
- transfer.storageMigration.consolidateTablesForDistcp
	Note: Simplify distcp plan by consolidating. May copy extra data. Use with caution.
- transfer.storageMigration.strict
	Note: Fail if non-standard partition locations found. Recommended: Start false, validate plans, then enable.

### If SQL Selected
- Iceberg Conversion (Optional)
	- beta
		Note: Must be set to true to enable Iceberg conversion features
	- icebergConversion.enable
		Note: Set to true to convert Hive tables to Iceberg format during storage migration
	- icebergConversion.version
		Note: 1 or 2. Default=2 (recommended). Iceberg table format version.
	- icebergConversion.fileTypeTranslation
		Note: STANDARD. File type translation strategy for Iceberg conversion.
	- icebergConversion.inplace
		Note: Must be true for STORAGE_MIGRATION since it's within same cluster
	- icebergConversion.tableProperties
		Note: Iceberg-specific table properties as key-value pairs. Example: write.format.default=parquet
	- Requirements
		Note: Cluster must support Iceberg (CDP 7.3.1+, CDW 1.5.1+, Public Cloud Aug 2023+). Review CDP Iceberg limitations. Conversion is typically one-way.
- Optimization (SQL via Hive Engine)
	- optimization.autoTune
		Note: RECOMMENDED. Auto-optimize based on file statistics
	- optimization.sortDynamicPartitionInserts
		Note: Use sorted dynamic partition inserts
	- optimization.skip
		Note: No optimizations
	- optimization.compressTextOutput
		Note: Compress TEXT output
	- optimization.skipStatsCollection
		Note: Skip filesystem stats. Use for slow storage.
	- optimization.buildShadowStatistics
		Note: Build shadow table statistics
	- optimization.overrides.properties
		Note: Hive property overrides
	- saveWorkingTables
		Note: Keep working tables for debugging

### Translation Strategy (Required)
- transfer.storageMigration.translationType
	- RELATIVE
		Note: Simple namespace swap. /old/path → /new/path
	- ALIGNED
		Note: Organized multi-tenant layout using warehouse plans. RECOMMENDED for reorganization.

### If ALIGNED Selected
- transfer.warehouse
	- source
		Note: Set to GLOBAL
	- externalDirectory
		Note: Global external table base path
	- managedDirectory
		Note: Global managed table base path
- translator.warehouseMapBuilder.warehousePlans
	Note: RECOMMENDED. Per-database warehouse plans for organized multi-tenant layout.
	- db-name.source
		Note: Set to PLAN
	- db-name.externalDirectory
		Note: Path relative to targetNamespace. Final path = targetNamespace + externalDirectory + db-name
	- db-name.managedDirectory
		Note: Path relative to targetNamespace for managed tables
- translator.forceExternalLocation
	Note: Force LOCATION clause in CREATE statements

### Storage Migration Options
- transfer.storageMigration.skipDatabaseLocationAdjustments
	Note: Don't modify database location. Use for archiving tables while keeping DB location for new tables.
- transfer.storageMigration.createArchive
	Note: Archive original tables instead of modifying them. Creates archive copy before migration.

### Migration Type Selection
- ACID Tables
	- migrateACID
		Note: Conditional support. Best with dataMovementStrategy=SQL
- Views
	Note: NOT APPLICABLE for STORAGE_MIGRATION

### Transfer Settings
- transfer.transferPrefix
	Note: Prefix for transfer tables
- transfer.shadowPrefix
	Note: Prefix for shadow tables
- transfer.storageMigrationPostfix
	Note: Postfix for storage migration artifacts

### Optional Features
- skipLegacyTranslation
	Note: Skip SerDe translations

### Execution Mode (Final Step)
- execute
	Note: false=DRY RUN (STRONGLY RECOMMENDED), true=Execute
- If SQL
	- saveWorkingTables
		Note: Keep working tables

### Common Use Cases
Note: HDFS→Ozone. Move to encrypted zone. Cloud migration (HDFS→S3). Multi-tenant reorganization. Convert to Iceberg during storage migration.

### Best Practices
Note: Use ALIGNED with warehouse plans. Test with strict=false first. Validate distcp plans. Consider metastore_direct for accuracy.

## LINKED
Purpose: Create schemas on RIGHT that point to LEFT cluster data. For testing/validation only.

### Connection Selection
- LEFT connection
	Select from pre-configured connection profiles
- RIGHT connection
	Note: Must have access to LEFT cluster's storage

### General Behavior
- copyAvroSchemaUrls
	Note: Copy AVRO schema files
- databaseOnly
	Note: Migrate only database definitions

### Cluster Connectivity
- skipLinkCheck
	Note: Must be false. Clusters MUST be linked for this strategy.

### Migration Type Selection
- ACID Tables
	Note: NOT SUPPORTED. ACID tables cannot be linked.
- Views
	- migrateVIEW.on
		Note: Migrate views
- Non-Native Tables
	- migrateNonNative
		Note: Migrate non-native tables

### Location Translation
- transfer.targetNamespace
	Note: Target namespace
- transfer.storageMigration.translationType
	Note: RELATIVE or ALIGNED
- If ALIGNED
	- transfer.warehouse
		Note: Global warehouse
	- translator.warehouseMapBuilder.warehousePlans
		Note: Per-database plans
- translator.forceExternalLocation
	Note: Force LOCATION clause

### Transfer Settings
- transfer.shadowPrefix
	Note: Prefix for linked tables on RIGHT

### Testing Options
- Is this a test migration?
	- dbPrefix
		Note: Prepend to database names
	- dbRename
		Note: Rename single database

### Ownership Transfer
- ownershipTransfer.database
	Note: Transfer database ownership
- ownershipTransfer.table
	Note: Transfer table ownership

### Optional Features
- skipFeatures
	Note: Skip automatic corrections
- skipLegacyTranslation
	Note: Skip SerDe translations
- quiet
	Note: Reduce output

### Execution Mode (Final Step)
- execute
	Note: false=DRY RUN, true=Execute
- readOnly
	Note: MUST SET TRUE. Creates tables without PURGE to protect LEFT data.
- noPurge
	Note: MUST SET TRUE. Prevents accidental data deletion on LEFT.
- sync
	Note: Synchronize schemas

### Safety Warnings
Note: FOR TESTING ONLY. Tables point to LEFT data. DROP DATABASE CASCADE on RIGHT WILL DELETE LEFT DATA! Always use readOnly=true and noPurge=true.

## DUMP
Purpose: Extract schemas to SQL files for manual replay. No target cluster needed.

### Connection Selection
- Source connection
	Select from pre-configured connection profiles
- dumpSource
	Note: LEFT or RIGHT. Specifies which cluster to extract from. Default=LEFT.

### Target Context
- transfer.targetNamespace
	Note: Target namespace for location translation in dumped DDL. Example: hdfs://new-cluster

### General Behavior
- databaseOnly
	Note: Dump only database definitions, skip tables

### Migration Type Selection
- ACID Tables
	- migrateACID.on
		Note: Include ACID tables in dump
	- migrateACID.only
		Note: Dump only ACID tables
	- migrateACID.artificialBucketThreshold
		Note: Bucket definition removal threshold
	- migrateACID.partitionLimit
		Note: Partition count limit
	- migrateACID.downgrade
		Note: Downgrade ACID in dumped DDL
- Views
	- migrateVIEW.on
		Note: Include views in dump
- Non-Native Tables
	- migrateNonNative
		Note: Include non-native tables in dump

### Optional Features
- skipLegacyTranslation
	Note: Skip SerDe translations in dumped DDL

### Execution Mode (Final Step)
- execute
	Note: Always false for DUMP. Only generates SQL files, no execution.

### Output
Note: SQL files to manually replay on target cluster. No RIGHT cluster needed. No data movement. No optimization settings.

## COMMON
Purpose: Both clusters share same physical storage. Only metadata migrates.

### Connection Selection
- LEFT connection
	Select from pre-configured connection profiles
- RIGHT connection
	Note: Must access same storage as LEFT

### General Behavior
- copyAvroSchemaUrls
	Note: Copy AVRO schema files
- databaseOnly
	Note: Migrate only database definitions
- skipLinkCheck
	Note: Can be true since storage is shared

### Shared Storage Verification
Note: Confirm both clusters access same storage. Examples: Isilon, Spectrum Scale, shared S3 bucket.

### Migration Type Selection
- ACID Tables
	Note: NOT SUPPORTED for COMMON strategy
- Views
	- migrateVIEW.on
		Note: Migrate views
- Non-Native Tables
	- migrateNonNative
		Note: Migrate non-native tables

### Location Translation
- transfer.targetNamespace
	Note: Usually same as LEFT since storage is shared
- transfer.storageMigration.translationType
	Note: RELATIVE or ALIGNED
- If ALIGNED
	- transfer.warehouse
		Note: Global warehouse
	- translator.warehouseMapBuilder.warehousePlans
		Note: Per-database plans
	- translator.forceExternalLocation
		Note: Force LOCATION clause

### Transfer Settings
- transfer.shadowPrefix
	Note: Prefix for shadow tables

### Testing Options
Note: Same as SCHEMA_ONLY

### Ownership Transfer
Note: Same as SCHEMA_ONLY

### Optional Features
Note: Same as SCHEMA_ONLY

### Execution Mode (Final Step)
- execute
	Note: false=DRY RUN, true=Execute
- readOnly
	Note: Create without PURGE flag
- noPurge
	Note: Force no PURGE
- sync
	Note: Synchronize schemas
- resetRight
	Note: DROP CASCADE first

### Use Case
Note: Shared storage (NAS, S3). Data doesn't move. Only metadata migrates. EXTERNAL tables only.

## SCHEMA_ONLY
Purpose: Migrate metadata only, no data movement. Generates distcp plans for separate data migration.

### Connection Selection
- LEFT connection
	Select from pre-configured connection profiles
- RIGHT connection
	Select from pre-configured connection profiles

### General Behavior
- databaseOnly
	Note: Set to true if you only want to migrate database definitions without any tables
- copyAvroSchemaUrls
	Note: Copy AVRO schema files between clusters. Requires clusters to be linked.
- skipLinkCheck
	Note: Skip verification that clusters can access each other's storage. Use when clusters aren't linked.

### Migration Type Selection
- ACID Tables
	- migrateACID.on
		Note: Include ACID tables in migration along with other table types
	- migrateACID.only
		Note: Migrate ONLY ACID tables, exclude all other table types
	- migrateACID.artificialBucketThreshold
		Note: Default=2. Remove CLUSTERED BY bucket definitions if bucket count ≤ this value. Helps clean up legacy artificial bucketing.
	- migrateACID.partitionLimit
		Note: Default=500. Skip ACID tables with more partitions than this limit
	- migrateACID.downgrade
		Note: Convert ACID tables to EXTERNAL tables with PURGE flag
	- migrateACID.inplace
		Note: Downgrade ACID tables on the same cluster (no migration)
- Views
	- migrateVIEW.on
		Note: Migrate views. Requires underlying tables to exist on target cluster first.
- Non-Native Tables
	- migrateNonNative
		Note: Migrate non-native tables (HBase, Kafka, JDBC). Requires external systems to be configured and accessible.

### Location Translation Strategy
- transfer.storageMigration.translationType
	- RELATIVE
		Note: Simple namespace swap. /old/path → /new/path with same relative structure
	- ALIGNED
		Note: Reorganize data layout using warehouse plans. Allows multi-tenant organization.
- transfer.targetNamespace
	Note: Target storage namespace. Example: hdfs://new-cluster or s3a://bucket
- If ALIGNED
	- transfer.warehouse.source
		Note: Set to "GLOBAL" for global warehouse configuration
	- transfer.warehouse.externalDirectory
		Note: Base path for external tables across all databases
	- transfer.warehouse.managedDirectory
		Note: Base path for managed tables across all databases
	- translator.warehouseMapBuilder.warehousePlans
		Note: Per-database warehouse plans. Overrides global settings for specific databases.  
		####### db-name.externalDirectory  
		Note: External table path for this specific database  
		####### db-name.managedDirectory  
		Note: Managed table path for this specific database
	- translator.forceExternalLocation
		Note: Force LOCATION clause in CREATE statements for external tables. Needed for older Hive versions.

### Distcp Plan Generation
Note: SCHEMA_ONLY always generates distcp plans and scripts to facilitate separate data movement

### Testing Options
- Is this a test migration?
	- dbPrefix
		Note: Prepend this string to database names on RIGHT cluster. Use for testing to avoid conflicts.
	- dbRename
		Note: Rename single database to new name. Only valid when migrating one database.

### Ownership Transfer
- ownershipTransfer.database
	Note: Transfer database ownership from LEFT to RIGHT. Requires permissions on both clusters.
- ownershipTransfer.table
	Note: Transfer table ownership from LEFT to RIGHT. Makes extra SQL calls per table.

### Optional Features
- skipFeatures
	Note: Skip automatic schema corrections/fixes that hms-mirror normally applies
- skipLegacyTranslation
	Note: Skip automatic translation of legacy SerDe classes to modern equivalents
- quiet
	Note: Reduce console output verbosity

### Execution Mode (Final Step)
- execute
	Note: false=DRY RUN (recommended first), true=Execute migration
- readOnly
	Note: Create tables without PURGE flag. Use for testing/DR scenarios.
- noPurge
	Note: Force removal of PURGE flag from all tables
- sync
	Note: Drop and recreate tables if schemas differ. Enables bi-directional synchronization.
- resetRight
	Note: DROP CASCADE all databases on RIGHT before migration. Dangerous - use with caution.
