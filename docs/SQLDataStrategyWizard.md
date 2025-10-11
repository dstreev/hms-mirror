# SQL Data Strategy Configuration Wizard

This document outlines the screen design and flow for a wizard that guides users through configuring HMS-Mirror for the SQL Data Strategy.

## Wizard Overview

The SQL Data Strategy uses Hive SQL INSERT statements to move data between clusters. This wizard organizes configuration properties into logical screens, prioritizing required/supported (✅) properties first, followed by optional/conditional (🟡) properties.

## Screen Flow Order

### 1. **Data Strategy Selection Screen**
- **Purpose**: Let user select the data strategy
- **Properties**: 
  - `dataStrategy` (✅ Required)
- **UI Elements**:
  - Radio buttons or dropdown for strategy selection
  - Description of each strategy
  - "SQL" should be pre-selected or highlighted

---

### 2. **General Migration Settings**
- **Purpose**: Configure basic migration behavior
- **Required Properties** (✅):
  - `databaseOnly` - Migrate only database definitions, skip tables
- **Optional Properties** (🟡):
  - `copyAvroSchemaUrls` - Copy AVRO schema files between clusters
  - `skipLinkCheck` - Skip cluster storage accessibility check
- **Universal Properties** (⚪):
  - `comment` - Optional comment for this migration
- **UI Elements**:
  - Checkbox for `databaseOnly`
  - Checkbox for `copyAvroSchemaUrls` with tooltip explaining AVRO schemas
  - Checkbox for `skipLinkCheck` with warning about validation
  - Text area for `comment`

---

### 3. **Source Cluster Configuration (LEFT)**
- **Purpose**: Configure connection to source Hive cluster
- **Required Properties** (✅):
  - `clusters.LEFT.platformType` - Platform type (CDP, HDP, etc.)
  - `clusters.LEFT.hcfsNamespace` - HDFS namespace/URI
  - `clusters.LEFT.hiveServer2.*` - HiveServer2 connection details
- **Optional Properties** (🟡):
  - `clusters.LEFT.metastore_direct.*` - Direct metastore connection
- **Universal Properties** (⚪):
  - `clusters.LEFT.environment` - Environment name/label
  - `clusters.LEFT.createIfNotExists` - Create cluster config if missing
  - `clusters.LEFT.partitionDiscovery.auto` - Auto partition discovery
  - `clusters.LEFT.partitionDiscovery.initMSCK` - Initialize MSCK repair
  - `clusters.LEFT.enableAutoTableStats` - Auto table statistics
  - `clusters.LEFT.enableAutoColumnStats` - Auto column statistics
- **UI Elements**:
  - Dropdown for platform type
  - Text input for HDFS namespace
  - Expandable section for HiveServer2 connection (host, port, database, etc.)
  - Expandable "Advanced" section for metastore direct connection
  - Checkbox options for partition discovery and statistics

---

### 4. **Target Cluster Configuration (RIGHT)**
- **Purpose**: Configure connection to target Hive cluster
- **Required Properties** (✅):
  - `clusters.RIGHT.platformType` - Target platform type
  - `clusters.RIGHT.hcfsNamespace` - Target HDFS namespace
  - `clusters.RIGHT.hiveServer2.*` - Target HiveServer2 connection
  - `clusters.RIGHT.partitionDiscovery.auto` - Auto partition discovery on target
  - `clusters.RIGHT.partitionDiscovery.initMSCK` - Initialize MSCK on target
- **Optional Properties** (🟡):
  - `clusters.RIGHT.metastore_direct.*` - Target direct metastore connection
- **Universal Properties** (⚪):
  - `clusters.RIGHT.environment` - Target environment name
  - `clusters.RIGHT.createIfNotExists` - Create target cluster config
  - `clusters.RIGHT.enableAutoTableStats` - Auto table statistics on target
  - `clusters.RIGHT.enableAutoColumnStats` - Auto column statistics on target
- **UI Elements**:
  - Similar to LEFT cluster but for target
  - Option to copy/mirror settings from LEFT cluster
  - Validation to ensure target is different from source

---

### 5. **Database Selection & Filtering**
- **Purpose**: Select which databases and tables to migrate
- **Required Properties** (✅):
  - `databases` - List of databases to migrate
  - `dbPrefix` - Prefix to add to database names
  - `dbRename` - Database renaming mappings
  - `filter.dbRegEx` - Database regex filter
  - `filter.tblExcludeRegEx` - Table exclusion regex
  - `filter.tblRegEx` - Table inclusion regex
- **Universal Properties** (⚪):
  - `filter.dbPropertySkipList` - Database properties to skip
  - `filter.tblSizeLimit` - Table size limit
  - `filter.tblPartitionLimit` - Table partition limit
- **UI Elements**:
  - Multi-select or text area for database list
  - Text inputs for prefix and renaming rules
  - Text inputs for regex patterns with validation
  - Expandable "Advanced Filtering" section

---

### 6. **ACID Tables Migration**
- **Purpose**: Configure ACID table specific settings
- **Required Properties** (✅):
  - `migrateACID.on` - Enable ACID table migration
  - `migrateACID.only` - Migrate only ACID tables
  - `migrateACID.artificialBucketThreshold` - Remove bucket definitions if count ≤ threshold
  - `migrateACID.partitionLimit` - Skip ACID tables exceeding partition count
  - `migrateACID.downgrade` - Convert ACID to EXTERNAL/PURGE tables
- **Optional Properties** (🟡):
  - `migrateACID.inplace` - Downgrade on same cluster
  - `hybrid.sqlPartitionLimit` - SQL partition limit for hybrid mode
  - `hybrid.sqlSizeLimit` - SQL size limit for hybrid mode
- **UI Elements**:
  - Checkbox to enable ACID migration
  - Conditional fields that show when ACID is enabled
  - Number inputs for thresholds and limits
  - Checkbox for downgrade option
  - Advanced section for hybrid-related settings

---

### 7. **Additional Migration Options**
- **Purpose**: Configure views, non-native tables, and legacy translations
- **Required Properties** (✅):
  - `migrateVIEW.on` - Migrate views after underlying tables exist
  - `migrateNonNative` - Migrate HBase, Kafka, JDBC tables
  - `legacyTranslations.rowSerde` - Legacy row serde translations
- **UI Elements**:
  - Checkbox for view migration
  - Checkbox for non-native table migration
  - Checkbox for legacy translations

---

### 8. **Execution Control**
- **Purpose**: Configure how the migration will be executed
- **Required Properties** (✅):
  - `execute` - Actually execute the migration (vs. dry run)
  - `readOnly` - Create tables without PURGE flag
  - `noPurge` - Don't use PURGE operations
  - `sync` - Synchronize metadata
  - `saveWorkingTables` - Save intermediate working tables
- **Optional Properties** (🟡):
  - `replace` - Replace existing tables
  - `resetRight` - Reset right cluster before migration
- **Universal Properties** (⚪):
  - `skipFeatures` - Skip certain features
  - `skipLegacyTranslation` - Skip legacy translations
- **UI Elements**:
  - Checkbox for execute (with warning about dry run vs live)
  - Checkbox for read-only mode
  - Checkbox for sync operations
  - Advanced section for replace and reset options

---

### 9. **Data Movement Optimization**
- **Purpose**: Configure performance settings for SQL data movement
- **Required Properties** (✅):
  - `optimization.sortDynamicPartitionInserts` - Sort dynamic partition inserts
  - `optimization.skip` - Skip optimization
  - `optimization.autoTune` - Automatically optimize based on table characteristics
  - `optimization.compressTextOutput` - Compress TEXT format files
  - `optimization.overrides.properties` - Hive property overrides (key-value pairs)
  - `optimization.buildShadowStatistics` - Build shadow statistics
- **Universal Properties** (⚪):
  - `optimization.skipStatsCollection` - Skip statistics collection
- **UI Elements**:
  - Checkbox for auto-tune (recommended)
  - Checkbox for compression
  - Text area for property overrides (key=value format)
  - Advanced section for shadow statistics and other options

---

### 10. **Storage & Transfer Settings**
- **Purpose**: Configure storage locations and data transfer
- **Required Properties** (✅):
  - `transfer.transferPrefix` - Prefix for transfer operations
  - `transfer.shadowPrefix` - Prefix for shadow/working tables
  - `transfer.targetNamespace` - Target storage namespace
- **Optional Properties** (🟡):
  - `transfer.intermediateStorage` - Shared storage for data transit
  - `transfer.storageMigration.translationType` - Location translation strategy
- **Universal Properties** (⚪):
  - `transfer.remoteWorkingDirectory` - Remote working directory
- **UI Elements**:
  - Text inputs for prefixes
  - Text input for target namespace with validation
  - Optional intermediate storage configuration
  - Dropdown for translation type

---

### 11. **Warehouse Configuration**
- **Purpose**: Configure warehouse directory settings
- **Required Properties** (✅):
  - `transfer.warehouse.source` - Source warehouse directory
  - `transfer.warehouse.externalDirectory` - External tables directory
  - `transfer.warehouse.managedDirectory` - Managed tables directory
  - `translator.warehouseMapBuilder.warehousePlans` - Warehouse translation plans
- **Optional Properties** (🟡):
  - `translator.forceExternalLocation` - Force external table locations
  - `translator.autoGlobalLocationMap` - Auto global location mapping
  - `translator.userGlobalLocationMap` - User-defined global location mapping
  - `translator.warehouseMapBuilder.sources` - Warehouse map sources
  - `translator.warehouseMapBuilder.inSync` - Keep warehouse maps in sync
- **UI Elements**:
  - Text inputs for warehouse directories
  - Advanced translator settings in expandable section
  - Option to use default warehouse locations

---

### 12. **Review & Advanced Settings**
- **Purpose**: Review all settings and configure remaining universal properties
- **Universal Properties** (⚪):
  - `commandLineOptions` - Additional command line options
  - `connectionPoolLib` - Connection pool library choice
  - `dumpTestData` - Dump test data
  - `loadTestDataFile` - Load test data file
  - `encryptedPasswords` - Use encrypted passwords
  - `quiet` - Suppress verbose output
  - `sqlOutput` - Output SQL statements
  - `suppressCliWarnings` - Suppress CLI warnings
  - `ownershipTransfer.database` - Transfer database ownership
  - `ownershipTransfer.table` - Transfer table ownership
- **UI Elements**:
  - Collapsible sections for each configuration category
  - Summary of all configured values
  - Advanced/Expert settings section
  - Validation status indicators

---

### 13. **Configuration Summary & Export**
- **Purpose**: Final review and configuration export
- **UI Elements**:
  - Complete configuration summary
  - Option to save configuration as YAML file
  - Option to run migration immediately
  - Option to generate command line equivalent
  - Validation results and warnings

## Navigation & User Experience

### Navigation Controls
- **Back/Next buttons** on each screen
- **Skip optional sections** capability
- **Jump to section** via sidebar/progress indicator
- **Save draft** at any point

### Validation
- **Real-time validation** on each field
- **Cross-field validation** (e.g., ensure target ≠ source)
- **Connection testing** for cluster configurations
- **Required field indicators**

### Help & Documentation
- **Tooltips** for technical properties
- **Help links** to full documentation
- **Examples** for complex fields like regex patterns
- **Defaults** pre-populated where appropriate

### Progressive Disclosure
- **Basic vs Advanced** modes
- **Collapsible sections** for optional properties
- **Conditional fields** that appear based on other selections
- **Expert mode** for all properties

This wizard design ensures that users can configure the SQL Data Strategy systematically while providing appropriate guidance and validation at each step.