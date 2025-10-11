### SQL Data Strategy Configuration Wizard

This wizard focuses on configuring the **SQL** data strategy for HMS-Mirror. It assumes that **cluster connections** and **database collections** have been pre-defined and saved as separate entities before starting the wizard.

## Prerequisites

Before using this wizard, the following must be completed:
1. **Cluster Connections**: Define and save source (LEFT) and target (RIGHT) cluster connection profiles
2. **Database Collections**: Create and save collections of databases and tables to be migrated
3. The wizard will only reference these pre-saved entities and will not execute migrations - it only builds the configuration

## Wizard Flow

#### Screen 1: Connection and Data Selection
Select pre-saved cluster connections and database collection for the SQL migration strategy.

| Field | Property Mapped | Input Type | Description/Validation                                                                                                                                                  |
|-------|-----------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Source Connection | `clusters.LEFT.*` (all properties) | Dropdown | Select from saved cluster connections. Required; shows connection name and summary.                                                                                     |
| Target Connection | `clusters.RIGHT.*` (all properties) | Dropdown | Select from saved cluster connections. Required for all Data Strategies except DUMP, and STORAGE_MIGRATION; must differ from source. |
| Database Collection | `databases`, `filter.*` | Dropdown | Select from saved database collections. Required; shows collection name and summary (DB count, table count).                                                            |

**Connection Preview**: Show summary of selected connections (platform type, namespace, server details)
**Collection Preview**: Show summary of selected collection (database names, table counts, filters applied)

**Next**: Proceed to Migration Behavior.

#### Screen 2: Migration Behavior
Configure basic migration behavior and table type handling.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Database Only Mode | `databaseOnly` | Checkbox (default: false) | Migrate only database schemas (no tables/data). Required. |
| Migrate Views | `migrateVIEW.on` | Checkbox (default: true) | Include view migration. Required. |
| Migrate Non-Native Tables | `migrateNonNative` | Checkbox (default: true) | Handle non-Hive tables (e.g., JDBC). Required. |

**ACID Migration Settings**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Enable ACID Migration | `migrateACID.on` | Checkbox (default: true) | Turn on ACID table migration. Required. |
| ACID-Only Mode | `migrateACID.only` | Checkbox (default: false) | Migrate only ACID tables. Required. |
| Artificial Bucket Threshold | `migrateACID.artificialBucketThreshold` | Number (default: 100000) | Bucket size threshold for artificial bucketing. Required; positive integer. |
| ACID Partition Limit | `migrateACID.partitionLimit` | Number (default: 1000) | Max partitions per ACID table. Required; non-negative. |
| Downgrade ACID Tables | `migrateACID.downgrade` | Checkbox (default: false) | Downgrade ACID to non-ACID if incompatible. Required. |
| In-Place ACID Migration | `migrateACID.inplace` | Checkbox (default: false) | Perform ACID migration in-place without full data copy. Required. |

**Next**: Proceed to Transfer Settings.

#### Screen 3: Transfer and Warehouse Settings
Core settings for data transfer paths and warehouses. Auto-derives target namespace from selected target cluster.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| External Directory | `transfer.warehouse.externalDirectory` | Text (default: "/user/hive/external") | External table base path. Required. |
| Managed Directory | `transfer.warehouse.managedDirectory` | Text (default: "/user/hive/warehouse") | Managed table base path. Required. |
| Warehouse Map Plans | `translator.warehouseMapBuilder.warehousePlans` | Text area (default: auto-build from clusters) | JSON-like plans for warehouse mapping. Required; parse as array. |

**Advanced Transfer Settings (Optional)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Transfer Prefix | `transfer.transferPrefix` | Text (default: "mig_") | Prefix for migrated objects. Optional. |
| Shadow Prefix | `transfer.shadowPrefix` | Text (default: "_shadow") | Prefix for shadow objects. Optional. |

**Next**: Proceed to Conversions.

#### Screen 4: Conversions (Optional)
Configure table format conversions, including Iceberg.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Enable Iceberg Conversion | `icebergConversion.enable` | Checkbox (default: false) | Activates Iceberg conversion. Required. |
| File Type Translation | `icebergConversion.fileTypeTranslation` | Text area (default: none) | Map source formats to Iceberg (e.g., "ORC:Parquet"). Optional. |
| Iceberg Version | `icebergConversion.version` | Dropdown ("2.0", "2.1"; default: "2.0") | Target Iceberg spec version. Required if Iceberg enabled. |
| Table Properties | `icebergConversion.tableProperties` | Text area (default: none) | Custom props (e.g., "write.format.default=parquet"). Optional. |
| In-Place Conversion | `icebergConversion.inplace` | Checkbox (default: false) | Convert without data copy. Required if Iceberg enabled. |

**Next**: Generate configuration.

#### Screen 5: Configuration Generation
Review and save the SQL migration configuration.

**Configuration Summary**: Display comprehensive preview of all selected settings:
- Source and target connections
- Database collection details
- Migration behavior settings
- Transfer and warehouse paths
- Conversion settings (if enabled)

**Configuration Options**:
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Configuration Name | N/A | Text | Name for saving this configuration. Required. |
| Description | N/A | Text area | Optional description of the configuration purpose. |

**Actions**:
- **Save Configuration**: Generate YAML/JSON config file with all values
- **Download Configuration**: Download configuration file locally
- **Return to Edit**: Go back to modify settings

**Note**: Configuration is ready for execution via HMS-Mirror CLI or API. If Iceberg conversion was enabled, the configuration includes Iceberg-specific settings.

This streamlined 5-screen wizard focuses on configuration building with pre-defined entities. Total time: ~3-5 minutes.