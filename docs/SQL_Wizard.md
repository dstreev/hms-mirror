### Data Migration Wizard for SQL Strategy (Revised)

This revised wizard maintains the focus on the **SQL** data strategy, covering only **required properties** (✅ marked in the documentation). Cluster selections auto-populate required properties from preconfigured setups. The flow is linear with a mandatory Conversions screen.

#### Screen 1: Cluster Selection
Select preconfigured source (LEFT) and target (RIGHT) clusters. Upon selection, required properties (e.g., `clusters.LEFT/RIGHT.platformType`, `hcfsNamespace`, `hiveServer2.*`, `clusters.RIGHT.partitionDiscovery.auto/initMSCK`) are auto-populated from the config.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Source Cluster | `clusters.LEFT.*` (all required) | Dropdown (e.g., "Prod-Hive-Cluster-A", "Dev-Databricks-B") | Select from available preconfigured clusters. Required; lists only validated setups with full required props. |
| Target Cluster | `clusters.RIGHT.*` (all required, incl. partitionDiscovery) | Dropdown (e.g., "Prod-Hive-Cluster-C", "Staging-Iceberg-D") | Select from available preconfigured clusters. Required; must differ from source. Auto-sets `partitionDiscovery.auto` (default: true) and `initMSCK` (default: false); show preview tooltip with values. |

**Overrides (if needed, shown as collapsible section post-selection)**: Quick-edit for any mismatched required props (e.g., update `hiveServer2.password` if expired). Validation: Ensure selections provide all required fields; error if incomplete.

**Next**: Proceed to Input Criteria.

#### Screen 2: Input Criteria
Select or filter datasets to migrate, including type handling and ACID settings. If a pre-supplied list (e.g., JSON/CSV of DBs/tables) is detected, auto-populate and skip to "Type Selection" sub-section.

**Sub-section A: Database Selection (if no pre-supplied list)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Database Only Mode | `databaseOnly` | Checkbox (default: false) | Migrate only database schemas (no tables/data). Required. |
| Databases List | `databases` | Multi-select/Text area | Comma-separated DB names (e.g., "db1,db2"). Required if not filtering. |
| DB Prefix | `dbPrefix` | Text (default: none) | Prefix for DB names (e.g., "prod_"). Required; optional empty for none. |
| DB Regex Filter | `filter.dbRegEx` | Text (default: ".*") | Regex to include DBs (e.g., "^prod.*$"). Required; validate regex syntax. |

**Sub-section B: Table Filtering (if no pre-supplied list)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Table Include Regex | `filter.tblRegEx` | Text (default: ".*") | Regex for tables to include. Required. |
| Table Exclude Regex | `filter.tblExcludeRegEx` | Text (default: none) | Regex for tables to skip. Required; optional empty. |

**Sub-section C: Type Selection (always shown)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Migrate Views | `migrateVIEW.on` | Checkbox (default: true) | Include view migration. Required. |
| Migrate Non-Native Tables | `migrateNonNative` | Checkbox (default: true) | Handle non-Hive tables (e.g., JDBC). Required. |

**Sub-section D: ACID Migration (always shown)**
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
Core settings for data transfer paths and warehouses. Defaults: External = "/user/hive/external", Managed = "/user/hive/warehouse". Auto-derives `transfer.targetNamespace` from selected target cluster's `hcfsNamespace`.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| External Directory | `transfer.warehouse.externalDirectory` | Text (default: "/user/hive/external") | External table base path. Required. |
| Managed Directory | `transfer.warehouse.managedDirectory` | Text (default: "/user/hive/warehouse") | Managed table base path. Required. |
| Warehouse Map Plans | `translator.warehouseMapBuilder.warehousePlans` | Text area (default: auto-build from clusters) | JSON-like plans for warehouse mapping (e.g., [{"source": "/old", "target": "/new"}]). Required; parse as array. |

**Optional Sub-section: Advanced Transfer (expandable accordion)**
- Transfer Prefix: `transfer.transferPrefix` (Text, default: "mig_") – Skippable.
- Shadow Prefix: `transfer.shadowPrefix` (Text, default: "_shadow") – Skippable.

**Next**: Proceed to Conversions.

#### Screen 4: Conversions
Configure table format conversions, including Iceberg. Set `dataStrategy` to hybrid SQL+ICEBERG_MIGRATION if Iceberg is enabled.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Enable Iceberg Conversion | `icebergConversion.enable` | Checkbox (default: false) | Activates Iceberg conversion. Required. |
| File Type Translation | `icebergConversion.fileTypeTranslation` | Text area (default: none) | Map source formats to Iceberg (e.g., "ORC:Parquet"). Required; optional empty for none. |
| Iceberg Version | `icebergConversion.version` | Dropdown (e.g., "2.0", "2.1"; default: "2.0") | Target Iceberg spec version. Required. |
| Table Properties | `icebergConversion.tableProperties` | Text area (default: none) | Custom props (e.g., "write.format.default=parquet"). Required; optional empty. |
| In-Place Conversion | `icebergConversion.inplace` | Checkbox (default: false) | Convert without data copy. Required. |

**Next**: Proceed to Execution Control.

#### Screen 5: Execution Control
Final controls for running the migration.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Execute Migration | `execute` | Checkbox (default: true) | Run the migration (vs. dry-run). Required. |
| Read-Only Mode | `readOnly` | Checkbox (default: false) | Simulate without writes. Required. |
| No Purge on Target | `noPurge` | Checkbox (default: false) | Skip dropping existing target objects. Required. |
| Sync Mode | `sync` | Checkbox (default: true) | Incremental sync after initial load. Required. |

**Finish**: Generate config file (e.g., YAML/JSON) with all values, including auto-populated cluster details. Summary preview shown before save. If Iceberg was enabled, append note: "Hybrid SQL+Iceberg mode activated."

This revised 5-screen structure streamlines the flow with integrated criteria and mandatory conversions. Total time: ~4-8 minutes. For implementation, integrate with a cluster registry API for dynamic dropdowns.