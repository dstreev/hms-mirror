### Database Collection Wizard

This wizard is used to define and save collections of databases and tables that can be reused across multiple SQL Data Strategy configurations. Database collections define which databases and tables to include in migrations with optional filtering criteria.

## Purpose

Create reusable database and table collections for:
- **Database selection**: Choose specific databases to migrate
- **Table filtering**: Apply inclusion/exclusion patterns
- **Partition filtering**: Define partition-level criteria
- **Size and count limits**: Set boundaries for large datasets

These saved collections are referenced by Data Strategy wizards, eliminating the need to reconfigure database and table selections for each migration.

## Wizard Flow

#### Screen 1: Collection Definition
Define the basic collection parameters.

| Field | Input Type | Description/Validation |
|-------|------------|------------------------|
| Collection Name | Text | Required. Unique name for this database collection |
| Description | Text area | Optional. Description of this collection purpose |

**Next**: Proceed to Database Selection.

#### Screen 2: Database Selection
Choose specific databases to include in the collection.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Selection Mode | N/A | Radio buttons | Required. Options: Specific Databases / Pattern-based |

**Specific Databases Mode**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Source Connection | N/A | Dropdown | Select connection to browse available databases. Required. |
| Available Databases | `databases` | Multi-select list | Browse and select specific databases from the connection. Required. |

**Pattern-based Mode**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Source Connection | N/A | Dropdown | Select connection to validate patterns against. Required. |
| Include Pattern | `filter.dbFilterPattern` | Text | Regex pattern for database names to include. Required. |
| Exclude Pattern | `filter.dbExcludeFilterPattern` | Text | Regex pattern for database names to exclude. Optional. |

**Next**: Proceed to Table Filtering.

#### Screen 3: Table Filtering
Configure table-level filtering criteria.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Table Filter Mode | N/A | Radio buttons | Required. Options: All Tables / Include Pattern / Exclude Pattern |

**Include Pattern Settings**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Include Table Pattern | `filter.tblFilterPattern` | Text | Regex pattern for table names to include. Optional. |
| Include Size Limit | `filter.sizeLimit` | Number | Maximum table size in bytes. Optional; positive integer. |

**Exclude Pattern Settings**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Exclude Table Pattern | `filter.tblExcludeFilterPattern` | Text | Regex pattern for table names to exclude. Optional. |
| Exclude Size Threshold | `filter.excludeSizeThreshold` | Number | Exclude tables larger than threshold. Optional; positive integer. |

**Next**: Proceed to Partition Filtering.

#### Screen 4: Partition Filtering (Optional)
Configure partition-level filtering for partitioned tables.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Enable Partition Filtering | N/A | Checkbox | Enable partition-level filtering. Optional. |

**Partition Filter Settings (shown if enabled)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Partition Filter | `filter.partitionFilter` | Text area | Hive-style partition filter (e.g., year >= 2023). Optional. |
| Partition Limit | `filter.partitionLimit` | Number (default: 1000) | Maximum partitions per table. Optional; positive integer. |

**Advanced Partition Options**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Partition Sort | `filter.partitionSort` | Dropdown | Partition sorting order. Options: NONE, ASC, DESC. Default: NONE. |
| Partition Batch Size | `filter.partitionBatchSize` | Number (default: 100) | Partitions processed per batch. Optional; positive integer. |

**Next**: Proceed to Collection Limits.

#### Screen 5: Collection Limits
Set overall limits for the collection.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Database Limit | N/A | Number | Maximum databases in collection. Optional; positive integer. |
| Table Limit | N/A | Number | Maximum tables per database. Optional; positive integer. |
| Total Size Limit | N/A | Number | Maximum total size for entire collection. Optional; positive integer. |

**Collection Statistics Preview**
- **Estimated Databases**: Show count based on current filters
- **Estimated Tables**: Show estimated table count
- **Estimated Data Size**: Show estimated total data size

**Next**: Proceed to Collection Test and Save.

#### Screen 6: Collection Test and Save
Validate collection criteria and save the profile.

**Collection Validation**
- **Test Collection**: Button to validate collection criteria against source cluster
- **Validation Results**: Display discovered databases, tables, and sizes
- **Filter Effectiveness**: Show how filters reduce the dataset

**Collection Summary**
| Database | Tables Found | Total Size | Partitions | Status |
|----------|-------------|------------|------------|---------|
| db1 | 25 | 1.2 GB | 150 | ✅ Included |
| db2 | 8 | 500 MB | 45 | ✅ Included |
| db3 | 120 | 15 GB | 2500 | ❌ Excluded (size limit) |

**Save Collection Profile**
| Field | Input Type | Description/Validation |
|-------|------------|------------------------|
| Collection Name | Text (pre-filled) | Confirmation of collection name. Required. |
| Make Default | Checkbox | Set as default collection. Optional. |
| Save Validation Results | Checkbox | Store test results with collection. Optional. |

**Actions**:
- **Save Collection**: Save collection profile to repository
- **Save and Create Another**: Save and start new collection wizard
- **Cancel**: Discard collection profile
- **Back to Edit**: Return to modify settings

**Success**: Database collection saved and available for selection in Data Strategy wizards.

## Collection Profile Storage

Saved collection profiles include:
- Database selection criteria
- Table filtering patterns
- Partition filtering rules
- Size and count limits
- Validation test results (optional)
- Source connection reference

These profiles are referenced by name in Data Strategy wizards, enabling rapid reuse of complex database and table selections across multiple migration projects.

## Integration with Data Strategy Wizards

When selected in a Data Strategy wizard, the collection provides:
- Pre-configured `databases` array
- Applied `filter.*` properties
- Validation against target migration requirements
- Estimated migration scope and complexity

Total wizard time: ~8-12 minutes per collection, depending on validation complexity.