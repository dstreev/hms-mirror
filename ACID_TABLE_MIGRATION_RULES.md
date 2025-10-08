# ACID Table Migration Rules in HMS-Mirror

## Executive Summary

ACID (Atomicity, Consistency, Isolation, Durability) tables in Hive present unique challenges during migration due to their transactional nature, specific storage formats, and version-dependent features. This document details how HMS-Mirror handles ACID table migrations across different data strategies, including when and why transfer/intermediate tables are created.

## ACID Table Overview

### ACID Table Characteristics
- **Transactional Properties**: Support INSERT, UPDATE, DELETE operations
- **Special Storage Format**: ORC format with delta files
- **Version Dependencies**: 
  - Hive 1/2: ACID v1 (requires bucketing)
  - Hive 3+: ACID v2 (bucketing optional)
- **Managed Tables Only**: ACID tables must be managed tables
- **Location Restrictions**: Cannot directly manipulate data location

### Migration Challenges
1. **Cross-Version Incompatibility**: ACID v1 to ACID v2 requires data transformation
2. **Storage Format**: Cannot use simple file copy (distcp)
3. **Transactional Metadata**: Requires special handling of transaction IDs
4. **Bucket Requirements**: Legacy versions require artificial bucketing

## Configuration Parameters

### MigrateACID Configuration
```yaml
migrateACID:
  on: false                      # Enable ACID migration
  only: false                     # Migrate ONLY ACID tables
  artificialBucketThreshold: 2   # Remove buckets if <= this value
  partitionLimit: 500             # Max partitions to attempt migration
  downgrade: false                # Convert ACID to EXTERNAL
  inplace: false                  # In-place downgrade on same cluster
```

## Strategy-Specific ACID Rules

### 1. SCHEMA_ONLY Strategy

**ACID Support**: ❌ NOT SUPPORTED

**Behavior**:
- ACID tables are skipped
- Cannot migrate schema without data for ACID tables
- No transfer tables created

**Reason**: ACID tables require data migration through SQL engine due to their special storage format. Schema-only migration with external distcp is impossible.

### 2. LINKED Strategy

**ACID Support**: ❌ NOT SUPPORTED

**Behavior**:
- ACID tables generate errors
- Migration fails with message: "Can't LINK ACID tables"
- No transfer tables created

**Code Reference**: `LinkedDataStrategy.java:73-75`
```java
// Can't LINK ACID tables.
if (TableUtils.isACID(let)) {
    let.addError("Can't LINK ACID tables");
}
```

**Reason**: ACID tables cannot share storage across clusters due to transaction management requirements.

### 3. SQL Strategy

**ACID Support**: ✅ SUPPORTED (with transfer tables)

**Behavior**:
- ALWAYS creates intermediate/transfer tables
- Routes through `IntermediateDataStrategy`
- Multi-step migration process

**Transfer Table Creation**:
```
Source ACID Table → Transfer Table (EXTERNAL) → Target Table (ACID or EXTERNAL)
```

**Process Flow**:
1. Create transfer table with prefix (e.g., `hms_mirror_transfer_`)
2. Extract data from ACID source using SQL
3. Load into transfer table as EXTERNAL format
4. Create final target table
5. Move data from transfer to target
6. Drop transfer table (unless `saveWorkingTables=true`)

**When Transfer Tables Created**:
- Always for ACID tables
- When `intermediateStorage` is configured
- When `targetNamespace` is specified

### 4. EXPORT_IMPORT Strategy

**ACID Support**: ⚠️ LIMITED SUPPORT

**Behavior**:
- Version-dependent compatibility
- May fail for ACID v1 to v2 migrations
- Creates export staging area

**Transfer Table Creation**:
- Not typically used
- Export/Import handles format conversionResult

**Limitations**:
- Hive 1/2 EXPORT incompatible with Hive 3 IMPORT for ACID
- Recommended to use SQL strategy instead

### 5. HYBRID Strategy

**ACID Support**: ✅ SUPPORTED (special handling)

**Behavior**:
- ACID tables ALWAYS use `IntermediateDataStrategy`
- Overrides normal hybrid decision logic
- Forces SQL-based migration

**Decision Logic**:
```java
if (TableUtils.isACID(let) && configService.legacyMigration(config)) {
    tableMirror.setStrategy(DataStrategyEnum.ACID);
    if (config.getMigrateACID().isOn()) {
        rtn = intermediateDataStrategy.build(tableMirror);
    }
}
```

**Transfer Table Creation**:
- Always created for ACID tables
- Required for cross-version migrations

### 6. STORAGE_MIGRATION Strategy

**ACID Support**: ✅ SUPPORTED (with special handling)

**Behavior for ACID Tables**:
- In-place: Can downgrade ACID to EXTERNAL
- SQL mode: Creates archive and new tables
- Distcp mode: NOT supported for ACID

**Transfer Table Creation**:
- SQL mode: Creates archive table
- Downgrade: Creates intermediate EXTERNAL table
- Distcp: Not applicable (fails)

**Downgrade Process**:
1. Create EXTERNAL transfer table
2. Copy data from ACID using SQL
3. Drop original ACID table
4. Rename transfer to original name

### 7. COMMON Strategy

**ACID Support**: ⚠️ CONDITIONAL

**Behavior**:
- Only if both clusters support same ACID version
- Shared storage must support transactional semantics
- Rarely practical for ACID tables

### 8. DUMP Strategy

**ACID Support**: ✅ INFORMATIONAL ONLY

**Behavior**:
- Generates migration scripts
- Shows ACID handling requirements
- No actual migration performed

### 9. CONVERT_LINKED Strategy

**ACID Support**: ❌ NOT APPLICABLE

**Behavior**:
- Cannot convert LINKED ACID tables (they don't exist)
- Only works with previously LINKED non-ACID tables

### 10. ICEBERG_CONVERSION Strategy

**ACID Support**: ✅ SUPPORTED

**Behavior**:
- Converts ACID to Iceberg format
- Requires intermediate processing
- Complex multi-step conversionResult

## Transfer Table Details

### When Transfer Tables Are Created

| Condition | Transfer Table Created | Reason |
|-----------|----------------------|---------|
| ACID table with SQL strategy | YES | Required for data extraction |
| ACID table with HYBRID strategy | YES | Forced intermediate processing |
| Legacy to non-legacy migration | YES | Version incompatibility |
| intermediateStorage configured | YES | Staging requirement |
| targetNamespace configured | YES | Namespace translation |
| ACID downgrade | YES | Format conversionResult |
| ACID in-place migration | YES | Same-cluster transformation |
| Non-ACID with SCHEMA_ONLY | NO | Direct schema transfer |
| Non-ACID with LINKED | NO | No data movement |

### Transfer Table Characteristics

**Naming Convention**:
```
{transfer_prefix}_{original_table_name}
Default prefix: "hms_mirror_transfer_"
```

**Properties**:
- Type: EXTERNAL
- Format: Original format (or converted)
- Location: Intermediate storage or translated namespace
- Ownership: PURGE enabled (owns data temporarily)
- Lifecycle: Temporary (deleted after migration)

**Example Transfer Table DDL**:
```sql
CREATE EXTERNAL TABLE `hms_mirror_transfer_acid_table` (
  -- Original schema
) 
STORED AS ORC
LOCATION 's3a://intermediate/transfer/acid_table'
TBLPROPERTIES ('external.table.purge'='true');
```

## ACID Migration Scenarios

### Scenario 1: Legacy Hive (1/2) to Modern Hive (3+)

**Challenge**: ACID v1 to v2 incompatibility

**Solution**:
1. Use SQL or HYBRID strategy
2. Transfer table created automatically
3. Data converted during transfer
4. Artificial buckets removed (if threshold met)

**Configuration**:
```yaml
dataStrategy: SQL
migrateACID:
  on: true
  artificialBucketThreshold: 2
```

### Scenario 2: ACID Downgrade to EXTERNAL

**Challenge**: Convert transactional to non-transactional

**Solution**:
1. Set `downgrade: true`
2. Transfer table created as EXTERNAL
3. Original ACID table archived/dropped
4. Transfer renamed to original

**Configuration**:
```yaml
migrateACID:
  on: true
  downgrade: true
  inplace: false  # Cross-cluster
```

### Scenario 3: In-Place ACID Downgrade

**Challenge**: Downgrade on same cluster

**Solution**:
1. Set `inplace: true` and `downgrade: true`
2. Temporary transfer table created
3. Data migrated within cluster
4. Atomic table replacement

**Configuration**:
```yaml
migrateACID:
  on: true
  downgrade: true
  inplace: true  # Same cluster
```

### Scenario 4: ACID with Partition Limit

**Challenge**: Large partitioned ACID table

**Solution**:
- Check against `partitionLimit`
- Fail if exceeded
- Recommend partition pruning or batch migration

**Error Message**:
```
"Partition count (1000) exceeds limit (500) for ACID migration"
```

## Special Considerations

### Bucket Handling

**Artificial Bucket Removal**:
- Legacy ACID required buckets
- Modern ACID doesn't require buckets
- Threshold check: `artificialBucketThreshold`
- Buckets removed if count ≤ threshold

**Example**:
```java
if (bucketCount <= config.getMigrateACID().getArtificialBucketThreshold()) {
    // Remove bucket definition in new table
}
```

### Transaction ID Management

**Compaction Requirements**:
- Major compaction recommended before migration
- Reduces delta files
- Improves migration performance

**Command**:
```sql
ALTER TABLE acid_table COMPACT 'major';
```

### Performance Optimization

**For Large ACID Tables**:
1. Run major compaction first
2. Consider partition-based migration
3. Use appropriate execution engine (Tez recommended)
4. Monitor memory settings

**Session Variables**:
```sql
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=2000;
```

## Error Handling

### Common ACID Migration Errors

| Error | Cause | Solution |
|-------|-------|----------|
| "Can't LINK ACID tables" | Using LINKED strategy | Switch to SQL/HYBRID |
| "ACID table partition limit exceeded" | Too many partitions | Reduce partitions or adjust limit |
| "ACID not supported in SCHEMA_ONLY" | Wrong strategy | Use SQL/HYBRID strategy |
| "Bucket count exceeds threshold" | In-place with high buckets | Adjust threshold or use different strategy |

### Recovery Procedures

**Failed Transfer Table**:
```sql
-- Check for orphaned transfer tables
SHOW TABLES LIKE 'hms_mirror_transfer_%';

-- Manual cleanup if needed
DROP TABLE IF EXISTS hms_mirror_transfer_tablename;
```

## Best Practices

### Pre-Migration Checklist
1. ✅ Identify all ACID tables
2. ✅ Check partition counts
3. ✅ Run major compaction
4. ✅ Verify version compatibility
5. ✅ Configure appropriate strategy
6. ✅ Set migrateACID parameters

### Strategy Selection for ACID

**Decision Tree**:
```
Is table ACID?
├── NO → Use any strategy
└── YES
    ├── Same Hive version? → COMMON (if shared storage)
    ├── Need downgrade? → STORAGE_MIGRATION with downgrade
    ├── Legacy to modern? → SQL or HYBRID
    └── Large partitions? → SQL with partition limit check
```

### Post-Migration Validation

**Verification Steps**:
1. Check transaction properties preserved (if not downgraded)
2. Verify row counts match
3. Test ACID operations (INSERT/UPDATE/DELETE)
4. Confirm transfer tables cleaned up
5. Validate table properties

## Summary

ACID table migration in HMS-Mirror requires special handling due to:

1. **Transactional Nature**: Cannot use simple file operations
2. **Version Dependencies**: v1 to v2 requires transformation
3. **Transfer Tables**: Almost always required for ACID migrations
4. **Strategy Limitations**: Only SQL, HYBRID, and STORAGE_MIGRATION fully support ACID

Key Rules:
- **SCHEMA_ONLY** and **LINKED** cannot handle ACID tables
- **SQL** and **HYBRID** always create transfer tables for ACID
- **Downgrade** operations require intermediate EXTERNAL tables
- **In-place** migrations work within single cluster
- **Partition limits** prevent large table migrations

The transfer table pattern ensures data integrity during complex ACID transformations while providing rollback capability if migration fails.