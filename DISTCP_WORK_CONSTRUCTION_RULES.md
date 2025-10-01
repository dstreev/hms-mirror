# DistCp Work Construction Rules in HMS-Mirror

## Executive Summary

HMS-Mirror constructs DistCp work by analyzing table and partition locations, applying translation rules, and optimizing paths through consolidation and pruning. This document details how DistCp jobs are built from table location details, including handling of non-standard locations and consolidation logic.

## Core Concepts

### Location Components

**Standard Hive Table Location Structure**:
```
hdfs://nameservice/warehouse/database.db/table_name/partition_spec
```

**Components**:
- **Namespace**: `hdfs://nameservice`
- **Warehouse Directory**: `/warehouse`
- **Database Directory**: `/database.db`
- **Table Directory**: `/table_name`
- **Partition Directory**: `/partition_spec` (e.g., `/year=2024/month=01`)

### Translation Levels

The "level" parameter controls how much of the path is reduced during translation:

| Level | Reduction | Result for `/warehouse/db/table/part` |
|-------|-----------|---------------------------------------|
| 0 | None | `/warehouse/db/table/part` |
| 1 | Remove last segment | `/warehouse/db/table` |
| 2 | Remove 2 segments | `/warehouse/db` |
| 3 | Remove 3 segments | `/warehouse` |

## DistCp Construction Pipeline

### Phase 1: Location Collection

**Source Locations Gathered From**:
1. Table LOCATION property
2. Partition LOCATION properties
3. Database default locations
4. Warehouse configurations

**Data Structure**:
```java
SourceLocationMap {
  database: {
    TableType.EXTERNAL: {
      "/path/location1": ["table1", "table2"],
      "/path/location2": ["table3"]
    },
    TableType.MANAGED: {
      "/path/location3": ["table4"]
    }
  }
}
```

### Phase 2: Location Translation

**Translation Process**:
1. Strip namespace from source location
2. Apply Global Location Maps (GLM)
3. Apply warehouse plan translations
4. Reduce path by consolidation level
5. Combine with target namespace

**Example Translation**:
```
Source: hdfs://source:8020/apps/hive/warehouse/db.db/table1
Target Namespace: hdfs://target:8020
Warehouse Plan: /apps/hive/warehouse → /warehouse/tablespace/external

Result: hdfs://target:8020/warehouse/tablespace/external/db.db/table1
```

### Phase 3: Path Consolidation

**Consolidation Rules**:

#### Rule 1: Table-Level Consolidation
When `consolidateTablesForDistcp = true`:
- Reduce source paths to database level
- Single DistCp for entire database
- More efficient for many small tables

```java
if (consolidateTablesForDistcp) {
    adjustedSource = UrlUtils.reduceUrlBy(original, level);
    adjustedTarget = UrlUtils.reduceUrlBy(target, level);
} else {
    adjustedSource = UrlUtils.reduceUrlBy(original, level - 1);
    adjustedTarget = UrlUtils.reduceUrlBy(target, level);
}
```

#### Rule 2: Partition Pruning
Avoid redundant copies when table location covers partitions:
- If partition path starts with table path → prune
- Increase consolidation level to table directory
- Prevents duplicate data transfer

**Example**:
```
Table Location: /warehouse/db/table
Partition 1: /warehouse/db/table/year=2024  → PRUNED (covered by table)
Partition 2: /external/data/year=2024       → INCLUDED (non-standard)
```

### Phase 4: Non-Standard Location Handling

#### Detection of Non-Standard Locations

**Standard Location Checks**:
1. Table location matches `database_location/table_name`
2. Partition location matches `table_location/partition_spec`
3. Database locations align with warehouse configuration

**Non-Standard Location Indicators**:
```java
// Partition location doesn't match partition spec
if (!partLocation.endsWith(normalizedPartSpecLocation)) {
    // Non-standard partition location detected
    String msg = MessageFormat.format(
        DISTCP_WITH_MISMATCHING_LOCATIONS.getDesc(),
        "Partition", partSpec, partLocation, normalizedPartSpecLocation
    );
}
```

#### Handling Non-Standard Locations

**Strategy 1: Individual Path Mapping**
- Create separate DistCp entry for each non-standard location
- Maintain exact source → target mapping
- No consolidation possible

**Strategy 2: Warning and Manual Verification**
```java
// Table directory name doesn't match table name
if (!tableDirName.equals(tableMirror.getName())) {
    String msg = MessageFormat.format(
        DISTCP_WITH_MISMATCHING_TABLE_LOCATION.getDesc(),
        "Partition", partSpec, partLocation, tableDirName
    );
    tableMirror.addIssue(Environment.LEFT, msg);
}
```

**Strategy 3: Fail with Error**
- For critical mismatches that would cause data loss
- Requires manual intervention or strategy change

## Consolidation Algorithm

### Step 1: Build Translation Map

```java
Map<String, Map<String, Set<String>>> buildDistcpListForDatabase(
    HmsMirrorConfig config, 
    String database, 
    Environment environment,
    int consolidationLevel, 
    boolean consolidateTablesForDistcp) {
    
    // Get translation levels for database
    Set<TranslationLevel> dbTranslationLevel = 
        config.getTranslator().getTranslationMap(database, environment);
    
    // Build location map
    Map<String, String> dbLocationMap = new TreeMap<>();
    for (TranslationLevel level : dbTranslationLevel) {
        dbLocationMap.put(
            level.getAdjustedOriginal(), 
            level.getAdjustedTarget()
        );
    }
}
```

### Step 2: Reverse Map Construction

Create target → sources mapping for consolidation:

```java
// Build reverse map: target -> set of sources
Map<String, Set<String>> reverseMap = new TreeMap<>();
for (Map.Entry<String, String> entry : dbLocationMap.entrySet()) {
    String source = entry.getKey();
    String target = entry.getValue();
    
    reverseMap.computeIfAbsent(target, k -> new TreeSet<>())
              .add(source);
}
```

### Step 3: Optimization

**Path Deduplication**:
```java
// Remove redundant paths
Set<String> optimizedPaths = new TreeSet<>();
for (String path : allPaths) {
    boolean isRedundant = false;
    for (String existing : optimizedPaths) {
        if (path.startsWith(existing + "/")) {
            isRedundant = true;
            break;
        }
    }
    if (!isRedundant) {
        optimizedPaths.add(path);
    }
}
```

## DistCp Script Generation

### Script Structure

**Generated Components**:
1. Environment setup
2. Source file upload
3. DistCp commands
4. Validation checks

**Example Generated Script**:
```bash
#!/usr/bin/env sh

# Environment Setup
export HCFS_BASE_DIR="/user/hms-mirror"
export DISTCP_OPTS="-Dmapreduce.map.memory.mb=8192"

# Upload source lists
hdfs dfs -mkdir -p $HCFS_BASE_DIR
hdfs dfs -put database_distcp_source.txt $HCFS_BASE_DIR/

# Execute DistCp
echo "Running 'distcp'"
hadoop distcp ${DISTCP_OPTS} \
    hdfs://source:8020/warehouse/db.db \
    hdfs://target:8020/warehouse/tablespace/external/db.db
```

### Cross-Protocol Handling

When source and target use different protocols:
```java
if (!sourceProtocol.equals(targetProtocol)) {
    // Add -skipcrccheck for cross-protocol transfers
    distcpScript.append("hadoop distcp ${DISTCP_OPTS} -skipcrccheck ")
                .append(source).append(" ")
                .append(target);
}
```

## Location Validation Rules

### Rule 1: Partition Spec Alignment

**Requirement**: Partition location must end with partition specification

**Valid**:
```
Partition: year=2024,month=01
Location: /warehouse/table/year=2024/month=01 ✓
```

**Invalid**:
```
Partition: year=2024,month=01
Location: /custom/location/data_202401 ✗
```

### Rule 2: Table Name Alignment

**Requirement**: Table directory name must match table name

**Valid**:
```
Table: customer_data
Location: /warehouse/db/customer_data ✓
```

**Invalid**:
```
Table: customer_data
Location: /warehouse/db/cust_data ✗
```

### Rule 3: Warehouse Alignment

**Requirement**: Locations should align with warehouse configuration

**Check**:
```java
if (TableUtils.isExternal(table)) {
    String dbExtDir = database.getProperty(DB_LOCATION);
    if (!location.startsWith(dbExtDir)) {
        // Warning: Location not in external warehouse
    }
}
```

## Global Location Maps (GLM)

### GLM Construction

**Purpose**: Map non-standard source locations to standard targets

**Process**:
1. Analyze source locations
2. Identify patterns
3. Create mappings
4. Apply during translation

**Example GLM**:
```yaml
globalLocationMap:
  - from: "/legacy/warehouse"
    to: "/warehouse/tablespace/managed"
    tableType: MANAGED
  - from: "/external/data"
    to: "/warehouse/tablespace/external"
    tableType: EXTERNAL
```

### GLM Application Order

1. User-defined GLMs (highest priority)
2. Warehouse plan mappings
3. Default translations (lowest priority)

## Special Cases

### Case 1: ACID Tables

**Rule**: Cannot use DistCp for ACID tables
```java
if (TableUtils.isACID(table)) {
    tableMirror.addError("distcp cannot be used for ACID tables");
    return false;
}
```

### Case 2: Partitioned Tables with Mixed Locations

**Scenario**: Partitions scattered across multiple locations

**Handling**:
```java
Map<String, String> partitionMap = table.getPartitions();
for (Entry<String, String> partition : partitionMap.entrySet()) {
    String partSpec = partition.getKey();
    String partLocation = partition.getValue();
    
    if (!isStandardLocation(partLocation)) {
        // Add individual DistCp entry
        addTranslation(database, environment, 
                      partLocation, 
                      translateLocation(partLocation),
                      0, // No reduction for non-standard
                      false); // No consolidation
    }
}
```

### Case 3: Cross-Filesystem Migration

**Challenge**: Different filesystem schemes (HDFS → S3)

**Solution**:
```bash
# Add compatibility flags
hadoop distcp \
    -skipcrccheck \  # Skip CRC validation
    -update \        # Update changed files only
    hdfs://source/path \
    s3a://bucket/path
```

## Consolidation Examples

### Example 1: Standard Database Migration

**Input**:
```
/warehouse/sales.db/customers
/warehouse/sales.db/orders
/warehouse/sales.db/products
```

**Without Consolidation**:
```bash
distcp /warehouse/sales.db/customers /target/sales.db/customers
distcp /warehouse/sales.db/orders /target/sales.db/orders
distcp /warehouse/sales.db/products /target/sales.db/products
```

**With Consolidation** (`consolidateTablesForDistcp=true`):
```bash
distcp /warehouse/sales.db /target/sales.db
```

### Example 2: Mixed Location Database

**Input**:
```
/warehouse/db/standard_table
/external/custom/special_table
/warehouse/db/another_table/year=2024
/archive/old_data/year=2023
```

**DistCp Plan**:
```bash
# Consolidated standard locations
distcp /warehouse/db /target/warehouse/db

# Individual non-standard locations
distcp /external/custom/special_table /target/external/db/special_table
distcp /archive/old_data/year=2023 /target/warehouse/db/table/year=2023
```

## Performance Optimization

### Optimization Strategies

1. **Path Consolidation**: Reduce number of DistCp jobs
2. **Parallel Execution**: Run multiple DistCp jobs concurrently
3. **Bandwidth Management**: Use `-bandwidth` flag
4. **Incremental Updates**: Use `-update` flag for retry

### Consolidation Trade-offs

| Strategy | Pros | Cons |
|----------|------|------|
| Full Consolidation | Fewer jobs, simpler management | May copy unnecessary data |
| No Consolidation | Precise data movement | Many small jobs, overhead |
| Smart Consolidation | Balance of efficiency and precision | Complex logic required |

## Error Handling

### Common Issues and Solutions

**Issue 1: Mismatched Locations**
```
Error: DISTCP_WITH_MISMATCHING_LOCATIONS
Solution: Review warehouse plans, use GLM, or switch to SQL strategy
```

**Issue 2: ACID Table Detection**
```
Error: DISTCP_FOR_SO_ACID
Solution: Use SQL or HYBRID strategy for ACID tables
```

**Issue 3: Missing Warehouse Configuration**
```
Error: DISTCP_RDL_WO_WAREHOUSE_DIR
Solution: Specify -wd and -ewd parameters
```

## Best Practices

### 1. Pre-Migration Analysis
- Identify non-standard locations
- Document partition distribution
- Check for ACID tables
- Verify namespace accessibility

### 2. Configuration Guidelines
```yaml
# Optimal configuration for standard layouts
transfer:
  storageMigration:
    dataMovementStrategy: DISTCP
    consolidateTablesForDistcp: true
    translationType: ALIGNED
    
# Configuration for mixed locations
transfer:
  storageMigration:
    dataMovementStrategy: DISTCP
    consolidateTablesForDistcp: false
    translationType: RELATIVE
```

### 3. Validation Steps
1. Review generated DistCp scripts
2. Verify source and target paths
3. Check consolidation decisions
4. Test with small dataset first

## Summary

HMS-Mirror's DistCp work construction follows a sophisticated pipeline:

1. **Collection**: Gather all table/partition locations
2. **Translation**: Apply namespace and warehouse mappings
3. **Consolidation**: Optimize paths based on configuration
4. **Validation**: Check for standard compliance
5. **Generation**: Create executable DistCp scripts

The system handles non-standard locations through:
- Individual path mapping
- Warning generation
- Manual verification requirements
- Alternative strategy recommendations

Consolidation decisions balance:
- Efficiency (fewer DistCp jobs)
- Precision (exact data movement)
- Compatibility (standard vs. non-standard layouts)

Key success factors:
- Proper warehouse configuration
- Accurate GLM definitions
- Understanding of consolidation impact
- Validation of generated plans before execution