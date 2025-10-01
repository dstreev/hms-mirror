Please# DistCp Integration in HMS-Mirror Data Strategies

## Executive Summary

DistCp (Distributed Copy) is Hadoop's large-scale data movement utility that HMS-Mirror integrates with several data strategies to facilitate efficient data migration between clusters. This document details how distcp is integrated into each data strategy, its requirements, capabilities, and operational details.

## DistCp Overview in HMS-Mirror

DistCp is a critical component for data movement in HMS-Mirror, particularly for strategies that require physical data transfer between different storage namespaces. HMS-Mirror generates distcp-ready scripts, workbooks, and source files to facilitate data migration at scale.

### Key DistCp Components

1. **DistCp Service** (`DistCpService.java`) - Generates reports and scripts
2. **DistCp Plan Generator** - Creates optimized copy plans
3. **DistCp Flow Control** - Manages PUSH/PULL data flow patterns
4. **Translation Layer** - Handles namespace and path translations

## Strategy-Specific DistCp Integration

### 1. SCHEMA_ONLY Strategy

**DistCp Role**: PRIMARY - Essential for actual data movement

**Integration Details**:
- HMS-Mirror creates schema on target cluster with translated locations
- Generates distcp scripts for external data movement
- Creates source file lists for batch processing
- Produces workbooks documenting migration plan

**DistCp Workflow**:
1. Schema migration completes first
2. DistCp source files generated per database/table
3. Scripts created with proper namespace translations
4. User executes distcp commands separately
5. Data validation post-migration

**Generated Artifacts**:
- `{database}_distcp_source.txt` - Source path listings
- `{database}_distcp_script.sh` - Executable shell scripts
- `{database}_distcp_workbook.md` - Documentation
- `{database}_distcp_plans.yaml` - Detailed migration plans

**Configuration**:
```yaml
dataStrategy: SCHEMA_ONLY
transfer:
  targetNamespace: "hdfs://target-cluster:8020"
  storageMigration:
    dataMovementStrategy: DISTCP
    dataFlow: PULL # or PUSH
```

**Special Considerations**:
- Requires manual distcp execution
- Network bandwidth critical
- Supports incremental migrations
- Partition-level granularity available

### 2. LINKED Strategy

**DistCp Role**: NONE - No data movement required

**Integration Details**:
- No distcp integration as data remains in original location
- Tables point to source cluster storage
- Cross-cluster data access via shared storage

**Why No DistCp**:
- Strategy designed for zero data movement
- Relies on shared/accessible storage
- Temporary solution for testing/validation

### 3. SQL Strategy

**DistCp Role**: NONE - SQL engine handles data movement

**Integration Details**:
- Data transferred via SQL INSERT/SELECT
- No distcp scripts generated
- SQL engine manages data flow

**Alternative with Intermediate Storage**:
- When intermediate storage configured, may generate distcp for staging areas
- Limited use for temporary data movement

### 4. EXPORT_IMPORT Strategy

**DistCp Role**: OPTIONAL - For export/import staging areas

**Integration Details**:
- Primary data movement via Hive EXPORT/IMPORT
- DistCp may be used for staging directory transfers
- Useful when export location not directly accessible

**DistCp Usage Scenarios**:
- Moving export files to shared location
- Staging area management
- Cross-region export transfers

### 5. HYBRID Strategy

**DistCp Role**: CONDITIONAL - Depends on selected sub-strategy

**Integration Details**:
- Inherits distcp behavior from chosen strategy
- SQL path: No distcp
- EXPORT_IMPORT path: Optional distcp
- Decision based on partition count and table size

### 6. STORAGE_MIGRATION Strategy

**DistCp Role**: PRIMARY - Core data movement mechanism

**Integration Details**:
- Most sophisticated distcp integration
- Supports multiple migration patterns
- Handles complex location translations
- Manages partition-level movements

**DistCp Modes**:

#### Mode 1: Direct Location Update (No Archive)
```yaml
storageMigration:
  dataMovementStrategy: DISTCP
  createArchive: false
  consolidateTablesForDistcp: false
```
- Updates table/partition locations in-place
- Generates minimal distcp commands
- Most efficient for simple migrations

#### Mode 2: Archive Creation
```yaml
storageMigration:
  dataMovementStrategy: DISTCP
  createArchive: true
```
- Creates archived copy of original table
- New table with translated locations
- Preserves original data access

#### Mode 3: Consolidated DistCp
```yaml
storageMigration:
  consolidateTablesForDistcp: true
```
- Optimizes distcp commands by consolidating paths
- Reduces number of distcp jobs
- Better for tables with many partitions

**Location Translation Requirements**:
- Partition locations must match partition specs for proper translation
- Table directory names must align with table names
- Mismatched locations generate warnings/errors

**Flow Direction Control**:
```yaml
dataFlow: PULL # Target cluster pulls from source
# or
dataFlow: PUSH # Source cluster pushes to target
```

### 7. COMMON Strategy

**DistCp Role**: NONE - Shared storage, no movement

**Integration Details**:
- No data movement required
- Tables use common storage location
- No distcp scripts generated

### 8. DUMP Strategy

**DistCp Role**: INFORMATIONAL - Scripts for reference

**Integration Details**:
- Generates distcp scripts for planning
- No execution, documentation only
- Useful for capacity planning

### 9. CONVERT_LINKED Strategy

**DistCp Role**: ASSUMED COMPLETE - Data already moved

**Integration Details**:
- Assumes distcp already completed
- Converts locations from source to target namespace
- No new distcp scripts generated

**Prerequisites**:
- Data must be migrated via external distcp
- Locations must match expected patterns

### 10. ICEBERG_CONVERSION Strategy

**DistCp Role**: DEPENDS - On conversion approach

**Integration Details**:
- May require distcp for data files
- Metadata conversion separate from data
- Complex integration with Iceberg catalogs

## DistCp Optimization Features

### 1. Path Consolidation
- Combines multiple small paths into fewer distcp jobs
- Reduces overhead and improves throughput
- Configurable via `consolidateTablesForDistcp`

### 2. Partition Pruning
- Avoids redundant copies when table location covers partitions
- Intelligent path analysis for efficiency
- Automatic detection of covered paths

### 3. Incremental Support
- Partition-level tracking for incremental copies
- Timestamp-based filtering capabilities
- Resume support for failed transfers

### 4. Parallelization Planning
- Generates multiple distcp commands for parallel execution
- Database and table-level parallelization
- Workload distribution recommendations

## DistCp Execution Patterns

### Pattern 1: Pull Model (Default)
```bash
# Executed on RIGHT (target) cluster
hadoop distcp \
  -f hdfs:///user/hms-mirror/distcp_source.txt \
  hdfs://source-cluster:8020/warehouse \
  hdfs://target-cluster:8020/warehouse
```

**Use Cases**:
- On-premise to cloud migrations
- Newer cluster has better resources
- Network egress considerations

### Pattern 2: Push Model
```bash
# Executed on LEFT (source) cluster
hadoop distcp \
  -f hdfs:///user/hms-mirror/distcp_source.txt \
  hdfs://source-cluster:8020/warehouse \
  hdfs://target-cluster:8020/warehouse
```

**Use Cases**:
- Cloud to on-premise migrations
- Source cluster has compute capacity
- Security/firewall requirements

## DistCp Script Structure

### Generated Script Components

1. **Environment Setup**
```bash
#!/usr/bin/env sh
# Environment variables
export HCFS_BASE_DIR="/user/hms-mirror"
export DISTCP_OPTS="-Dmapreduce.map.memory.mb=8192"
```

2. **Source File Upload**
```bash
# Upload source lists to HDFS
hdfs dfs -mkdir -p $HCFS_BASE_DIR
hdfs dfs -put *_distcp_source.txt $HCFS_BASE_DIR/
```

3. **DistCp Execution**
```bash
# Database-level distcp
hadoop distcp $DISTCP_OPTS \
  -f $HCFS_BASE_DIR/db_name_distcp_source.txt \
  /target/location
```

4. **Validation**
```bash
# Verify copy completion
hdfs dfs -count /target/location/*
```

## Configuration Requirements

### Essential Configuration
```yaml
# Strategy selection
dataStrategy: SCHEMA_ONLY # or STORAGE_MIGRATION

# Transfer settings
transfer:
  targetNamespace: "hdfs://target:8020"
  storageMigration:
    dataMovementStrategy: DISTCP
    dataFlow: PULL
    consolidateTablesForDistcp: false
    createArchive: false

# Optional optimization
optimization:
  sortDynamicPartitions: true
  skipStatsCollection: true
```

### Advanced Configuration
```yaml
# Warehouse plans for aligned migrations
transfer:
  warehousePlans:
    - sourcePath: "/apps/hive/warehouse"
      targetPath: "/warehouse/tablespace/managed"
    - sourcePath: "/apps/hive/external"
      targetPath: "/warehouse/tablespace/external"

# Translation settings
translator:
  forceExternalLocation: true
  globalLocationMap:
    - from: "hdfs://source"
      to: "s3a://bucket"
```

## Performance Considerations

### Bandwidth Optimization
- Use `-bandwidth` flag to limit per-mapper bandwidth
- Schedule during off-peak hours
- Consider WAN accelerators for cross-region

### Parallelization
- Adjust `-m` (mapper count) based on cluster capacity
- Balance between parallelism and resource contention
- Monitor NameNode load during execution

### Chunking Strategies
- Break large migrations into smaller batches
- Database or table-level execution
- Time-based scheduling for continuous operations

## Error Handling and Recovery

### Common Issues

1. **Location Mismatch Errors**
   - Partition location doesn't match partition spec
   - Table directory name doesn't match table name
   - Solution: Use SQL-based migration or fix locations

2. **Network Failures**
   - Transient network issues
   - Solution: Use `-update` flag for incremental retry

3. **Permission Issues**
   - Source/target access permissions
   - Solution: Verify user permissions, use delegation tokens

### Recovery Procedures
```bash
# Resume failed distcp
hadoop distcp -update -skipcrccheck \
  -f $HCFS_BASE_DIR/failed_paths.txt \
  /target/location

# Verify and fix missing data
hadoop distcp -update \
  -f $HCFS_BASE_DIR/db_distcp_source.txt \
  /target/location
```

## Monitoring and Validation

### Progress Monitoring
- Monitor MapReduce job progress
- Track bandwidth utilization
- Watch for failed mappers

### Data Validation
```bash
# Count comparison
hdfs dfs -count /source/path
hdfs dfs -count /target/path

# Checksum validation
hadoop distcp -update -skipcrccheck \
  -f source_list.txt /target

# Size verification
hdfs dfs -du -s /source/path
hdfs dfs -du -s /target/path
```

## Best Practices

### Planning Phase
1. Analyze data volume and distribution
2. Test with small datasets first
3. Plan network capacity and windows
4. Prepare rollback procedures

### Execution Phase
1. Run database-level distcp in parallel where possible
2. Monitor progress continuously
3. Validate data immediately after copy
4. Keep detailed logs for troubleshooting

### Post-Migration
1. Verify row counts match
2. Run sample queries for validation
3. Update statistics on target tables
4. Document any discrepancies

## Integration with Warehouse Plans

When using warehouse plans with ALIGNED translation:
```yaml
transfer:
  warehousePlans:
    db_name:
      managedDirectory: "/warehouse/managed/db_name.db"
      externalDirectory: "/warehouse/external/db_name.db"
  storageMigration:
    translationType: ALIGNED
```

This enables:
- Precise location control
- Database-specific paths
- Managed/external separation
- Clean namespace organization

## Security Considerations

### Kerberos Integration
```bash
# Kinit before distcp
kinit -kt /etc/security/keytabs/hive.keytab hive/host@REALM

# Run distcp with delegation token
hadoop distcp \
  -Dmapreduce.job.hdfs-servers.token.renewal.exclude=source-cluster \
  -f source_list.txt /target
```

### Encryption
- Use `dfs.encrypt.data.transfer=true` for wire encryption
- Enable TLS for cross-region transfers
- Consider VPN for sensitive data

### Access Controls
- Verify source read permissions
- Ensure target write permissions
- Manage ranger/sentry policies appropriately

## Summary

DistCp integration in HMS-Mirror varies significantly by strategy:

| Strategy | DistCp Role | When Used | Generated Artifacts |
|----------|------------|-----------|-------------------|
| SCHEMA_ONLY | PRIMARY | Always | Scripts, sources, workbooks |
| LINKED | NONE | Never | None |
| SQL | NONE | Never* | None |
| EXPORT_IMPORT | OPTIONAL | Staging | Limited scripts |
| HYBRID | CONDITIONAL | Varies | Depends on sub-strategy |
| STORAGE_MIGRATION | PRIMARY | Always | Full suite |
| COMMON | NONE | Never | None |
| DUMP | INFORMATIONAL | Reference | Scripts only |
| CONVERT_LINKED | ASSUMED | Pre-completed | None |
| ICEBERG_CONVERSION | DEPENDS | Varies | Conditional |

*May generate for intermediate storage scenarios

The choice of strategy directly impacts whether and how distcp is utilized, with SCHEMA_ONLY and STORAGE_MIGRATION being the primary strategies leveraging distcp for large-scale data movement.