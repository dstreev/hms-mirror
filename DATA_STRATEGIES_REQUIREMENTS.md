# HMS-Mirror Data Strategies Requirements Document

## Executive Summary

HMS-Mirror provides multiple data migration strategies to transfer Hive metadata and data between different Hadoop/Hive environments. Each strategy addresses specific use cases and has distinct requirements, capabilities, and limitations. This document outlines the comprehensive requirements for each data strategy supported by HMS-Mirror.

## Core Data Strategy Framework

### Base Requirements (All Strategies)

1. **Connection Requirements**
   - Source cluster (LEFT) Hive Metastore connection
   - Target cluster (RIGHT) Hive Metastore connection (except for DUMP strategy)
   - Optional HiveServer2 connections for SQL execution
   - Database connection pooling (DBCP2, Hikari, or Hybrid)

2. **Configuration Requirements**
   - YAML configuration file defining cluster connections
   - Database connection parameters (URL, username, password)
   - Optional encrypted password support
   - Namespace configuration for data locations

3. **Java Runtime**
   - Minimum Java 17
   - Memory settings: Default -Xms4096m -Xmx8192m

## Strategy-Specific Requirements

### 1. SCHEMA_ONLY Strategy

**Purpose**: Transfers table metadata only, updating locations to target namespace

**Requirements:**
- Source and target cluster connectivity
- Target namespace configuration
- External process for data migration (e.g., distcp)

**Capabilities:**
- Schema replication without data movement
- Location translation to target namespace
- Support for managed-to-external table conversion
- Ownership management (PURGE option)
- Sync mode for schema consistency

**Limitations:**
- No data transfer (requires external process)
- Cannot migrate ACID tables directly
- Requires manual data movement coordination

**Use Cases:**
- Large-scale migrations where distcp is preferred
- Schema-only replication for testing
- Migrations requiring parallel data movement

### 2. LINKED Strategy

**Purpose**: Creates tables on target cluster pointing to source cluster storage

**Requirements:**
- Shared storage accessibility between clusters
- Network connectivity for cross-cluster storage access
- No ACID table support

**Capabilities:**
- Zero data movement
- Immediate access to data from target cluster
- Testing Hive functionality on target cluster
- Schema synchronization

**Limitations:**
- Cannot link ACID tables
- Requires persistent storage accessibility
- No data ownership transfer
- Performance depends on cross-cluster network

**Use Cases:**
- Testing new Hive versions
- Temporary cluster migrations
- Read-only access from secondary clusters

### 3. SQL Strategy

**Purpose**: Uses SQL INSERT statements to migrate data between clusters

**Requirements:**
- JDBC connectivity to both clusters
- Sufficient compute resources for SQL execution
- Intermediate storage for ACID tables
- Shadow table creation capability

**Capabilities:**
- Direct data transfer via SQL
- Support for non-ACID tables
- ACID table migration via intermediate storage
- Partition-aware data movement
- Data transformation during migration

**Limitations:**
- Performance limited by SQL engine
- High resource consumption for large tables
- Requires compute cluster availability
- May timeout on very large datasets

**Use Cases:**
- Small to medium dataset migrations
- Migrations requiring data transformation
- Selective data migration with filters

### 4. EXPORT_IMPORT Strategy

**Purpose**: Uses Hive EXPORT/IMPORT commands for data transfer

**Requirements:**
- Shared storage location for export data
- EXPORT/IMPORT command support in Hive versions
- Sufficient storage for temporary export files
- Both clusters must access export location

**Capabilities:**
- Preserves table metadata completely
- Handles partitioned tables efficiently
- Maintains statistics and properties
- Supports incremental migrations

**Limitations:**
- Requires intermediate storage space
- Not suitable for ACID tables in all versions
- May have version compatibility issues
- Slower for very large tables

**Use Cases:**
- Cross-version Hive migrations
- Migrations preserving all metadata
- Batch processing scenarios

### 5. HYBRID Strategy

**Purpose**: Automatically selects between SQL and EXPORT_IMPORT based on table characteristics

**Requirements:**
- All requirements from SQL and EXPORT_IMPORT strategies
- Configuration of selection criteria
- Partition count limits configuration

**Capabilities:**
- Intelligent strategy selection
- Optimized for mixed workloads
- Automatic ACID table handling
- Configurable thresholds

**Decision Criteria:**
- Partition count threshold (default: exportImportPartitionLimit)
- ACID table detection
- Table size considerations
- Resource availability

**Use Cases:**
- Mixed workload migrations
- Automated large-scale migrations
- Environments with varied table types

### 6. STORAGE_MIGRATION Strategy

**Purpose**: Migrates tables within same cluster to new storage locations

**Requirements:**
- Single cluster operation (LEFT or RIGHT with --flip)
- Target namespace configuration
- Database location properties modification
- Warehouse plan definitions

**Capabilities:**
- In-place storage migration
- Database-level location updates
- Managed and external location separation
- Support for multiple migration techniques

**Limitations:**
- Single cluster operation only
- Cannot migrate cross-cluster
- Requires careful planning for ACID tables
- May require significant temporary storage

**Use Cases:**
- Storage system migrations
- Namespace reorganization
- Compliance-driven data movement
- Storage tiering implementation

### 7. COMMON Strategy

**Purpose**: Shared storage between clusters without data migration

**Requirements:**
- Shared storage system accessible by both clusters
- Identical storage namespace configuration
- Compatible file system protocols

**Capabilities:**
- No data movement required
- Immediate availability on both clusters
- Maintains single data copy
- Preserves all table properties

**Limitations:**
- Requires shared storage infrastructure
- Potential for conflicts in concurrent access
- Limited to compatible cluster versions

**Use Cases:**
- Active-active cluster configurations
- Disaster recovery setups
- Development/production sharing

### 8. DUMP Strategy

**Purpose**: Generates migration scripts without execution

**Requirements:**
- Source cluster connectivity only
- Output directory for scripts
- No target cluster needed

**Capabilities:**
- Script generation for review
- Supports all table types
- Customizable script output
- Pre-migration validation

**Limitations:**
- No actual migration performed
- Scripts require manual execution
- No validation against target cluster

**Use Cases:**
- Migration planning
- Change review processes
- Compliance documentation

### 9. CONVERT_LINKED Strategy

**Purpose**: Converts previously LINKED tables to standalone tables

**Requirements:**
- Existing LINKED tables on target
- Data already migrated via external process
- Namespace translation configuration

**Capabilities:**
- In-place schema conversion
- Location namespace updates
- Ownership transfer
- Purge flag management

**Limitations:**
- Only works on previously LINKED tables
- Requires completed data migration
- No data validation

**Use Cases:**
- Completing LINKED migrations
- Cluster independence establishment
- Storage ownership transfer

### 10. ICEBERG_CONVERSION Strategy

**Purpose**: Converts Hive tables to Iceberg format

**Requirements:**
- Iceberg runtime support
- Compatible table formats
- Metadata catalog configuration
- Spark or similar engine for conversion

**Capabilities:**
- Format conversion to Iceberg
- Schema evolution support
- Snapshot isolation
- Time travel capabilities

**Limitations:**
- Limited to compatible table types
- Requires Iceberg infrastructure
- May require table rebuilding

**Use Cases:**
- Modern table format adoption
- ACID capability enhancement
- Performance optimization

## Hidden/Internal Strategies

### ACID Strategy
- Applied automatically to ACID tables
- Handles special ACID table requirements
- Manages transaction metadata

### INTERMEDIATE Strategy
- Internal strategy for multi-step migrations
- Manages temporary/shadow tables
- Coordinates complex migrations

### SQL_ACID_DOWNGRADE_INPLACE Strategy
- Downgrades ACID tables to external tables
- In-place conversion process
- Preserves data while changing table type

### EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE Strategy
- ACID downgrade using EXPORT/IMPORT
- Manages complex downgrade scenarios

### HYBRID_ACID_DOWNGRADE_INPLACE Strategy
- Intelligent selection for ACID downgrade
- Combines SQL and EXPORT_IMPORT approaches

## Configuration Requirements

### Global Configuration
```yaml
# Cluster definitions
clusters:
  LEFT:
    hiveServer2: # HiveServer2 connection
    metastoreUris: # Metastore thrift URI
  RIGHT:
    hiveServer2: # HiveServer2 connection
    metastoreUris: # Metastore thrift URI

# Data strategy selection
dataStrategy: SCHEMA_ONLY|LINKED|SQL|EXPORT_IMPORT|HYBRID|STORAGE_MIGRATION|COMMON|DUMP|CONVERT_LINKED|ICEBERG_CONVERSION

# Transfer configuration
transfer:
  targetNamespace: # Target storage namespace
  intermediatStorage: # Temporary storage location
  commonStorage: # Shared storage location
  shadowPrefix: # Prefix for shadow tables

# Feature flags
execute: false # Dry-run by default
sync: false # Synchronization mode
readOnly: false # Read-only mode
```

### Strategy-Specific Configuration

#### HYBRID Strategy
```yaml
hybrid:
  exportImportPartitionLimit: 100 # Threshold for strategy selection
  sqlSizeLimit: # Size threshold for SQL strategy
```

#### STORAGE_MIGRATION Strategy
```yaml
transfer:
  targetNamespace: # Required: New storage namespace
  storageMigration: # Migration type selection
  warehousePlans: # Database location mappings
```

#### ICEBERG_CONVERSION Strategy
```yaml
icebergConversion:
  catalogs: # Iceberg catalog configurations
  tableFormats: # Supported format conversions
```

## Performance Considerations

### Strategy Performance Characteristics

| Strategy | Speed | Resource Usage | Network Load | Storage Requirements |
|----------|-------|---------------|--------------|---------------------|
| SCHEMA_ONLY | Fast | Low | Low | External |
| LINKED | Fast | Low | Medium | None |
| SQL | Medium | High | High | Low |
| EXPORT_IMPORT | Slow | Medium | Medium | High |
| HYBRID | Variable | Variable | Variable | Variable |
| STORAGE_MIGRATION | Medium | Medium | Low | High |
| COMMON | Fast | Low | Low | Shared |

## Selection Guidelines

### Decision Matrix

1. **Data Size**
   - Small (<1TB): SQL, EXPORT_IMPORT
   - Medium (1-10TB): HYBRID, SQL with optimization
   - Large (>10TB): SCHEMA_ONLY with distcp

2. **Table Characteristics**
   - ACID tables: INTERMEDIATE, specialized strategies
   - Partitioned tables: Consider partition count for strategy selection
   - External tables: All strategies supported

3. **Infrastructure**
   - Shared storage: LINKED, COMMON
   - Limited network: SCHEMA_ONLY with offline transfer
   - Limited compute: EXPORT_IMPORT, SCHEMA_ONLY

4. **Requirements**
   - Zero downtime: LINKED, COMMON
   - Data validation: SQL, EXPORT_IMPORT
   - Minimal storage: SQL, LINKED

## Validation and Testing Requirements

### Pre-Migration Validation
- Connectivity verification
- Permission validation
- Storage availability checks
- Version compatibility verification

### Post-Migration Validation
- Row count verification
- Schema comparison
- Data sampling
- Performance testing

## Security Requirements

### Authentication
- Kerberos support for secure clusters
- Username/password authentication
- Encrypted password storage
- Certificate-based authentication

### Authorization
- Table-level permissions
- Database-level permissions
- Storage permissions
- Metastore access controls

### Data Protection
- Encryption in transit
- Encryption at rest
- Audit logging
- Sensitive data handling

## Monitoring and Logging

### Metrics
- Table migration progress
- Data transfer rates
- Error rates
- Resource utilization

### Logging
- Detailed operation logs
- Error logs with stack traces
- SQL statement logging
- Performance metrics

## Error Handling and Recovery

### Error Categories
1. **Connection Errors**: Retry with backoff
2. **Permission Errors**: Fail fast with clear messaging
3. **Data Errors**: Table-level isolation
4. **Resource Errors**: Throttling and queuing

### Recovery Mechanisms
- Checkpoint-based recovery
- Table-level retry
- Transaction rollback
- Manual intervention points

## Conclusion

HMS-Mirror's data strategies provide comprehensive options for various migration scenarios. Strategy selection should consider:
- Data characteristics
- Infrastructure capabilities
- Business requirements
- Performance constraints
- Resource availability

Each strategy has specific strengths and optimal use cases. Understanding these requirements ensures successful migration planning and execution.