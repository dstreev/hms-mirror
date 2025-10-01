# HMS-Mirror User Scenarios and Use Cases

## Executive Summary

HMS-Mirror addresses a wide range of real-world Hive migration and modernization challenges. This document presents practical user scenarios organized by business objectives, showing how HMS-Mirror's sophisticated rule system solves complex data platform challenges.

---

## 1. Platform Migration Scenarios

### Use Case 1.1: Legacy HDP to CDP Migration
**Scenario**: "We're upgrading from HDP 2.6 (Hive 1) to CDP 7.1 (Hive 3) and need to migrate 10,000 tables including 500 ACID tables."

**Challenges**:
- ACID v1 to v2 incompatibility
- Managed table behavior changes
- Artificial bucket removal needed
- Legacy permissions preservation

**HMS-Mirror Solution**:
```yaml
dataStrategy: HYBRID
migrateACID:
  on: true
  artificialBucketThreshold: 2
  partitionLimit: 500
clusters:
  LEFT:
    legacyHive: true
  RIGHT:
    hdpHive3: false
```

**Workflow**:
- ACID tables automatically routed through intermediate tables
- Artificial buckets removed during migration
- Managed tables converted to external with PURGE
- Hybrid strategy optimizes based on partition count

### Use Case 1.2: On-Premise to Cloud Migration
**Scenario**: "Moving our 50TB Hive warehouse from on-premise HDFS cluster to AWS S3-backed EMR."

**Challenges**:
- Network bandwidth limitations
- Storage path restructuring (HDFS → S3)
- Cost optimization for cloud storage
- Incremental migration requirement

**HMS-Mirror Solution**:
```yaml
dataStrategy: SCHEMA_ONLY
transfer:
  targetNamespace: "s3a://company-datalake"
  storageMigration:
    dataMovementStrategy: DISTCP
    dataFlow: PUSH
    consolidateTablesForDistcp: true
  warehousePlans:
    sales_db:
      externalDirectory: "/data/external"
      managedDirectory: "/data/managed"
```

**Workflow**:
- Schema migrated first, data follows via distcp
- Warehouse Plans reorganize for S3 efficiency
- Consolidated distcp reduces job overhead
- Incremental updates supported

### Use Case 1.3: Cross-Cloud Migration
**Scenario**: "Migrating from Azure HDInsight (WASB storage) to Google Dataproc (GCS storage)."

**Challenges**:
- Different cloud storage protocols
- Cross-cloud data transfer costs
- Metadata translation complexity
- Different Hive versions

**HMS-Mirror Solution**:
```yaml
dataStrategy: SQL
transfer:
  intermediateStorage: "wasb://shared-container@account"
translator:
  globalLocationMap:
    - from: "wasb://source"
      to: "gs://target-bucket"
      tableType: EXTERNAL
```

**Workflow**:
- Intermediate storage bridges clouds
- SQL strategy handles protocol differences
- GLM manages storage translations
- Transfer tables facilitate data movement

---

## 2. Storage Modernization Scenarios

### Use Case 2.1: HDFS to Object Storage Migration
**Scenario**: "Converting scattered HDFS locations to organized Ozone buckets while keeping the cluster operational."

**Challenges**:
- Tables scattered across 50+ directories
- Need zero-downtime migration
- Maintain data locality optimization
- Preserve partition structures

**HMS-Mirror Solution**:
```yaml
dataStrategy: STORAGE_MIGRATION
transfer:
  targetNamespace: "ofs://ozone-service"
  storageMigration:
    dataMovementStrategy: DISTCP
    translationType: ALIGNED
    consolidateTablesForDistcp: false
    createArchive: true
  warehousePlans:
    analytics_db:
      externalDirectory: "/warehouse/external"
      managedDirectory: "/warehouse/managed"
```

**Workflow**:
- Archive tables preserve access during migration
- Aligned translation maintains partition structure
- Warehouse Plans consolidate scattered locations
- Distcp handles physical data movement

### Use Case 2.2: Storage Tiering Implementation
**Scenario**: "Implement hot/warm/cold storage tiers based on table access patterns."

**Challenges**:
- Identify and classify 5000+ tables
- Different storage types (SSD/HDD/Archive)
- Maintain query performance
- Automate tier transitions

**HMS-Mirror Solution**:
```yaml
# Hot tier configuration
translator:
  globalLocationMap:
    - from: "/data/frequently_accessed"
      to: "hdfs://ssd-storage/hot"
      tableType: EXTERNAL
    
# Warm tier
    - from: "/data/monthly_access"
      to: "hdfs://hdd-storage/warm"
      tableType: EXTERNAL

# Cold tier  
    - from: "/data/archive"
      to: "s3a://glacier/cold"
      tableType: EXTERNAL
```

**Workflow**:
- Tables classified by access patterns
- GLM routes to appropriate storage tier
- Non-standard locations handled individually
- Maintains transparent access

---

## 3. ACID Table Management Scenarios

### Use Case 3.1: ACID Table Downgrade
**Scenario**: "Need to convert 200 ACID tables to External tables for compatibility with Spark processing."

**Challenges**:
- ACID format incompatibility with Spark
- Preserve data during conversion
- Maintain transaction history
- Minimize downtime

**HMS-Mirror Solution**:
```yaml
dataStrategy: STORAGE_MIGRATION
migrateACID:
  on: true
  downgrade: true
  inplace: false
transfer:
  transferPrefix: "migration_"
```

**Workflow**:
- Transfer tables created as EXTERNAL
- SQL extracts data from ACID format
- Original tables archived
- Atomic switch to new tables

### Use Case 3.2: ACID Version Upgrade
**Scenario**: "Upgrading ACID v1 tables (Hive 1.2) to ACID v2 (Hive 3.1) with 100TB of data."

**Challenges**:
- Incompatible ACID formats
- Remove artificial buckets
- Handle large partition counts
- Preserve transaction semantics

**HMS-Mirror Solution**:
```yaml
dataStrategy: SQL
migrateACID:
  on: true
  artificialBucketThreshold: 4
  partitionLimit: 1000
transfer:
  intermediateStorage: "hdfs://staging/migration"
```

**Workflow**:
- Intermediate tables bridge versions
- Buckets removed if ≤4
- Partitions processed in batches
- Transaction properties preserved

### Use Case 3.3: In-Place ACID Downgrade
**Scenario**: "Convert ACID tables to External within same cluster for tool compatibility."

**Challenges**:
- Same cluster operation
- Preserve data locations
- Atomic table replacement
- Rollback capability

**HMS-Mirror Solution**:
```yaml
dataStrategy: SQL_ACID_DOWNGRADE_INPLACE
migrateACID:
  on: true
  only: true
  downgrade: true
  inplace: true
```

**Workflow**:
- Creates temporary external tables
- Transfers data via SQL
- Drops ACID, renames external
- Single cluster operation

---

## 4. Development and Testing Scenarios

### Use Case 4.1: Production to Dev Environment Sync
**Scenario**: "Create a development environment with production schema but subset of data."

**Challenges**:
- Selective table replication
- Data sampling requirements
- Sensitive data masking
- Regular sync updates

**HMS-Mirror Solution**:
```yaml
dataStrategy: LINKED
filter:
  tableLike: "test_%"
  tableExclude: ".*_sensitive$"
readOnly: true
sync: true
```

**Workflow**:
- LINKED strategy provides instant access
- No data copy, just metadata
- Read-only prevents accidents
- Sync keeps schemas current

### Use Case 4.2: Disaster Recovery Setup
**Scenario**: "Maintain DR cluster with synchronized metadata and replicated data."

**Challenges**:
- Cross-datacenter replication
- RPO/RTO requirements
- Incremental updates
- Failover readiness

**HMS-Mirror Solution**:
```yaml
dataStrategy: COMMON
transfer:
  commonStorage: "hdfs://shared-dr-storage"
sync: true
execute: false  # Dry-run first
```

**Workflow**:
- Common storage between sites
- Metadata kept in sync
- No data movement required
- Instant failover capability

### Use Case 4.3: Testing Hive Version Upgrade
**Scenario**: "Test Hive 3 features without affecting production Hive 2 environment."

**Challenges**:
- Version compatibility testing
- Feature validation
- Performance comparison
- Rollback safety

**HMS-Mirror Solution**:
```yaml
dataStrategy: LINKED
clusters:
  LEFT:
    legacyHive: true
  RIGHT:
    hdpHive3: true
filter:
  databaseOnly: false
  tableLimit: 100  # Test subset
```

**Workflow**:
- LINKED tables for testing
- No production impact
- Version differences handled
- Easy cleanup

---

## 5. Compliance and Governance Scenarios

### Use Case 5.1: Data Localization for GDPR
**Scenario**: "Separate EU customer data into region-specific storage for GDPR compliance."

**Challenges**:
- Identify EU-related tables
- Regional storage requirements
- Maintain query transparency
- Audit trail needs

**HMS-Mirror Solution**:
```yaml
dataStrategy: STORAGE_MIGRATION
translator:
  globalLocationMap:
    - from: "/data/customers"
      to: "s3a://eu-west-1-bucket/gdpr"
      tableType: EXTERNAL
filter:
  tableLike: ".*_eu_.*"
transfer:
  targetNamespace: "s3a://eu-compliant-storage"
```

**Workflow**:
- Tables filtered by region
- GLM enforces location rules
- Maintains accessibility
- Clear audit trail

### Use Case 5.2: Sensitive Data Isolation
**Scenario**: "Move PII-containing tables to encrypted storage with restricted access."

**Challenges**:
- Identify PII tables
- Encryption requirements
- Access control migration
- Performance impact

**HMS-Mirror Solution**:
```yaml
dataStrategy: SQL
filter:
  tableLike: ".*_pii$|.*_sensitive$"
transfer:
  targetNamespace: "hdfs://encrypted-zone"
  shadowPrefix: "secure_"
translator:
  globalLocationMap:
    - from: "/standard/warehouse"
      to: "/encrypted/warehouse"
      tableType: MANAGED
```

**Workflow**:
- PII tables identified by pattern
- SQL strategy ensures data rewrite
- Shadow tables for validation
- Encrypted zone enforcement

---

## 6. Performance Optimization Scenarios

### Use Case 6.1: Partition Pruning Optimization
**Scenario**: "Reorganize 50,000 partition table with non-standard locations for better query performance."

**Challenges**:
- Scattered partition locations
- Query performance degradation
- Metadata overhead
- Storage inefficiency

**HMS-Mirror Solution**:
```yaml
dataStrategy: STORAGE_MIGRATION
transfer:
  storageMigration:
    translationType: ALIGNED
    consolidateTablesForDistcp: false
  warehousePlans:
    analytics_db:
      externalDirectory: "/optimized/external"
      managedDirectory: "/optimized/managed"
optimization:
  sortDynamicPartitions: true
  skipStatsCollection: false
```

**Workflow**:
- Aligned translation preserves structure
- Consolidates scattered partitions
- Sorted for better pruning
- Stats rebuilt for optimizer

### Use Case 6.2: Table Format Modernization
**Scenario**: "Convert legacy text tables to modern columnar formats (ORC/Parquet) for 10x query improvement."

**Challenges**:
- Format conversion at scale
- Storage space during conversion
- Query compatibility
- Rollback planning

**HMS-Mirror Solution**:
```yaml
dataStrategy: SQL
transfer:
  intermediateStorage: "hdfs://conversion-staging"
# Custom properties for format change
clusters:
  RIGHT:
    createIfNotExists: true
    # Force ORC format in new cluster config
```

**Workflow**:
- SQL rewrites data in new format
- Intermediate storage for safety
- Parallel conversion possible
- Original preserved until validated

### Use Case 6.3: Iceberg Table Adoption
**Scenario**: "Convert high-churn dimension tables to Iceberg format for time-travel and ACID without Hive ACID overhead."

**Challenges**:
- Format conversion complexity
- Metadata catalog setup
- Query engine compatibility
- Historical data preservation

**HMS-Mirror Solution**:
```yaml
dataStrategy: ICEBERG_CONVERSION
icebergConversion:
  enabled: true
  catalog: "hadoop_catalog"
  targetFormats:
    - from: "HIVE_ACID"
      to: "ICEBERG"
    - from: "EXTERNAL"
      to: "ICEBERG"
```

**Workflow**:
- Identifies conversion candidates
- Creates Iceberg metadata
- Preserves table history
- Enables time-travel queries

---

## 7. Large-Scale Migration Scenarios

### Use Case 7.1: Petabyte-Scale Migration
**Scenario**: "Migrate 2PB warehouse with 50,000 tables from old Hadoop cluster to new infrastructure."

**Challenges**:
- Scale of operation
- Network bandwidth limits
- Minimal downtime window
- Progress tracking

**HMS-Mirror Solution**:
```yaml
dataStrategy: SCHEMA_ONLY
transfer:
  storageMigration:
    dataMovementStrategy: DISTCP
    dataFlow: PULL
    consolidateTablesForDistcp: true
filter:
  databaseLimit: 10  # Batch processing
execute: false  # Planning phase
```

**Workflow**:
- Batch processing by database
- Schema-first approach
- Consolidated distcp for efficiency
- Parallel execution plans
- Progress tracking via reports

### Use Case 7.2: Multi-Tenant Migration
**Scenario**: "Migrate 500 tenant databases with different schemas, sizes, and SLAs."

**Challenges**:
- Tenant isolation requirements
- Different migration priorities
- Resource allocation
- SLA compliance

**HMS-Mirror Solution**:
```yaml
# Per-tenant configuration
dataStrategy: HYBRID
hybrid:
  exportImportPartitionLimit: 100
filter:
  database: "tenant_${id}_db"
transfer:
  warehousePlans:
    tenant_001_db:
      externalDirectory: "/tenant001/external"
      managedDirectory: "/tenant001/managed"
```

**Workflow**:
- Per-tenant warehouse plans
- Priority-based scheduling
- Hybrid optimizes per table
- Isolation maintained

---

## 8. Special Integration Scenarios

### Use Case 8.1: Hive to Spark SQL Migration
**Scenario**: "Enable Spark SQL processing on Hive tables without disrupting existing Hive workloads."

**Challenges**:
- ACID incompatibility
- Metadata synchronization
- Performance optimization
- Concurrent access

**HMS-Mirror Solution**:
```yaml
dataStrategy: LINKED
migrateACID:
  on: true
  downgrade: true  # For Spark compatibility
readOnly: true
filter:
  tableExclude: ".*_temp$|.*_staging$"
```

**Workflow**:
- ACID tables downgraded for Spark
- LINKED provides immediate access
- Read-only prevents conflicts
- Selective table exposure

### Use Case 8.2: Real-Time to Batch Integration
**Scenario**: "Integrate Kafka-based real-time tables with batch processing warehouse."

**Challenges**:
- Streaming table compatibility
- Compaction requirements
- Late-arriving data
- Schema evolution

**HMS-Mirror Solution**:
```yaml
dataStrategy: SQL
transfer:
  intermediateStorage: "hdfs://staging/streaming"
optimization:
  compactSmallFiles: true
  sortDynamicPartitions: true
```

**Workflow**:
- Streaming tables compacted
- SQL merges small files
- Sorted for batch efficiency
- Schema evolution handled

---

## 9. Recovery and Rollback Scenarios

### Use Case 9.1: Failed Migration Recovery
**Scenario**: "Recover from partial migration failure after 30% completion."

**Challenges**:
- Identify completed tables
- Clean up partial transfers
- Resume from checkpoint
- Maintain consistency

**HMS-Mirror Solution**:
```yaml
# Recovery configuration
dataStrategy: SQL
saveWorkingTables: true  # Preserve for inspection
filter:
  tableExclude: ".*_transfer$"  # Skip completed
execute: false  # Dry run first
```

**Workflow**:
- Transfer tables indicate progress
- Exclude completed tables
- Dry run validates state
- Resume from failure point

### Use Case 9.2: Rollback After Migration
**Scenario**: "Rollback to original cluster after discovering issues in new environment."

**Challenges**:
- Preserve original state
- Data consistency
- Minimal downtime
- Clear rollback path

**HMS-Mirror Solution**:
```yaml
dataStrategy: STORAGE_MIGRATION
transfer:
  storageMigration:
    createArchive: true  # Preserve originals
  transferPrefix: "rollback_"
```

**Workflow**:
- Archive tables preserve originals
- Clear naming for tracking
- Simple rename for rollback
- Data remains accessible

---

## 10. Cost Optimization Scenarios

### Use Case 10.1: Cloud Storage Cost Reduction
**Scenario**: "Reduce S3 storage costs by 40% through intelligent data placement and lifecycle policies."

**Challenges**:
- Identify cold data
- Storage class selection
- Lifecycle automation
- Query performance balance

**HMS-Mirror Solution**:
```yaml
translator:
  globalLocationMap:
    # Frequent access - S3 Standard
    - from: "/hot/data"
      to: "s3a://bucket/standard"
    # Infrequent - S3 IA
    - from: "/warm/data"
      to: "s3a://bucket/infrequent-access"
    # Archive - Glacier
    - from: "/cold/data"
      to: "s3a://bucket/glacier"
```

**Workflow**:
- Tables classified by access
- GLM routes to storage classes
- Automated lifecycle policies
- Transparent to queries

### Use Case 10.2: Compute Resource Optimization
**Scenario**: "Optimize cluster resources by separating compute-intensive and storage-intensive workloads."

**Challenges**:
- Workload characterization
- Resource allocation
- Queue management
- Cost attribution

**HMS-Mirror Solution**:
```yaml
dataStrategy: COMMON
transfer:
  commonStorage: "s3a://shared-storage"
clusters:
  LEFT:  # Storage cluster
    hiveServer2: "jdbc:hive2://storage-cluster:10000"
  RIGHT: # Compute cluster
    hiveServer2: "jdbc:hive2://compute-cluster:10000"
```

**Workflow**:
- Common storage shared
- Compute/storage separated
- Resource optimization
- Cost tracking simplified

---

## Summary of Capabilities

HMS-Mirror addresses these user scenarios through:

### **Migration Capabilities**
- Cross-version compatibility (Hive 1 → 3)
- Cross-platform support (HDP → CDP)
- Cross-cloud migrations (AWS → Azure → GCP)
- Scale handling (TB to PB)

### **Data Strategy Flexibility**
- ACID table handling (upgrade/downgrade)
- Format conversions (Text → ORC/Parquet → Iceberg)
- Storage reorganization (HDFS → Object Storage)
- Incremental migrations

### **Operational Excellence**
- Zero-downtime options (LINKED, COMMON)
- Rollback capabilities
- Progress tracking
- Error recovery

### **Optimization Features**
- Storage consolidation via Warehouse Plans
- Partition pruning improvements
- DistCp consolidation
- Cost-based routing via GLM

### **Compliance Support**
- Data localization
- Encryption zone support
- Audit trail maintenance
- Access control preservation

These scenarios demonstrate HMS-Mirror's comprehensive approach to solving real-world Hive migration and modernization challenges, providing users with flexible, reliable, and efficient solutions for their specific needs.