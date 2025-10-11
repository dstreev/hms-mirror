# Strategy Selection Guide

## Move schemas + data between clusters

### Clusters can access each other's storage
- Mix of small/large partitioned tables
	- Use HYBRID
		Note: Auto-selects best method per table based on partition count. Supports Iceberg conversion.
- Mostly < 100 partitions per table
	- Use EXPORT_IMPORT
		Note: Good for small partitioned tables. Supports Iceberg conversion.
- Mostly > 100 partitions per table
	- Use SQL
		Note: Better for large partitioned tables. Supports Iceberg conversion.

### Clusters cannot access storage but have intermediate storage
- Use SQL with intermediateStorage
	Note: Shared storage location for transit. Supports Iceberg conversion.
- Use EXPORT_IMPORT with intermediateStorage
	Note: Alternative method with intermediate storage. Supports Iceberg conversion.

## Move schemas only, handle data with distcp

### Use SCHEMA_ONLY
Note: Generates distcp plans. Move data separately.

## Convert to Iceberg format

### Use SQL, or STORAGE_MIGRATION (with SQL)
Note: Enable icebergConversion settings. Requires compatible CDP version. Target cluster must support Iceberg.

## Move data within cluster to new storage

### Use STORAGE_MIGRATION
Note: Choose SQL or DISTCP for data movement. HDFS→Ozone, HDFS→S3, etc. SQL option supports Iceberg conversion.

## Test new cluster with old data (read-only)

### Use LINKED
Note: FOR TESTING ONLY. RIGHT points to LEFT data. Must use readOnly=true and noPurge=true.

## Clusters share same physical storage

### Use COMMON
Note: Only metadata moves. No data movement needed.

## Extract schemas only (no target yet)

### Use DUMP
Note: No RIGHT cluster needed. Generates SQL files for manual replay.
