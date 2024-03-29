# HYBRID

This data strategy will use a combination of EXPORT_IMPORT and SQL to move data between clusters.  

This strategy will select either [SQL](hms-mirror-sql.md) or [EXPORT_IMPORT](hms-mirror-export-import.md) based on the table type, and table partition counts.

The initial check will attempt to use [EXPORT_IMPORT](hms-mirror-export-import.md).  If the table is ACID, or the partition count exceeds the limit (`-ep`), the [SQL](hms-mirror-sql.md) strategy will be used.

## Interesting Options

When the cluster don't have direct line of sight to each other and can NOT be [linked](Linking-Cluster-Storage-Layers.md), you can use options like `-cs` or `-is` to bridge the gap.

