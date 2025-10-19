Successfully Translated Fields

  HmsMirrorConfig → ConfigLiteDto:
  - comment
  - migrateNonNative
  - copyAvroSchemaUrls
  - saveWorkingTables
  - createIfNotExists (from Cluster)
  - enableAutoTableStats (from Cluster)
  - enableAutoColumnStats (from Cluster)
  - icebergConversion
  - migrateACID
  - migrateVIEW
  - optimization
  - transfer
  - ownershipTransfer

  HmsMirrorConfig → DatasetDto:
  - databases (list of database names)
  - filter (table-level filters: tblRegEx, tblExcludeRegEx, tblSizeLimit, tblPartitionLimit)
  - dbPrefix
  - dbRename
  - transfer.warehouse

  HmsMirrorConfig → ConnectionDto (LEFT and RIGHT):
  - clusters[Environment] → name, description
  - platformType
  - hcfsNamespace
  - hiveServer2 (uri, username, password, connectionProperties)
  - metastoreDirect (enabled, uri, type, username, password, minConnections, maxConnections)
  - partitionDiscovery (auto, initMSCK)
  - createIfNotExists
  - enableAutoTableStats
  - enableAutoColumnStats

  HmsMirrorConfig → JobDto:
  - dataStrategy
  - hybrid
  - databaseOnly
  - readOnly (converted to disasterRecovery flag)
  - noPurge (converted to disasterRecovery flag)
  - sync

  Untranslated Fields

  Top-Level HmsMirrorConfig Fields:
  1. acceptance - Interactive acceptance configuration (not needed for automated jobs)
  2. beta - Beta features flag
  3. commandLineOptions - CLI options string (runtime-only)
  4. configFilename - Config file path (JsonIgnore, runtime-only)
  5. connectionPoolLib - Connection pool type (DBCP2, Hikari, Hybrid)
  6. dumpTestData - Testing/debugging flag
  7. loadTestDataFile - Test data file path for loading
  8. filter.dbRegEx - Database regex filter (only table filters were migrated)
  9. skipLinkCheck - Link validation bypass flag
  10. legacyTranslations - Legacy translation configuration
  11. dumpSource - Source environment for dumps (LEFT/RIGHT)
  12. execute - Execution flag (runtime decision)
  13. flip - Deprecated cluster flip flag
  14. outputDirectory - Report output directory (JsonIgnore, runtime-only)
  15. userSetOutputDirectory - User override flag (JsonIgnore)
  16. finalOutputDirectory - Final output path (JsonIgnore)
  17. encryptedPasswords - Password encryption flag
  18. passwordKey - Decryption key (JsonIgnore, security-sensitive)
  19. quiet - Quiet mode flag (runtime-only)
  20. replace - Replace table flag (not implemented)
  21. resetRight - Reset right database flag (destructive operation)
  22. skipFeatures - Skip features flag
  23. skipLegacyTranslation - Skip translation flag
  24. sqlOutput - SQL output flag (deprecated, always true)
  25. suppressCliWarnings - CLI warnings suppression flag
  26. translator - Translator configuration with warehouse map builder
  27. runMarker - Run timestamp marker (JsonIgnore, runtime-only)
  28. flags - CLI flags list (JsonIgnore, runtime-only)
  29. supportFileSystems - Supported filesystems list (JsonIgnore, runtime-only)
  30. initDate - Initialization date (JsonIgnore, runtime-only)

  Cluster-Level Fields (not in ConnectionDto):
  1. environment - Environment enum (LEFT/RIGHT) - implicitly used for naming
  2. legacyHive - Legacy Hive 1/2 flag (deprecated)
  3. hdpHive3 - HDP Hive 3 specific flag (deprecated)
  4. envVars - Environment variables map (JsonIgnore, runtime-only)
  5. initialized - Initialization flag (JsonIgnore, runtime-only)
  6. environmentWarehouse - Warehouse object (JsonIgnore, runtime-only)

  TransferConfig Fields (not in ConfigLiteDto):
  1. transferPrefix - Prefix for transfer tables
  2. shadowPrefix - Prefix for shadow tables
  3. storageMigrationPostfix - Postfix for storage migration
  4. exportBaseDirPrefix - Export base directory prefix
  5. remoteWorkingDirectory - Remote working directory
  6. intermediateStorage - Intermediate storage location
  7. targetNamespace - Target namespace for translations
  8. storageMigration - Storage migration configuration object

  HiveServer2Config Fields (not fully captured):
  1. disconnected - Disconnected mode flag
  2. driverClassName - JDBC driver class name
  3. jarFile - JDBC driver JAR file path
  4. version - Hive version string

  Reasoning for Untranslated Fields:

  Most untranslated fields fall into these categories:
  1. Runtime-only fields (JsonIgnore): Not persisted, only used during execution
  2. CLI-specific fields: Command-line interface options not relevant to web-based jobs
  3. Testing/debugging fields: Used for development, not production jobs
  4. Deprecated fields: Legacy compatibility fields being phased out
  5. Security-sensitive fields: Encryption keys that shouldn't be copied
  6. Infrastructure configuration: Connection pooling, output directories handled separately
  7. Complex nested objects: Translator, LegacyTranslations require separate handling
  8. Not implemented features: Like replace flag

  Usage Example:

  HmsMirrorConfig config = loadConfig();

  HmsMirrorConfigConverter.ConversionResult result =
      HmsMirrorConfigConverter.convert(config, "my-config", "my-dataset", "my-job");

  ConfigLiteDto configDto = result.getConfigLiteDto();
  DatasetDto datasetDto = result.getDatasetDto();
  ConnectionDto leftConnection = result.getLeftConnectionDto();
  ConnectionDto rightConnection = result.getRightConnectionDto();
  JobDto jobDto = result.getJobDto();

  // Save these DTOs via the appropriate services

