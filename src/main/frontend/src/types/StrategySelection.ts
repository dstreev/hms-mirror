// Strategy Selection Types and Constants

export enum MIGRATION_GOALS {
  MOVE_SCHEMAS_DATA = 'move_schemas_data',
  SCHEMAS_ONLY = 'schemas_only',
  CONVERT_ICEBERG = 'convert_iceberg',
  MOVE_WITHIN_CLUSTER = 'move_within_cluster',
  TEST_READ_ONLY = 'test_read_only',
  SHARED_STORAGE = 'shared_storage',
  EXTRACT_SCHEMAS = 'extract_schemas'
}

export enum CLUSTER_ACCESS {
  DIRECT_ACCESS = 'direct_access',
  INTERMEDIATE_STORAGE = 'intermediate_storage',
  NO_ACCESS = 'no_access'
}

export enum TABLE_CHARACTERISTICS {
  MIXED_PARTITIONS = 'mixed_partitions',
  SMALL_PARTITIONS = 'small_partitions',
  LARGE_PARTITIONS = 'large_partitions'
}

export enum ICEBERG_LOCATION {
  SAME_CLUSTER = 'same_cluster',
  DIFFERENT_CLUSTER = 'different_cluster'
}

export enum STRATEGIES {
  SQL = 'SQL',
  HYBRID = 'HYBRID',
  EXPORT_IMPORT = 'EXPORT_IMPORT',
  SCHEMA_ONLY = 'SCHEMA_ONLY',
  STORAGE_MIGRATION = 'STORAGE_MIGRATION',
  LINKED = 'LINKED',
  COMMON = 'COMMON',
  DUMP = 'DUMP'
}

export interface MigrationGoalOption {
  value: MIGRATION_GOALS;
  label: string;
  description: string;
  icon: string;
  isCommon?: boolean;
}

export interface ClusterAccessOption {
  value: CLUSTER_ACCESS;
  label: string;
  icon: string;
}

export interface TableCharacteristicsOption {
  value: TABLE_CHARACTERISTICS;
  label: string;
  description: string;
  strategy: STRATEGIES;
  icon: string;
}

export interface IcebergLocationOption {
  value: ICEBERG_LOCATION;
  label: string;
  strategy: STRATEGIES;
  icon: string;
}

export interface StrategyInfo {
  name: string;
  description: string;
  features: string[];
  requirements: string[];
}

export interface StrategyRecommendation {
  strategy: STRATEGIES;
  reason: string;
  path: string[];
  intermediateStorage?: boolean;
  error?: string;
  suggestion?: string;
}

export interface StrategySelectionResult {
  strategy: string;
  reason: string;
  path: string[];
  intermediateStorage: boolean;
}

export const migrationGoalOptions: MigrationGoalOption[] = [
  {
    value: MIGRATION_GOALS.MOVE_SCHEMAS_DATA,
    label: 'Move schemas + data between clusters',
    description: 'Migrate both metadata and data to a different cluster',
    icon: '🔄',
    isCommon: true
  },
  {
    value: MIGRATION_GOALS.SCHEMAS_ONLY,
    label: 'Move schemas only, handle data separately',
    description: 'Migrate metadata only, use distcp for data movement',
    icon: '📋'
  },
  {
    value: MIGRATION_GOALS.CONVERT_ICEBERG,
    label: 'Convert to Iceberg format',
    description: 'Convert existing tables to Iceberg table format',
    icon: '🧊'
  },
  {
    value: MIGRATION_GOALS.MOVE_WITHIN_CLUSTER,
    label: 'Move data within cluster to new storage',
    description: 'Change storage location within same cluster (HDFS→Ozone, HDFS→S3)',
    icon: '📦'
  },
  {
    value: MIGRATION_GOALS.TEST_READ_ONLY,
    label: 'Test new cluster with old data (read-only)',
    description: 'Create read-only access to existing data for testing',
    icon: '🔗'
  },
  {
    value: MIGRATION_GOALS.SHARED_STORAGE,
    label: 'Clusters share same physical storage',
    description: 'Only metadata needs to move, data is already accessible',
    icon: '🤝'
  },
  {
    value: MIGRATION_GOALS.EXTRACT_SCHEMAS,
    label: 'Extract schemas only (no target yet)',
    description: 'Generate SQL files for later manual execution',
    icon: '💾'
  }
];

export const clusterAccessOptions: ClusterAccessOption[] = [
  {
    value: CLUSTER_ACCESS.DIRECT_ACCESS,
    label: 'Yes, clusters can access each other\'s storage',
    icon: '✅'
  },
  {
    value: CLUSTER_ACCESS.INTERMEDIATE_STORAGE,
    label: 'No, but we have intermediate storage both can access',
    icon: '❌'
  },
  {
    value: CLUSTER_ACCESS.NO_ACCESS,
    label: 'No shared storage access at all',
    icon: '🚫'
  }
];

export const tableCharacteristicsOptions: TableCharacteristicsOption[] = [
  {
    value: TABLE_CHARACTERISTICS.MIXED_PARTITIONS,
    label: 'Mix of small and large partitioned tables',
    description: 'Auto-selects best method per table based on partition count',
    strategy: STRATEGIES.HYBRID,
    icon: '🔀'
  },
  {
    value: TABLE_CHARACTERISTICS.SMALL_PARTITIONS,
    label: 'Mostly tables with < 100 partitions',
    description: 'Good for small partitioned tables',
    strategy: STRATEGIES.EXPORT_IMPORT,
    icon: '📊'
  },
  {
    value: TABLE_CHARACTERISTICS.LARGE_PARTITIONS,
    label: 'Mostly tables with > 100 partitions',
    description: 'Better for large partitioned tables',
    strategy: STRATEGIES.SQL,
    icon: '📈'
  }
];

export const icebergLocationOptions: IcebergLocationOption[] = [
  {
    value: ICEBERG_LOCATION.SAME_CLUSTER,
    label: 'Same cluster (in-place conversion)',
    strategy: STRATEGIES.STORAGE_MIGRATION,
    icon: '🎯'
  },
  {
    value: ICEBERG_LOCATION.DIFFERENT_CLUSTER,
    label: 'Different cluster (conversion during migration)',
    strategy: STRATEGIES.SQL,
    icon: '🔄'
  }
];

export const strategyInfo: Record<STRATEGIES, StrategyInfo> = {
  [STRATEGIES.SQL]: {
    name: 'SQL',
    description: 'Uses SQL INSERT statements for data movement',
    features: [
      'Supports Iceberg conversion',
      'Better for large partitioned tables',
      'Uses SQL INSERT statements for data movement'
    ],
    requirements: [
      'Target cluster must be accessible',
      'Sufficient processing capacity for SQL operations'
    ]
  },
  [STRATEGIES.HYBRID]: {
    name: 'HYBRID',
    description: 'Mix of SQL and EXPORT_IMPORT strategies',
    features: [
      'Auto-selects method per table',
      'Good if you\'re unsure about partition sizes',
      'Supports Iceberg conversion'
    ],
    requirements: [
      'Target cluster must be accessible',
      'Mixed workload processing capacity'
    ]
  },
  [STRATEGIES.EXPORT_IMPORT]: {
    name: 'EXPORT_IMPORT',
    description: 'Hive Export/Import mechanism',
    features: [
      'Better for smaller partitioned tables',
      'More robust for complex table structures',
      'Supports Iceberg conversion'
    ],
    requirements: [
      'Target cluster must be accessible',
      'Hive export/import functionality'
    ]
  },
  [STRATEGIES.SCHEMA_ONLY]: {
    name: 'SCHEMA_ONLY',
    description: 'Metadata migration only',
    features: [
      'Generates distcp plans for separate data movement',
      'Fast metadata-only migration',
      'Manual data transfer control'
    ],
    requirements: [
      'Target cluster must be accessible',
      'Separate data movement solution'
    ]
  },
  [STRATEGIES.STORAGE_MIGRATION]: {
    name: 'STORAGE_MIGRATION',
    description: 'In-cluster storage migration',
    features: [
      'Choose SQL or DISTCP for data movement',
      'HDFS→Ozone, HDFS→S3, etc.',
      'SQL option supports Iceberg conversion'
    ],
    requirements: [
      'Access to both storage systems',
      'Sufficient cluster resources'
    ]
  },
  [STRATEGIES.LINKED]: {
    name: 'LINKED',
    description: 'Read-only testing access',
    features: [
      'FOR TESTING ONLY',
      'RIGHT points to LEFT data',
      'No data movement'
    ],
    requirements: [
      'Must use readOnly=true and noPurge=true',
      'Shared storage access'
    ]
  },
  [STRATEGIES.COMMON]: {
    name: 'COMMON',
    description: 'Shared storage metadata migration',
    features: [
      'Only metadata moves',
      'No data movement needed',
      'Fast migration'
    ],
    requirements: [
      'Clusters must share physical storage',
      'Compatible metadata formats'
    ]
  },
  [STRATEGIES.DUMP]: {
    name: 'DUMP',
    description: 'Schema extraction only',
    features: [
      'No target cluster required',
      'Generates SQL files for manual replay',
      'Offline migration preparation'
    ],
    requirements: [
      'Access to source cluster only'
    ]
  }
};