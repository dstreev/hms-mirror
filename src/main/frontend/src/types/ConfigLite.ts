/**
 * TypeScript interfaces matching the backend ConfigLiteDto structure.
 * This ensures proper field mapping when creating/editing configurations.
 */

// Enum types
export enum IcebergFileTypeTranslation {
  STANDARD = 'STANDARD',
  PARQUET = 'PARQUET',
  ORC = 'ORC',
  AVRO = 'AVRO'
}

export enum TranslationType {
  ALIGNED = 'ALIGNED',
  RELATIVE = 'RELATIVE',
  EXPLICIT = 'EXPLICIT'
}

export enum DataMovementStrategy {
  SQL = 'SQL',
  DISTCP = 'DISTCP'
}

export enum DistcpFlow {
  PULL = 'PULL',
  PUSH = 'PUSH'
}

export enum WarehouseSource {
  GLOBAL = 'GLOBAL',
  PLAN = 'PLAN',
  ENVIRONMENT = 'ENVIRONMENT'
}

export enum SideType {
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
  BOTH = 'BOTH'
}

// Nested configuration interfaces
export interface IcebergConversion {
  enable?: boolean;
  fileTypeTranslation?: IcebergFileTypeTranslation;
  version?: number;
  tableProperties?: Record<string, string>;
  inplace?: boolean;
}

export interface MigrateACID {
  on?: boolean;
  only?: boolean;
  artificialBucketThreshold?: number;
  partitionLimit?: number;
  downgrade?: boolean;
  inplace?: boolean;
}

export interface MigrateVIEW {
  on?: boolean;
}

export interface Overrides {
  properties?: Record<string, Record<SideType, string>>;
}

export interface Optimization {
  sortDynamicPartitionInserts?: boolean;
  skip?: boolean;
  autoTune?: boolean;
  compressTextOutput?: boolean;
  skipStatsCollection?: boolean;
  overrides?: Overrides;
  buildShadowStatistics?: boolean;
}

export interface StorageMigration {
  translationType?: TranslationType;
  dataMovementStrategy?: DataMovementStrategy;
  dataFlow?: DistcpFlow;
  skipDatabaseLocationAdjustments?: boolean;
  createArchive?: boolean;
  consolidateTablesForDistcp?: boolean;
  strict?: boolean;
}

export interface Warehouse {
  source?: WarehouseSource;
  externalDirectory?: string;
  managedDirectory?: string;
}

export interface TransferConfig {
  transferPrefix?: string;
  shadowPrefix?: string;
  storageMigrationPostfix?: string;
  exportBaseDirPrefix?: string;
  remoteWorkingDirectory?: string;
  targetNamespace?: string | null;
  intermediateStorage?: string | null;
  storageMigration?: StorageMigration;
  warehouse?: Warehouse;
}

export interface TransferOwnership {
  database?: boolean;
  table?: boolean;
}

// Main ConfigLite interface matching backend ConfigLiteDto
export interface ConfigLiteDto {
  // Identifiers
  key?: string;
  name: string;
  description?: string;

  // Timestamps
  created?: string;
  modified?: string;

  // Feature flags
  migrateNonNative?: boolean;
  createIfNotExists?: boolean;
  enableAutoTableStats?: boolean;
  enableAutoColumnStats?: boolean;
  saveWorkingTables?: boolean;
  copyAvroSchemaUrls?: boolean;
  forceExternalLocation?: boolean;

  // Nested configuration objects
  icebergConversion?: IcebergConversion;
  migrateACID?: MigrateACID;
  migrateVIEW?: MigrateVIEW;
  optimization?: Optimization;
  transfer?: TransferConfig;
  ownershipTransfer?: TransferOwnership;
}

// Default values for new configurations
export const DEFAULT_CONFIG_LITE: ConfigLiteDto = {
  name: '',
  description: '',
  migrateNonNative: false,
  createIfNotExists: true,
  enableAutoTableStats: false,
  enableAutoColumnStats: false,
  saveWorkingTables: false,
  copyAvroSchemaUrls: false,
  forceExternalLocation: false,
  icebergConversion: {
    enable: false,
    fileTypeTranslation: IcebergFileTypeTranslation.STANDARD,
    version: 2,
    tableProperties: {},
    inplace: false
  },
  migrateACID: {
    on: false,
    only: false,
    artificialBucketThreshold: 2,
    partitionLimit: 500,
    downgrade: false,
    inplace: false
  },
  migrateVIEW: {
    on: false
  },
  optimization: {
    sortDynamicPartitionInserts: false,
    skip: false,
    autoTune: false,
    compressTextOutput: false,
    skipStatsCollection: false,
    overrides: {
      properties: {}
    },
    buildShadowStatistics: false
  },
  transfer: {
    transferPrefix: 'hms_mirror_transfer_',
    shadowPrefix: 'hms_mirror_shadow_',
    storageMigrationPostfix: '_storage_migration',
    exportBaseDirPrefix: '/apps/hive/warehouse/export_',
    remoteWorkingDirectory: 'hms_mirror_working',
    targetNamespace: null,
    intermediateStorage: null,
    storageMigration: {
      translationType: TranslationType.RELATIVE,
      dataMovementStrategy: DataMovementStrategy.SQL,
      dataFlow: DistcpFlow.PULL,
      skipDatabaseLocationAdjustments: false,
      createArchive: false,
      consolidateTablesForDistcp: false,
      strict: true
    },
    warehouse: {
      source: WarehouseSource.GLOBAL,
      externalDirectory: undefined,
      managedDirectory: undefined
    }
  },
  ownershipTransfer: {
    database: false,
    table: false
  }
};
