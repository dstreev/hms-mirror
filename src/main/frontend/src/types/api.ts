// API Response Types
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  message?: string;
  errors?: string[];
}

// Configuration Types
export interface HmsMirrorConfig {
  id?: string;
  name?: string;
  dataStrategy: DataStrategy;
  clusters: {
    LEFT: ClusterConfig;
    RIGHT: ClusterConfig;
  };
  databases: string[];
  dbPrefix?: string;
  dbRename?: string;
  filter: FilterConfig;
  translator?: TranslatorConfig;
  transfer: TransferConfig;
  optimization: OptimizationConfig;
  execute: ExecuteConfig;
  hybrid?: HybridConfig;
  migrateACID?: MigrateACIDConfig;
}

export interface ClusterConfig {
  name: string;
  hiveServer2: {
    uri: string;
    username?: string;
    password?: string;
    driverClassName?: string;
    jarFile?: string;
  };
  metastoreDirect?: {
    uri: string;
    username?: string;
    password?: string;
    type: 'MYSQL' | 'POSTGRES' | 'ORACLE';
  };
  hcfsNamespace?: string;
}

export interface DatabaseConfig {
  name: string;
  selected: boolean;
  tables?: number;
}

export interface FilterConfig {
  dbRegEx?: string;
  tblRegEx?: string;
  tblExcludeRegEx?: string;
  tblSizeLimit?: number;
  tblPartitionLimit?: number;
  dbPropertySkipList?: string[];
}

export interface TransferConfig {
  transferPrefix?: string;
  shadowPrefix?: string;
  storageMigrationPostfix?: string;
  exportBaseDirPrefix?: string;
  remoteWorkingDirectory?: string;
  warehouse?: {
    source?: 'GLOBAL' | 'RELATIVE';
    managedDirectory?: string;
    externalDirectory?: string;
  };
  storageMigration?: {
    translationType?: 'ALIGNED' | 'RELATIVE' | 'NAMESPACE';
    dataMovementStrategy?: 'SQL' | 'EXPORT_IMPORT' | 'DISTCP';
    dataFlow?: 'PULL' | 'PUSH';
    skipDatabaseLocationAdjustments?: boolean;
    createArchive?: boolean;
    consolidateTablesForDistcp?: boolean;
    strict?: boolean;
  };
}

export interface OptimizationConfig {
  skipStatsCollection?: boolean;
  buildShadowStatistics?: boolean;
  compressTextOutput?: boolean;
  autoTune?: boolean;
}

export interface ExecuteConfig {
  dryRun: boolean;
  concurrency?: number;
}

// Data Strategy Types
export type DataStrategy = 
  | 'SCHEMA_ONLY'
  | 'DUMP'
  | 'EXPORT_IMPORT'
  | 'HYBRID'
  | 'SQL'
  | 'LINKED'
  | 'COMMON'
  | 'STORAGE_MIGRATION';

// Validation Types
export interface ConfigValidationResult {
  valid: boolean;
  errors?: string[];
  warnings?: string[];
}

// Connection Types
export interface ConnectionStatus {
  endpoint: string;
  status: 'CONNECTED' | 'DISCONNECTED' | 'ERROR' | 'VALIDATING';
  message?: string;
  lastChecked?: string;
}

// Execution Types
export interface ExecutionStatus {
  status: 'IDLE' | 'RUNNING' | 'PAUSED' | 'COMPLETED' | 'ERROR';
  progress: number;
  startTime?: string;
  endTime?: string;
  tablesProcessed?: number;
  totalTables?: number;
  currentTable?: string;
  errors?: string[];
  warnings?: string[];
}

// Report Types
export interface Report {
  id: string;
  name: string;
  type: 'MIGRATION' | 'VALIDATION' | 'DISTCP';
  status: 'COMPLETED' | 'FAILED';
  createdDate: string;
  configName?: string;
  summary?: ReportSummary;
}

export interface ReportSummary {
  totalDatabases: number;
  totalTables: number;
  successfulTables: number;
  failedTables: number;
  skippedTables: number;
  executionTime: number;
}

export interface TranslatorConfig {
  warehouseMapBuilder?: {
    warehousePlans?: Record<string, WarehousePlan>;
  };
}

export interface WarehousePlan {
  externalDirectory: string;
  managedDirectory: string;
}

export interface HybridConfig {
  sqlPartitionLimit?: number;
  exportImportPartitionLimit?: number;
  sqlSizeLimit?: number;
}

export interface MigrateACIDConfig {
  partitionLimit?: number;
}