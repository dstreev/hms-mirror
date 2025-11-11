export interface DatasetFormData {
  // Hidden field for updates
  key?: string;

  name: string;
  description: string;
  databases: DatabaseSpec[];
}

export interface DatabaseSpec {
  // Entry mode configuration
  entryMode?: 'manual' | 'connection'; // NEW - determines how database/tables are specified
  sourceConnectionKey?: string; // NEW - connection key when using connection mode

  // Database configuration
  databaseName: string;
  tables: string[];
  filter?: TableFilter;
  warehouse: Warehouse;
  userGlobalLocationMap?: Record<string, Record<string, string>>; // source path -> { TableType -> target location }
  dbPrefix?: string;
  dbRename?: string;
}

export interface TableFilter {
  tblRegEx?: string;
  tblExcludeRegEx?: string;
  tableTypes?: string[];
  minPartitions?: number;
  maxPartitions?: number;
  minSizeMb?: number;
  maxSizeMb?: number;
}

export interface Warehouse {
  warehouseSource: WarehouseSource;
  managedDirectory?: string;
  externalDirectory?: string;
}

export enum WarehouseSource {
  PLAN = 'PLAN'
}

export const DEFAULT_DATASET_FORM: DatasetFormData = {
  name: '',
  description: '',
  databases: []
};

export const createDefaultDatabaseSpec = (): DatabaseSpec => ({
  entryMode: 'manual', // Default to manual entry mode
  sourceConnectionKey: undefined,
  databaseName: '',
  tables: [],
  filter: undefined,
  warehouse: {
    warehouseSource: WarehouseSource.PLAN,
    managedDirectory: '',
    externalDirectory: ''
  },
  userGlobalLocationMap: undefined,
  dbPrefix: undefined,
  dbRename: undefined
});

export const DEFAULT_TABLE_FILTER: TableFilter = {
  tblRegEx: '',
  tblExcludeRegEx: '',
  tableTypes: [],
  minPartitions: 0,
  maxPartitions: 0,
  minSizeMb: 0,
  maxSizeMb: 0
};

export interface DatasetWizardStepProps {
  formData: DatasetFormData;
  errors: Record<string, string>;
  updateFormData: (updates: Partial<DatasetFormData>) => void;
}