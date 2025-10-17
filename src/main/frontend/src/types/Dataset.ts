export interface DatasetFormData {
  name: string;
  description: string;
  databases: DatabaseSpec[];
}

export interface DatabaseSpec {
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
  tblSizeLimit?: number;
  tblPartitionLimit?: number;
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
  databaseName: '',
  tables: [],
  filter: undefined,
  warehouse: {
    warehouseSource: WarehouseSource.PLAN,
    managedDirectory: '',
    externalDirectory: ''
  }
});

export const DEFAULT_TABLE_FILTER: TableFilter = {
  tblRegEx: '',
  tblExcludeRegEx: '',
  tblSizeLimit: 0,
  tblPartitionLimit: 0
};

export interface DatasetWizardStepProps {
  formData: DatasetFormData;
  errors: Record<string, string>;
  updateFormData: (updates: Partial<DatasetFormData>) => void;
}