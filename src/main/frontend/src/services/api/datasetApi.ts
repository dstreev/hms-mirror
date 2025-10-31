import BaseApi from './baseApi';
import { DatasetFormData } from '../../types/Dataset';

export interface DatasetDto {
  key?: string;
  name: string;
  description: string;
  databases: DatabaseSpecDto[];
  createdDate?: string;
  modifiedDate?: string;
}

export interface DatabaseSpecDto {
  databaseName: string;
  tables: string[];
  filter?: TableFilterDto;
  warehouse: WarehouseDto;
  userGlobalLocationMap?: Record<string, Record<string, string>>;
  dbPrefix?: string;
  dbRename?: string;
}

export interface TableFilterDto {
  includeRegEx?: string;
  excludeRegEx?: string;
  tableTypes?: string[];
  minPartitions?: number;
  maxPartitions?: number;
  minSizeMb?: number;
  maxSizeMb?: number;
}

export interface WarehouseDto {
  source: string;
  managedDirectory?: string;
  externalDirectory?: string;
}

export interface DatasetListResponse {
  status: string;
  data?: {
    [name: string]: DatasetDto;
  };
  message?: string;
}

export interface DatasetResponse {
  status: string;
  data?: DatasetDto;
  message?: string;
  operation?: string;
}

export interface DatasetValidationResponse {
  status: string;
  valid?: boolean;
  errors?: string[];
  message?: string;
}

class DatasetApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v1');
  }

  private mapFormDataToDto(formData: DatasetFormData): DatasetDto {
    return {
      key: formData.key, // Preserve key for updates
      name: formData.name,
      description: formData.description,
      databases: formData.databases.map(db => ({
        databaseName: db.databaseName,
        tables: db.tables,
        filter: db.filter ? {
          includeRegEx: db.filter.tblRegEx,
          excludeRegEx: db.filter.tblExcludeRegEx,
          tableTypes: db.filter.tableTypes,
          minPartitions: db.filter.minPartitions,
          maxPartitions: db.filter.maxPartitions,
          minSizeMb: db.filter.minSizeMb,
          maxSizeMb: db.filter.maxSizeMb
        } : undefined,
        warehouse: {
          source: db.warehouse.warehouseSource,
          managedDirectory: db.warehouse.managedDirectory,
          externalDirectory: db.warehouse.externalDirectory
        },
        userGlobalLocationMap: db.userGlobalLocationMap,
        dbPrefix: db.dbPrefix,
        dbRename: db.dbRename
      }))
    };
  }

  private mapDtoToFormData(dto: DatasetDto): DatasetFormData {
    return {
      key: dto.key, // Preserve key for updates
      name: dto.name,
      description: dto.description,
      databases: dto.databases.map(db => ({
        databaseName: db.databaseName,
        tables: db.tables,
        filter: db.filter ? {
          tblRegEx: db.filter.includeRegEx,
          tblExcludeRegEx: db.filter.excludeRegEx,
          tableTypes: db.filter.tableTypes,
          minPartitions: db.filter.minPartitions,
          maxPartitions: db.filter.maxPartitions,
          minSizeMb: db.filter.minSizeMb,
          maxSizeMb: db.filter.maxSizeMb
        } : undefined,
        warehouse: {
          warehouseSource: db.warehouse.source as any,
          managedDirectory: db.warehouse.managedDirectory,
          externalDirectory: db.warehouse.externalDirectory
        },
        userGlobalLocationMap: db.userGlobalLocationMap,
        dbPrefix: db.dbPrefix,
        dbRename: db.dbRename
      }))
    };
  }

  async getDatasets(): Promise<DatasetListResponse | null> {
    try {
      return await this.get<DatasetListResponse>('/datasets');
    } catch (error) {
      console.error('Failed to fetch datasets:', error);
      return null;
    }
  }

  async getDataset(datasetKey: string): Promise<DatasetFormData | null> {
    try {
      const response = await this.get<DatasetResponse>(`/datasets/${datasetKey}`);
      console.log(`getDataset response for ${datasetKey}:`, response);

      if (response?.status === 'SUCCESS' && response.data) {
        return this.mapDtoToFormData(response.data);
      }

      console.warn(`Dataset ${datasetKey} not found or invalid response:`, response);
      return null;
    } catch (error: any) {
      console.error(`Failed to fetch dataset ${datasetKey}:`, error);
      console.error('Error details:', error.response?.data || error.message);
      return null;
    }
  }

  async saveDataset(formData: DatasetFormData): Promise<{ success: boolean; message?: string; operation?: string }> {
    try {
      const dto = this.mapFormDataToDto(formData);
      console.log('Saving dataset - DTO being sent:', JSON.stringify(dto, null, 2));
      const response = await this.post<DatasetResponse>('/datasets', dto);

      if (response?.status === 'SUCCESS') {
        return {
          success: true,
          operation: response.operation || 'saved'
        };
      } else {
        console.error('Save dataset failed with response:', response);
        return {
          success: false,
          message: response?.message || 'Failed to save dataset'
        };
      }
    } catch (error: any) {
      console.error('Failed to save dataset - error:', error);
      console.error('Error response data:', error.response?.data);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while saving dataset'
      };
    }
  }

  async updateDataset(datasetKey: string, formData: DatasetFormData): Promise<{ success: boolean; message?: string }> {
    try {
      const dto = this.mapFormDataToDto(formData);
      const response = await this.put<DatasetResponse>(`/datasets/${datasetKey}`, dto);

      if (response?.status === 'SUCCESS') {
        return { success: true };
      } else {
        return {
          success: false,
          message: response?.message || 'Failed to update dataset'
        };
      }
    } catch (error: any) {
      console.error(`Failed to update dataset ${datasetKey}:`, error);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while updating dataset'
      };
    }
  }

  async deleteDataset(datasetKey: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.delete<DatasetResponse>(`/datasets/${datasetKey}`);

      if (response?.status === 'SUCCESS') {
        return { success: true };
      } else {
        return {
          success: false,
          message: response?.message || 'Failed to delete dataset'
        };
      }
    } catch (error: any) {
      console.error(`Failed to delete dataset ${datasetKey}:`, error);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while deleting dataset'
      };
    }
  }

  async copyDataset(sourceDatasetName: string, targetDatasetName: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.post<DatasetResponse>(`/datasets/copy/${sourceDatasetName}`, {
        targetDatasetName
      });
      
      if (response?.status === 'SUCCESS') {
        return { success: true };
      } else {
        return {
          success: false,
          message: response?.message || 'Failed to copy dataset'
        };
      }
    } catch (error: any) {
      console.error(`Failed to copy dataset ${sourceDatasetName} to ${targetDatasetName}:`, error);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while copying dataset'
      };
    }
  }

  async validateDataset(formData: DatasetFormData): Promise<{ success: boolean; message?: string; errors?: string[] }> {
    try {
      const dto = this.mapFormDataToDto(formData);
      const response = await this.post<DatasetValidationResponse>('/datasets/validate', dto);
      
      if (response?.status === 'success') {
        return { success: true };
      } else {
        return {
          success: false,
          message: response?.message || 'Dataset validation failed',
          errors: response?.errors || []
        };
      }
    } catch (error: any) {
      console.error('Failed to validate dataset:', error);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while validating dataset'
      };
    }
  }
}

export const datasetApi = new DatasetApi();
export default DatasetApi;