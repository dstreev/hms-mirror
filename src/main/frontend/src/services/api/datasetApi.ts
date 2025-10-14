import BaseApi from './baseApi';
import { DatasetFormData } from '../../types/Dataset';

export interface DatasetDto {
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
}

export interface TableFilterDto {
  includePattern?: string;
  excludePattern?: string;
  tableTypes?: string[];
  minPartitions?: number;
  maxPartitions?: number;
  minSizeBytes?: number;
  maxSizeBytes?: number;
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
      name: formData.name,
      description: formData.description,
      databases: formData.databases.map(db => ({
        databaseName: db.databaseName,
        tables: db.tables,
        filter: db.filter ? {
          includePattern: db.filter.tblRegEx,
          excludePattern: db.filter.tblExcludeRegEx,
          minSizeBytes: db.filter.tblSizeLimit,
          maxPartitions: db.filter.tblPartitionLimit
        } : undefined,
        warehouse: {
          source: db.warehouse.warehouseSource,
          managedDirectory: db.warehouse.managedDirectory,
          externalDirectory: db.warehouse.externalDirectory
        }
      }))
    };
  }

  private mapDtoToFormData(dto: DatasetDto): DatasetFormData {
    return {
      name: dto.name,
      description: dto.description,
      databases: dto.databases.map(db => ({
        databaseName: db.databaseName,
        tables: db.tables,
        filter: db.filter ? {
          tblRegEx: db.filter.includePattern,
          tblExcludeRegEx: db.filter.excludePattern,
          tblSizeLimit: db.filter.minSizeBytes,
          tblPartitionLimit: db.filter.maxPartitions
        } : undefined,
        warehouse: {
          warehouseSource: db.warehouse.source as any,
          managedDirectory: db.warehouse.managedDirectory,
          externalDirectory: db.warehouse.externalDirectory
        }
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

  async getDataset(datasetName: string): Promise<DatasetFormData | null> {
    try {
      const response = await this.get<DatasetResponse>(`/datasets/${datasetName}`);
      console.log(`getDataset response for ${datasetName}:`, response);

      if (response?.status === 'SUCCESS' && response.data) {
        return this.mapDtoToFormData(response.data);
      }

      console.warn(`Dataset ${datasetName} not found or invalid response:`, response);
      return null;
    } catch (error: any) {
      console.error(`Failed to fetch dataset ${datasetName}:`, error);
      console.error('Error details:', error.response?.data || error.message);
      return null;
    }
  }

  async saveDataset(formData: DatasetFormData): Promise<{ success: boolean; message?: string; operation?: string }> {
    try {
      const dto = this.mapFormDataToDto(formData);
      const response = await this.post<DatasetResponse>('/datasets', dto);
      
      if (response?.status === 'SUCCESS') {
        return {
          success: true,
          operation: response.operation || 'saved'
        };
      } else {
        return {
          success: false,
          message: response?.message || 'Failed to save dataset'
        };
      }
    } catch (error: any) {
      console.error('Failed to save dataset:', error);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while saving dataset'
      };
    }
  }

  async updateDataset(datasetName: string, formData: DatasetFormData): Promise<{ success: boolean; message?: string }> {
    try {
      const dto = this.mapFormDataToDto(formData);
      const response = await this.put<DatasetResponse>(`/datasets/${datasetName}`, dto);
      
      if (response?.status === 'SUCCESS') {
        return { success: true };
      } else {
        return {
          success: false,
          message: response?.message || 'Failed to update dataset'
        };
      }
    } catch (error: any) {
      console.error(`Failed to update dataset ${datasetName}:`, error);
      return {
        success: false,
        message: error.response?.data?.message || error.message || 'Network error occurred while updating dataset'
      };
    }
  }

  async deleteDataset(datasetName: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.delete<DatasetResponse>(`/datasets/${datasetName}`);
      
      if (response?.status === 'SUCCESS') {
        return { success: true };
      } else {
        return {
          success: false,
          message: response?.message || 'Failed to delete dataset'
        };
      }
    } catch (error: any) {
      console.error(`Failed to delete dataset ${datasetName}:`, error);
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