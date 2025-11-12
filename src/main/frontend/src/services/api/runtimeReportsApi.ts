import BaseApi from './baseApi';

export interface ConversionResult {
  key: string;
  created?: string; // LocalDateTime as ISO string
  modified?: string; // LocalDateTime as ISO string
  mockTestDataset?: boolean;
  outputDirectory?: string;
  config?: {
    name: string;
    dataStrategy?: string;
    description?: string;
  };
  dataset?: {
    name: string;
    description?: string;
    databases?: Array<{
      databaseName: string;
      tables?: string[];
    }>;
  };
  connections?: Record<string, {
    name: string;
    environment?: string;
    platformType?: string;
  }>;
}

export interface RuntimeReportsListResponse {
  status: string;
  data?: ConversionResult[];
  page?: number;
  pageSize?: number;
  totalCount?: number;
  totalPages?: number;
  hasMore?: boolean;
  message?: string;
}

export interface RuntimeReportResponse {
  status: string;
  conversionResult?: ConversionResult;
  message?: string;
}

class RuntimeReportsApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v1');
  }

  /**
   * Get paginated list of conversion results (reports)
   */
  async list(page: number = 0, pageSize: number = 10): Promise<RuntimeReportsListResponse | null> {
    try {
      const response = await super.get<RuntimeReportsListResponse>(`/runtime/reports?page=${page}&pageSize=${pageSize}`);
      return response;
    } catch (error) {
      console.error('Failed to fetch runtime reports:', error);
      return null;
    }
  }

  /**
   * Get a specific conversion result by key
   */
  async get(key: string): Promise<ConversionResult | null> {
    try {
      const response = await super.get<RuntimeReportResponse>(`/runtime/reports?key=${encodeURIComponent(key)}`);
      if (response?.status === 'SUCCESS' && response.conversionResult) {
        return response.conversionResult;
      }
      return null;
    } catch (error) {
      console.error(`Failed to fetch runtime report ${key}:`, error);
      return null;
    }
  }

  /**
   * Delete a conversion result
   */
  async delete(key: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await super.delete<RuntimeReportResponse>(`/runtime/reports?key=${encodeURIComponent(key)}`);

      if (response?.status === 'SUCCESS') {
        return {
          success: true,
          message: response.message
        };
      }

      return {
        success: false,
        message: response?.message || 'Failed to delete runtime report'
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Network error occurred'
      };
    }
  }

  /**
   * Format date as human-readable string
   */
  formatDate(dateStr?: string): string {
    if (!dateStr) return 'N/A';
    try {
      const date = new Date(dateStr);
      return date.toLocaleString();
    } catch (e) {
      return dateStr;
    }
  }

  /**
   * Get summary info for display
   */
  getSummary(result: ConversionResult): {
    configName: string;
    datasetName: string;
    databaseCount: number;
    tableCount: number;
  } {
    const configName = result.config?.name || 'N/A';
    const datasetName = result.dataset?.name || 'N/A';
    const databaseCount = result.dataset?.databases?.length || 0;
    const tableCount = result.dataset?.databases?.reduce(
      (total, db) => total + (db.tables?.length || 0),
      0
    ) || 0;

    return { configName, datasetName, databaseCount, tableCount };
  }
}

export const runtimeReportsApi = new RuntimeReportsApi();
export default RuntimeReportsApi;
