import BaseApi from './baseApi';

export interface RunStatus {
  key: string;
  start?: string; // LocalDateTime as ISO string
  end?: string; // LocalDateTime as ISO string
  comment?: string;
  errorMessages: string[];
  warningMessages: string[];
  configMessages: string[];
  unSuccessfulTableCount?: number;
  reportName?: string;
  appVersion?: string;
  progress?: string;
  stages?: Record<string, string>;
  operationStatistics?: {
    tableMirrors?: number;
    successfulMirrors?: number;
    failedMirrors?: number;
  };
}

export interface RuntimeJobsListResponse {
  status: string;
  data?: RunStatus[];
  page?: number;
  pageSize?: number;
  totalCount?: number;
  totalPages?: number;
  hasMore?: boolean;
  message?: string;
}

export interface RuntimeJobResponse {
  status: string;
  runStatus?: RunStatus;
  message?: string;
}

class RuntimeJobsApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v1');
  }

  /**
   * Get paginated list of runtime job statuses
   */
  async list(page: number = 0, pageSize: number = 10): Promise<RuntimeJobsListResponse | null> {
    try {
      const response = await this.get<RuntimeJobsListResponse>(`/runtime/jobs?page=${page}&pageSize=${pageSize}`);
      return response;
    } catch (error) {
      console.error('Failed to fetch runtime jobs:', error);
      return null;
    }
  }

  /**
   * Get a specific runtime job status by key
   */
  async get(key: string): Promise<RunStatus | null> {
    try {
      const response = await this.get<RuntimeJobResponse>(`/runtime/jobs/${key}`);
      if (response?.status === 'SUCCESS' && response.runStatus) {
        return response.runStatus;
      }
      return null;
    } catch (error) {
      console.error(`Failed to fetch runtime job ${key}:`, error);
      return null;
    }
  }

  /**
   * Delete a runtime job status
   */
  async delete(key: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.delete<RuntimeJobResponse>(`/runtime/jobs/${key}`);

      if (response?.status === 'SUCCESS') {
        return {
          success: true,
          message: response.message
        };
      }

      return {
        success: false,
        message: response?.message || 'Failed to delete runtime job'
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Network error occurred'
      };
    }
  }

  /**
   * Check if a job is currently running (has start but no end)
   */
  isRunning(runStatus: RunStatus): boolean {
    return !!runStatus.start && !runStatus.end;
  }

  /**
   * Get duration in milliseconds
   */
  getDuration(runStatus: RunStatus): number | null {
    if (!runStatus.start) return null;

    const startTime = new Date(runStatus.start).getTime();
    const endTime = runStatus.end ? new Date(runStatus.end).getTime() : Date.now();

    return endTime - startTime;
  }

  /**
   * Format duration as human-readable string
   */
  formatDuration(milliseconds: number): string {
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) {
      return `${days}d ${hours % 24}h ${minutes % 60}m`;
    } else if (hours > 0) {
      return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }
}

export const runtimeJobsApi = new RuntimeJobsApi();
export default RuntimeJobsApi;
