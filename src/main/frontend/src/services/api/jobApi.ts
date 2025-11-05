import BaseApi from './baseApi';
import { Job, JobFormData } from '../../types/Job';

export interface JobDto {
  id?: string;
  name: string;
  description?: string;
  datasetReference?: string;
  configReference?: string;
  leftConnectionReference?: string;
  rightConnectionReference?: string;
  strategy?: string;
  disasterRecovery?: boolean;
  sync?: boolean;
  status?: 'draft' | 'ready' | 'running' | 'completed' | 'failed';
  createdDate?: string;
  modifiedDate?: string;
  lastRunDate?: string;
}

export interface JobListResponse {
  status: string;
  jobs?: Array<{
    jobKey: string;
    name: string;
    description?: string;
    createdDate?: string;
    modifiedDate?: string;
    datasetReference?: string;
    configReference?: string;
    leftConnectionReference?: string;
    rightConnectionReference?: string;
    strategy?: string;
    disasterRecovery?: boolean;
    sync?: boolean;
  }>;
  count?: number;
  message?: string;
}

export interface JobResponse {
  status: string;
  job?: JobDto;
  jobKey?: string;
  message?: string;
  operation?: string;
}

class JobApi extends BaseApi {
  constructor() {
    super();
  }

  /**
   * Get all jobs
   */
  async getJobs(): Promise<JobListResponse | null> {
    try {
      const response = await this.get<JobListResponse>('/v1/jobs');
      return response;
    } catch (error) {
      console.error('Failed to fetch jobs:', error);
      return null;
    }
  }

  /**
   * Get a specific job by key - returns full JobDto with jobKey for editing
   */
  async getJob(jobKey: string): Promise<{ job: JobDto; jobKey: string } | null> {
    try {
      const response = await this.get<JobResponse>(`/v1/jobs/${jobKey}`);
      if (response?.status === 'SUCCESS' && response.job && response.jobKey) {
        return { job: response.job, jobKey: response.jobKey };
      }
      return null;
    } catch (error) {
      console.error(`Failed to fetch job ${jobKey}:`, error);
      return null;
    }
  }

  /**
   * Save a new job
   */
  async saveJob(jobKey: string, jobDto: JobDto): Promise<{ success: boolean; message?: string; operation?: string }> {
    try {
      const response = await this.post<JobResponse>(`/v1/jobs/${jobKey}`, jobDto);

      if (response?.status === 'SUCCESS' || response?.status === 'success') {
        return {
          success: true,
          message: response.message,
          operation: response.operation
        };
      }

      return {
        success: false,
        message: response?.message || 'Failed to save job'
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Network error occurred'
      };
    }
  }

  /**
   * Update an existing job
   */
  async updateJob(jobKey: string, jobDto: JobDto): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.put<JobResponse>(`/v1/jobs/${jobKey}`, jobDto);

      if (response?.status === 'SUCCESS' || response?.status === 'success') {
        return {
          success: true,
          message: response.message
        };
      }

      return {
        success: false,
        message: response?.message || 'Failed to update job'
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Network error occurred'
      };
    }
  }

  /**
   * Delete a job
   */
  async deleteJob(jobName: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.delete<{ status: string; message?: string }>(`/v1/jobs/${jobName}`);

      if (response?.status === 'SUCCESS' || response?.status === 'success') {
        return {
          success: true,
          message: response.message
        };
      }

      return {
        success: false,
        message: response?.message || 'Failed to delete job'
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Network error occurred'
      };
    }
  }

  /**
   * Copy a job
   */
  async copyJob(sourceJobName: string, targetJobName: string): Promise<{ success: boolean; message?: string }> {
    try {
      const response = await this.post<JobResponse>(`/v1/jobs/copy/${sourceJobName}`, {
        targetJobName
      });

      if (response?.status === 'SUCCESS' || response?.status === 'success') {
        return {
          success: true,
          message: response.message
        };
      }

      return {
        success: false,
        message: response?.message || 'Failed to copy job'
      };
    } catch (error: any) {
      return {
        success: false,
        message: error.message || 'Network error occurred'
      };
    }
  }

  /**
   * Validate a job's configuration
   */
  async validateJob(jobKey: string): Promise<{
    success: boolean;
    valid?: boolean;
    message?: string;
    errors?: string[];
    warnings?: string[];
  }> {
    try {
      const response = await this.post<{
        status: string;
        valid: boolean;
        message?: string;
        errors?: string[];
        warnings?: string[];
      }>(`/v1/jobs/${jobKey}/validate`, {});

      if (response?.status === 'SUCCESS' || response?.status === 'success') {
        return {
          success: true,
          valid: response.valid,
          message: response.message,
          errors: response.errors || [],
          warnings: response.warnings || []
        };
      }

      return {
        success: false,
        valid: false,
        message: response?.message || 'Failed to validate job',
        errors: response?.errors || [],
        warnings: response?.warnings || []
      };
    } catch (error: any) {
      return {
        success: false,
        valid: false,
        message: error.message || 'Network error occurred',
        errors: [],
        warnings: []
      };
    }
  }

}

export const jobApi = new JobApi();
