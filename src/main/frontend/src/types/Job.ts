export interface Job {
  jobKey?: string;
  name: string;
  description?: string;
  configurationName?: string;
  datasetName?: string;
  leftConnectionName?: string;
  rightConnectionName?: string;
  strategy?: string;
  status?: 'draft' | 'ready' | 'running' | 'completed' | 'failed';
  createdDate?: string;
  modifiedDate?: string;
  lastRunDate?: string;
}

export interface JobFormData {
  // Hidden field for updates
  key?: string;

  name: string;
  description?: string;
  datasetReference: string;
  configReference: string;
  leftConnectionReference: string;
  rightConnectionReference: string;
  strategy: string;
  disasterRecovery: boolean;
  sync: boolean;
}

export const DEFAULT_JOB_FORM: JobFormData = {
  name: '',
  description: '',
  datasetReference: '',
  configReference: '',
  leftConnectionReference: '',
  rightConnectionReference: '',
  strategy: '',
  disasterRecovery: false,
  sync: false
};
