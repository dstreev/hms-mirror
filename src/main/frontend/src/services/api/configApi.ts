import BaseApi from './baseApi';
import { HmsMirrorConfig, ConfigValidationResult } from '../../types/api';

export interface ConfigurationDto {
  name: string;
  dataStrategy: string;
  comment?: string;
  beta?: boolean;
  execute?: boolean;
  databaseOnly?: boolean;
  migrateNonNative?: boolean;
  readOnly?: boolean;
  noPurge?: boolean;
  replace?: boolean;
  resetRight?: boolean;
  sync?: boolean;
  quiet?: boolean;
  skipFeatures?: boolean;
  skipLegacyTranslation?: boolean;
  skipLinkCheck?: boolean;
  suppressCliWarnings?: boolean;
  saveWorkingTables?: boolean;
  copyAvroSchemaUrls?: boolean;
  dumpTestData?: boolean;
  encryptedPasswords?: boolean;
  databases?: string[];
  dbPrefix?: string;
  dbRename?: string;
  loadTestDataFile?: string;
  dumpSource?: string;
  connectionPoolLib?: string;
}

export interface ConfigurationListResponse {
  status: string;
  data: {
    [strategy: string]: Array<{
      name: string;
      yamlConfig: string;
      createdDate: string;
      modifiedDate: string;
    }>;
  };
  totalConfigurations: number;
  strategies: string[];
}

export interface ConfigurationResponse {
  status: string;
  key?: string;
  configuration?: HmsMirrorConfig;
  yaml?: string;
  size?: number;
  exists?: boolean;
  message?: string;
}

export interface DataStrategiesResponse {
  status: string;
  strategies: Array<{
    value: string;
    label: string;
  }>;
}

class ConfigApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v1');
  }

  async getConfigurations(): Promise<ConfigurationListResponse | null> {
    try {
      return await this.get<ConfigurationListResponse>('/config');
    } catch (error) {
      console.error('Failed to fetch configurations:', error);
      return null;
    }
  }

  async getConfiguration(configName: string): Promise<ConfigurationResponse | null> {
    try {
      return await this.get<ConfigurationResponse>(`/config/${configName}`);
    } catch (error) {
      console.error(`Failed to fetch configuration ${configName}:`, error);
      return null;
    }
  }

  async saveConfiguration(configDto: ConfigurationDto): Promise<boolean> {
    try {
      const response = await this.post<ConfigurationResponse>('/config', configDto);
      return response?.status === 'SUCCESS';
    } catch (error) {
      console.error('Failed to save configuration:', error);
      return false;
    }
  }

  async updateConfiguration(dataStrategy: string, configName: string, configDto: ConfigurationDto): Promise<boolean> {
    try {
      const response = await this.put<ConfigurationResponse>(`/config/${dataStrategy}/${configName}`, configDto);
      return response?.status === 'SUCCESS';
    } catch (error) {
      console.error(`Failed to update configuration ${dataStrategy}/${configName}:`, error);
      return false;
    }
  }

  async deleteConfiguration(configName: string): Promise<boolean> {
    try {
      const response = await this.delete<ConfigurationResponse>(`/config/${configName}`);
      return response?.status === 'SUCCESS';
    } catch (error) {
      console.error(`Failed to delete configuration ${configName}:`, error);
      return false;
    }
  }

  async copyConfiguration(sourceDataStrategy: string, sourceConfigName: string, 
                         targetDataStrategy: string, targetConfigName: string): Promise<boolean> {
    try {
      const response = await this.post<ConfigurationResponse>(`/config/copy/${sourceDataStrategy}/${sourceConfigName}`, {
        targetDataStrategy,
        targetConfigName
      });
      return response?.status === 'SUCCESS';
    } catch (error) {
      console.error(`Failed to copy configuration ${sourceDataStrategy}/${sourceConfigName}:`, error);
      return false;
    }
  }

  async getDataStrategies(): Promise<DataStrategiesResponse | null> {
    try {
      return await this.get<DataStrategiesResponse>('/config/strategies');
    } catch (error) {
      console.error('Failed to fetch data strategies:', error);
      return null;
    }
  }

  // Legacy methods (maintained for backward compatibility)
  async getConfig(): Promise<HmsMirrorConfig | null> {
    console.warn('getConfig() is deprecated. Configuration loading is currently unavailable due to API migration.');
    return null;
  }

  async saveConfig(config: HmsMirrorConfig): Promise<boolean> {
    console.warn('saveConfig() is deprecated. Use saveConfiguration() with ConfigurationDto instead.');
    return false;
  }

  async loadConfig(filename: string): Promise<HmsMirrorConfig | null> {
    console.warn('loadConfig() is deprecated. Configuration file loading is currently unavailable due to API migration.');
    return null;
  }

  async validateConfig(config: HmsMirrorConfig): Promise<ConfigValidationResult> {
    try {
      return await this.post<ConfigValidationResult>('/config/validate', config);
    } catch (error) {
      console.error('Failed to validate config:', error);
      return { valid: false, errors: ['Validation request failed'] };
    }
  }

  async getTemplates(): Promise<string[]> {
    try {
      return await this.get<string[]>('/config/templates');
    } catch (error) {
      console.error('Failed to fetch templates:', error);
      return [];
    }
  }

  async createFromTemplate(templateName: string): Promise<HmsMirrorConfig | null> {
    try {
      return await this.post<HmsMirrorConfig>('/config/from-template', { templateName });
    } catch (error) {
      console.error('Failed to create config from template:', error);
      return null;
    }
  }

  async encryptPasswords(config: HmsMirrorConfig, passwordKey: string): Promise<HmsMirrorConfig | null> {
    try {
      console.log('Calling encrypt passwords API with passwordKey length:', passwordKey.length);
      console.log('Config has clusters:', config.clusters ? Object.keys(config.clusters) : 'none');
      const result = await this.post<HmsMirrorConfig>('/config/encrypt-passwords', { 
        config, 
        passwordKey 
      });
      console.log('Encrypt passwords API response received');
      return result;
    } catch (error: any) {
      console.error('Failed to encrypt passwords:', error);
      console.error('Error response:', error.response?.data);
      console.error('Error status:', error.response?.status);
      return null;
    }
  }

  async decryptPasswords(config: HmsMirrorConfig, passwordKey: string): Promise<HmsMirrorConfig | null> {
    try {
      return await this.post<HmsMirrorConfig>('/config/decrypt-passwords', { 
        config, 
        passwordKey 
      });
    } catch (error) {
      console.error('Failed to decrypt passwords:', error);
      return null;
    }
  }

  async getConfigAsYaml(config: HmsMirrorConfig): Promise<string | null> {
    try {
      const response = await this.post<{ yaml: string }>('/config/to-yaml', config);
      return response?.yaml || null;
    } catch (error) {
      console.error('Failed to convert config to YAML:', error);
      return null;
    }
  }
}

export const configApi = new ConfigApi();
export default ConfigApi;