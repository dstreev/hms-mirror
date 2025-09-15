import BaseApi from './baseApi';
import { HmsMirrorConfig, ConfigValidationResult } from '../../types/api';

class ConfigApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v2');
  }

  async getConfig(): Promise<HmsMirrorConfig | null> {
    try {
      return await this.get<HmsMirrorConfig>('/config');
    } catch (error) {
      console.error('Failed to fetch config:', error);
      return null;
    }
  }

  async saveConfig(config: HmsMirrorConfig): Promise<boolean> {
    try {
      await this.post('/config', config);
      return true;
    } catch (error) {
      console.error('Failed to save config:', error);
      return false;
    }
  }

  async loadConfig(filename: string): Promise<HmsMirrorConfig | null> {
    try {
      return await this.post<HmsMirrorConfig>('/config/load', { filename });
    } catch (error) {
      console.error('Failed to load config:', error);
      return null;
    }
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