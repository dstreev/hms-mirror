import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { ChevronLeftIcon, ChevronRightIcon, CheckIcon } from '@heroicons/react/24/outline';
import WizardProgress from '../connections/wizard/WizardProgress';

interface HmsMirrorConfig {
  // Migration Behavior
  databaseOnly: boolean;
  migrateVIEW: { on: boolean };
  migrateNonNative: boolean;
  
  // ACID Migration Settings
  migrateACID: {
    on: boolean;
    only: boolean;
    artificialBucketThreshold: number;
    partitionLimit: number;
    downgrade: boolean;
    inplace: boolean;
  };
  
  // Transfer and Warehouse Settings
  transfer: {
    warehouse: {
      externalDirectory: string;
      managedDirectory: string;
    };
    transferPrefix: string;
    shadowPrefix: string;
  };
  
  // Conversions (Optional)
  icebergConversion: {
    enable: boolean;
    fileTypeTranslation: string;
    version: string;
    tableProperties: string;
    inplace: boolean;
  };
  
  // Optimization Settings
  optimization?: {
    skip: boolean;
    sortDynamicPartition: boolean;
    buildShadowStatistics: boolean;
    compressTextOutput: boolean;
    skipStatsCollection: boolean;
    autoTune: boolean;
  };
  
  // Configuration metadata
  configName: string;
  description: string;
  dataStrategy: string;
}

const ConfigWizard: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  
  const [currentStep, setCurrentStep] = useState(0);
  const [config, setConfig] = useState<HmsMirrorConfig>({
    // Default values based on spec
    databaseOnly: false,
    migrateVIEW: { on: true },
    migrateNonNative: true,
    migrateACID: {
      on: true,
      only: false,
      artificialBucketThreshold: 100000,
      partitionLimit: 1000,
      downgrade: false,
      inplace: false,
    },
    transfer: {
      warehouse: {
        externalDirectory: "/user/hive/external",
        managedDirectory: "/user/hive/warehouse",
      },
      transferPrefix: "mig_",
      shadowPrefix: "_shadow",
    },
    icebergConversion: {
      enable: false,
      fileTypeTranslation: "",
      version: "2",
      tableProperties: "",
      inplace: false,
    },
    configName: "",
    description: "",
    dataStrategy: location.state?.dataStrategy || 'SQL',
  });

  const [errors, setErrors] = useState<{ [key: string]: string }>({});
  const [isLoading, setIsLoading] = useState(false);

  // Load existing configuration if passed via navigation state
  useEffect(() => {
    const loadExistingConfig = async () => {
      if ((location.state?.isEditing || location.state?.isCopying) && location.state?.configurationData) {
        setIsLoading(true);
        try {
          const existingConfig = location.state.configurationData;
          
          // Map the existing configuration data to our interface
          console.log('Loading configuration data:', existingConfig);
          setConfig(prev => ({
            ...prev,
            // Basic configuration
            configName: location.state.configName || '',
            description: existingConfig.comment || '',
            dataStrategy: existingConfig.dataStrategy || prev.dataStrategy,
            databaseOnly: existingConfig.databaseOnly || false,
            migrateNonNative: existingConfig.migrateNonNative || true,
            
            // ACID Migration settings
            migrateACID: {
              on: existingConfig.migrateACID?.on || false,
              only: existingConfig.migrateACID?.only || false,
              artificialBucketThreshold: existingConfig.migrateACID?.artificialBucketThreshold || 100000,
              partitionLimit: existingConfig.migrateACID?.partitionLimit || 1000,
              downgrade: existingConfig.migrateACID?.downgrade || false,
              inplace: existingConfig.migrateACID?.inplace || false,
            },
            
            // View migration settings
            migrateVIEW: {
              on: existingConfig.migrateVIEW?.on || true,
            },
            
            // Transfer settings
            transfer: {
              warehouse: {
                externalDirectory: existingConfig.transfer?.warehouse?.externalDirectory || "/user/hive/external",
                managedDirectory: existingConfig.transfer?.warehouse?.managedDirectory || "/user/hive/warehouse",
              },
              transferPrefix: existingConfig.transfer?.transferPrefix || "mig_",
              shadowPrefix: existingConfig.transfer?.shadowPrefix || "_shadow",
            },
            
            
            // Iceberg conversion settings
            icebergConversion: {
              enable: existingConfig.icebergConversion?.enable || false,
              fileTypeTranslation: existingConfig.icebergConversion?.fileTypeTranslation || "",
              version: existingConfig.icebergConversion?.version?.toString() || "2",
              tableProperties: formatTableProperties(existingConfig.icebergConversion?.tableProperties),
              inplace: existingConfig.icebergConversion?.inplace || false,
            },
            
            // Add optimization settings if they exist in the saved config
            optimization: {
              skip: existingConfig.optimization?.skip || false,
              sortDynamicPartition: existingConfig.optimization?.sortDynamicPartitionInserts || false,
              buildShadowStatistics: existingConfig.optimization?.buildShadowStatistics || false,
              compressTextOutput: existingConfig.optimization?.compressTextOutput || false,
              skipStatsCollection: existingConfig.optimization?.skipStatsCollection || false,
              autoTune: existingConfig.optimization?.autoTune || false,
            },
          }));
          
          console.log('Loaded existing configuration for editing:', location.state.configName);
        } catch (error) {
          console.error('Error processing existing configuration:', error);
          setErrors({ general: 'Failed to load existing configuration data' });
        } finally {
          setIsLoading(false);
        }
      }
    };

    loadExistingConfig();
  }, [location.state]);

  const parseTableProperties = (propertiesString: string): { [key: string]: string } => {
    if (!propertiesString || propertiesString.trim() === '') {
      return {};
    }
    
    const properties: { [key: string]: string } = {};
    const lines = propertiesString.split('\n').map(line => line.trim()).filter(line => line);
    
    for (const line of lines) {
      if (line.includes('=')) {
        const [key, ...valueParts] = line.split('=');
        const value = valueParts.join('='); // Handle values that contain '='
        if (key && key.trim()) {
          properties[key.trim()] = value ? value.trim() : '';
        }
      }
    }
    
    return properties;
  };

  const formatTableProperties = (propertiesObj: { [key: string]: string } | null | undefined): string => {
    if (!propertiesObj || typeof propertiesObj !== 'object') {
      return '';
    }
    
    return Object.entries(propertiesObj)
      .map(([key, value]) => `${key}=${value}`)
      .join('\n');
  };

  const updateConfig = (path: string, value: any) => {
    setConfig(prev => {
      const newConfig = { ...prev };
      const keys = path.split('.');
      let current: any = newConfig;
      
      for (let i = 0; i < keys.length - 1; i++) {
        if (!current[keys[i]]) {
          current[keys[i]] = {};
        }
        current = current[keys[i]];
      }
      
      current[keys[keys.length - 1]] = value;
      return newConfig;
    });
    
    // Clear error when user starts typing
    if (errors[path]) {
      setErrors(prev => ({ ...prev, [path]: '' }));
    }
  };

  const validateStep = (step: number): boolean => {
    const newErrors: { [key: string]: string } = {};
    
    switch (step) {
      case 0:
        // Migration Behavior - all fields are required checkboxes/numbers
        if (config.migrateACID.artificialBucketThreshold <= 0) {
          newErrors['migrateACID.artificialBucketThreshold'] = 'Must be a positive integer';
        }
        if (config.migrateACID.partitionLimit < 0) {
          newErrors['migrateACID.partitionLimit'] = 'Must be non-negative';
        }
        break;
      case 1:
        // Transfer and Warehouse Settings
        if (!config.transfer.warehouse.externalDirectory.trim()) {
          newErrors['transfer.warehouse.externalDirectory'] = 'External directory is required';
        }
        if (!config.transfer.warehouse.managedDirectory.trim()) {
          newErrors['transfer.warehouse.managedDirectory'] = 'Managed directory is required';
        }
        break;
      case 3:
        // Configuration Generation
        if (!config.configName.trim()) {
          newErrors['configName'] = 'Configuration name is required';
        }
        break;
    }
    
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const nextStep = () => {
    if (validateStep(currentStep)) {
      setCurrentStep(prev => Math.min(prev + 1, 3));
    }
  };

  const prevStep = () => {
    setCurrentStep(prev => Math.max(prev - 1, 0));
  };

  const saveConfiguration = async () => {
    if (!validateStep(currentStep)) return;
    
    try {
      // Transform wizard config to ConfigurationDto format
      console.log('Full config object being saved:', config);
      
      const configDto = {
        name: config.configName,
        dataStrategy: config.dataStrategy,
        comment: config.description || '',
        databaseOnly: config.databaseOnly,
        migrateNonNative: config.migrateNonNative,
        
        // Map ACID migration settings
        migrateACID: {
          on: config.migrateACID?.on || false,
          only: config.migrateACID?.only || false,
          artificialBucketThreshold: config.migrateACID?.artificialBucketThreshold || 2,
          partitionLimit: config.migrateACID?.partitionLimit || 100,
          downgrade: config.migrateACID?.downgrade || false,
          inplace: config.migrateACID?.inplace || false,
        },
        
        // Map view migration settings
        migrateVIEW: {
          on: config.migrateVIEW?.on || false,
        },
        
        // Map transfer settings
        transfer: {
          warehouse: {
            externalDirectory: config.transfer?.warehouse?.externalDirectory || '',
            managedDirectory: config.transfer?.warehouse?.managedDirectory || '',
          },
          transferPrefix: config.transfer?.transferPrefix || '',
          shadowPrefix: config.transfer?.shadowPrefix || '',
        },
        
        
        // Map Iceberg conversion settings
        icebergConversion: {
          enable: config.icebergConversion?.enable || false,
          fileTypeTranslation: config.icebergConversion?.fileTypeTranslation || 'STANDARD',
          version: parseInt(config.icebergConversion?.version || '2'),
          tableProperties: parseTableProperties(config.icebergConversion?.tableProperties || ''),
          inplace: config.icebergConversion?.inplace || false,
        },
        
        // Map optimization settings
        optimization: {
          skip: config.optimization?.skip || false,
          sortDynamicPartitionInserts: config.optimization?.sortDynamicPartition || false,
          buildShadowStatistics: config.optimization?.buildShadowStatistics || false,
          compressTextOutput: config.optimization?.compressTextOutput || false,
          skipStatsCollection: config.optimization?.skipStatsCollection || false,
          autoTune: config.optimization?.autoTune || false,
          overrides: {
            left: {},
            right: {}
          }
        },
      };
      
      console.log('ConfigDto being sent to API:', configDto);
      
      const isEditing = location.state?.isEditing;
      let response;
      
      if (isEditing) {
        // Update existing configuration using only config name (no data strategy prefix)
        response = await fetch(`/hms-mirror/api/v1/config/${config.configName}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(configDto),
        });
      } else {
        // Create new configuration
        response = await fetch('/hms-mirror/api/v1/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(configDto),
        });
      }
      
      if (response.ok) {
        const result = await response.json();
        if (result.status === 'SUCCESS') {
          const action = isEditing ? 'updated' : 'created';
          // Navigate back to configuration management
          navigate('/config', { 
            state: { 
              message: `${config.dataStrategy} configuration "${config.configName}" ${action} successfully` 
            }
          });
        } else {
          throw new Error(result.message || 'Save failed');
        }
      } else {
        throw new Error('Network error saving configuration');
      }
    } catch (error) {
      console.error('Error saving configuration:', error);
      alert('Failed to save configuration. Please check the application logs.');
    }
  };


  const renderStep1 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Migration Behavior</h3>
        <p className="text-sm text-gray-600 mb-6">Configure basic migration behavior and table type handling.</p>
        
        <div className="space-y-4">
          <div className="flex items-center">
            <input
              type="checkbox"
              id="databaseOnly"
              checked={config.databaseOnly}
              onChange={(e) => updateConfig('databaseOnly', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="databaseOnly" className="ml-2 block text-sm text-gray-900">
              Database Only Mode
            </label>
          </div>
          <p className="text-xs text-gray-500 ml-6">Migrate only database schemas (no tables/data)</p>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              id="migrateVIEW"
              checked={config.migrateVIEW.on}
              onChange={(e) => updateConfig('migrateVIEW.on', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="migrateVIEW" className="ml-2 block text-sm text-gray-900">
              Migrate Views
            </label>
          </div>
          <p className="text-xs text-gray-500 ml-6">Include view migration</p>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              id="migrateNonNative"
              checked={config.migrateNonNative}
              onChange={(e) => updateConfig('migrateNonNative', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="migrateNonNative" className="ml-2 block text-sm text-gray-900">
              Migrate Non-Native Tables
            </label>
          </div>
          <p className="text-xs text-gray-500 ml-6">Handle non-Hive tables (e.g., JDBC)</p>
        </div>
      </div>

      <div>
        <h4 className="text-md font-medium text-gray-900 mb-4">ACID Migration Settings</h4>
        
        <div className="space-y-4">
          <div className="flex items-center">
            <input
              type="checkbox"
              id="migrateACID"
              checked={config.migrateACID.on}
              onChange={(e) => updateConfig('migrateACID.on', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="migrateACID" className="ml-2 block text-sm text-gray-900">
              Enable ACID Migration
            </label>
          </div>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              id="migrateACIDOnly"
              checked={config.migrateACID.only}
              onChange={(e) => updateConfig('migrateACID.only', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="migrateACIDOnly" className="ml-2 block text-sm text-gray-900">
              ACID-Only Mode
            </label>
          </div>
          
          <div>
            <label htmlFor="artificialBucketThreshold" className="block text-sm font-medium text-gray-700">
              Artificial Bucket Threshold
            </label>
            <input
              type="number"
              id="artificialBucketThreshold"
              value={config.migrateACID.artificialBucketThreshold}
              onChange={(e) => updateConfig('migrateACID.artificialBucketThreshold', parseInt(e.target.value) || 0)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
            />
            {errors['migrateACID.artificialBucketThreshold'] && (
              <p className="mt-1 text-sm text-red-600">{errors['migrateACID.artificialBucketThreshold']}</p>
            )}
          </div>
          
          <div>
            <label htmlFor="partitionLimit" className="block text-sm font-medium text-gray-700">
              ACID Partition Limit
            </label>
            <input
              type="number"
              id="partitionLimit"
              value={config.migrateACID.partitionLimit}
              onChange={(e) => updateConfig('migrateACID.partitionLimit', parseInt(e.target.value) || 0)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
            />
            {errors['migrateACID.partitionLimit'] && (
              <p className="mt-1 text-sm text-red-600">{errors['migrateACID.partitionLimit']}</p>
            )}
          </div>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              id="downgrade"
              checked={config.migrateACID.downgrade}
              onChange={(e) => updateConfig('migrateACID.downgrade', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="downgrade" className="ml-2 block text-sm text-gray-900">
              Downgrade ACID Tables
            </label>
          </div>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              id="inplace"
              checked={config.migrateACID.inplace}
              onChange={(e) => updateConfig('migrateACID.inplace', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="inplace" className="ml-2 block text-sm text-gray-900">
              In-Place ACID Migration
            </label>
          </div>
        </div>
      </div>
    </div>
  );

  const renderStep2 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Transfer and Warehouse Settings</h3>
        <p className="text-sm text-gray-600 mb-6">Core settings for data transfer paths and warehouses.</p>
        
        <div className="space-y-4">
          <div>
            <label htmlFor="externalDirectory" className="block text-sm font-medium text-gray-700">
              External Directory
            </label>
            <input
              type="text"
              id="externalDirectory"
              value={config.transfer.warehouse.externalDirectory}
              onChange={(e) => updateConfig('transfer.warehouse.externalDirectory', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              placeholder="/user/hive/external"
            />
            {errors['transfer.warehouse.externalDirectory'] && (
              <p className="mt-1 text-sm text-red-600">{errors['transfer.warehouse.externalDirectory']}</p>
            )}
            <p className="mt-1 text-xs text-gray-500">External table base path</p>
          </div>
          
          <div>
            <label htmlFor="managedDirectory" className="block text-sm font-medium text-gray-700">
              Managed Directory
            </label>
            <input
              type="text"
              id="managedDirectory"
              value={config.transfer.warehouse.managedDirectory}
              onChange={(e) => updateConfig('transfer.warehouse.managedDirectory', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              placeholder="/user/hive/warehouse"
            />
            {errors['transfer.warehouse.managedDirectory'] && (
              <p className="mt-1 text-sm text-red-600">{errors['transfer.warehouse.managedDirectory']}</p>
            )}
            <p className="mt-1 text-xs text-gray-500">Managed table base path</p>
          </div>
          
        </div>
      </div>

      <div>
        <h4 className="text-md font-medium text-gray-900 mb-4">Advanced Transfer Settings (Optional)</h4>
        
        <div className="space-y-4">
          <div>
            <label htmlFor="transferPrefix" className="block text-sm font-medium text-gray-700">
              Transfer Prefix
            </label>
            <input
              type="text"
              id="transferPrefix"
              value={config.transfer.transferPrefix}
              onChange={(e) => updateConfig('transfer.transferPrefix', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              placeholder="mig_"
            />
            <p className="mt-1 text-xs text-gray-500">Prefix for migrated objects</p>
          </div>
          
          <div>
            <label htmlFor="shadowPrefix" className="block text-sm font-medium text-gray-700">
              Shadow Prefix
            </label>
            <input
              type="text"
              id="shadowPrefix"
              value={config.transfer.shadowPrefix}
              onChange={(e) => updateConfig('transfer.shadowPrefix', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              placeholder="_shadow"
            />
            <p className="mt-1 text-xs text-gray-500">Prefix for shadow objects</p>
          </div>
        </div>
      </div>
    </div>
  );

  const renderStep3 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Conversions (Optional)</h3>
        <p className="text-sm text-gray-600 mb-6">Configure table format conversions, including Iceberg.</p>
        
        <div className="space-y-4">
          <div className="flex items-center">
            <input
              type="checkbox"
              id="enableIceberg"
              checked={config.icebergConversion.enable}
              onChange={(e) => updateConfig('icebergConversion.enable', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor="enableIceberg" className="ml-2 block text-sm text-gray-900">
              Enable Iceberg Conversion
            </label>
          </div>
          <p className="text-xs text-gray-500 ml-6">Activates Iceberg conversion</p>
          
          {config.icebergConversion.enable && (
            <>
              <div>
                <label htmlFor="fileTypeTranslation" className="block text-sm font-medium text-gray-700">
                  File Type Translation
                </label>
                <textarea
                  id="fileTypeTranslation"
                  rows={3}
                  value={config.icebergConversion.fileTypeTranslation}
                  onChange={(e) => updateConfig('icebergConversion.fileTypeTranslation', e.target.value)}
                  className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="ORC:Parquet"
                />
                <p className="mt-1 text-xs text-gray-500">Map source formats to Iceberg (e.g., "ORC:Parquet")</p>
              </div>
              
              <div>
                <label htmlFor="icebergVersion" className="block text-sm font-medium text-gray-700">
                  Iceberg Version
                </label>
                <select
                  id="icebergVersion"
                  value={config.icebergConversion.version}
                  onChange={(e) => updateConfig('icebergConversion.version', e.target.value)}
                  className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="1">1</option>
                  <option value="2">2</option>
                  <option value="3">3</option>
                </select>
                <p className="mt-1 text-xs text-gray-500">Target Iceberg spec version</p>
              </div>
              
              <div>
                <label htmlFor="tableProperties" className="block text-sm font-medium text-gray-700">
                  Table Properties
                </label>
                <textarea
                  id="tableProperties"
                  rows={3}
                  value={config.icebergConversion.tableProperties}
                  onChange={(e) => updateConfig('icebergConversion.tableProperties', e.target.value)}
                  className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="write.format.default=parquet"
                />
                <p className="mt-1 text-xs text-gray-500">Custom properties (e.g., "write.format.default=parquet")</p>
              </div>
              
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="icebergInplace"
                  checked={config.icebergConversion.inplace}
                  onChange={(e) => updateConfig('icebergConversion.inplace', e.target.checked)}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="icebergInplace" className="ml-2 block text-sm text-gray-900">
                  In-Place Conversion
                </label>
              </div>
              <p className="text-xs text-gray-500 ml-6">Convert without data copy</p>
            </>
          )}
        </div>
      </div>
    </div>
  );

  const renderStep4 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Configuration Generation</h3>
        <p className="text-sm text-gray-600 mb-6">Review and save the {config.dataStrategy} migration configuration.</p>
        
        <div className="bg-gray-50 rounded-lg p-4 mb-6">
          <h4 className="text-md font-medium text-gray-900 mb-3">Configuration Summary</h4>
          <div className="text-sm text-gray-600 space-y-2">
            <div>• Migration behavior: {config.databaseOnly ? 'Database only' : 'Full migration'}</div>
            <div>• ACID migration: {config.migrateACID.on ? 'Enabled' : 'Disabled'}</div>
            <div>• External directory: {config.transfer.warehouse.externalDirectory}</div>
            <div>• Managed directory: {config.transfer.warehouse.managedDirectory}</div>
            {config.icebergConversion.enable && (
              <div>• Iceberg conversion: Enabled (Version {config.icebergConversion.version})</div>
            )}
          </div>
        </div>
        
        <div className="space-y-4">
          <div>
            <label htmlFor="configName" className="block text-sm font-medium text-gray-700">
              Configuration Name *
            </label>
            <input
              type="text"
              id="configName"
              value={config.configName}
              onChange={(e) => updateConfig('configName', e.target.value)}
              readOnly={location.state?.isEditing}
              className={`mt-1 block w-full border-gray-300 rounded-md shadow-sm ${
                location.state?.isEditing 
                  ? 'bg-gray-100 text-gray-600 cursor-not-allowed' 
                  : 'focus:ring-blue-500 focus:border-blue-500'
              }`}
              placeholder={location.state?.isCopying ? `Copy of ${location.state.configName || config.dataStrategy} Configuration` : `My ${config.dataStrategy} Configuration`}
            />
            {errors['configName'] && (
              <p className="mt-1 text-sm text-red-600">{errors['configName']}</p>
            )}
          </div>
          
          <div>
            <label htmlFor="description" className="block text-sm font-medium text-gray-700">
              Description
            </label>
            <textarea
              id="description"
              rows={3}
              value={config.description}
              onChange={(e) => updateConfig('description', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              placeholder="Optional description of the configuration purpose"
            />
          </div>
        </div>
      </div>
    </div>
  );

  const steps = [
    { id: 'migration', title: 'Migration', component: renderStep1 },
    { id: 'transfer', title: 'Transfer', component: renderStep2 },
    { id: 'conversions', title: 'Conversions', component: renderStep3 },
    { id: 'config', title: 'Config', component: renderStep4 },
  ];

  if (isLoading) {
    return (
      <div className="max-w-4xl mx-auto p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-gray-500">Loading configuration...</div>
        </div>
      </div>
    );
  }

  const isEditing = location.state?.isEditing;
  const isCopying = location.state?.isCopying;

  return (
    <div className="max-w-4xl mx-auto p-6">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">
          {isEditing ? 'Edit' : isCopying ? 'Copy' : 'Create'} {config.dataStrategy} Configuration
        </h1>
        <p className="text-gray-600 mt-2">
          {isEditing 
            ? `Editing ${config.dataStrategy} configuration: ${config.configName || 'Unknown'}`
            : isCopying
            ? `Creating a copy of ${config.dataStrategy} configuration. Please provide a new name.`
            : `Configure ${config.dataStrategy} migration settings for HMS-Mirror`
          }
        </p>
        {errors.general && (
          <div className="mt-2 text-sm text-red-600">{errors.general}</div>
        )}
      </div>

      {/* Progress Steps */}
      <div className="mb-8">
        <WizardProgress 
          steps={steps} 
          currentStep={currentStep} 
        />
      </div>

      {/* Current Step Content */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        {steps[currentStep].component()}
      </div>

      {/* Navigation Buttons */}
      <div className="flex justify-between">
        <button
          onClick={prevStep}
          disabled={currentStep === 0}
          className={`flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium ${
            currentStep === 0
              ? 'text-gray-400 cursor-not-allowed'
              : 'text-gray-700 bg-white hover:bg-gray-50'
          }`}
        >
          <ChevronLeftIcon className="w-4 h-4 mr-2" />
          Previous
        </button>
        
        {currentStep < 3 ? (
          <button
            onClick={nextStep}
            className="flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700"
          >
            Next
            <ChevronRightIcon className="w-4 h-4 ml-2" />
          </button>
        ) : (
          <button
            onClick={saveConfiguration}
            className="flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-green-600 hover:bg-green-700"
          >
            Save Configuration
            <CheckIcon className="w-4 h-4 ml-2" />
          </button>
        )}
      </div>
    </div>
  );
};

export default ConfigWizard;