import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { ChevronLeftIcon, ChevronRightIcon, CheckIcon, ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';
import WizardProgress from '../connections/wizard/WizardProgress';
import { configApi } from '../../services/api/configApi';
import type { ConfigLiteDto } from '../../types/ConfigLite';
import FieldWithTooltip from '../common/FieldWithTooltip';
import schemaService from '../../services/schemaService';

interface HmsMirrorConfig {
  // Migration Behavior
  migrateVIEW: { on: boolean };
  migrateNonNative: boolean;

  // Table Behavior
  createIfNotExists: boolean;
  enableAutoTableStats: boolean;
  enableAutoColumnStats: boolean;
  saveWorkingTables: boolean;

  // File and Data Handling
  copyAvroSchemaUrls: boolean;

  // Ownership Transfer
  ownershipTransfer: {
    database: boolean;
    table: boolean;
  };

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
    transferPrefix: string;
    shadowPrefix: string;
    storageMigrationPostfix: string;
    exportBaseDirPrefix: string;
    remoteWorkingDirectory: string;
    intermediateStorage?: string;
    warehouse: {
      source: 'GLOBAL' | 'RELATIVE';
      externalDirectory: string;
      managedDirectory: string;
    };
    storageMigration?: {
      translationType: 'ALIGNED' | 'RELATIVE' | 'NAMESPACE';
      dataMovementStrategy: 'SQL' | 'EXPORT_IMPORT' | 'DISTCP';
      dataFlow: 'PULL' | 'PUSH';
      skipDatabaseLocationAdjustments: boolean;
      createArchive: boolean;
      consolidateTablesForDistcp: boolean;
      strict: boolean;
    };
  };

  // Force external location in table DDLs
  forceExternalLocation: boolean;

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
    migrateVIEW: { on: false },
    migrateNonNative: false,
    createIfNotExists: true,
    enableAutoTableStats: false,
    enableAutoColumnStats: false,
    saveWorkingTables: false,
    copyAvroSchemaUrls: false,
    ownershipTransfer: {
      database: false,
      table: false,
    },
    migrateACID: {
      on: false,
      only: false,
      artificialBucketThreshold: 2,
      partitionLimit: 500,
      downgrade: false,
      inplace: false,
    },
    transfer: {
      transferPrefix: "hms_mirror_transfer_",
      shadowPrefix: "hms_mirror_shadow_",
      storageMigrationPostfix: "_storage_migration",
      exportBaseDirPrefix: "/apps/hive/warehouse/export_",
      remoteWorkingDirectory: "hms_mirror_working",
      intermediateStorage: undefined,
      warehouse: {
        source: "GLOBAL",
        externalDirectory: "/user/hive/external",
        managedDirectory: "/user/hive/warehouse",
      },
      storageMigration: {
        translationType: "RELATIVE",
        dataMovementStrategy: "SQL",
        dataFlow: "PULL",
        skipDatabaseLocationAdjustments: false,
        createArchive: false,
        consolidateTablesForDistcp: false,
        strict: false,
      },
    },
    forceExternalLocation: false,
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
  const [isAdvancedTransferCollapsed, setIsAdvancedTransferCollapsed] = useState(true);
  const [schemaDescriptions, setSchemaDescriptions] = useState<Map<string, string>>(new Map());

  // Fetch schema descriptions on mount
  useEffect(() => {
    const fetchDescriptions = async () => {
      const descriptions = await schemaService.getClassDescriptions('ConfigLiteDto');
      setSchemaDescriptions(descriptions);
    };
    fetchDescriptions();
  }, []);

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
            description: existingConfig.description || '',
            dataStrategy: existingConfig.dataStrategy || prev.dataStrategy,
            migrateNonNative: existingConfig.migrateNonNative !== undefined ? existingConfig.migrateNonNative : false,

            // Table Behavior settings
            createIfNotExists: existingConfig.createIfNotExists !== undefined ? existingConfig.createIfNotExists : true,
            enableAutoTableStats: existingConfig.enableAutoTableStats || false,
            enableAutoColumnStats: existingConfig.enableAutoColumnStats || false,
            saveWorkingTables: existingConfig.saveWorkingTables || false,

            // File and Data Handling
            copyAvroSchemaUrls: existingConfig.copyAvroSchemaUrls || false,

            // Ownership Transfer
            ownershipTransfer: {
              database: existingConfig.ownershipTransfer?.database || false,
              table: existingConfig.ownershipTransfer?.table || false,
            },

            // ACID Migration settings
            migrateACID: {
              on: existingConfig.migrateACID?.on || false,
              only: existingConfig.migrateACID?.only || false,
              artificialBucketThreshold: existingConfig.migrateACID?.artificialBucketThreshold || 2,
              partitionLimit: existingConfig.migrateACID?.partitionLimit || 500,
              downgrade: existingConfig.migrateACID?.downgrade || false,
              inplace: existingConfig.migrateACID?.inplace || false,
            },

            // View migration settings
            migrateVIEW: {
              on: existingConfig.migrateVIEW?.on || false,
            },

            // Transfer settings
            transfer: {
              transferPrefix: existingConfig.transfer?.transferPrefix || "hms_mirror_transfer_",
              shadowPrefix: existingConfig.transfer?.shadowPrefix || "hms_mirror_shadow_",
              storageMigrationPostfix: existingConfig.transfer?.storageMigrationPostfix || "_storage_migration",
              exportBaseDirPrefix: existingConfig.transfer?.exportBaseDirPrefix || "/apps/hive/warehouse/export_",
              remoteWorkingDirectory: existingConfig.transfer?.remoteWorkingDirectory || "hms_mirror_working",
              intermediateStorage: existingConfig.transfer?.intermediateStorage || undefined,
              warehouse: {
                source: existingConfig.transfer?.warehouse?.source || "GLOBAL",
                externalDirectory: existingConfig.transfer?.warehouse?.externalDirectory || "/user/hive/external",
                managedDirectory: existingConfig.transfer?.warehouse?.managedDirectory || "/user/hive/warehouse",
              },
              storageMigration: {
                translationType: existingConfig.transfer?.storageMigration?.translationType || "RELATIVE",
                dataMovementStrategy: existingConfig.transfer?.storageMigration?.dataMovementStrategy || "SQL",
                dataFlow: existingConfig.transfer?.storageMigration?.dataFlow || "PULL",
                skipDatabaseLocationAdjustments: existingConfig.transfer?.storageMigration?.skipDatabaseLocationAdjustments || false,
                createArchive: existingConfig.transfer?.storageMigration?.createArchive || false,
                consolidateTablesForDistcp: existingConfig.transfer?.storageMigration?.consolidateTablesForDistcp || false,
                strict: existingConfig.transfer?.storageMigration?.strict || false,
              },
            },

            // Force external location setting
            forceExternalLocation: existingConfig.forceExternalLocation || false,

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

  // Fetch schema descriptions on mount
  useEffect(() => {
    const fetchDescriptions = async () => {
      // Fetch descriptions from ConfigLiteDto and all nested classes
      const [configDesc, migrateACIDDesc, transferDesc, icebergDesc, ownershipDesc, migrateVIEWDesc] = await Promise.all([
        schemaService.getClassDescriptions('ConfigLiteDto'),
        schemaService.getClassDescriptions('MigrateACID'),
        schemaService.getClassDescriptions('Transfer'),
        schemaService.getClassDescriptions('IcebergConversion'),
        schemaService.getClassDescriptions('OwnershipTransfer'),
        schemaService.getClassDescriptions('MigrateVIEW')
      ]);

      // Merge all descriptions into a single map with prefixed keys for nested objects
      const allDescriptions = new Map<string, string>();

      // Add ConfigLiteDto top-level fields
      configDesc.forEach((value, key) => allDescriptions.set(key, value));

      // Add MigrateACID fields with prefix
      migrateACIDDesc.forEach((value, key) => allDescriptions.set(`migrateACID.${key}`, value));

      // Add Transfer fields with prefix
      transferDesc.forEach((value, key) => allDescriptions.set(`transfer.${key}`, value));

      // Add IcebergConversion fields with prefix
      icebergDesc.forEach((value, key) => allDescriptions.set(`icebergConversion.${key}`, value));

      // Add OwnershipTransfer fields with prefix
      ownershipDesc.forEach((value, key) => allDescriptions.set(`ownershipTransfer.${key}`, value));

      // Add MigrateVIEW fields with prefix
      migrateVIEWDesc.forEach((value, key) => allDescriptions.set(`migrateVIEW.${key}`, value));

      setSchemaDescriptions(allDescriptions);
    };
    fetchDescriptions();
  }, []);

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
        // Config Details - name is required
        if (!config.configName.trim()) {
          newErrors['configName'] = 'Configuration name is required';
        }
        break;
      case 1:
        // Migration Behavior - no required validation
        break;
      case 2:
        // ACID Migration Settings - validate only if ACID is enabled
        if (config.migrateACID.on) {
          if (config.migrateACID.artificialBucketThreshold <= 0) {
            newErrors['migrateACID.artificialBucketThreshold'] = 'Must be a positive integer';
          }
          if (config.migrateACID.partitionLimit < 0) {
            newErrors['migrateACID.partitionLimit'] = 'Must be non-negative';
          }
        }
        break;
      case 3:
        // Transfer and Warehouse Settings
        if (!config.transfer.warehouse.externalDirectory.trim()) {
          newErrors['transfer.warehouse.externalDirectory'] = 'External directory is required';
        }
        if (!config.transfer.warehouse.managedDirectory.trim()) {
          newErrors['transfer.warehouse.managedDirectory'] = 'Managed directory is required';
        }
        break;
      case 4:
        // Conversions - no required fields
        break;
      case 5:
        // Review - all validation already done
        break;
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const nextStep = () => {
    if (validateStep(currentStep)) {
      setCurrentStep(prev => Math.min(prev + 1, 5));
    }
  };

  const prevStep = () => {
    setCurrentStep(prev => Math.max(prev - 1, 0));
  };

  const saveConfiguration = async () => {
    if (!validateStep(currentStep)) return;

    try {
      // Transform wizard config to ConfigLiteDto format
      console.log('Full config object being saved:', config);

      const configDto: ConfigLiteDto = {
        name: config.configName,
        description: config.description || '',
        migrateNonNative: config.migrateNonNative,

        // Table Behavior settings
        createIfNotExists: config.createIfNotExists,
        enableAutoTableStats: config.enableAutoTableStats,
        enableAutoColumnStats: config.enableAutoColumnStats,
        saveWorkingTables: config.saveWorkingTables,

        // File and Data Handling
        copyAvroSchemaUrls: config.copyAvroSchemaUrls,

        // Ownership Transfer
        ownershipTransfer: {
          database: config.ownershipTransfer?.database || false,
          table: config.ownershipTransfer?.table || false,
        },

        // ACID migration settings
        migrateACID: {
          on: config.migrateACID?.on || false,
          only: config.migrateACID?.only || false,
          artificialBucketThreshold: config.migrateACID?.artificialBucketThreshold || 2,
          partitionLimit: config.migrateACID?.partitionLimit || 500,
          downgrade: config.migrateACID?.downgrade || false,
          inplace: config.migrateACID?.inplace || false,
        },

        // View migration settings
        migrateVIEW: {
          on: config.migrateVIEW?.on || false,
        },

        // Transfer settings
        transfer: {
          transferPrefix: config.transfer?.transferPrefix || 'hms_mirror_transfer_',
          shadowPrefix: config.transfer?.shadowPrefix || 'hms_mirror_shadow_',
          storageMigrationPostfix: config.transfer?.storageMigrationPostfix || '_storage_migration',
          exportBaseDirPrefix: config.transfer?.exportBaseDirPrefix || '/apps/hive/warehouse/export_',
          remoteWorkingDirectory: config.transfer?.remoteWorkingDirectory || 'hms_mirror_working',
          intermediateStorage: config.transfer?.intermediateStorage || undefined,
          warehouse: {
            source: config.transfer?.warehouse?.source as any || 'GLOBAL',
            externalDirectory: config.transfer?.warehouse?.externalDirectory || undefined,
            managedDirectory: config.transfer?.warehouse?.managedDirectory || undefined,
          },
          storageMigration: {
            translationType: config.transfer?.storageMigration?.translationType as any || 'RELATIVE',
            dataMovementStrategy: config.transfer?.storageMigration?.dataMovementStrategy as any || 'SQL',
            dataFlow: config.transfer?.storageMigration?.dataFlow as any || 'PULL',
            skipDatabaseLocationAdjustments: config.transfer?.storageMigration?.skipDatabaseLocationAdjustments || false,
            createArchive: config.transfer?.storageMigration?.createArchive || false,
            consolidateTablesForDistcp: config.transfer?.storageMigration?.consolidateTablesForDistcp || false,
            strict: config.transfer?.storageMigration?.strict || false,
          },
        },

        // Force external location setting
        forceExternalLocation: config.forceExternalLocation || false,

        // Iceberg conversion settings
        icebergConversion: {
          enable: config.icebergConversion?.enable || false,
          fileTypeTranslation: config.icebergConversion?.fileTypeTranslation as any || 'STANDARD',
          version: parseInt(config.icebergConversion?.version || '2'),
          tableProperties: parseTableProperties(config.icebergConversion?.tableProperties || ''),
          inplace: config.icebergConversion?.inplace || false,
        },

        // Optimization settings
        optimization: {
          skip: config.optimization?.skip || false,
          sortDynamicPartitionInserts: config.optimization?.sortDynamicPartition || false,
          buildShadowStatistics: config.optimization?.buildShadowStatistics || false,
          compressTextOutput: config.optimization?.compressTextOutput || false,
          skipStatsCollection: config.optimization?.skipStatsCollection || false,
          autoTune: config.optimization?.autoTune || false,
          overrides: {
            properties: {}
          }
        },
      };

      console.log('ConfigLiteDto being sent to API:', configDto);

      const isEditing = location.state?.isEditing;
      let result;

      if (isEditing) {
        // Update existing configuration
        result = await configApi.updateConfiguration(config.configName, configDto);
      } else {
        // Create new configuration
        result = await configApi.saveConfiguration(configDto);
      }

      if (result.success) {
        const action = isEditing ? 'updated' : 'created';
        console.log('Configuration saved successfully, navigating to /config');
        // Navigate back to configuration management
        navigate('/config', {
          state: {
            message: `Configuration "${config.configName}" ${action} successfully`
          }
        });
      } else {
        console.error('Save failed:', result.message);
        throw new Error(result.message || 'Save failed');
      }
    } catch (error) {
      console.error('Error saving configuration:', error);
      alert('Failed to save configuration. Please check the application logs.');
    }
  };

  const renderStep0 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Configuration Details</h3>
        <p className="text-sm text-gray-600 mb-6">Review and save the migration configuration.</p>

        <div className="space-y-4">
          <div>
            <FieldWithTooltip
              label="Configuration Name"
              tooltip={schemaDescriptions.get('name')}
              required={true}
              htmlFor="configName"
              className="mb-2"
            />
            <input
              type="text"
              id="configName"
              value={config.configName}
              onChange={(e) => updateConfig('configName', e.target.value)}
              readOnly={location.state?.isEditing}
              className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                errors['configName'] ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
              } ${location.state?.isEditing ? 'bg-gray-100 cursor-not-allowed' : ''}`}
              placeholder={location.state?.isCopying ? `Copy of ${location.state.configName || ''} Configuration` : `My Migration Configuration`}
            />
            {errors['configName'] && (
              <p className="mt-1 text-sm text-red-600">{errors['configName']}</p>
            )}
            {location.state?.isEditing && (
              <p className="mt-1 text-sm text-gray-500">Configuration name cannot be changed when editing</p>
            )}
          </div>

          <div>
            <FieldWithTooltip
              label="Description"
              tooltip={schemaDescriptions.get('description')}
              htmlFor="description"
              className="mb-2"
            />
            <textarea
              id="description"
              rows={3}
              value={config.description}
              onChange={(e) => updateConfig('description', e.target.value)}
              className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
              placeholder="Optional description of the configuration purpose"
            />
          </div>
        </div>
      </div>
    </div>
  );

  const renderStep1 = () => (
    <div className="space-y-6">
      {/* Page Header */}
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-2">Migration Behavior</h3>
        <p className="text-sm text-gray-600 mb-6">Configure basic migration behavior and table type handling.</p>
      </div>

      {/* Two-column layout for all sections */}
      <div className="grid grid-cols-2 gap-x-8 gap-y-6">
        {/* Left Column */}
        <div className="space-y-6">
          {/* Migration Behavior Section */}
          <div>
            <h4 className="text-md font-medium text-gray-900 mb-4">Basic Migration</h4>
            <div className="space-y-4">
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="migrateVIEW"
                    checked={config.migrateVIEW.on}
                    onChange={(e) => updateConfig('migrateVIEW.on', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Migrate Views"
                      tooltip={schemaDescriptions.get('migrateVIEW.on')}
                      htmlFor="migrateVIEW"
                    />
                  </div>
                </div>
              </div>

              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="migrateNonNative"
                    checked={config.migrateNonNative}
                    onChange={(e) => updateConfig('migrateNonNative', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Migrate Non-Native Tables"
                      tooltip={schemaDescriptions.get('migrateNonNative')}
                      htmlFor="migrateNonNative"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Table Behavior Section */}
          <div>
            <h4 className="text-md font-medium text-gray-900 mb-4">Table Behavior</h4>
            <div className="space-y-4">
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="createIfNotExists"
                    checked={config.createIfNotExists}
                    onChange={(e) => updateConfig('createIfNotExists', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Create If Not Exists"
                      tooltip={schemaDescriptions.get('createIfNotExists')}
                      htmlFor="createIfNotExists"
                    />
                  </div>
                </div>
              </div>

              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="enableAutoTableStats"
                    checked={config.enableAutoTableStats}
                    onChange={(e) => updateConfig('enableAutoTableStats', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Enable Auto Table Stats"
                      tooltip={schemaDescriptions.get('enableAutoTableStats')}
                      htmlFor="enableAutoTableStats"
                    />
                  </div>
                </div>
              </div>

              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="enableAutoColumnStats"
                    checked={config.enableAutoColumnStats}
                    onChange={(e) => updateConfig('enableAutoColumnStats', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Enable Auto Column Stats"
                      tooltip={schemaDescriptions.get('enableAutoColumnStats')}
                      htmlFor="enableAutoColumnStats"
                    />
                  </div>
                </div>
              </div>

              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="saveWorkingTables"
                    checked={config.saveWorkingTables}
                    onChange={(e) => updateConfig('saveWorkingTables', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Save Working Tables"
                      tooltip={schemaDescriptions.get('saveWorkingTables')}
                      htmlFor="saveWorkingTables"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Right Column */}
        <div className="space-y-6">
          {/* File and Data Handling Section */}
          <div>
            <h4 className="text-md font-medium text-gray-900 mb-4">File and Data Handling</h4>
            <div className="space-y-4">
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="copyAvroSchemaUrls"
                    checked={config.copyAvroSchemaUrls}
                    onChange={(e) => updateConfig('copyAvroSchemaUrls', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Copy Avro Schema URLs"
                      tooltip={schemaDescriptions.get('copyAvroSchemaUrls')}
                      htmlFor="copyAvroSchemaUrls"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Ownership Transfer Section */}
          <div>
            <h4 className="text-md font-medium text-gray-900 mb-4">Ownership Transfer</h4>
            <div className="space-y-4">
              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="ownershipTransferDatabase"
                    checked={config.ownershipTransfer.database}
                    onChange={(e) => updateConfig('ownershipTransfer.database', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Transfer Database Ownership"
                      tooltip={schemaDescriptions.get('ownershipTransfer')}
                      htmlFor="ownershipTransferDatabase"
                    />
                  </div>
                </div>
              </div>

              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="ownershipTransferTable"
                    checked={config.ownershipTransfer.table}
                    onChange={(e) => updateConfig('ownershipTransfer.table', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="Transfer Table Ownership"
                      tooltip={schemaDescriptions.get('ownershipTransfer')}
                      htmlFor="ownershipTransferTable"
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  const renderStep2 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">ACID Migration Settings</h3>
        <p className="text-sm text-gray-600 mb-6">Configure ACID table migration behavior and limits.</p>

        <div className="space-y-4">
          <div>
            <div className="flex items-center">
              <input
                type="checkbox"
                id="migrateACID"
                checked={config.migrateACID.on}
                onChange={(e) => updateConfig('migrateACID.on', e.target.checked)}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <div className="ml-2">
                <FieldWithTooltip
                  label="Enable ACID Migrations"
                  tooltip={schemaDescriptions.get('migrateACID.on')}
                  htmlFor="migrateACID"
                />
              </div>
            </div>
          </div>

          {config.migrateACID.on && (
            <>
              <div className="ml-6 space-y-4 mt-4 p-4 bg-gray-50 rounded-lg">
                <div>
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id="migrateACIDOnly"
                      checked={config.migrateACID.only}
                      onChange={(e) => updateConfig('migrateACID.only', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <div className="ml-2">
                      <FieldWithTooltip
                        label="ACID-Only Mode"
                        tooltip={schemaDescriptions.get('migrateACID.only')}
                        htmlFor="migrateACIDOnly"
                      />
                    </div>
                  </div>
                </div>

                <div>
                  <FieldWithTooltip
                    label="Artificial Bucket Threshold"
                    tooltip={schemaDescriptions.get('migrateACID.artificialBucketThreshold')}
                    htmlFor="artificialBucketThreshold"
                    className="mb-2"
                  />
                  <input
                    type="number"
                    id="artificialBucketThreshold"
                    value={config.migrateACID.artificialBucketThreshold}
                    onChange={(e) => updateConfig('migrateACID.artificialBucketThreshold', parseInt(e.target.value) || 0)}
                    className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                      errors['migrateACID.artificialBucketThreshold'] ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
                    }`}
                  />
                  {errors['migrateACID.artificialBucketThreshold'] && (
                    <p className="mt-1 text-sm text-red-600">{errors['migrateACID.artificialBucketThreshold']}</p>
                  )}
                </div>

                <div>
                  <FieldWithTooltip
                    label="ACID Partition Limit"
                    tooltip={schemaDescriptions.get('migrateACID.partitionLimit')}
                    htmlFor="partitionLimit"
                    className="mb-2"
                  />
                  <input
                    type="number"
                    id="partitionLimit"
                    value={config.migrateACID.partitionLimit}
                    onChange={(e) => updateConfig('migrateACID.partitionLimit', parseInt(e.target.value) || 0)}
                    className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                      errors['migrateACID.partitionLimit'] ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
                    }`}
                  />
                  {errors['migrateACID.partitionLimit'] && (
                    <p className="mt-1 text-sm text-red-600">{errors['migrateACID.partitionLimit']}</p>
                  )}
                </div>

                <div>
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id="downgrade"
                      checked={config.migrateACID.downgrade}
                      onChange={(e) => updateConfig('migrateACID.downgrade', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <div className="ml-2">
                      <FieldWithTooltip
                        label="Downgrade ACID Tables"
                        tooltip={schemaDescriptions.get('migrateACID.downgrade')}
                        htmlFor="downgrade"
                      />
                    </div>
                  </div>
                </div>

                <div>
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id="inplace"
                      checked={config.migrateACID.inplace}
                      onChange={(e) => updateConfig('migrateACID.inplace', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <div className="ml-2">
                      <FieldWithTooltip
                        label="In-Place ACID Migration"
                        tooltip={schemaDescriptions.get('migrateACID.inplace')}
                        htmlFor="inplace"
                      />
                    </div>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );

  const renderStep3 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Transfer and Warehouse Settings</h3>
        <p className="text-sm text-gray-600 mb-6">Core settings for data transfer paths and warehouses.</p>
      </div>

      {/* Storage Migration Settings - 2 Column Layout */}
      <div>
        <h4 className="text-md font-medium text-gray-900 mb-4">Storage Migration Settings</h4>
        <p className="text-sm text-gray-600 mb-4">Configure how data is moved during storage migration.</p>

        <div className="grid grid-cols-2 gap-x-6 gap-y-4">
          <div>
            <FieldWithTooltip
              label="Translation Type"
              tooltip={schemaDescriptions.get('transfer.storageMigration.translationType')}
              htmlFor="translationType"
              className="mb-1"
            />
            <select
              id="translationType"
              value={config.transfer.storageMigration?.translationType}
              onChange={(e) => updateConfig('transfer.storageMigration.translationType', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="ALIGNED">ALIGNED</option>
              <option value="RELATIVE">RELATIVE</option>
              <option value="NAMESPACE">NAMESPACE</option>
            </select>
            <p className="mt-1 text-xs text-gray-500">Type of translation to use for location migration</p>
          </div>

          <div>
            <FieldWithTooltip
              label="Data Movement Strategy"
              tooltip={schemaDescriptions.get('transfer.storageMigration.dataMovementStrategy')}
              htmlFor="dataMovementStrategy"
              className="mb-1"
            />
            <select
              id="dataMovementStrategy"
              value={config.transfer.storageMigration?.dataMovementStrategy}
              onChange={(e) => updateConfig('transfer.storageMigration.dataMovementStrategy', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="SQL">SQL</option>
              <option value="EXPORT_IMPORT">EXPORT_IMPORT</option>
              <option value="DISTCP">DISTCP</option>
            </select>
            <p className="mt-1 text-xs text-gray-500">Strategy for moving data (SQL, EXPORT_IMPORT, or DISTCP)</p>
          </div>

          <div>
            <FieldWithTooltip
              label="Data Flow Direction"
              tooltip={schemaDescriptions.get('transfer.storageMigration.dataFlow')}
              htmlFor="dataFlow"
              className="mb-1"
            />
            <select
              id="dataFlow"
              value={config.transfer.storageMigration?.dataFlow}
              onChange={(e) => updateConfig('transfer.storageMigration.dataFlow', e.target.value)}
              className="mt-1 block w-full border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="PULL">PULL</option>
              <option value="PUSH">PUSH</option>
            </select>
            <p className="mt-1 text-xs text-gray-500">Direction for distcp jobs (where they should run from)</p>
          </div>

          <div className="flex items-start pt-6">
            <input
              type="checkbox"
              id="skipDatabaseLocationAdjustments"
              checked={config.transfer.storageMigration?.skipDatabaseLocationAdjustments || false}
              onChange={(e) => updateConfig('transfer.storageMigration.skipDatabaseLocationAdjustments', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded mt-0.5"
            />
            <div className="ml-2">
              <FieldWithTooltip
                label="Skip Database Location Adjustments"
                tooltip={schemaDescriptions.get('transfer.storageMigration.skipDatabaseLocationAdjustments')}
                htmlFor="skipDatabaseLocationAdjustments"
              />
              <p className="text-xs text-gray-500">Don't adjust database location to match table locations</p>
            </div>
          </div>

          <div className="flex items-start pt-6">
            <input
              type="checkbox"
              id="createArchive"
              checked={config.transfer.storageMigration?.createArchive || false}
              onChange={(e) => updateConfig('transfer.storageMigration.createArchive', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded mt-0.5"
            />
            <div className="ml-2">
              <FieldWithTooltip
                label="Create Archive"
                tooltip={schemaDescriptions.get('transfer.storageMigration.createArchive')}
                htmlFor="createArchive"
              />
              <p className="text-xs text-gray-500">Archive tables instead of changing metadata</p>
            </div>
          </div>

          <div className="flex items-start">
            <input
              type="checkbox"
              id="consolidateTablesForDistcp"
              checked={config.transfer.storageMigration?.consolidateTablesForDistcp || false}
              onChange={(e) => updateConfig('transfer.storageMigration.consolidateTablesForDistcp', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded mt-0.5"
            />
            <div className="ml-2">
              <FieldWithTooltip
                label="Consolidate Tables for DistCp"
                tooltip={schemaDescriptions.get('transfer.storageMigration.consolidateTablesForDistcp')}
                htmlFor="consolidateTablesForDistcp"
              />
              <p className="text-xs text-gray-500">Consolidate tables into single directory for distcp</p>
            </div>
          </div>

          <div className="flex items-start">
            <input
              type="checkbox"
              id="strict"
              checked={config.transfer.storageMigration?.strict || false}
              onChange={(e) => updateConfig('transfer.storageMigration.strict', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded mt-0.5"
            />
            <div className="ml-2">
              <FieldWithTooltip
                label="Strict Mode"
                tooltip={schemaDescriptions.get('transfer.storageMigration.strict')}
                htmlFor="strict"
              />
              <p className="text-xs text-gray-500">Fail migration if any issues occur during evaluation</p>
            </div>
          </div>
        </div>
      </div>

      {/* Warehouse Settings */}
      <div>
        <h4 className="text-md font-medium text-gray-900 mb-4">Warehouse Configuration</h4>

        <div className="space-y-4">
          <div>
            <FieldWithTooltip
              label="Warehouse Source"
              tooltip={schemaDescriptions.get('transfer.warehouse.source')}
              htmlFor="warehouseSource"
              className="mb-1"
            />
            <input
              type="text"
              id="warehouseSource"
              value={config.transfer.warehouse.source}
              readOnly
              className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 bg-gray-100 cursor-not-allowed"
            />
            <p className="mt-1 text-xs text-gray-500">Source of the warehouse location (read-only)</p>
          </div>

          <div>
            <FieldWithTooltip
              label="External Directory"
              tooltip={schemaDescriptions.get('transfer')}
              htmlFor="externalDirectory"
              className="mb-2"
            />
            <input
              type="text"
              id="externalDirectory"
              value={config.transfer.warehouse.externalDirectory}
              onChange={(e) => updateConfig('transfer.warehouse.externalDirectory', e.target.value)}
              className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                errors['transfer.warehouse.externalDirectory'] ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
              }`}
              placeholder="/user/hive/external"
            />
            {errors['transfer.warehouse.externalDirectory'] && (
              <p className="mt-1 text-sm text-red-600">{errors['transfer.warehouse.externalDirectory']}</p>
            )}
          </div>

          <div>
            <FieldWithTooltip
              label="Managed Directory"
              tooltip={schemaDescriptions.get('transfer')}
              htmlFor="managedDirectory"
              className="mb-2"
            />
            <input
              type="text"
              id="managedDirectory"
              value={config.transfer.warehouse.managedDirectory}
              onChange={(e) => updateConfig('transfer.warehouse.managedDirectory', e.target.value)}
              className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
                errors['transfer.warehouse.managedDirectory'] ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
              }`}
              placeholder="/user/hive/warehouse"
            />
            {errors['transfer.warehouse.managedDirectory'] && (
              <p className="mt-1 text-sm text-red-600">{errors['transfer.warehouse.managedDirectory']}</p>
            )}
          </div>

          <div className="mt-6 p-4 bg-amber-50 border border-amber-200 rounded-md">
            <div className="flex items-start">
              <input
                type="checkbox"
                id="forceExternalLocation"
                checked={config.forceExternalLocation}
                onChange={(e) => updateConfig('forceExternalLocation', e.target.checked)}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded mt-0.5"
              />
              <div className="ml-3">
                <FieldWithTooltip
                  label="Force External Location"
                  tooltip={schemaDescriptions.get('forceExternalLocation')}
                  htmlFor="forceExternalLocation"
                />
                <p className="text-xs text-gray-700 mt-1">
                  <strong className="text-amber-800">Recommend not enabling this feature.</strong>
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Advanced Transfer Settings - Collapsible */}
      <div>
        <button
          type="button"
          onClick={() => setIsAdvancedTransferCollapsed(!isAdvancedTransferCollapsed)}
          className="flex items-center justify-between w-full text-left"
        >
          <h4 className="text-md font-medium text-gray-900">Advanced Transfer Settings</h4>
          {isAdvancedTransferCollapsed ? (
            <ChevronDownIcon className="w-5 h-5 text-gray-500" />
          ) : (
            <ChevronUpIcon className="w-5 h-5 text-gray-500" />
          )}
        </button>

        {!isAdvancedTransferCollapsed && (
          <div className="mt-4 space-y-4">
            <div>
              <FieldWithTooltip
                label="Transfer Prefix"
                tooltip={schemaDescriptions.get('transfer.transferPrefix')}
                htmlFor="transferPrefix"
                className="mb-2"
              />
              <input
                type="text"
                id="transferPrefix"
                value={config.transfer.transferPrefix}
                onChange={(e) => updateConfig('transfer.transferPrefix', e.target.value)}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                placeholder="hms_mirror_transfer_"
              />
            </div>

            <div>
              <FieldWithTooltip
                label="Shadow Prefix"
                tooltip={schemaDescriptions.get('transfer.shadowPrefix')}
                htmlFor="shadowPrefix"
                className="mb-2"
              />
              <input
                type="text"
                id="shadowPrefix"
                value={config.transfer.shadowPrefix}
                onChange={(e) => updateConfig('transfer.shadowPrefix', e.target.value)}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                placeholder="hms_mirror_shadow_"
              />
            </div>

            <div>
              <FieldWithTooltip
                label="Storage Migration Postfix"
                tooltip={schemaDescriptions.get('transfer.storageMigrationPostfix')}
                htmlFor="storageMigrationPostfix"
                className="mb-2"
              />
              <input
                type="text"
                id="storageMigrationPostfix"
                value={config.transfer.storageMigrationPostfix}
                onChange={(e) => updateConfig('transfer.storageMigrationPostfix', e.target.value)}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                placeholder="_storage_migration"
              />
            </div>

            <div>
              <FieldWithTooltip
                label="Export Base Directory Prefix"
                tooltip={schemaDescriptions.get('transfer.exportBaseDirPrefix')}
                htmlFor="exportBaseDirPrefix"
                className="mb-2"
              />
              <input
                type="text"
                id="exportBaseDirPrefix"
                value={config.transfer.exportBaseDirPrefix}
                onChange={(e) => updateConfig('transfer.exportBaseDirPrefix', e.target.value)}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                placeholder="/apps/hive/warehouse/export_"
              />
            </div>

            <div>
              <FieldWithTooltip
                label="Remote Working Directory"
                tooltip={schemaDescriptions.get('transfer.remoteWorkingDirectory')}
                htmlFor="remoteWorkingDirectory"
                className="mb-2"
              />
              <input
                type="text"
                id="remoteWorkingDirectory"
                value={config.transfer.remoteWorkingDirectory}
                onChange={(e) => updateConfig('transfer.remoteWorkingDirectory', e.target.value)}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                placeholder="hms_mirror_working"
              />
            </div>

            <div>
              <FieldWithTooltip
                label="Intermediate Storage (Optional)"
                tooltip={schemaDescriptions.get('transfer.intermediateStorage')}
                htmlFor="intermediateStorage"
                className="mb-2"
              />
              <input
                type="text"
                id="intermediateStorage"
                value={config.transfer.intermediateStorage || ''}
                onChange={(e) => updateConfig('transfer.intermediateStorage', e.target.value)}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                placeholder="s3a://temp-bucket/migration/intermediate/"
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );

  const renderStep4 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Conversions (Optional)</h3>
        <p className="text-sm text-gray-600 mb-6">Configure table format conversions, including Iceberg.</p>
        
        <div className="space-y-4">
          <div>
            <div className="flex items-center">
              <input
                type="checkbox"
                id="enableIceberg"
                checked={config.icebergConversion.enable}
                onChange={(e) => updateConfig('icebergConversion.enable', e.target.checked)}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <div className="ml-2">
                <FieldWithTooltip
                  label="Enable Iceberg Conversion"
                  tooltip={schemaDescriptions.get('icebergConversion.enable')}
                  htmlFor="enableIceberg"
                />
              </div>
            </div>
          </div>

          {config.icebergConversion.enable && (
            <>
              <div>
                <FieldWithTooltip
                  label="File Type Translation"
                  tooltip={schemaDescriptions.get('icebergConversion.fileTypeTranslation')}
                  htmlFor="fileTypeTranslation"
                  className="mb-2"
                />
                <textarea
                  id="fileTypeTranslation"
                  rows={3}
                  value={config.icebergConversion.fileTypeTranslation}
                  onChange={(e) => updateConfig('icebergConversion.fileTypeTranslation', e.target.value)}
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                  placeholder="ORC:Parquet"
                />
              </div>

              <div>
                <FieldWithTooltip
                  label="Iceberg Version"
                  tooltip={schemaDescriptions.get('icebergConversion.version')}
                  htmlFor="icebergVersion"
                  className="mb-2"
                />
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
              </div>

              <div>
                <FieldWithTooltip
                  label="Table Properties"
                  tooltip={schemaDescriptions.get('icebergConversion.tableProperties')}
                  htmlFor="tableProperties"
                  className="mb-2"
                />
                <textarea
                  id="tableProperties"
                  rows={3}
                  value={config.icebergConversion.tableProperties}
                  onChange={(e) => updateConfig('icebergConversion.tableProperties', e.target.value)}
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                  placeholder="write.format.default=parquet"
                />
              </div>

              <div>
                <div className="flex items-center">
                  <input
                    type="checkbox"
                    id="icebergInplace"
                    checked={config.icebergConversion.inplace}
                    onChange={(e) => updateConfig('icebergConversion.inplace', e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <div className="ml-2">
                    <FieldWithTooltip
                      label="In-Place Conversion"
                      tooltip={schemaDescriptions.get('icebergConversion.inplace')}
                      htmlFor="icebergInplace"
                    />
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );

  const renderStep5 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium text-gray-900 mb-4">Review Configuration</h3>
        <p className="text-sm text-gray-600 mb-6">Review your configuration settings before saving.</p>

        <div className="bg-gray-50 rounded-lg p-4 space-y-4">
          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">Configuration Details</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• Name: {config.configName}</div>
              {config.description && <div>• Description: {config.description}</div>}
            </div>
          </div>

          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">Migration Behavior</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• Migrate VIEWs: {config.migrateVIEW.on ? 'Yes' : 'No'}</div>
              <div>• Migrate non-native tables: {config.migrateNonNative ? 'Yes' : 'No'}</div>
            </div>
          </div>

          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">Table Behavior</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• Create if not exists: {config.createIfNotExists ? 'Yes' : 'No'}</div>
              <div>• Auto table stats: {config.enableAutoTableStats ? 'Yes' : 'No'}</div>
              <div>• Auto column stats: {config.enableAutoColumnStats ? 'Yes' : 'No'}</div>
              <div>• Save working tables: {config.saveWorkingTables ? 'Yes' : 'No'}</div>
            </div>
          </div>

          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">File and Data Handling</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• Copy Avro schema URLs: {config.copyAvroSchemaUrls ? 'Yes' : 'No'}</div>
            </div>
          </div>

          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">Ownership Transfer</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• Database ownership: {config.ownershipTransfer.database ? 'Yes' : 'No'}</div>
              <div>• Table ownership: {config.ownershipTransfer.table ? 'Yes' : 'No'}</div>
            </div>
          </div>

          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">ACID Migration</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• ACID migration: {config.migrateACID.on ? 'Enabled' : 'Disabled'}</div>
              {config.migrateACID.on && (
                <>
                  <div>• ACID only: {config.migrateACID.only ? 'Yes' : 'No'}</div>
                  <div>• Bucket threshold: {config.migrateACID.artificialBucketThreshold}</div>
                  <div>• Partition limit: {config.migrateACID.partitionLimit}</div>
                  <div>• Downgrade: {config.migrateACID.downgrade ? 'Yes' : 'No'}</div>
                  <div>• In-place: {config.migrateACID.inplace ? 'Yes' : 'No'}</div>
                </>
              )}
            </div>
          </div>

          <div>
            <h4 className="text-md font-medium text-gray-900 mb-2">Warehouse Settings</h4>
            <div className="text-sm text-gray-600 space-y-1">
              <div>• External directory: {config.transfer.warehouse.externalDirectory}</div>
              <div>• Managed directory: {config.transfer.warehouse.managedDirectory}</div>
              <div>• Transfer prefix: {config.transfer.transferPrefix}</div>
              <div>• Shadow prefix: {config.transfer.shadowPrefix}</div>
            </div>
          </div>

          {config.icebergConversion.enable && (
            <div>
              <h4 className="text-md font-medium text-gray-900 mb-2">Iceberg Conversion</h4>
              <div className="text-sm text-gray-600 space-y-1">
                <div>• Enabled: Yes</div>
                <div>• Version: {config.icebergConversion.version}</div>
                <div>• In-place: {config.icebergConversion.inplace ? 'Yes' : 'No'}</div>
                {config.icebergConversion.fileTypeTranslation && (
                  <div>• File type translation: {config.icebergConversion.fileTypeTranslation}</div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const steps = [
    { id: 'details', title: 'Details', component: renderStep0 },
    { id: 'migration', title: 'Migration', component: renderStep1 },
    { id: 'acid', title: 'ACID', component: renderStep2 },
    { id: 'transfer', title: 'Transfer', component: renderStep3 },
    { id: 'conversions', title: 'Conversions', component: renderStep4 },
    { id: 'review', title: 'Review', component: renderStep5 },
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
          {isEditing ? 'Edit' : isCopying ? 'Copy' : 'Create'} Configuration
        </h1>
        <p className="text-gray-600 mt-2">
          {isEditing
            ? `Editing configuration: ${config.configName || 'Unknown'}`
            : isCopying
            ? `Creating a copy of configuration. Please provide a new name.`
            : `Configure migration settings for HMS-Mirror`
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
        
        {currentStep < 5 ? (
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