import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { JobDto } from '../../services/api/jobApi';
import {
  ChevronLeftIcon,
  ChevronRightIcon,
  CheckIcon,
  ExclamationTriangleIcon,
  MagnifyingGlassIcon,
  SparklesIcon
} from '@heroicons/react/24/outline';
import WizardProgress from '../connections/wizard/WizardProgress';
import SearchableInput from '../common/SearchableInput';
import StrategySelectionWizard from './StrategySelectionWizard';

interface JobFormData {
  // Hidden field for updates
  key?: string;

  name: string;
  description?: string;
  datasetReference: string;
  configReference: string;
  leftConnectionReference: string;
  rightConnectionReference: string;
  strategy: string;
  databaseOnly: boolean;
  consolidateDBCreateStatements: boolean;
  disasterRecovery: boolean;
  sync: boolean;
  intermediateStorage?: string;
  targetNamespace?: string;
  hybrid: {
    exportImportPartitionLimit: number;
    sqlPartitionLimit: number;
    sqlSizeLimit: number;
  };
}

interface Dataset {
  name: string;
  description?: string;
}

interface Configuration {
  name: string;
  description?: string;
}

interface Connection {
  name: string;
  description?: string;
  uri?: string;
}

const JobBuildWizard: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [currentStep, setCurrentStep] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const [errors, setErrors] = useState<{ [key: string]: string }>({});

  // Edit and copy mode state
  const editMode = location.state?.mode === 'edit';
  const copyMode = location.state?.mode === 'copy';
  const existingJob: JobDto | null = location.state?.job || null;
  const existingJobKey: string = location.state?.jobKey || '';
  const [jobKey, setJobKey] = useState<string>(existingJobKey);
  
  // Available data for dropdowns
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [configurations, setConfigurations] = useState<Configuration[]>([]);
  const [connectionDtos, setConnections] = useState<Connection[]>([]);
  const [loadingData, setLoadingData] = useState(true);
  
  // Dataset search functionality
  const [datasetSearchQuery, setDatasetSearchQuery] = useState('');

  // Configuration search functionality
  const [configSearchQuery, setConfigSearchQuery] = useState('');

  // Connection search functionality
  const [leftConnectionSearchQuery, setLeftConnectionSearchQuery] = useState('');
  const [rightConnectionSearchQuery, setRightConnectionSearchQuery] = useState('');

  // Strategy selection mode
  const [useStrategyWizard, setUseStrategyWizard] = useState(!editMode && !copyMode);

  const [jobData, setJobData] = useState<JobFormData>({
    // Preserve key in edit mode, but not in copy mode
    key: (editMode && !copyMode) ? existingJobKey : undefined,
    name: copyMode ? '' : (existingJob?.name || ''),
    description: existingJob?.description || '',
    datasetReference: existingJob?.datasetReference || '',
    configReference: existingJob?.configReference || '',
    leftConnectionReference: existingJob?.leftConnectionReference || '',
    rightConnectionReference: existingJob?.rightConnectionReference || '',
    strategy: existingJob?.strategy || '',
    databaseOnly: existingJob?.databaseOnly || false,
    consolidateDBCreateStatements: existingJob?.consolidateDBCreateStatements || false,
    disasterRecovery: existingJob?.disasterRecovery || false,
    sync: existingJob?.sync || false,
    intermediateStorage: existingJob?.intermediateStorage || '',
    targetNamespace: existingJob?.targetNamespace || '',
    hybrid: {
      exportImportPartitionLimit: existingJob?.hybrid?.exportImportPartitionLimit || 100,
      sqlPartitionLimit: existingJob?.hybrid?.sqlPartitionLimit || 500,
      sqlSizeLimit: existingJob?.hybrid?.sqlSizeLimit || 1073741824, // 1GB
    }
  });

  const dataStrategies = [
    { value: 'SCHEMA_ONLY', label: 'Schema Only', description: 'Transfer schema only, data moved via external process' },
    { value: 'LINKED', label: 'Linked', description: 'Transfer schema with original location paths for linked clusters' },
    { value: 'SQL', label: 'SQL', description: 'Use SQL to migrate data between linked clusters' },
    { value: 'EXPORT_IMPORT', label: 'Export/Import', description: 'Use Hive EXPORT/IMPORT for data transfer' },
    { value: 'HYBRID', label: 'Hybrid', description: 'Choose SQL or EXPORT_IMPORT based on table criteria' },
    { value: 'COMMON', label: 'Common', description: 'Shared storage between clusters, no data migration' },
    { value: 'STORAGE_MIGRATION', label: 'Storage Migration', description: 'Migrate tables to new storage location' },
    { value: 'DUMP', label: 'Dump', description: 'Extract schemas only without target cluster' },
    { value: 'CONVERT_LINKED', label: 'Convert Linked', description: 'Convert existing LINKED schemas to SCHEMA_ONLY' }
  ];

  const steps = [
    { id: 'basic', title: 'Job Details', description: 'Basic information' },
    { id: 'dataset', title: 'Dataset', description: 'Select dataset' },
    { id: 'config', title: 'Configuration', description: 'Select configuration' },
    { id: 'strategy', title: 'Data Strategy', description: 'Choose strategy' },
    { id: 'connectionDtos', title: 'Connections', description: 'Select connectionDtos' },
    { id: 'options', title: 'Options', description: 'Additional settings' },
    { id: 'summary', title: 'Summary', description: 'Review and create' }
  ];

  const extractDescriptionFromYaml = (yaml: string): string => {
    try {
      const commentMatch = yaml.match(/comment:\s*"([^"]*)"/);
      return commentMatch ? commentMatch[1] : '';
    } catch {
      return '';
    }
  };

  // Load available datasets, configurations, and connectionDtos
  useEffect(() => {
    const loadData = async () => {
      setLoadingData(true);
      try {
        // Load datasets from RocksDB
        const datasetsResponse = await fetch('/hms-mirror/api/v1/datasets');
        if (datasetsResponse.ok) {
          const datasetsData = await datasetsResponse.json();
          if (datasetsData.status === 'SUCCESS' && datasetsData.data) {
            // Convert from {name: DatasetDto} format to array of datasets
            const datasetsArray = Object.entries(datasetsData.data).map(([name, dataset]) => ({
              ...dataset,
              name
            }));
            setDatasets(datasetsArray);
          } else {
            setDatasets([]);
          }
        }

        // Load configurations from RocksDB
        const configsResponse = await fetch('/hms-mirror/api/v1/config');
        if (configsResponse.ok) {
          const configsData = await configsResponse.json();
          if (configsData.status === 'success' && configsData.data) {
            // Parse configurations from the nested data structure
            const configurationsArray = [];
            for (const [strategy, configs] of Object.entries(configsData.data)) {
              for (const config of configs) {
                configurationsArray.push({
                  name: config.name,
                  description: config.yamlConfig ? extractDescriptionFromYaml(config.yamlConfig) : ''
                });
              }
            }
            setConfigurations(configurationsArray);
          } else {
            setConfigurations([]);
          }
        }

        // Load connections from RocksDB
        const connectionsResponse = await fetch('/hms-mirror/api/v1/connections');
        if (connectionsResponse.ok) {
          const connectionsData = await connectionsResponse.json();
          setConnections(connectionsData.connections || []);
        }
      } catch (error) {
        console.error('Error loading data:', error);
      } finally {
        setLoadingData(false);
      }
    };

    loadData();
  }, []);

  // Dataset search functionality - use the already loaded datasets
  const getDatasetKeys = () => {
    return datasets.map(dataset => dataset.name);
  };

  // Filtered dataset keys for SearchableInput component
  const filteredDatasetKeys = useMemo(() => {
    const keys = getDatasetKeys();
    if (!datasetSearchQuery.trim()) {
      return keys;
    }
    return keys.filter(key => 
      key.toLowerCase().includes(datasetSearchQuery.toLowerCase())
    );
  }, [datasetSearchQuery, datasets]);

  // Configuration search functionality
  const getConfigurationNames = () => {
    return configurations.map(config => config.name);
  };

  const configurationNameMatchesSearch = (name: string, query: string): boolean => {
    if (query === '') return true;
    return name.toLowerCase().includes(query.toLowerCase());
  };

  // Filtered configuration names for SearchableInput component
  const filteredConfigurationNames = useMemo(() => {
    const names = getConfigurationNames();
    if (!configSearchQuery.trim()) {
      return names;
    }
    return names.filter(name => 
      configurationNameMatchesSearch(name, configSearchQuery)
    );
  }, [configurations, configSearchQuery]);

  // Connection filtering helper
  const connectionMatchesSearch = (connectionDto: any, query: string): boolean => {
    if (query === '') return true;
    return connectionDto.name.toLowerCase().includes(query.toLowerCase()) ||
           (connectionDto.uri && connectionDto.uri.toLowerCase().includes(query.toLowerCase()));
  };

  // Filtered connectionDtos for SearchableInput components
  const filteredLeftConnections = useMemo(() => {
    if (!leftConnectionSearchQuery.trim()) {
      return connectionDtos;
    }
    return connectionDtos.filter(conn =>
      connectionMatchesSearch(conn, leftConnectionSearchQuery)
    );
  }, [connectionDtos, leftConnectionSearchQuery]);

  const filteredRightConnections = useMemo(() => {
    if (!rightConnectionSearchQuery.trim()) {
      return connectionDtos;
    }
    return connectionDtos.filter(conn =>
      connectionMatchesSearch(conn, rightConnectionSearchQuery)
    );
  }, [connectionDtos, rightConnectionSearchQuery]);



  // Helper to check if RIGHT connection is required based on strategy
  const isRightConnectionRequired = (): boolean => {
    // RIGHT connection is NOT required for STORAGE_MIGRATION and DUMP strategies
    const strategiesWithoutRightConnection = ['STORAGE_MIGRATION', 'DUMP'];
    return !strategiesWithoutRightConnection.includes(jobData.strategy);
  };

  const validateStep = (stepIndex: number): boolean => {
    const newErrors: { [key: string]: string } = {};

    switch (stepIndex) {
      case 0: // Basic job details
        if (!jobData.name.trim()) {
          newErrors.name = 'Job name is required';
        } else if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$/.test(jobData.name)) {
          newErrors.name = 'Job name must be alphanumeric with underscores/hyphens, 2+ characters';
        }
        break;
      case 1: // Dataset selection
        if (!jobData.datasetReference) {
          newErrors.datasetReference = 'Dataset selection is required';
        }
        break;
      case 2: // Configuration selection
        if (!jobData.configReference) {
          newErrors.configReference = 'Configuration selection is required';
        }
        break;
      case 3: // Data strategy
        if (!jobData.strategy) {
          newErrors.strategy = 'Data strategy selection is required';
        }
        break;
      case 4: // Connections selection
        if (!jobData.leftConnectionReference) {
          newErrors.leftConnectionReference = 'Left connectionDto is required';
        }
        // RIGHT connection is only required for certain strategies
        if (isRightConnectionRequired() && !jobData.rightConnectionReference) {
          newErrors.rightConnectionReference = 'Right connectionDto is required';
        }
        // Only validate different connections if both are provided and both are required
        if (jobData.leftConnectionReference && jobData.rightConnectionReference &&
            jobData.leftConnectionReference === jobData.rightConnectionReference) {
          newErrors.rightConnectionReference = 'Left and right connectionDtos must be different';
        }
        break;
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleNext = () => {
    if (validateStep(currentStep)) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handlePrevious = () => {
    setCurrentStep(currentStep - 1);
    setErrors({});
  };

  const handleInputChange = (field: keyof JobFormData, value: any) => {
    setJobData(prev => ({
      ...prev,
      [field]: value
    }));
    // Clear error when user starts typing
    if (errors[field]) {
      setErrors(prev => ({ ...prev, [field]: '' }));
    }
  };

  const handleSubmit = async () => {
    if (!validateStep(currentStep)) return;

    setIsLoading(true);
    try {
      let finalJobKey = jobKey;

      // Generate UUID and jobKey for new jobs and copies
      if (!editMode || copyMode) {
        const jobId = crypto.randomUUID();
        finalJobKey = `${jobData.name}-${jobId}`;
        setJobKey(finalJobKey);
      }

      const jobPayload = {
        id: (editMode && !copyMode) ? existingJob?.id : crypto.randomUUID(),
        name: jobData.name,
        description: jobData.description,
        datasetReference: jobData.datasetReference,
        configReference: jobData.configReference,
        leftConnectionReference: jobData.leftConnectionReference,
        rightConnectionReference: jobData.rightConnectionReference,
        strategy: jobData.strategy,
        databaseOnly: jobData.databaseOnly,
        consolidateDBCreateStatements: jobData.consolidateDBCreateStatements,
        disasterRecovery: jobData.disasterRecovery,
        sync: jobData.sync,
        intermediateStorage: jobData.intermediateStorage,
        targetNamespace: jobData.targetNamespace,
        hybrid: {
          exportImportPartitionLimit: jobData.hybrid.exportImportPartitionLimit,
          sqlPartitionLimit: jobData.hybrid.sqlPartitionLimit,
          sqlSizeLimit: jobData.hybrid.sqlSizeLimit
        },
        createdDate: (editMode && !copyMode) ? existingJob?.createdDate : new Date().toISOString(),
        modifiedDate: new Date().toISOString()
      };

      const response = await fetch(`/hms-mirror/api/v1/jobs/${finalJobKey}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(jobPayload)
      });

      if (response.ok) {
        const action = editMode && !copyMode ? 'updated' : copyMode ? 'copied' : 'created';
        navigate('/jobs/list', {
          state: {
            message: `Job "${jobData.name}" ${action} successfully`,
            type: 'success'
          }
        });
      } else {
        const errorData = await response.json();
        const action = editMode && !copyMode ? 'update' : 'create';
        setErrors({ submit: errorData.message || `Failed to ${action} job` });
      }
    } catch (error) {
      const action = editMode && !copyMode ? 'updating' : 'creating';
      console.error(`Error ${action} job:`, error);
      setErrors({ submit: `Failed to ${action} job. Please try again.` });
    } finally {
      setIsLoading(false);
    }
  };

  const renderStep = () => {
    if (loadingData) {
      return (
        <div className="text-center py-8">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-2 text-gray-600">Loading data...</p>
        </div>
      );
    }

    switch (currentStep) {
      case 0: // Job Details
        return (
          <div className="space-y-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Job Name *
              </label>
              <input
                type="text"
                value={jobData.name}
                onChange={(e) => handleInputChange('name', e.target.value)}
                disabled={editMode}
                className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 ${
                  errors.name ? 'border-red-500' : 'border-gray-300'
                } ${editMode ? 'bg-gray-100 cursor-not-allowed' : ''}`}
                placeholder={copyMode ? "Enter a new name for the copied job" : "Enter a unique job name"}
              />
              {errors.name && (
                <p className="mt-1 text-sm text-red-600">{errors.name}</p>
              )}
              <p className="mt-1 text-sm text-gray-500">
                {editMode
                  ? 'Job name cannot be changed when editing'
                  : copyMode
                    ? 'Provide a unique name for this copy'
                    : 'Name cannot be changed after creation'}
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Description
              </label>
              <textarea
                value={jobData.description}
                onChange={(e) => handleInputChange('description', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                placeholder="Enter a description for this job (optional)"
                rows={3}
              />
              <p className="mt-1 text-sm text-gray-500">
                Optional description to help identify the purpose of this job
              </p>
            </div>
          </div>
        );

      case 1: // Dataset Selection
        return (
          <div className="space-y-6">
            <SearchableInput
              value={jobData.datasetReference}
              onSearch={(query) => {
                setDatasetSearchQuery(query);
                // Filter datasets based on search query
              }}
              onSelect={(datasetKey) => {
                handleInputChange('datasetReference', datasetKey);
              }}
              onClear={() => {
                handleInputChange('datasetReference', '');
              }}
              options={filteredDatasetKeys}
              getOptionLabel={(key) => key}
              getOptionKey={(key) => key}
              label="Dataset Reference *"
              placeholder="Type to search for datasets..."
              error={errors.datasetReference}
            />
          </div>
        );

      case 2: // Configuration Selection
        return (
          <div className="space-y-6">
            <SearchableInput
              value={jobData.configReference}
              onSearch={(query) => {
                setConfigSearchQuery(query);
              }}
              onSelect={(configName) => {
                handleInputChange('configReference', configName);
              }}
              onClear={() => {
                handleInputChange('configReference', '');
              }}
              options={filteredConfigurationNames}
              getOptionLabel={(name) => {
                const config = configurations.find(c => c.name === name);
                return config && config.description ? `${name} - ${config.description}` : name;
              }}
              getOptionKey={(name) => name}
              label="Configuration Reference *"
              placeholder="Type to search for configurations..."
              error={errors.configReference}
            />
            
            {/* No configurations warning */}
            {configurations.length === 0 && (
              <p className="mt-1 text-sm text-amber-600">
                <ExclamationTriangleIcon className="w-4 h-4 inline mr-1" />
                No configurations found. Please create a configuration first.
              </p>
            )}
          </div>
        );

      case 3: // Data Strategy Selection
        // Show wizard for new jobs, list for edit/copy mode
        if (useStrategyWizard) {
          return (
            <StrategySelectionWizard
              initialStrategy={jobData.strategy}
              onStrategySelected={(strategy, reasoning) => {
                handleInputChange('strategy', strategy);
                setUseStrategyWizard(false);
              }}
              onCancel={() => {
                if (editMode || copyMode) {
                  setUseStrategyWizard(false);
                }
              }}
            />
          );
        }

        return (
          <div className="space-y-6">
            <div>
              <div className="flex items-center justify-between mb-4">
                <label className="block text-sm font-medium text-gray-700">
                  Select Data Strategy *
                </label>
                <button
                  onClick={() => setUseStrategyWizard(true)}
                  className="inline-flex items-center px-3 py-1 text-sm font-medium text-blue-700 bg-blue-50 border border-blue-300 rounded-md hover:bg-blue-100 transition-colors"
                >
                  <SparklesIcon className="h-4 w-4 mr-1" />
                  Use Selection Wizard
                </button>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {dataStrategies.map((strategy) => (
                  <div
                    key={strategy.value}
                    className={`p-4 border rounded-lg cursor-pointer transition-colors ${
                      jobData.strategy === strategy.value
                        ? 'border-blue-500 bg-blue-50'
                        : 'border-gray-200 hover:border-gray-300'
                    }`}
                    onClick={() => handleInputChange('strategy', strategy.value)}
                  >
                    <div className="flex items-start">
                      <input
                        type="radio"
                        name="strategy"
                        value={strategy.value}
                        checked={jobData.strategy === strategy.value}
                        onChange={() => handleInputChange('strategy', strategy.value)}
                        className="mt-1 mr-3"
                      />
                      <div>
                        <h4 className="font-medium text-gray-900">{strategy.label}</h4>
                        <p className="text-sm text-gray-600 mt-1">{strategy.description}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              {errors.strategy && (
                <p className="mt-2 text-sm text-red-600">{errors.strategy}</p>
              )}
            </div>
          </div>
        );

      case 4: // Connections Selection
        const rightConnRequired = isRightConnectionRequired();
        return (
          <div className="space-y-6">
            <div>
              <SearchableInput
                value={jobData.leftConnectionReference}
                onSearch={setLeftConnectionSearchQuery}
                onSelect={(connectionDto: any) => handleInputChange('leftConnectionReference', connectionDto.name)}
                onClear={() => handleInputChange('leftConnectionReference', '')}
                options={filteredLeftConnections}
                getOptionLabel={(conn: any) => `${conn.name}${conn.uri ? ` (${conn.uri})` : ''}`}
                getOptionKey={(conn: any) => conn.name}
                placeholder="Type to search connectionDtos..."
                label="Left Connection *"
                error={errors.leftConnectionReference}
              />
            </div>

            {rightConnRequired && (
              <div>
                <SearchableInput
                  value={jobData.rightConnectionReference}
                  onSearch={setRightConnectionSearchQuery}
                  onSelect={(connectionDto: any) => handleInputChange('rightConnectionReference', connectionDto.name)}
                  onClear={() => handleInputChange('rightConnectionReference', '')}
                  options={filteredRightConnections}
                  getOptionLabel={(conn: any) => `${conn.name}${conn.uri ? ` (${conn.uri})` : ''}`}
                  getOptionKey={(conn: any) => conn.name}
                  placeholder="Type to search connectionDtos..."
                  label="Right Connection *"
                  error={errors.rightConnectionReference}
                />
              </div>
            )}

            {!rightConnRequired && (
              <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
                <p className="text-sm text-blue-800">
                  ℹ️ The <strong>{jobData.strategy}</strong> strategy does not require a RIGHT (target) connection.
                  {jobData.strategy === 'STORAGE_MIGRATION' && ' This strategy moves data within the same cluster.'}
                  {jobData.strategy === 'DUMP' && ' This strategy only extracts schema SQL scripts.'}
                </p>
              </div>
            )}

            {connectionDtos.length === 0 && (
              <p className="mt-1 text-sm text-amber-600">
                <ExclamationTriangleIcon className="w-4 h-4 inline mr-1" />
                No connectionDtos found. Please create connectionDtos first.
              </p>
            )}
          </div>
        );

      case 5: // Options
        return (
          <div className="space-y-6">
            {/* Target Namespace and Intermediate Storage Settings */}
            <div>
              <h3 className="text-lg font-medium text-gray-900 mb-4">Storage and Namespace Configuration</h3>
              <div className="space-y-4">
                {/* Target Namespace - Only show for STORAGE_MIGRATION strategy */}
                {jobData.strategy === 'STORAGE_MIGRATION' && (
                  <div>
                    <label htmlFor="targetNamespace" className="block text-sm font-medium text-gray-700 mb-2">
                      Target Namespace
                    </label>
                    <input
                      type="text"
                      id="targetNamespace"
                      value={jobData.targetNamespace}
                      onChange={(e) => handleInputChange('targetNamespace', e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      placeholder="Enter target namespace (required for STORAGE_MIGRATION)"
                    />
                    <p className="mt-1 text-sm text-gray-500">
                      Target namespace for the storage migration
                    </p>
                  </div>
                )}

                <div>
                  <label htmlFor="intermediateStorage" className="block text-sm font-medium text-gray-700 mb-2">
                    Intermediate Storage
                  </label>
                  <input
                    type="text"
                    id="intermediateStorage"
                    value={jobData.intermediateStorage}
                    onChange={(e) => handleInputChange('intermediateStorage', e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="Enter intermediate storage location (optional)"
                  />
                  <p className="mt-1 text-sm text-gray-500">
                    Optional intermediate storage location for data transfer operations
                  </p>
                </div>
              </div>
            </div>

            <div className="mt-6 pt-6 border-t border-gray-200">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Additional Options</h3>

              <div className="space-y-4">
                <div className="flex items-start">
                  <input
                    type="checkbox"
                    id="databaseOnly"
                    checked={jobData.databaseOnly}
                    onChange={(e) => handleInputChange('databaseOnly', e.target.checked)}
                    className="mt-1 mr-3"
                  />
                  <div>
                    <label htmlFor="databaseOnly" className="font-medium text-gray-900 cursor-pointer">
                      Database Only
                    </label>
                    <p className="text-sm text-gray-600 mt-1">
                      Only migrate database metadata, skip table migrations
                    </p>
                  </div>
                </div>

                <div className="flex items-start">
                  <input
                    type="checkbox"
                    id="consolidateDBCreateStatements"
                    checked={jobData.consolidateDBCreateStatements}
                    onChange={(e) => handleInputChange('consolidateDBCreateStatements', e.target.checked)}
                    className="mt-1 mr-3"
                  />
                  <div>
                    <label htmlFor="consolidateDBCreateStatements" className="font-medium text-gray-900 cursor-pointer">
                      Consolidate DB Create Statements
                    </label>
                    <p className="text-sm text-gray-600 mt-1">
                      Consolidate database creation statements into a single script
                    </p>
                  </div>
                </div>

                <div className="flex items-start">
                  <input
                    type="checkbox"
                    id="disasterRecovery"
                    checked={jobData.disasterRecovery}
                    onChange={(e) => {
                      handleInputChange('disasterRecovery', e.target.checked);
                      // Reset sync when disaster recovery is disabled
                      if (!e.target.checked) {
                        handleInputChange('sync', false);
                      }
                    }}
                    className="mt-1 mr-3"
                  />
                  <div>
                    <label htmlFor="disasterRecovery" className="font-medium text-gray-900 cursor-pointer">
                      Disaster Recovery Job
                    </label>
                    <p className="text-sm text-gray-600 mt-1">
                      Configure this job for disaster recovery purposes
                    </p>
                  </div>
                </div>

                {jobData.disasterRecovery && (
                  <div className="ml-6 pl-4 border-l-2 border-gray-200">
                    <div className="flex items-start">
                      <input
                        type="checkbox"
                        id="sync"
                        checked={jobData.sync}
                        onChange={(e) => handleInputChange('sync', e.target.checked)}
                        className="mt-1 mr-3"
                      />
                      <div>
                        <label htmlFor="sync" className="font-medium text-gray-900 cursor-pointer">
                          Enable Sync
                        </label>
                        <p className="text-sm text-gray-600 mt-1">
                          Enable synchronization operations for disaster recovery
                        </p>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Hybrid Strategy Settings - only show for SQL, EXPORT_IMPORT, or HYBRID strategies */}
            {(jobData.strategy === 'SQL' || jobData.strategy === 'EXPORT_IMPORT' || jobData.strategy === 'HYBRID') && (
              <div className="mt-6 pt-6 border-t border-gray-200">
                <h4 className="text-md font-medium text-gray-900 mb-4">Hybrid Strategy Settings</h4>
                <p className="text-sm text-gray-600 mb-4">
                  Configure partition and size limits for hybrid data migration strategies.
                </p>

                <div className="space-y-4">
                  <div>
                    <label htmlFor="exportImportPartitionLimit" className="block text-sm font-medium text-gray-700 mb-2">
                      Export/Import Partition Limit
                    </label>
                    <input
                      type="number"
                      id="exportImportPartitionLimit"
                      value={jobData.hybrid.exportImportPartitionLimit}
                      onChange={(e) => {
                        const value = parseInt(e.target.value, 10);
                        setJobData(prev => ({
                          ...prev,
                          hybrid: {
                            ...prev.hybrid,
                            exportImportPartitionLimit: value
                          }
                        }));
                      }}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      min="1"
                    />
                    <p className="mt-1 text-sm text-gray-500">
                      Maximum number of partitions to use EXPORT_IMPORT strategy (default: 100)
                    </p>
                  </div>

                  <div>
                    <label htmlFor="sqlPartitionLimit" className="block text-sm font-medium text-gray-700 mb-2">
                      SQL Partition Limit
                    </label>
                    <input
                      type="number"
                      id="sqlPartitionLimit"
                      value={jobData.hybrid.sqlPartitionLimit}
                      onChange={(e) => {
                        const value = parseInt(e.target.value, 10);
                        setJobData(prev => ({
                          ...prev,
                          hybrid: {
                            ...prev.hybrid,
                            sqlPartitionLimit: value
                          }
                        }));
                      }}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      min="1"
                    />
                    <p className="mt-1 text-sm text-gray-500">
                      Maximum number of partitions to use SQL strategy (default: 500)
                    </p>
                  </div>

                  <div>
                    <label htmlFor="sqlSizeLimit" className="block text-sm font-medium text-gray-700 mb-2">
                      SQL Size Limit (bytes)
                    </label>
                    <input
                      type="number"
                      id="sqlSizeLimit"
                      value={jobData.hybrid.sqlSizeLimit}
                      onChange={(e) => {
                        const value = parseInt(e.target.value, 10);
                        setJobData(prev => ({
                          ...prev,
                          hybrid: {
                            ...prev.hybrid,
                            sqlSizeLimit: value
                          }
                        }));
                      }}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      min="1"
                    />
                    <p className="mt-1 text-sm text-gray-500">
                      Maximum table size in bytes for SQL strategy (default: 1073741824 = 1GB, current: {(jobData.hybrid.sqlSizeLimit / (1024 * 1024 * 1024)).toFixed(2)} GB)
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        );

      case 6: // Summary
        const selectedDataset = datasets.find(d => d.name === jobData.datasetReference);
        const selectedConfig = configurations.find(c => c.name === jobData.configReference);
        const selectedLeftConn = connectionDtos.find(c => c.name === jobData.leftConnectionReference);
        const selectedRightConn = connectionDtos.find(c => c.name === jobData.rightConnectionReference);
        const selectedStrategy = dataStrategies.find(s => s.value === jobData.strategy);

        return (
          <div className="space-y-6">
            <h3 className="text-lg font-medium text-gray-900">Review Job Configuration</h3>
            
            <div className="bg-gray-50 p-4 rounded-lg space-y-4">
              <div>
                <h4 className="font-medium text-gray-900">Job Details</h4>
                <p className="text-sm text-gray-600">Name: {jobData.name}</p>
                {jobData.description && (
                  <p className="text-sm text-gray-600">Description: {jobData.description}</p>
                )}
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900">Dataset</h4>
                <p className="text-sm text-gray-600">{selectedDataset?.name}</p>
                {selectedDataset?.description && (
                  <p className="text-xs text-gray-500">{selectedDataset.description}</p>
                )}
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900">Configuration</h4>
                <p className="text-sm text-gray-600">{selectedConfig?.name}</p>
                {selectedConfig?.description && (
                  <p className="text-xs text-gray-500">{selectedConfig.description}</p>
                )}
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900">Connections</h4>
                <p className="text-sm text-gray-600">Left: {selectedLeftConn?.name || 'Not selected'}</p>
                {selectedRightConn?.name && (
                  <p className="text-sm text-gray-600">Right: {selectedRightConn.name}</p>
                )}
                {!selectedRightConn?.name && (jobData.strategy === 'STORAGE_MIGRATION' || jobData.strategy === 'DUMP') && (
                  <p className="text-sm text-gray-500 italic">Right connection not required for {jobData.strategy} strategy</p>
                )}
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900">Data Strategy</h4>
                <p className="text-sm text-gray-600">{selectedStrategy?.label}</p>
                <p className="text-xs text-gray-500">{selectedStrategy?.description}</p>
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900">Options</h4>
                <p className="text-sm text-gray-600">
                  Database Only: {jobData.databaseOnly ? 'Yes' : 'No'}
                </p>
                <p className="text-sm text-gray-600">
                  Consolidate DB Create Statements: {jobData.consolidateDBCreateStatements ? 'Yes' : 'No'}
                </p>
                <p className="text-sm text-gray-600">
                  Disaster Recovery: {jobData.disasterRecovery ? 'Yes' : 'No'}
                </p>
                {jobData.disasterRecovery && (
                  <p className="text-sm text-gray-600">
                    Sync Enabled: {jobData.sync ? 'Yes' : 'No'}
                  </p>
                )}
                {jobData.intermediateStorage && (
                  <p className="text-sm text-gray-600">
                    Intermediate Storage: {jobData.intermediateStorage}
                  </p>
                )}
                {jobData.targetNamespace && (
                  <p className="text-sm text-gray-600">
                    Target Namespace: {jobData.targetNamespace}
                  </p>
                )}
              </div>

              {/* Show Hybrid Strategy Settings only for SQL, EXPORT_IMPORT, or HYBRID strategies */}
              {(jobData.strategy === 'SQL' || jobData.strategy === 'EXPORT_IMPORT' || jobData.strategy === 'HYBRID') && (
                <div>
                  <h4 className="font-medium text-gray-900 mb-2">Hybrid Strategy Settings</h4>
                  <div className="text-sm text-gray-600 space-y-1">
                    <div>• Export/Import partition limit: {jobData.hybrid.exportImportPartitionLimit}</div>
                    <div>• SQL partition limit: {jobData.hybrid.sqlPartitionLimit}</div>
                    <div>• SQL size limit: {jobData.hybrid.sqlSizeLimit} bytes ({(jobData.hybrid.sqlSizeLimit / (1024 * 1024 * 1024)).toFixed(2)} GB)</div>
                  </div>
                </div>
              )}
            </div>

            {errors.submit && (
              <div className="bg-red-50 border border-red-200 rounded-md p-4">
                <p className="text-sm text-red-600">{errors.submit}</p>
              </div>
            )}
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-4xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">
            {editMode ? 'Edit Job' : copyMode ? 'Copy Job' : 'Create New Job'}
          </h1>
          <p className="mt-2 text-gray-600">
            {editMode
              ? `Update the HMS-Mirror job "${existingJob?.name}"`
              : copyMode
                ? `Create a copy of "${existingJob?.name}" with a new name`
                : 'Build a new HMS-Mirror job by configuring datasets, connectionDtos, and migration options'}
          </p>
        </div>

        {/* Progress Indicator */}
        <WizardProgress steps={steps} currentStep={currentStep} />

        {/* Main Content */}
        <div className="bg-white shadow rounded-lg">
          <div className="px-6 py-8">
            <div className="mb-6">
              <h2 className="text-xl font-semibold text-gray-900">
                {steps[currentStep].title}
              </h2>
              <p className="text-gray-600">{steps[currentStep].description}</p>
            </div>

            {renderStep()}
          </div>

          {/* Navigation */}
          <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex justify-between">
            <button
              onClick={handlePrevious}
              disabled={currentStep === 0}
              className={`inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md transition-colors ${
                currentStep === 0
                  ? 'text-gray-400 cursor-not-allowed'
                  : 'text-gray-700 bg-white hover:bg-gray-50'
              }`}
            >
              <ChevronLeftIcon className="w-4 h-4 mr-2" />
              Previous
            </button>

            <div className="flex space-x-3">
              <button
                onClick={() => navigate('/jobs/build')}
                className="px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 transition-colors"
              >
                Cancel
              </button>

              {currentStep === steps.length - 1 ? (
                <button
                  onClick={handleSubmit}
                  disabled={isLoading}
                  className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white transition-colors ${
                    isLoading
                      ? 'bg-gray-400 cursor-not-allowed'
                      : 'bg-blue-600 hover:bg-blue-700'
                  }`}
                >
                  {isLoading ? (
                    <>
                      <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                      {editMode && !copyMode ? 'Updating...' : copyMode ? 'Copying...' : 'Creating...'}
                    </>
                  ) : (
                    <>
                      <CheckIcon className="w-4 h-4 mr-2" />
                      {editMode && !copyMode ? 'Update Job' : copyMode ? 'Copy Job' : 'Create Job'}
                    </>
                  )}
                </button>
              ) : (
                <button
                  onClick={handleNext}
                  className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 transition-colors"
                >
                  Next
                  <ChevronRightIcon className="w-4 h-4 ml-2" />
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JobBuildWizard;