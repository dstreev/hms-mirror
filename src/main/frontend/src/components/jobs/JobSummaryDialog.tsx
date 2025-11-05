import React, { useState, useEffect } from 'react';
import { XMarkIcon, CheckCircleIcon, ArrowPathIcon } from '@heroicons/react/24/outline';
import { Job } from '../../types/Job';
import { datasetApi, DatasetDto } from '../../services/api/datasetApi';
import { configApi, ConfigurationResponse } from '../../services/api/configApi';
import { jobApi } from '../../services/api/jobApi';
import { HmsMirrorConfig } from '../../types/api';

interface Connection {
  name: string;
  config: {
    hiveServer2?: {
      uri: string;
    };
    metastoreDirect?: {
      uri: string;
    };
  };
}

interface JobSummaryDialogProps {
  isOpen: boolean;
  onClose: () => void;
  job: Job;
  onValidate?: (jobName: string, isValid: boolean, message: string, errors: string[], warnings: string[]) => void;
}

const JobSummaryDialog: React.FC<JobSummaryDialogProps> = ({ isOpen, onClose, job, onValidate }) => {
  const [leftConnection, setLeftConnection] = useState<Connection | null>(null);
  const [rightConnection, setRightConnection] = useState<Connection | null>(null);
  const [config, setConfig] = useState<HmsMirrorConfig | null>(null);
  const [configName, setConfigName] = useState<string | null>(null);
  const [dataset, setDataset] = useState<DatasetDto | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [validating, setValidating] = useState(false);

  useEffect(() => {
    if (isOpen) {
      loadSummaryData();
    }
  }, [isOpen, job]);

  const loadSummaryData = async () => {
    setLoading(true);
    setError(null);

    try {
      console.log('Loading summary data for job:', job);
      console.log('Config name:', job.configurationName);

      // Fetch connections
      const [leftConnResponse, rightConnResponse, configResponse, datasetResponse] = await Promise.all([
        job.leftConnectionName ? fetch(`/hms-mirror/api/v1/connections/${job.leftConnectionName}`).then(r => r.ok ? r.json() : null) : Promise.resolve(null),
        job.rightConnectionName ? fetch(`/hms-mirror/api/v1/connections/${job.rightConnectionName}`).then(r => r.ok ? r.json() : null) : Promise.resolve(null),
        job.configurationName ? configApi.getConfiguration(job.configurationName) : Promise.resolve(null),
        job.datasetName ? datasetApi.getDataset(job.datasetName) : Promise.resolve(null)
      ]);

      console.log('Config response:', configResponse);

      setLeftConnection(leftConnResponse);
      setRightConnection(rightConnResponse);

      // Extract configuration from response
      if (configResponse?.status === 'SUCCESS' && configResponse.configuration) {
        setConfig(configResponse.configuration);
        setConfigName(configResponse.key || job.configurationName || null);
      } else {
        setConfig(null);
        setConfigName(job.configurationName || null);
      }

      setDataset(datasetResponse);
    } catch (err) {
      console.error('Error loading summary data:', err);
      setError('Failed to load summary data');
    } finally {
      setLoading(false);
    }
  };

  const handleValidate = async () => {
    if (!job.jobKey) {
      setError('Cannot validate job: missing job key');
      return;
    }

    try {
      setValidating(true);
      setError(null);

      const result = await jobApi.validateJob(job.jobKey);

      if (result.success && onValidate) {
        // Call parent's onValidate callback to show validation dialog
        onValidate(
          job.name,
          result.valid || false,
          result.message || '',
          result.errors || [],
          result.warnings || []
        );
        // Close the summary dialog
        onClose();
      } else {
        setError(result.message || 'Failed to validate job');
      }
    } catch (err) {
      console.error('Error validating job:', err);
      setError('Failed to validate job');
    } finally {
      setValidating(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
        {/* Background overlay */}
        <div
          className="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75"
          onClick={onClose}
        />

        {/* Dialog */}
        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-6xl sm:w-full">
          {/* Header */}
          <div className="bg-gradient-to-r from-blue-600 to-blue-700 px-6 py-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-medium text-white">
                📋 Job Summary
              </h3>
              <button
                onClick={onClose}
                className="text-white hover:text-gray-200 transition-colors"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>
          </div>

          {/* Content */}
          <div className="bg-gray-50 px-6 py-6">
            {loading ? (
              <div className="flex items-center justify-center py-12">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
                <span className="ml-3 text-gray-600">Loading summary...</span>
              </div>
            ) : error ? (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <p className="text-red-800">{error}</p>
              </div>
            ) : (
              <div className="space-y-6">
                {/* Row 1: Job Information and Configuration (2 columns) */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  {/* Job Information */}
                  <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                    <h4 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                      <span className="text-2xl mr-2">🚀</span>
                      Job Information
                    </h4>
                    <div className="space-y-3">
                      <div>
                        <span className="text-sm font-medium text-gray-600">Name:</span>
                        <p className="text-sm text-gray-900 mt-1">{job.name}</p>
                      </div>
                      {job.description && (
                        <div>
                          <span className="text-sm font-medium text-gray-600">Description:</span>
                          <p className="text-sm text-gray-900 mt-1">{job.description}</p>
                        </div>
                      )}
                      {job.strategy && (
                        <div>
                          <span className="text-sm font-medium text-gray-600">Strategy:</span>
                          <p className="text-sm text-gray-900 mt-1">{job.strategy}</p>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Configuration */}
                  <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                    <h4 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                      <span className="text-2xl mr-2">⚙️</span>
                      Configuration
                    </h4>
                    {config && configName ? (
                      <div className="space-y-3">
                        <div>
                          <span className="text-sm font-medium text-gray-600">Name:</span>
                          <p className="text-sm text-gray-900 mt-1">{configName}</p>
                        </div>
                        <div>
                          <span className="text-sm font-medium text-gray-600">Key Settings:</span>
                          <ul className="text-xs text-gray-700 mt-1 space-y-1 ml-2">
                            {config.transfer?.warehouse && (
                              <li>• Warehouse Transfer: {config.transfer.warehouse.externalDirectory ? 'External' : ''} {config.transfer.warehouse.managedDirectory ? 'Managed' : ''}</li>
                            )}
                            {config.migrateACID && (
                              <li>• Migrate ACID: {config.migrateACID.downgrade ? 'Downgrade' : 'Enabled'}</li>
                            )}
                            {config.optimization?.compressTextOutput !== undefined && (
                              <li>• Text Compression: {config.optimization.compressTextOutput ? 'Enabled' : 'Disabled'}</li>
                            )}
                            {config.filter?.tblRegEx && (
                              <li>• Table Filter: {config.filter.tblRegEx}</li>
                            )}
                            {config.databases && config.databases.length > 0 && (
                              <li>• Databases: {config.databases.join(', ')}</li>
                            )}
                          </ul>
                        </div>
                      </div>
                    ) : configName ? (
                      <div className="space-y-3">
                        <div>
                          <span className="text-sm font-medium text-gray-600">Name:</span>
                          <p className="text-sm text-gray-900 mt-1">{configName}</p>
                        </div>
                        <p className="text-sm text-gray-500 italic">Configuration details not available</p>
                      </div>
                    ) : (
                      <p className="text-sm text-gray-500 italic">No configuration selected</p>
                    )}
                  </div>
                </div>

                {/* Row 2: Connections (2 columns) */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  {/* Left Connection */}
                  <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                    <h4 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                      <span className="text-2xl mr-2">🔗</span>
                      LEFT Connection
                    </h4>
                    {leftConnection ? (
                      <div className="space-y-3">
                        <div>
                          <span className="text-sm font-medium text-gray-600">Name:</span>
                          <p className="text-sm text-gray-900 mt-1">{leftConnection.name}</p>
                        </div>
                        {leftConnection.config?.hiveServer2?.uri && (
                          <div>
                            <span className="text-sm font-medium text-gray-600">HiveServer2 URI:</span>
                            <p className="text-xs text-gray-900 mt-1 break-all font-mono bg-gray-50 p-2 rounded">
                              {leftConnection.config.hiveServer2.uri}
                            </p>
                          </div>
                        )}
                        {leftConnection.config?.metastoreDirect?.uri && (
                          <div>
                            <span className="text-sm font-medium text-gray-600">Metastore URI:</span>
                            <p className="text-xs text-gray-900 mt-1 break-all font-mono bg-gray-50 p-2 rounded">
                              {leftConnection.config.metastoreDirect.uri}
                            </p>
                          </div>
                        )}
                      </div>
                    ) : (
                      <p className="text-sm text-gray-500 italic">No LEFT connection configured</p>
                    )}
                  </div>

                  {/* Right Connection */}
                  <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                    <h4 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                      <span className="text-2xl mr-2">🔗</span>
                      RIGHT Connection
                    </h4>
                    {rightConnection ? (
                      <div className="space-y-3">
                        <div>
                          <span className="text-sm font-medium text-gray-600">Name:</span>
                          <p className="text-sm text-gray-900 mt-1">{rightConnection.name}</p>
                        </div>
                        {rightConnection.config?.hiveServer2?.uri && (
                          <div>
                            <span className="text-sm font-medium text-gray-600">HiveServer2 URI:</span>
                            <p className="text-xs text-gray-900 mt-1 break-all font-mono bg-gray-50 p-2 rounded">
                              {rightConnection.config.hiveServer2.uri}
                            </p>
                          </div>
                        )}
                        {rightConnection.config?.metastoreDirect?.uri && (
                          <div>
                            <span className="text-sm font-medium text-gray-600">Metastore URI:</span>
                            <p className="text-xs text-gray-900 mt-1 break-all font-mono bg-gray-50 p-2 rounded">
                              {rightConnection.config.metastoreDirect.uri}
                            </p>
                          </div>
                        )}
                      </div>
                    ) : (
                      <p className="text-sm text-gray-500 italic">No RIGHT connection configured</p>
                    )}
                  </div>
                </div>

                {/* Row 3: Dataset (1 column) */}
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <h4 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
                    <span className="text-2xl mr-2">📊</span>
                    Dataset
                  </h4>
                  {dataset ? (
                    <div className="space-y-4">
                      <div>
                        <span className="text-sm font-medium text-gray-600">Name:</span>
                        <p className="text-sm text-gray-900 mt-1">{dataset.name}</p>
                      </div>
                      {dataset.description && (
                        <div>
                          <span className="text-sm font-medium text-gray-600">Description:</span>
                          <p className="text-sm text-gray-900 mt-1">{dataset.description}</p>
                        </div>
                      )}

                      {/* Database Specs */}
                      {dataset.databases && dataset.databases.length > 0 && (
                        <div>
                          <span className="text-sm font-medium text-gray-600 mb-2 block">
                            Databases ({dataset.databases.length}):
                          </span>
                          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                            {dataset.databases.map((db, idx) => (
                              <div key={idx} className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                                <h5 className="text-sm font-semibold text-blue-900 mb-2">
                                  {db.databaseName}
                                </h5>
                                <div className="space-y-2 text-xs text-blue-800">
                                  {db.tables && db.tables.length > 0 && (
                                    <div>
                                      <span className="font-medium">Tables:</span> {db.tables.length}
                                      {db.tables.length <= 3 ? (
                                        <ul className="ml-2 mt-1">
                                          {db.tables.map((table, tIdx) => (
                                            <li key={tIdx}>• {table}</li>
                                          ))}
                                        </ul>
                                      ) : (
                                        <span className="ml-1">
                                          ({db.tables.slice(0, 2).join(', ')}, +{db.tables.length - 2} more)
                                        </span>
                                      )}
                                    </div>
                                  )}
                                  {db.filter && (
                                    <div>
                                      <span className="font-medium">Filter:</span>
                                      {db.filter.includeRegEx && <div className="ml-2">Include: {db.filter.includeRegEx}</div>}
                                      {db.filter.excludeRegEx && <div className="ml-2">Exclude: {db.filter.excludeRegEx}</div>}
                                    </div>
                                  )}
                                  {db.warehouse && (db.warehouse.managedDirectory || db.warehouse.externalDirectory) && (
                                    <div>
                                      <span className="font-medium">Warehouse:</span>
                                      {db.warehouse.managedDirectory && <div className="ml-2 break-all">Managed: {db.warehouse.managedDirectory}</div>}
                                      {db.warehouse.externalDirectory && <div className="ml-2 break-all">External: {db.warehouse.externalDirectory}</div>}
                                    </div>
                                  )}
                                  {db.dbPrefix && (
                                    <div>
                                      <span className="font-medium">Prefix:</span> {db.dbPrefix}
                                    </div>
                                  )}
                                  {db.dbRename && (
                                    <div>
                                      <span className="font-medium">Rename:</span> {db.dbRename}
                                    </div>
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <p className="text-sm text-gray-500 italic">No dataset selected</p>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="bg-gray-100 px-6 py-4 flex justify-between items-center">
            <button
              onClick={handleValidate}
              disabled={validating || loading}
              className="inline-flex items-center px-4 py-2 border border-green-300 rounded-md text-sm font-medium text-green-700 bg-white hover:bg-green-50 disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none focus:ring-2 focus:ring-green-500"
            >
              {validating ? (
                <>
                  <ArrowPathIcon className="h-5 w-5 mr-2 animate-spin" />
                  Validating...
                </>
              ) : (
                <>
                  <CheckCircleIcon className="h-5 w-5 mr-2" />
                  Validate
                </>
              )}
            </button>

            <button
              onClick={onClose}
              className="px-4 py-2 bg-white border border-gray-300 rounded-md text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JobSummaryDialog;
