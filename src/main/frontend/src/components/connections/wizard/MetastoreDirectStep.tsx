import React, { useState, useEffect } from 'react';
import {
  ConnectionFormData,
  METASTORE_CONNECTION_EXAMPLES
} from '../../../types/Connection';
import { InformationCircleIcon, ClipboardDocumentIcon } from '@heroicons/react/24/outline';
import FieldWithTooltip from '../../common/FieldWithTooltip';
import schemaService from '../../../services/schemaService';

interface MetastoreDirectStepProps {
  formData: ConnectionFormData;
  errors: Record<string, string>;
  onChange: (updates: Partial<ConnectionFormData>) => void;
  onNext: () => void;
  onBack: () => void;
}

const MetastoreDirectStep: React.FC<MetastoreDirectStepProps> = ({
  formData,
  errors,
  onChange,
  onNext,
  onBack
}) => {
  const [showExamples, setShowExamples] = useState(false);
  const [schemaDescriptions, setSchemaDescriptions] = useState<Map<string, string>>(new Map());

  // Fetch schema descriptions on mount
  useEffect(() => {
    const fetchDescriptions = async () => {
      const descriptions = await schemaService.getClassDescriptions('ConnectionDto');
      setSchemaDescriptions(descriptions);
    };
    fetchDescriptions();
  }, []);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  // Auto-detect database type from JDBC URL
  const detectDatabaseType = (uri: string): string => {
    if (!uri || !uri.startsWith('jdbc:')) return '';

    const lowerUri = uri.toLowerCase();

    if (lowerUri.startsWith('jdbc:mysql:')) return 'MYSQL';
    if (lowerUri.startsWith('jdbc:mariadb:')) return 'MARIADB';
    if (lowerUri.startsWith('jdbc:postgresql:')) return 'POSTGRES';
    if (lowerUri.startsWith('jdbc:oracle:')) return 'ORACLE';
    if (lowerUri.startsWith('jdbc:sqlserver:')) return 'MSSQL';

    return '';
  };

  const handleUriChange = (uri: string) => {
    const detectedType = detectDatabaseType(uri);
    onChange({
      metastoreDirectUri: uri,
      metastoreDirectType: detectedType
    });
  };

  const useExample = (example: { uri: string; description: string }) => {
    handleUriChange(example.uri);
  };

  return (
    <div className="space-y-6">
      {/* Enable Metastore Direct */}
      <div>
        <div className="flex items-center mb-4">
          <input
            id="metastoreDirectEnabled"
            type="checkbox"
            checked={formData.metastoreDirectEnabled}
            onChange={(e) => onChange({ metastoreDirectEnabled: e.target.checked })}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="metastoreDirectEnabled" className="ml-2 block text-sm font-medium text-gray-900">
            Enable Metastore Direct Access (Optional)
          </label>
        </div>
        
        {/* Info about Metastore Direct */}
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <h4 className="text-sm font-medium text-yellow-900 mb-2">ℹ️ About Metastore Direct</h4>
          <p className="text-sm text-yellow-800">
            Metastore Direct provides optimized access to the Hive Metastore database for faster 
            table and partition metadata operations. This connects directly to the underlying 
            RDBMS (MySQL, PostgreSQL, or Oracle) that stores the Hive Metastore data.
          </p>
        </div>
      </div>

      {formData.metastoreDirectEnabled && (
        <>
          {/* Database URI */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <FieldWithTooltip
                label="Database URI"
                tooltip={schemaDescriptions.get('metastoreDirectUri')}
                htmlFor="metastoreDirectUri"
                required
              />
              <button
                type="button"
                onClick={() => setShowExamples(!showExamples)}
                className="text-blue-600 hover:text-blue-700 text-xs flex items-center"
              >
                <InformationCircleIcon className="h-4 w-4 mr-1" />
                {showExamples ? 'Hide' : 'Show'} Examples
              </button>
            </div>
            <input
              type="text"
              id="metastoreDirectUri"
              value={formData.metastoreDirectUri}
              onChange={(e) => handleUriChange(e.target.value)}
              className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
                errors.metastoreDirectUri ? 'border-red-300' : 'border-gray-300'
              }`}
              placeholder="jdbc:mysql://mysql-server:3306/hive_metastore"
              required
            />
            {errors.metastoreDirectUri && (
              <p className="mt-1 text-sm text-red-600">{errors.metastoreDirectUri}</p>
            )}
            
            {/* Connection Examples */}
            {showExamples && (
              <div className="mt-3 p-3 bg-gray-50 rounded-lg">
                <h4 className="text-sm font-medium text-gray-900 mb-2">Database Connection Examples:</h4>
                <div className="space-y-3">
                  {Object.entries(METASTORE_CONNECTION_EXAMPLES).map(([key, example]) => (
                    <div key={key} className="border border-gray-200 rounded p-2">
                      <div className="flex items-center justify-between mb-1">
                        <p className="text-xs font-medium text-gray-700">{example.description}</p>
                        <div className="flex space-x-1">
                          <button
                            type="button"
                            onClick={() => copyToClipboard(example.uri)}
                            className="text-gray-400 hover:text-gray-600"
                            title="Copy to clipboard"
                          >
                            <ClipboardDocumentIcon className="h-4 w-4" />
                          </button>
                          <button
                            type="button"
                            onClick={() => useExample(example)}
                            className="text-xs text-blue-600 hover:text-blue-700"
                          >
                            Use
                          </button>
                        </div>
                      </div>
                      <code className="text-xs text-gray-600 break-all">{example.uri}</code>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Database Type (Auto-detected, Read-only) */}
          <div>
            <FieldWithTooltip
              label="Database Type (Auto-detected)"
              tooltip={schemaDescriptions.get('metastoreDirectType')}
              htmlFor="metastoreDirectType"
              className="mb-2"
            />
            <input
              type="text"
              id="metastoreDirectType"
              value={formData.metastoreDirectType || 'Not detected - please enter a valid JDBC URL'}
              readOnly
              disabled
              className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-gray-100 text-gray-700 cursor-not-allowed"
              placeholder="Auto-detected from URI"
            />
            <p className="mt-1 text-xs text-gray-500">
              The database type is automatically detected from your JDBC URI (e.g., jdbc:mysql://, jdbc:postgresql://, etc.)
            </p>
            {errors.metastoreDirectType && (
              <p className="mt-1 text-sm text-red-600">{errors.metastoreDirectType}</p>
            )}
          </div>

          {/* Database Credentials */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <FieldWithTooltip
                label="Database Username"
                tooltip={schemaDescriptions.get('metastoreDirectUsername')}
                htmlFor="metastoreDirectUsername"
                required
                className="mb-2"
              />
              <input
                type="text"
                id="metastoreDirectUsername"
                value={formData.metastoreDirectUsername}
                onChange={(e) => onChange({ metastoreDirectUsername: e.target.value })}
                className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
                  errors.metastoreDirectUsername ? 'border-red-300' : 'border-gray-300'
                }`}
                placeholder="hive"
                required
              />
              {errors.metastoreDirectUsername && (
                <p className="mt-1 text-sm text-red-600">{errors.metastoreDirectUsername}</p>
              )}
            </div>
            
            <div>
              <FieldWithTooltip
                label="Database Password"
                tooltip={schemaDescriptions.get('metastoreDirectPassword')}
                htmlFor="metastoreDirectPassword"
                required
                className="mb-2"
              />
              <input
                type="password"
                id="metastoreDirectPassword"
                value={formData.metastoreDirectPassword}
                onChange={(e) => onChange({ metastoreDirectPassword: e.target.value })}
                className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
                  errors.metastoreDirectPassword ? 'border-red-300' : 'border-gray-300'
                }`}
                placeholder="Password"
                required
              />
              {errors.metastoreDirectPassword && (
                <p className="mt-1 text-sm text-red-600">{errors.metastoreDirectPassword}</p>
              )}
            </div>
          </div>

          {/* Connection Pool Settings */}
          <div className="border-t pt-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Connection Pool Settings</h3>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <FieldWithTooltip
                  label="Minimum Connections"
                  tooltip={schemaDescriptions.get('metastoreDirectMinConnections')}
                  htmlFor="metastoreDirectMinConnections"
                  className="mb-2"
                />
                <input
                  type="number"
                  id="metastoreDirectMinConnections"
                  value={formData.metastoreDirectMinConnections}
                  onChange={(e) => onChange({ metastoreDirectMinConnections: parseInt(e.target.value) || 0 })}
                  className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
                    errors.metastoreDirectMinConnections ? 'border-red-300' : 'border-gray-300'
                  }`}
                  min="1"
                  max="20"
                />
                {errors.metastoreDirectMinConnections && (
                  <p className="mt-1 text-sm text-red-600">{errors.metastoreDirectMinConnections}</p>
                )}
              </div>
              
              <div>
                <FieldWithTooltip
                  label="Maximum Connections"
                  tooltip={schemaDescriptions.get('metastoreDirectMaxConnections')}
                  htmlFor="metastoreDirectMaxConnections"
                  className="mb-2"
                />
                <input
                  type="number"
                  id="metastoreDirectMaxConnections"
                  value={formData.metastoreDirectMaxConnections}
                  onChange={(e) => onChange({ metastoreDirectMaxConnections: parseInt(e.target.value) || 0 })}
                  className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
                    errors.metastoreDirectMaxConnections ? 'border-red-300' : 'border-gray-300'
                  }`}
                  min="1"
                  max="50"
                />
                {errors.metastoreDirectMaxConnections && (
                  <p className="mt-1 text-sm text-red-600">{errors.metastoreDirectMaxConnections}</p>
                )}
              </div>
            </div>
          </div>
        </>
      )}

      {/* Help Section */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="text-sm font-medium text-blue-900 mb-2">💡 Metastore Direct Tips</h4>
        <ul className="text-sm text-blue-700 space-y-1">
          <li>• Metastore Direct is optional but provides significant performance improvements</li>
          <li>• This connects directly to the RDBMS storing Hive Metastore data</li>
          <li>• Ensure the database user has read access to the Hive Metastore schema</li>
          <li>• Connection pooling helps manage concurrent metadata operations</li>
          {!formData.metastoreDirectEnabled && (
            <li>• You can skip this step and configure later if needed</li>
          )}
        </ul>
      </div>
    </div>
  );
};

export default MetastoreDirectStep;