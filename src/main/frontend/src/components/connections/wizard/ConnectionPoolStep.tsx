import React, { useState, useEffect } from 'react';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';
import { ConnectionFormData, POOL_TYPE_OPTIONS } from '../../../types/Connection';
import FieldWithTooltip from '../../common/FieldWithTooltip';
import schemaService from '../../../services/schemaService';

interface ConnectionPoolStepProps {
  formData: ConnectionFormData;
  errors: Record<string, string>;
  onChange: (updates: Partial<ConnectionFormData>) => void;
  onNext: () => void;
  onBack: () => void;
}

const ConnectionPoolStep: React.FC<ConnectionPoolStepProps> = ({
  formData,
  errors,
  onChange,
  onNext,
  onBack
}) => {
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [schemaDescriptions, setSchemaDescriptions] = useState<Map<string, string>>(new Map());

  // Fetch schema descriptions on mount
  useEffect(() => {
    const fetchDescriptions = async () => {
      const descriptions = await schemaService.getClassDescriptions('ConnectionDto');
      setSchemaDescriptions(descriptions);
    };
    fetchDescriptions();
  }, []);

  return (
    <div className="space-y-6">
      {/* Pool Type */}
      <div>
        <FieldWithTooltip
          label="Pool Type"
          tooltip={schemaDescriptions.get('connectionPoolLib')}
          htmlFor="connectionPoolLib"
          className="mb-2"
        />
        <select
          id="connectionPoolLib"
          value={formData.connectionPoolLib}
          onChange={(e) => onChange({ connectionPoolLib: e.target.value as any })}
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
        >
          {POOL_TYPE_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <p className="mt-1 text-xs text-gray-500">
          Connection pool implementation for managing database connections
        </p>
      </div>

      {/* Driver Class Name */}
      <div>
        <FieldWithTooltip
          label="Driver Class Name"
          tooltip={schemaDescriptions.get('jarFile')}
          htmlFor="jarFile"
          required
          className="mb-2"
        />
        <input
          type="text"
          id="jarFile"
          value={formData.jarFile}
          onChange={(e) => onChange({ jarFile: e.target.value })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.jarFile ? 'border-red-300' : 'border-gray-300'
          }`}
          placeholder="org.apache.hive.jdbc.HiveDriver"
          required
        />
        {errors.jarFile && (
          <p className="mt-1 text-sm text-red-600">{errors.jarFile}</p>
        )}
      </div>

      {/* Maximum Pool Size */}
      <div>
        <FieldWithTooltip
          label="Maximum Pool Size"
          tooltip={schemaDescriptions.get('maxConnections')}
          htmlFor="maxConnections"
          required
          className="mb-2"
        />
        <div className="flex items-center space-x-2">
          <input
            type="number"
            id="maxConnections"
            min="1"
            max="100"
            value={formData.maxConnections}
            onChange={(e) => onChange({ maxConnections: parseInt(e.target.value) || 10 })}
            className={`block w-32 px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
              errors.maxConnections ? 'border-red-300' : 'border-gray-300'
            }`}
            required
          />
          <span className="text-sm text-gray-500">connections</span>
        </div>
        {errors.maxConnections && (
          <p className="mt-1 text-sm text-red-600">{errors.maxConnections}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          Range: 1-100 connections
        </p>
      </div>

      {/* Advanced Pool Settings */}
      <div>
        <button
          type="button"
          onClick={() => setShowAdvanced(!showAdvanced)}
          className="flex items-center text-sm font-medium text-blue-600 hover:text-blue-700"
        >
          {showAdvanced ? (
            <ChevronUpIcon className="h-4 w-4 mr-1" />
          ) : (
            <ChevronDownIcon className="h-4 w-4 mr-1" />
          )}
          Advanced Pool Settings (Optional)
        </button>

        {showAdvanced && (
          <div className="mt-4 p-4 border border-gray-200 rounded-lg bg-gray-50 space-y-4">
            {/* Initial Pool Size */}
            <div>
              <FieldWithTooltip
                label="Initial Pool Size"
                tooltip={schemaDescriptions.get('initialConnections')}
                htmlFor="initialConnections"
                className="mb-2"
              />
              <div className="flex items-center space-x-2">
                <input
                  type="number"
                  id="initialConnections"
                  min="0"
                  max="100"
                  value={formData.initialConnections}
                  onChange={(e) => onChange({ initialConnections: parseInt(e.target.value) || 3 })}
                  className="block w-32 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
                <span className="text-sm text-gray-500">connections</span>
              </div>
            </div>

            {/* Min Idle */}
            <div>
              <FieldWithTooltip
                label="Minimum Idle"
                tooltip={schemaDescriptions.get('minIdle')}
                htmlFor="minIdle"
                className="mb-2"
              />
              <div className="flex items-center space-x-2">
                <input
                  type="number"
                  id="minIdle"
                  min="0"
                  max="100"
                  value={formData.minIdle}
                  onChange={(e) => onChange({ minIdle: parseInt(e.target.value) || 3 })}
                  className="block w-32 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
                <span className="text-sm text-gray-500">connections</span>
              </div>
            </div>

            {/* Max Idle */}
            <div>
              <FieldWithTooltip
                label="Maximum Idle"
                tooltip={schemaDescriptions.get('maxIdle')}
                htmlFor="maxIdle"
                className="mb-2"
              />
              <div className="flex items-center space-x-2">
                <input
                  type="number"
                  id="maxIdle"
                  min="1"
                  max="100"
                  value={formData.maxIdle}
                  onChange={(e) => onChange({ maxIdle: parseInt(e.target.value) || 5 })}
                  className="block w-32 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
                <span className="text-sm text-gray-500">connections</span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Pool Type Information */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="text-sm font-medium text-blue-900 mb-2">Connection Pool Types</h4>
        <div className="text-sm text-blue-700 space-y-2">
          <div>
            <strong>DBCP2 (Default):</strong> Apache Commons DBCP2 - Reliable and well-tested
          </div>
          <div>
            <strong>HikariCP:</strong> High-performance connection pool with minimal overhead
          </div>
          <div>
            <strong>Hybrid:</strong> Automatic selection based on environment and workload
          </div>
        </div>
      </div>

      {/* Best Practices */}
      <details className="bg-green-50 border border-green-200 rounded-lg p-4">
        <summary className="text-sm font-medium text-green-900 cursor-pointer">
          💡 Best Practices
        </summary>
        <div className="mt-3 text-sm text-green-700 space-y-1">
          <p>• <strong>Production:</strong> Use 8-15 max connections depending on workload</p>
          <p>• <strong>Development:</strong> Use 3-5 max connections to conserve resources</p>
          <p>• <strong>Initial size:</strong> Should be 20-30% of max connections</p>
          <p>• <strong>Idle connections:</strong> Min idle should equal initial size</p>
        </div>
      </details>
    </div>
  );
};

export default ConnectionPoolStep;