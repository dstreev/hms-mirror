import React, { useState } from 'react';
import {
  ConnectionFormData,
  HIVESERVER2_CONNECTION_EXAMPLES
} from '../../../types/Connection';
import { InformationCircleIcon, ClipboardDocumentIcon } from '@heroicons/react/24/outline';
import ConnectionPropertiesEditor from './ConnectionPropertiesEditor';

interface HiveServer2StepProps {
  formData: ConnectionFormData;
  errors: Record<string, string>;
  onChange: (updates: Partial<ConnectionFormData>) => void;
  onNext: () => void;
  onBack: () => void;
}

const HiveServer2Step: React.FC<HiveServer2StepProps> = ({
  formData,
  errors,
  onChange,
  onNext,
  onBack
}) => {
  const [showExamples, setShowExamples] = useState(false);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const useExample = (example: { uri: string; description: string }) => {
    onChange({ hs2Uri: example.uri });
  };

  return (
    <div className="space-y-6">
      {/* HDFS Namespace */}
      <div>
        <label htmlFor="hcfsNamespace" className="block text-sm font-medium text-gray-700 mb-2">
          HDFS Namespace (Optional)
        </label>
        <input
          type="text"
          id="hcfsNamespace"
          value={formData.hcfsNamespace}
          onChange={(e) => onChange({ hcfsNamespace: e.target.value })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.hcfsNamespace ? 'border-red-300' : 'border-gray-300'
          }`}
          placeholder="hdfs://namenode:8020 or s3a://bucket-name"
        />
        {errors.hcfsNamespace && (
          <p className="mt-1 text-sm text-red-600">{errors.hcfsNamespace}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          📂 Optional: Used only to validate storage access from HMS-Mirror. Supports HDFS, S3, ADLS, etc.
        </p>
      </div>

          {/* HiveServer2 URI */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label htmlFor="hs2Uri" className="block text-sm font-medium text-gray-700">
                HiveServer2 URI *
              </label>
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
              id="hs2Uri"
              value={formData.hs2Uri}
              onChange={(e) => onChange({ hs2Uri: e.target.value })}
              className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
                errors.hs2Uri ? 'border-red-300' : 'border-gray-300'
              }`}
              placeholder="jdbc:hive2://hiveserver2:10000/default"
              required
            />
            {errors.hs2Uri && (
              <p className="mt-1 text-sm text-red-600">{errors.hs2Uri}</p>
            )}
            
            {/* Connection Examples */}
            {showExamples && (
              <div className="mt-3 p-3 bg-gray-50 rounded-lg">
                <h4 className="text-sm font-medium text-gray-900 mb-2">Connection Examples:</h4>
                <div className="space-y-3">
                  {Object.entries(HIVESERVER2_CONNECTION_EXAMPLES).map(([key, example]) => (
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

          {/* Authentication */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label htmlFor="hs2Username" className="block text-sm font-medium text-gray-700 mb-2">
                Username
              </label>
              <input
                type="text"
                id="hs2Username"
                value={formData.hs2Username}
                onChange={(e) => onChange({ hs2Username: e.target.value })}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="hive or your username"
              />
              <p className="mt-1 text-xs text-gray-500">
                Optional for Kerberos connections
              </p>
            </div>
            
            <div>
              <label htmlFor="hs2Password" className="block text-sm font-medium text-gray-700 mb-2">
                Password
              </label>
              <input
                type="password"
                id="hs2Password"
                value={formData.hs2Password}
                onChange={(e) => onChange({ hs2Password: e.target.value })}
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="Password (if required)"
              />
              <p className="mt-1 text-xs text-gray-500">
                Optional for Kerberos connections
              </p>
            </div>
          </div>

          {/* Connection Properties */}
          <div className="border-t pt-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Connection Properties</h3>
            <p className="text-sm text-gray-600 mb-4">
              Additional JDBC connection properties (key=value pairs). These will be added to the connection properties.
            </p>
            
            <ConnectionPropertiesEditor
              properties={formData.hs2ConnectionProperties}
              onChange={(properties) => onChange({ hs2ConnectionProperties: properties })}
            />
          </div>

      {/* Help Section */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="text-sm font-medium text-blue-900 mb-2">💡 HiveServer2 Connection Tips</h4>
        <ul className="text-sm text-blue-700 space-y-1">
          <li>• For Kerberos connections, include the principal in the URI</li>
          <li>• Username/password fields are optional for Kerberos authentication</li>
          <li>• Knox connections require SSL and specific HTTP transport settings</li>
          <li>• ZooKeeper discovery is useful for highly available HiveServer2</li>
        </ul>
      </div>
    </div>
  );
};

export default HiveServer2Step;