import React from 'react';
import { ConnectionFormData } from '../../../types/Connection';

interface ConnectionSettingsStepProps {
  formData: ConnectionFormData;
  errors: Record<string, string>;
  onChange: (updates: Partial<ConnectionFormData>) => void;
  onNext: () => void;
  onBack: () => void;
}

const ConnectionSettingsStep: React.FC<ConnectionSettingsStepProps> = ({
  formData,
  errors,
  onChange,
  onNext,
  onBack
}) => {
  return (
    <div className="space-y-6">
      {/* Legacy Mode */}
      <div className="flex items-center">
        <input
          type="checkbox"
          id="legacy"
          checked={formData.legacy}
          onChange={(e) => onChange({ legacy: e.target.checked })}
          className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
        />
        <label htmlFor="legacy" className="ml-3 block text-sm font-medium text-gray-700">
          ☑️ Legacy Mode
        </label>
      </div>
      <p className="text-sm text-gray-500 -mt-4 ml-7">
        Use legacy Hive connection mode for older cluster versions
      </p>

      {/* HDFS Namespace */}
      <div>
        <label htmlFor="hcfsNamespace" className="block text-sm font-medium text-gray-700 mb-2">
          HDFS Namespace *
        </label>
        <input
          type="text"
          id="hcfsNamespace"
          value={formData.hcfsNamespace}
          onChange={(e) => onChange({ hcfsNamespace: e.target.value })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.hcfsNamespace ? 'border-red-300' : 'border-gray-300'
          }`}
          placeholder="hdfs://namenode.example.com:8020"
          required
        />
        {errors.hcfsNamespace && (
          <p className="mt-1 text-sm text-red-600">{errors.hcfsNamespace}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          💡 The HDFS namespace URI for this cluster
        </p>
      </div>

      {/* HiveServer2 URI */}
      <div>
        <label htmlFor="hs2Uri" className="block text-sm font-medium text-gray-700 mb-2">
          HiveServer2 URI *
        </label>
        <input
          type="text"
          id="hs2Uri"
          value={formData.hs2Uri}
          onChange={(e) => onChange({ hs2Uri: e.target.value })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.hs2Uri ? 'border-red-300' : 'border-gray-300'
          }`}
          placeholder="jdbc:hive2://hiveserver2.example.com:10000"
          required
        />
        {errors.hs2Uri && (
          <p className="mt-1 text-sm text-red-600">{errors.hs2Uri}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          💡 JDBC connection string for HiveServer2
        </p>
      </div>

      {/* Metastore Direct URI */}
      <div>
        <label htmlFor="metastoreUri" className="block text-sm font-medium text-gray-700 mb-2">
          Metastore Direct URI
        </label>
        <input
          type="text"
          id="metastoreUri"
          value={formData.metastoreUri}
          onChange={(e) => onChange({ metastoreUri: e.target.value })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.metastoreUri ? 'border-red-300' : 'border-gray-300'
          }`}
          placeholder="thrift://metastore.example.com:9083"
        />
        {errors.metastoreUri && (
          <p className="mt-1 text-sm text-red-600">{errors.metastoreUri}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          💡 Optional: Direct metastore connection for improved performance
        </p>
      </div>

      {/* Help Section */}
      <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
        <h4 className="text-sm font-medium text-yellow-900 mb-2">🔒 Authentication</h4>
        <p className="text-sm text-yellow-700">
          Authentication is handled automatically through your environment's Kerberos configuration or connection URLs. 
          No manual authentication setup is required.
        </p>
      </div>

      {/* Examples Section */}
      <details className="bg-gray-50 border border-gray-200 rounded-lg p-4">
        <summary className="text-sm font-medium text-gray-700 cursor-pointer">
          📋 Connection Examples
        </summary>
        <div className="mt-3 space-y-3 text-sm text-gray-600">
          <div>
            <strong>CDP Public Cloud:</strong>
            <ul className="mt-1 ml-4 list-disc space-y-1">
              <li>HDFS: <code>hdfs://cluster-name</code></li>
              <li>HiveServer2: <code>jdbc:hive2://cluster-name-hs2.region.cloudera.site:443/default;ssl=true</code></li>
            </ul>
          </div>
          <div>
            <strong>CDP Private Cloud:</strong>
            <ul className="mt-1 ml-4 list-disc space-y-1">
              <li>HDFS: <code>hdfs://namenode.internal:8020</code></li>
              <li>HiveServer2: <code>jdbc:hive2://hiveserver2.internal:10000</code></li>
              <li>Metastore: <code>thrift://metastore.internal:9083</code></li>
            </ul>
          </div>
        </div>
      </details>
    </div>
  );
};

export default ConnectionSettingsStep;