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
  // Check if HiveServer2 URI is kerberized (contains 'principal' in URI OR in connection properties)
  const isKerberized = React.useMemo(() => {
    // Check if 'principal' is in the URI
    const inUri = formData.hs2Uri && formData.hs2Uri.toLowerCase().includes('principal');

    // Check if 'principal' is a key in connection properties
    const inProperties = formData.hs2ConnectionProperties &&
                        Object.keys(formData.hs2ConnectionProperties).some(key =>
                          key.toLowerCase() === 'principal'
                        );

    return inUri || inProperties;
  }, [formData.hs2Uri, formData.hs2ConnectionProperties]);

  const hasUri = formData.hs2Uri && formData.hs2Uri.trim() !== '';

  // Determine card background color using inline styles with useMemo
  const cardStyle = React.useMemo(() => {
    if (!hasUri) {
      return {
        backgroundColor: '#ffffff',
        borderColor: '#e5e7eb',
        borderWidth: '1px'
      };
    }
    if (isKerberized) {
      return {
        backgroundColor: '#fef3c7', // yellow-100
        borderColor: '#fbbf24', // yellow-400
        borderWidth: '3px'
      };
    }
    return {
      backgroundColor: '#d1fae5', // green-100
      borderColor: '#10b981', // green-500
      borderWidth: '3px'
    };
  }, [hasUri, isKerberized]);

  // Debug logging
  React.useEffect(() => {
    console.log('=== ConnectionSettingsStep - HS2 URI Change ===');
    console.log('HS2 URI:', formData.hs2Uri);
    console.log('Connection Properties:', formData.hs2ConnectionProperties);
    console.log('Has principal in URI:', formData.hs2Uri && formData.hs2Uri.toLowerCase().includes('principal'));
    console.log('Has principal in properties:', formData.hs2ConnectionProperties && Object.keys(formData.hs2ConnectionProperties).some(key => key.toLowerCase() === 'principal'));
    console.log('Has URI:', hasUri);
    console.log('Is Kerberized:', isKerberized);
    console.log('Card Style:', cardStyle);
  }, [formData.hs2Uri, formData.hs2ConnectionProperties, hasUri, isKerberized, cardStyle]);

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

      {/* HiveServer2 URI - Card with dynamic background */}
      <div style={cardStyle} className="p-4 rounded-lg border-solid transition-all duration-200">
        <label htmlFor="hs2Uri" className="block text-sm font-medium text-gray-700 mb-2">
          HiveServer2 URI *
          {hasUri && (
            <span className={`ml-2 text-xs font-bold ${isKerberized ? 'text-yellow-700' : 'text-green-700'}`}>
              {isKerberized ? '🔐 KERBERIZED' : '✓ NON-KERBERIZED'}
            </span>
          )}
        </label>
        <input
          type="text"
          id="hs2Uri"
          value={formData.hs2Uri}
          onChange={(e) => onChange({ hs2Uri: e.target.value })}
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-white"
          placeholder="jdbc:hive2://hiveserver2.example.com:10000"
          required
        />
        {errors.hs2Uri && (
          <p className="mt-1 text-sm text-red-600">{errors.hs2Uri}</p>
        )}

        <div className="mt-2 space-y-1">
          <p className="text-xs text-gray-600">
            💡 JDBC connection string for HiveServer2
          </p>
          {isKerberized && (
            <p className="text-xs text-yellow-800 font-medium">
              ⚠️ HMS Mirror will require a valid Kerberos ticket (kinit) when using this connection
            </p>
          )}
          {hasUri && !isKerberized && (
            <p className="text-xs text-green-800 font-medium">
              ✓ No Kerberos ticket required for this connection
            </p>
          )}
        </div>
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
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="text-sm font-medium text-blue-900 mb-2">🔒 Authentication Guide</h4>
        <div className="text-sm text-blue-700 space-y-2">
          <p>
            Authentication is determined by the connection URI configuration:
          </p>
          <ul className="ml-4 space-y-1 list-disc">
            <li>
              <strong className="text-green-800">Green background:</strong> Non-kerberized connection.
              No Kerberos ticket required.
            </li>
            <li>
              <strong className="text-yellow-800">Gold background:</strong> Kerberized connection (contains 'principal').
              HMS Mirror must run with a valid Kerberos ticket (kinit).
            </li>
          </ul>
        </div>
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
            <strong>CDP Private Cloud (Non-Kerberos):</strong>
            <ul className="mt-1 ml-4 list-disc space-y-1">
              <li>HDFS: <code>hdfs://namenode.internal:8020</code></li>
              <li>HiveServer2: <code>jdbc:hive2://hiveserver2.internal:10000</code></li>
              <li>Metastore: <code>thrift://metastore.internal:9083</code></li>
            </ul>
          </div>
          <div>
            <strong>CDP Private Cloud (Kerberized):</strong>
            <ul className="mt-1 ml-4 list-disc space-y-1">
              <li>HDFS: <code>hdfs://namenode.internal:8020</code></li>
              <li>HiveServer2: <code>jdbc:hive2://hiveserver2.internal:10000/default;principal=hive/_HOST@REALM.COM</code></li>
              <li>Metastore: <code>thrift://metastore.internal:9083</code></li>
            </ul>
          </div>
        </div>
      </details>
    </div>
  );
};

export default ConnectionSettingsStep;