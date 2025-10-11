import React, { useState } from 'react';
import { PlusIcon, TrashIcon } from '@heroicons/react/24/outline';

interface ConnectionPropertiesEditorProps {
  properties: { [key: string]: string };
  onChange: (properties: { [key: string]: string }) => void;
}

const ConnectionPropertiesEditor: React.FC<ConnectionPropertiesEditorProps> = ({
  properties,
  onChange
}) => {
  const [newKey, setNewKey] = useState('');
  const [newValue, setNewValue] = useState('');

  const propertyEntries = Object.entries(properties).filter(([key]) => key !== 'user' && key !== 'password');

  const addProperty = () => {
    if (newKey.trim() && newValue.trim()) {
      onChange({
        ...properties,
        [newKey.trim()]: newValue.trim()
      });
      setNewKey('');
      setNewValue('');
    }
  };

  const removeProperty = (keyToRemove: string) => {
    const { [keyToRemove]: removed, ...rest } = properties;
    onChange(rest);
  };

  const updateProperty = (oldKey: string, newKey: string, newValue: string) => {
    if (oldKey === newKey) {
      onChange({
        ...properties,
        [newKey]: newValue
      });
    } else {
      const { [oldKey]: removed, ...rest } = properties;
      onChange({
        ...rest,
        [newKey]: newValue
      });
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addProperty();
    }
  };

  return (
    <div className="space-y-4">
      {/* Existing Properties */}
      {propertyEntries.length > 0 && (
        <div className="space-y-3">
          {propertyEntries.map(([key, value]) => (
            <div key={key} className="flex items-center space-x-2">
              <input
                type="text"
                value={key}
                onChange={(e) => updateProperty(key, e.target.value, value)}
                className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="Property key"
              />
              <span className="text-gray-500">=</span>
              <input
                type="text"
                value={value}
                onChange={(e) => updateProperty(key, key, e.target.value)}
                className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                placeholder="Property value"
              />
              <button
                type="button"
                onClick={() => removeProperty(key)}
                className="p-2 text-red-600 hover:text-red-700 hover:bg-red-50 rounded-md"
                title="Remove property"
              >
                <TrashIcon className="h-4 w-4" />
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Add New Property */}
      <div className="border-t pt-4">
        <div className="flex items-center space-x-2">
          <input
            type="text"
            value={newKey}
            onChange={(e) => setNewKey(e.target.value)}
            onKeyDown={handleKeyDown}
            className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            placeholder="Property name (e.g., ssl, sslTrustStore)"
          />
          <span className="text-gray-500">=</span>
          <input
            type="text"
            value={newValue}
            onChange={(e) => setNewValue(e.target.value)}
            onKeyDown={handleKeyDown}
            className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            placeholder="Property value"
          />
          <button
            type="button"
            onClick={addProperty}
            disabled={!newKey.trim() || !newValue.trim()}
            className="p-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
            title="Add property"
          >
            <PlusIcon className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Common Properties Examples */}
      <div className="bg-gray-50 rounded-lg p-3">
        <p className="text-xs font-medium text-gray-700 mb-2">Common Connection Properties:</p>
        <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
          <div>
            <code>ssl=true</code> - Enable SSL
          </div>
          <div>
            <code>sslTrustStore=/path/to/truststore</code> - SSL Trust Store
          </div>
          <div>
            <code>transportMode=http</code> - HTTP Transport
          </div>
          <div>
            <code>httpPath=gateway/cdp-proxy-api/hive</code> - Knox Gateway Path
          </div>
          <div>
            <code>zooKeeperNamespace=hiveserver2</code> - ZooKeeper Namespace
          </div>
          <div>
            <code>serviceDiscoveryMode=zooKeeper</code> - Service Discovery
          </div>
        </div>
      </div>
    </div>
  );
};

export default ConnectionPropertiesEditor;